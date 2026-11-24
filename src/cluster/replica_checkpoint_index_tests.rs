// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES;
use crate::{outcome::PageOutcome, PageState};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replica-index-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> SegmentedOutcomeLog {
        SegmentedOutcomeLog::open(self.0.join("journal"), MIN_SEGMENT_BYTES).unwrap()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn id(n: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: n,
    }
}
fn page(bytes: Vec<u8>, sequence: u64) -> PageState {
    PageState {
        logical_length: bytes.len() as u64,
        write_sequence: sequence,
        checksum_fnv1a64: crate::fnv1a64(&bytes),
        data: Arc::new(bytes),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    }
}
fn seed(
    log: &SegmentedOutcomeLog,
    catalog: &mut PageCatalog,
    pages: usize,
) -> StoredOutcomePosition {
    let mut last = None;
    for start in (0..pages).step_by(64) {
        let state = page(vec![0x31; 16], 1);
        let outcomes: Vec<_> = (start..(start + 64).min(pages))
            .map(|n| PageOutcome::between(&id(n as u64), None, &state).unwrap())
            .collect();
        let mutations: Vec<_> = outcomes.iter().map(DecidedMutation::Page).collect();
        let record = log.append_decided_batch(&mutations).unwrap();
        catalog.fold(&record).unwrap();
        last = Some(record.position);
    }
    last.unwrap()
}
fn publish(
    log: &SegmentedOutcomeLog,
    catalog: &PageCatalog,
    state: &mut Option<IndexState>,
    dirty: &mut BTreeSet<PageId>,
    cut: StoredOutcomePosition,
    epoch: u64,
) -> CheckpointCommit {
    let (commit, position) = prepare(
        log,
        catalog,
        state,
        dirty,
        b"scalar-state".to_vec(),
        Some(cut),
        epoch,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(log)
    .unwrap();
    assert_indexed_protection(state.as_ref().unwrap(), log, catalog, &commit, &position);
    commit
}
fn assert_indexed_protection(
    index: &IndexState,
    log: &SegmentedOutcomeLog,
    pages: &PageCatalog,
    commit: &CheckpointCommit,
    position: &StoredOutcomePosition,
) -> BTreeSet<Vec<u8>> {
    let complete = index
        .protected_segments(log, pages, commit, position, &|| Ok(()))
        .unwrap();
    let cached = index
        .protected_segments_indexed(log, pages, commit, position, &BTreeSet::new(), &|| Ok(()))
        .unwrap();
    assert_eq!(cached, complete);
    cached
}

fn verify_tree(
    node: &Option<Box<Node>>,
    lower: Option<&PageId>,
    upper: Option<&PageId>,
) -> (u32, u64) {
    let Some(node) = node else {
        return (0, 0);
    };
    assert!(lower.is_none_or(|p| p < &node.key) && upper.is_none_or(|p| p > &node.key));
    let (lh, lc) = verify_tree(&node.left, lower, Some(&node.key));
    let (rh, rc) = verify_tree(&node.right, Some(&node.key), upper);
    assert!(lh.abs_diff(rh) <= 1);
    assert_eq!(node.height, 1 + lh.max(rh));
    assert_eq!(node.count, 1 + lc + rc);
    (node.height, node.count)
}
#[test]
fn checkpoint_index_balances_ordered_and_permuted_insertions_without_losing_updates() {
    for stride in [1, 7919] {
        let mut index = IndexState::default();
        for n in 0..4096 {
            index.put(
                id((n * stride) % 4096),
                Value {
                    page: None,
                    catalog: vec![1],
                },
            );
        }
        assert_eq!(verify_tree(&index.root, None, None).1, 4096);
        assert!(height(&index.root) < 20);
        for n in (0..4096).step_by(3) {
            index.put(
                id(n),
                Value {
                    page: None,
                    catalog: vec![2],
                },
            );
        }
        assert_eq!(verify_tree(&index.root, None, None).1, 4096);
        for n in 0..4096 {
            assert_eq!(
                index.root.as_mut().unwrap().find_mut(&id(n)).value.catalog,
                vec![if n % 3 == 0 { 2 } else { 1 }]
            );
        }
    }
}
#[test]
fn checkpoint_index_reuses_unchanged_records_and_reopens_updated_pages() {
    for pages in [128, 8192] {
        let dir = Dir::new();
        let log = dir.open();
        let mut catalog = PageCatalog::default();
        let cut = seed(&log, &mut catalog, pages);
        let mut index = None;
        let mut dirty = BTreeSet::new();
        let first = publish(&log, &catalog, &mut index, &mut dirty, cut, 1);
        assert_eq!(first.version, 2);
        let untouched = index
            .as_mut()
            .unwrap()
            .root
            .as_mut()
            .unwrap()
            .find_mut(&id((pages - 1) as u64))
            .value
            .clone();
        let before = page(vec![0x31; 16], 1);
        let mut bytes = (*before.data).clone();
        bytes.extend_from_slice(b"suffix");
        let after = page(bytes, 2);
        let record = log
            .append_page(&PageOutcome::between(&id(0), Some(&before), &after).unwrap())
            .unwrap();
        catalog.fold(&record).unwrap();
        dirty.insert(id(0));
        let start = log.logical_length().unwrap();
        let second = publish(&log, &catalog, &mut index, &mut dirty, record.position, 2);
        let written = log.logical_length().unwrap() - start;
        assert!(
            written < 96 * 1024,
            "point checkpoint grew with corpus: {pages} pages, {written} bytes"
        );
        assert_eq!(
            index
                .as_mut()
                .unwrap()
                .root
                .as_mut()
                .unwrap()
                .find_mut(&id((pages - 1) as u64))
                .value,
            untouched
        );
        let start = log.logical_length().unwrap();
        publish(
            &log,
            &catalog,
            &mut index,
            &mut dirty,
            second.through.clone().unwrap(),
            3,
        );
        assert!(
            log.logical_length().unwrap() - start <= 16 * 1024,
            "unchanged pages were serialized again"
        );
        drop(index);
        drop(catalog);
        drop(log);
        let log = dir.open();
        let mut loaded = catalog::latest(&log, pages).unwrap().unwrap();
        assert_eq!(loaded.header.version, 2);
        assert_eq!(loaded.header.metadata, b"scalar-state");
        assert_eq!(loaded.catalog.pages.len(), pages);
        let got = loaded.catalog.pages[&id(0)]
            .materialize(|e, o, n| log.read_payload_range(e, o, n))
            .unwrap();
        assert_eq!(got, after);
        let old = loaded.catalog.pages[&id((pages - 1) as u64)]
            .materialize(|e, o, n| log.read_payload_range(e, o, n))
            .unwrap();
        assert_eq!(old, before);
        let record = log
            .append_page(
                &PageOutcome::between(&id(0), Some(&after), &page(b"replacement".to_vec(), 3))
                    .unwrap(),
            )
            .unwrap();
        loaded.catalog.fold(&record).unwrap();
        let mut dirty = BTreeSet::from([id(0)]);
        publish(
            &log,
            &loaded.catalog,
            &mut loaded.index,
            &mut dirty,
            record.position,
            4,
        );
        assert_eq!(
            catalog::latest(&log, pages).unwrap().unwrap().commit.epoch,
            4
        );
    }
}
#[test]
fn checkpoint_index_upgrades_a_published_catalog_without_rewriting_payloads() {
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 128);
    catalog::prepare(
        &log,
        &pages,
        b"old-state".to_vec(),
        Some(cut.clone()),
        1,
        false,
    )
    .unwrap()
    .publish(&log)
    .unwrap();
    let loaded = catalog::latest(&log, 128).unwrap().unwrap();
    assert!(loaded.index.is_none());
    let mut index = None;
    publish(
        &log,
        &loaded.catalog,
        &mut index,
        &mut BTreeSet::new(),
        cut,
        2,
    );
    let loaded = catalog::latest(&log, 128).unwrap().unwrap();
    assert!(loaded.index.is_some());
    assert_eq!(
        loaded.catalog.pages[&id(127)]
            .materialize(|e, o, n| log.read_payload_range(e, o, n))
            .unwrap(),
        page(vec![0x31; 16], 1)
    );
}
#[test]
fn checkpoint_index_unpublished_preparation_keeps_the_previous_root_and_bad_newest_refuses() {
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 64);
    let mut index = None;
    let mut dirty = BTreeSet::new();
    publish(&log, &pages, &mut index, &mut dirty, cut.clone(), 1);
    let prepared = prepare(
        &log,
        &pages,
        &mut index,
        &mut dirty,
        b"new-state".to_vec(),
        Some(cut.clone()),
        2,
        false,
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(catalog::latest(&log, 64).unwrap().unwrap().commit.epoch, 1);
    drop(prepared);
    let mut prepared = prepare(
        &log,
        &pages,
        &mut index,
        &mut dirty,
        b"new-state".to_vec(),
        Some(cut),
        3,
        false,
        &|| Ok(()),
    )
    .unwrap();
    prepared
        .commit
        .index_root
        .as_mut()
        .unwrap()
        .node
        .as_mut()
        .unwrap()
        .sha256[0] ^= 1;
    prepared.publish(&log).unwrap();
    assert!(
        catalog::latest(&log, 64).is_err(),
        "a damaged newest root must not fall back"
    );
}
#[test]
fn checkpoint_index_refuses_limits_foreign_pointers_and_invalid_versions() {
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 64);
    let mut index = None;
    let commit = publish(&log, &pages, &mut index, &mut BTreeSet::new(), cut, 1);
    assert!(load(&log, commit.clone(), 63).is_err());
    let mut bad = commit.clone();
    bad.index_root
        .as_mut()
        .unwrap()
        .node
        .as_mut()
        .unwrap()
        .height = 97;
    assert!(load(&log, bad, 64).is_err());
    let mut bad = commit.clone();
    bad.index_root.as_mut().unwrap().version = 2;
    assert!(load(&log, bad, 64).is_err());
    let mut bad = commit.clone();
    bad.index_root
        .as_mut()
        .unwrap()
        .node
        .as_mut()
        .unwrap()
        .position
        .as_mut()
        .unwrap()
        .store_id = vec![0; 16];
    assert!(load(&log, bad, 64).is_err());
    let mut bad = commit;
    bad.version = 3;
    assert!(catalog::load(&log, bad, 64).is_err());
}

#[test]
fn checkpoint_index_rejects_child_height_overflow_and_same_record_cycles() {
    for cycle in [false, true] {
        let dir = Dir::new();
        let log = dir.open();
        let mut pages = PageCatalog::default();
        let cut = seed(&log, &mut pages, 64);
        let mut index = None;
        let mut dirty = BTreeSet::new();
        publish(&log, &pages, &mut index, &mut dirty, cut.clone(), 1);
        let root = index.as_mut().unwrap().root.as_mut().unwrap();
        if cycle {
            // A correctly hashed record may still contain a structurally invalid pointer.
            let mut child = root.saved.clone().unwrap();
            child.height = root.left.as_ref().unwrap().height;
            child.count = root.left.as_ref().unwrap().count;
            child.position = None;
            child.decision = 63;
            child.ordinal = u64::MAX;
            root.left.as_mut().unwrap().saved = Some(child);
        } else {
            root.left.as_mut().unwrap().saved.as_mut().unwrap().height = u32::MAX;
        }
        root.saved = None;
        let prepared = prepare(
            &log,
            &pages,
            &mut index,
            &mut dirty,
            b"state".to_vec(),
            Some(cut),
            2,
            false,
            &|| Ok(()),
        )
        .unwrap();
        prepared.publish(&log).unwrap();
        assert!(catalog::latest(&log, 64).is_err());
    }
}
#[test]
fn checkpoint_index_large_page_metadata_streams_and_reopens_without_a_large_index_node() {
    let dir = Dir::new();
    let log = dir.open();
    let state = page(vec![0x31; 65536], 1);
    let record = log
        .append_page(&PageOutcome::between(&id(0), None, &state).unwrap())
        .unwrap();
    let extent = Arc::new(record.batch.payloads[0].clone());
    let spans = (0..65536)
        .map(|offset| PayloadSpan {
            offset,
            length: 1,
            extent: extent.clone(),
            extent_offset: offset,
        })
        .collect();
    let mut pages = PageCatalog::default();
    pages.pages.insert(
        id(0),
        CatalogPage {
            metadata: CatalogPageMetadata::from_state(&id(0), &state).unwrap(),
            spans,
        },
    );
    let payload_position = record.position.clone();
    let advance = || {
        let count = log.segment_count().unwrap();
        let fragment = CheckpointFragment {
            version: 1,
            checkpoint_id: vec![0x83; 16],
            kind: CheckpointFragment::PAYLOAD,
            ordinal: 0,
            data: vec![0x19; CheckpointFragment::MAX_DATA_BYTES],
        };
        loop {
            let record = log
                .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
                .unwrap();
            if log.segment_count().unwrap() > count {
                break record.position;
            }
        }
    };
    advance();
    let mut index = None;
    let first = publish(
        &log,
        &pages,
        &mut index,
        &mut BTreeSet::new(),
        record.position,
        1,
    );
    assert!(!index
        .as_ref()
        .unwrap()
        .root
        .as_ref()
        .unwrap()
        .value
        .catalog
        .is_empty());
    let loaded = catalog::latest(&log, 1).unwrap().unwrap();
    assert_eq!(loaded.catalog.pages[&id(0)].spans.len(), 65536);
    assert_eq!(
        loaded.catalog.pages[&id(0)]
            .read_data_range(123, 17, |e, o, n| log.read_payload_range(e, o, n))
            .unwrap(),
        vec![0x31; 17]
    );
    drop(loaded);
    let cut = advance();
    let catalog_commit =
        decode_catalog(&index.as_ref().unwrap().root.as_ref().unwrap().value.catalog).unwrap();
    let old_catalog = catalog_commit.first.clone().unwrap();
    let catalog_id: [u8; 16] = old_catalog.store_id.as_slice().try_into().unwrap();
    let selected: Vec<_> = log
        .compaction_candidates(&cut, None, 64)
        .unwrap()
        .into_iter()
        .filter(|candidate| candidate.store_id == catalog_id)
        .collect();
    assert_eq!(selected.len(), 1);
    let mut coverage = BTreeMap::from([(catalog_id, Vec::new())]);
    index
        .as_ref()
        .unwrap()
        .compaction_metadata_ranges(&log, &selected, first.epoch, &mut coverage, &|| Ok(()))
        .unwrap();
    assert!(coverage[&catalog_id]
        .iter()
        .any(|(start, end)| *start <= old_catalog.offset && *end > old_catalog.offset));
    let mut dirty = BTreeSet::new();
    index
        .as_mut()
        .unwrap()
        .evacuate_metadata(&log, &selected, &mut dirty, &|| Ok(()))
        .unwrap();
    assert_eq!(dirty, BTreeSet::from([id(0)]));
    let (commit, position) = prepare(
        &log,
        &pages,
        &mut index,
        &mut dirty,
        b"scalar-state".to_vec(),
        Some(cut),
        2,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let keep = assert_indexed_protection(index.as_ref().unwrap(), &log, &pages, &commit, &position);
    assert!(!keep.contains(catalog_id.as_slice()));
    let retired = log
        .prepare_retention(&keep, commit.through.as_ref().unwrap(), &|| Ok(()))
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(retired.finish().unwrap().removed > 0);
    assert!(log.read_metadata(&old_catalog).is_err());
    assert!(
        log.read_metadata(&payload_position).is_ok(),
        "metadata evacuation moved or retired the unchanged payload"
    );
    drop(index);
    drop(pages);
    drop(log);
    let log = dir.open();
    let loaded = catalog::latest(&log, 1).unwrap().unwrap();
    assert_eq!(loaded.catalog.pages[&id(0)].spans.len(), 65536);
    assert_eq!(
        loaded.catalog.pages[&id(0)]
            .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
            .unwrap(),
        state
    );
}
#[test]
fn checkpoint_index_extent_cache_releases_overwritten_descriptor_history() {
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 1);
    let mut index = None;
    let mut dirty = BTreeSet::new();
    publish(&log, &pages, &mut index, &mut dirty, cut, 1);
    let mut before = page(vec![0x31; 16], 1);
    for epoch in 2..18 {
        let candidates = pages.pages[&id(0)]
            .spans
            .iter()
            .map(|s| catalog::extent_key(&s.extent).unwrap())
            .collect();
        let after = page(vec![epoch as u8; 16], epoch);
        let record = log
            .append_page(&PageOutcome::between(&id(0), Some(&before), &after).unwrap())
            .unwrap();
        pages.fold(&record).unwrap();
        index.as_mut().unwrap().prune(&candidates).unwrap();
        assert!(index.as_ref().unwrap().extents.is_empty());
        dirty.insert(id(0));
        publish(&log, &pages, &mut index, &mut dirty, record.position, epoch);
        assert_eq!(index.as_ref().unwrap().extents.len(), 1);
        before = after;
    }
}
#[test]
fn checkpoint_index_cancelled_preparation_does_not_publish_and_install_cannot_skip_a_write() {
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 64);
    let mut index = None;
    let mut dirty = BTreeSet::new();
    publish(&log, &pages, &mut index, &mut dirty, cut.clone(), 1);
    let cancelled = prepare(
        &log,
        &pages,
        &mut index,
        &mut dirty,
        b"state".to_vec(),
        Some(cut.clone()),
        2,
        false,
        &|| Err(io::ErrorKind::Interrupted.into()),
    );
    assert_eq!(cancelled.err().unwrap().kind(), io::ErrorKind::Interrupted);
    assert_eq!(catalog::latest(&log, 64).unwrap().unwrap().commit.epoch, 1);
    let calls = std::cell::Cell::new(0);
    let check = || {
        calls.set(calls.get() + 1);
        if calls.get() == 2 {
            log.append_page(
                &PageOutcome::between(&id(1000), None, &page(b"intervening".to_vec(), 1)).unwrap(),
            )?;
        }
        Ok(())
    };
    let prepared = prepare(
        &log,
        &pages,
        &mut index,
        &mut dirty,
        b"state".to_vec(),
        Some(cut),
        3,
        true,
        &check,
    )
    .unwrap();
    prepared.publish(&log).unwrap();
    assert!(
        catalog::latest(&log, 1024).is_err(),
        "install skipped an intervening logical write"
    );
}

#[test]
fn checkpoint_index_sparse_discard_and_shorter_states_survive_repeated_reopen() {
    let dir = Dir::new();
    let mut service = crate::MatrixReplicaService::new();
    service.set_keep_ec_sidecar(false);
    let mut expected = Vec::new();
    let mut catalog = PageCatalog::default();
    let mut index = None;
    let mut dirty = BTreeSet::new();
    let mut log = dir.open();
    let mut cut = None;
    for n in 0..4 {
        service
            .write_chunk(id(n), 0, &vec![0x41 + n as u8; 4096])
            .unwrap();
        let after = service.read_chunk_state(&id(n)).unwrap();
        let record = log
            .append_page(&PageOutcome::between(&id(n), None, &after).unwrap())
            .unwrap();
        catalog.fold(&record).unwrap();
        cut = Some(record.position);
        expected.push(after);
    }
    publish(&log, &catalog, &mut index, &mut dirty, cut.unwrap(), 1);
    for step in 0..5 {
        let n = if step < 2 {
            0
        } else if step == 2 {
            1
        } else if step == 3 {
            2
        } else {
            3
        };
        let before = expected[n].clone();
        let after = match step {
            0 => {
                service
                    .write_chunk(id(n as u64), 16384, b"sparse-tail")
                    .unwrap();
                service.read_chunk_state(&id(n as u64)).unwrap()
            }
            1 => {
                service.discard_chunk(&id(n as u64), 0, 128).unwrap();
                service.read_chunk_state(&id(n as u64)).unwrap()
            }
            2 => {
                service.write_chunk(id(n as u64), 17, b"overwrite").unwrap();
                service.read_chunk_state(&id(n as u64)).unwrap()
            }
            3 => page(vec![0; 16384], before.write_sequence + 1),
            _ => page(b"short".to_vec(), before.write_sequence + 1),
        };
        let record = log
            .append_page(&PageOutcome::between(&id(n as u64), Some(&before), &after).unwrap())
            .unwrap();
        catalog.fold(&record).unwrap();
        dirty.insert(id(n as u64));
        expected[n] = after;
        publish(
            &log,
            &catalog,
            &mut index,
            &mut dirty,
            record.position,
            step + 2,
        );
        drop(index);
        drop(catalog);
        drop(log);
        log = dir.open();
        let loaded = catalog::latest(&log, 4).unwrap().unwrap();
        for (n, expected) in expected.iter().enumerate() {
            assert_eq!(
                loaded.catalog.pages[&id(n as u64)]
                    .materialize(|e, o, n| log.read_payload_range(e, o, n))
                    .unwrap(),
                *expected
            );
        }
        catalog = loaded.catalog;
        index = loaded.index;
    }
}

#[test]
fn checkpoint_index_retention_preserves_every_duplicate_extent_record_after_reload() {
    fn next_segment(log: &SegmentedOutcomeLog) -> StoredOutcomePosition {
        let count = log.retained_segment_count().unwrap();
        let mut fragment = CheckpointFragment {
            version: 1,
            checkpoint_id: vec![0x88; 16],
            kind: CheckpointFragment::PAYLOAD,
            ordinal: 0,
            data: vec![0x32; CheckpointFragment::MAX_DATA_BYTES],
        };
        loop {
            let record = log
                .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
                .unwrap();
            if log.retained_segment_count().unwrap() != count {
                return record.position;
            }
            fragment.ordinal += 1;
            assert!(fragment.ordinal < 32);
        }
    }
    fn descriptor(
        log: &SegmentedOutcomeLog,
        index: &mut IndexState,
        extent: &Arc<PayloadExtent>,
        epoch: u64,
    ) -> BlobRef {
        let key = catalog::extent_key(extent).unwrap();
        let mut writer = Writer {
            log,
            id: identity().unwrap(),
            ordinal: 0,
            pending: Vec::new(),
            bytes: 0,
            check: &|| Ok(()),
        };
        let mut block = Block::new(log, epoch, EXTENT);
        block.extent = Some((**extent).clone());
        writer
            .push(index, Slot::Extent(key, extent.clone()), block, 0, 0)
            .unwrap();
        writer.flush(index).unwrap();
        index.extents[&key].saved.clone().unwrap()
    }
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    seed(&log, &mut pages, 2);
    let extent = pages.pages[&id(0)].spans[0].extent.clone();
    let mut index = IndexState::default();
    next_segment(&log);
    let first = descriptor(&log, &mut index, &extent, 1);
    let cut = next_segment(&log);
    let mut index = Some(index);
    let mut dirty = BTreeSet::from([id(0), id(1)]);
    publish(&log, &pages, &mut index, &mut dirty, cut, 1);
    next_segment(&log);
    let duplicate = descriptor(&log, index.as_mut().unwrap(), &extent, 2);
    next_segment(&log); // A completely unreachable storage-only segment.
    let cut = next_segment(&log);
    dirty.insert(id(1));
    publish(&log, &pages, &mut index, &mut dirty, cut, 2);
    assert_ne!(
        first.position().unwrap().store_id,
        duplicate.position().unwrap().store_id
    );
    drop(index);
    drop(pages);
    drop(log);
    let log = dir.open();
    let mut loaded = catalog::latest(&log, 2).unwrap().unwrap();
    let position = loaded.publication_position.as_ref().unwrap();
    let index = loaded.index.as_mut().unwrap();
    // Interning is allowed to choose either valid descriptor record. A leaf
    // that names the other one must remain recoverable after collection.
    index
        .set_extent_saved(catalog::extent_key(&extent).unwrap(), duplicate.clone())
        .unwrap();
    let keep = assert_indexed_protection(index, &log, &loaded.catalog, &loaded.commit, position);
    assert!(keep.contains(&first.position().unwrap().store_id));
    let before = log.retained_segment_count().unwrap();
    let committed = log
        .prepare_retention(&keep, loaded.commit.through.as_ref().unwrap(), &|| Ok(()))
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(committed.finish().unwrap().removed > 0);
    assert!(log.retained_segment_count().unwrap() < before);
    drop(loaded);
    drop(log);
    let log = dir.open();
    let mut loaded = catalog::latest(&log, 2).unwrap().unwrap();
    for n in 0..2 {
        assert_eq!(
            loaded.catalog.pages[&id(n)]
                .materialize(|e, o, n| log.read_payload_range(e, o, n))
                .unwrap(),
            page(vec![0x31; 16], 1)
        );
    }
    let first_id: [u8; 16] = first
        .position()
        .unwrap()
        .store_id
        .as_slice()
        .try_into()
        .unwrap();
    let selected: Vec<_> = log
        .compaction_candidates(loaded.commit.through.as_ref().unwrap(), None, 64)
        .unwrap()
        .into_iter()
        .filter(|candidate| candidate.store_id == first_id)
        .collect();
    assert_eq!(selected.len(), 1);
    let index = loaded.index.as_mut().unwrap();
    index
        .set_extent_saved(catalog::extent_key(&extent).unwrap(), duplicate)
        .unwrap();
    let mut ranges = BTreeMap::from([(first_id, Vec::new())]);
    index
        .compaction_metadata_ranges(
            &log,
            &selected,
            loaded.commit.epoch,
            &mut ranges,
            &|| Ok(()),
        )
        .unwrap();
    let original = first.position().unwrap();
    assert_cached_coverage(index, &loaded.catalog);
    assert!(
        index.planner.has_metadata(&first_id),
        "alternate leaf record lost metadata ownership"
    );
    assert!(
        ranges[&first_id]
            .iter()
            .any(|(start, end)| *start <= original.offset
                && *end >= original.offset + original.length)
    );
    let mut dirty = BTreeSet::new();
    index
        .evacuate_metadata(&log, &selected, &mut dirty, &|| Ok(()))
        .unwrap();
    assert!(
        dirty.contains(&id(0)),
        "the leaf using the alternate record was not rewritten"
    );
    let cut = loaded.publication_position.clone().unwrap();
    let (commit, position) = prepare(
        &log,
        &loaded.catalog,
        &mut loaded.index,
        &mut dirty,
        loaded.header.metadata.clone(),
        Some(cut),
        3,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let keep = assert_indexed_protection(
        loaded.index.as_ref().unwrap(),
        &log,
        &loaded.catalog,
        &commit,
        &position,
    );
    assert!(
        !keep.contains(first_id.as_slice()),
        "a rewritten alternate record still pins its old file"
    );
    let retired = log
        .prepare_retention(&keep, commit.through.as_ref().unwrap(), &|| Ok(()))
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(retired.finish().unwrap().removed > 0);
    assert!(log.read_metadata(original).is_err());
    drop(loaded);
    drop(log);
    let log = dir.open();
    let loaded = catalog::latest(&log, 2).unwrap().unwrap();
    for n in 0..2 {
        assert_eq!(loaded.catalog.pages[&id(n)].materialize(|extent, offset, length| log.read_payload_range(extent, offset, length)).unwrap(), page(vec![0x31; 16], 1));
    }
}

fn assert_cached_coverage(index: &IndexState, pages: &PageCatalog) {
    assert!(index.ordinary_coverage());
    let mut ranges: BTreeMap<[u8; 16], Vec<(u64, u64)>> = BTreeMap::new();
    let mut record = |value: &StoredOutcomePosition| {
        let id = value.store_id.as_slice().try_into().unwrap();
        ranges
            .entry(id)
            .or_default()
            .push((value.offset, value.offset + value.length));
    };
    let mut stack: Vec<_> = index.root.iter().map(|node| node.as_ref()).collect();
    while let Some(node) = stack.pop() {
        if let Some(saved) = &node.saved {
            record(saved.position().unwrap());
        }
        record(node.value.page.as_ref().unwrap().position().unwrap());
        for extent in &node.extent_records {
            record(&StoredOutcomePosition {
                version: 1,
                store_id: extent.store_id.to_vec(),
                offset: extent.offset,
                length: extent.length,
            });
        }
        stack.extend(
            node.left
                .iter()
                .chain(node.right.iter())
                .map(|node| node.as_ref()),
        );
    }
    for extent in index.extents.values() {
        if let Some(saved) = &extent.saved {
            record(saved.position().unwrap());
        }
    }
    let metadata_segments: BTreeSet<_> = ranges.keys().copied().collect();
    for page in pages.pages.values() {
        for span in &page.spans {
            ranges
                .entry(catalog::extent_key(&span.extent).unwrap().0)
                .or_default()
                .push((
                    span.extent.offset + span.extent_offset,
                    span.extent.offset + span.extent_offset + span.length,
                ));
        }
    }
    for segment in ranges.keys().copied().chain([[0; 16], [255; 16]]) {
        assert_eq!(
            index.planner.has_metadata(&segment),
            metadata_segments.contains(&segment)
        );
        let mut absent = segment;
        absent[0] ^= 0x80;
        assert_eq!(
            index.planner.has_metadata(&absent),
            metadata_segments.contains(&absent)
        );
    }
    let mut expected = BTreeMap::new();
    for (id, mut ranges) in ranges {
        ranges.sort_unstable();
        let mut end = 0;
        let mut bytes = 0;
        for (start, next) in ranges {
            if next > end {
                bytes += next - start.max(end);
                end = next;
            }
        }
        expected.insert(id, bytes);
    }
    assert_eq!(index.planner.totals(), expected);
    let mut owners: BTreeMap<[u8; 16], BTreeSet<PageId>> = BTreeMap::new();
    for (id, page) in &pages.pages {
        for span in &page.spans {
            owners
                .entry(catalog::extent_key(&span.extent).unwrap().0)
                .or_default()
                .insert(id.clone());
        }
    }
    assert_eq!(index.planner.payload_owner_sets(), &owners);
    for (segment, keys) in &owners {
        assert_eq!(
            index
                .planned_payload_pages(segment, pages.pages.len())
                .unwrap(),
            keys.iter().cloned().collect::<Vec<_>>()
        );
    }
    assert!(index
        .planned_payload_pages(&[0; 16], pages.pages.len())
        .unwrap()
        .is_empty());
    assert!(index
        .planned_payload_pages(&[0; 16], pages.pages.len() + 1)
        .is_err());
    let stored_spans: usize = pages.pages.values().map(|page| page.spans.len()).sum();
    assert!(
        index.planner.retained_payload_range_slots() <= stored_spans,
        "derived ranges retain more slots than the catalog has spans"
    );
}
#[test]
fn checkpoint_index_cached_coverage_matches_complete_scan_across_updates_and_restart() {
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 512);
    let mut index = None;
    let mut dirty = BTreeSet::new();
    publish(&log, &pages, &mut index, &mut dirty, cut, 1);
    assert_cached_coverage(index.as_ref().unwrap(), &pages);
    for epoch in 2..26u64 {
        let mut outcomes = Vec::new();
        let mut old = BTreeSet::new();
        for step in 0..64u64 {
            let key = if step < 16 {
                id(512 + (epoch - 2) * 16 + step)
            } else {
                id((epoch * 37 + step * 17) % 512)
            };
            let before = pages.pages.get(&key).map(|page| {
                page.materialize(|e, o, n| log.read_payload_range(e, o, n))
                    .unwrap()
            });
            if let Some(page) = pages.pages.get(&key) {
                for span in &page.spans {
                    old.insert(catalog::extent_key(&span.extent).unwrap());
                }
            }
            let mut data = before
                .as_ref()
                .map_or_else(Vec::new, |state| state.data.as_ref().clone());
            match (epoch + step) % 3 {
                0 => data.extend_from_slice(&[0x71; 16]),
                1 => {
                    data.resize(32, 0);
                    data[0] = epoch as u8;
                }
                _ => data.truncate((step as usize) % 17),
            }
            let after = page(
                data,
                before.as_ref().map_or(1, |page| page.write_sequence + 1),
            );
            outcomes.push(PageOutcome::between(&key, before.as_ref(), &after).unwrap());
            dirty.insert(key);
        }
        let decisions: Vec<_> = outcomes.iter().map(DecidedMutation::Page).collect();
        let record = log.append_decided_batch(&decisions).unwrap();
        pages.fold(&record).unwrap();
        let current = index.as_mut().unwrap();
        current.prune(&old).unwrap();
        current
            .refresh_planned_payloads(&pages, &dirty, &|| Ok(()))
            .unwrap();
        assert_cached_coverage(current, &pages);
        publish(&log, &pages, &mut index, &mut dirty, record.position, epoch);
        assert_cached_coverage(index.as_ref().unwrap(), &pages);
    }
    drop(index);
    drop(pages);
    drop(log);
    let log = dir.open();
    let mut loaded = catalog::latest(&log, 1024).unwrap().unwrap();
    assert_cached_coverage(loaded.index.as_ref().unwrap(), &loaded.catalog);
    let key = id(7);
    let before = loaded.catalog.pages[&key]
        .materialize(|e, o, n| log.read_payload_range(e, o, n))
        .unwrap();
    let after = page(vec![0x52; 128], before.write_sequence + 1);
    let record = log
        .append_page(&PageOutcome::between(&key, Some(&before), &after).unwrap())
        .unwrap();
    loaded.catalog.fold(&record).unwrap();
    let mut dirty = BTreeSet::from([key]);
    publish(
        &log,
        &loaded.catalog,
        &mut loaded.index,
        &mut dirty,
        record.position,
        26,
    );
    assert_cached_coverage(loaded.index.as_ref().unwrap(), &loaded.catalog);
}

#[test]
fn checkpoint_index_protection_is_bounded_and_dirty_catalog_uses_complete_scan() {
    use std::cell::Cell;
    let dir = Dir::new();
    let log = dir.open();
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 4096);
    let mut index = None;
    let mut dirty = BTreeSet::new();
    let (commit, position) = prepare(
        &log,
        &pages,
        &mut index,
        &mut dirty,
        b"scalar-state".to_vec(),
        Some(cut),
        1,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let index = index.as_ref().unwrap();
    let fast_calls = Cell::new(0usize);
    let cached = index
        .protected_segments_indexed(&log, &pages, &commit, &position, &dirty, &|| {
            fast_calls.set(fast_calls.get() + 1);
            Ok(())
        })
        .unwrap();
    let complete_calls = Cell::new(0usize);
    let complete = index
        .protected_segments(&log, &pages, &commit, &position, &|| {
            complete_calls.set(complete_calls.get() + 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(cached, complete);
    assert!(fast_calls.get() <= 1 + 2 * cached.len());
    assert!(complete_calls.get() > 8192);
    assert!(fast_calls.get() < 64);
    // Logical edits leave the old root valid but require protecting new sources.
    let mut fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![0x93; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x47; CheckpointFragment::MAX_DATA_BYTES],
    };
    let initial = log.retained_segment_count().unwrap();
    while log.retained_segment_count().unwrap() == initial {
        log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
            .unwrap();
        fragment.ordinal += 1;
        assert!(fragment.ordinal < 32);
    }
    let before = pages.pages[&id(0)]
        .materialize(|e, o, n| log.read_payload_range(e, o, n))
        .unwrap();
    let after = page(vec![0x67; 32], before.write_sequence + 1);
    let record = log
        .append_page(&PageOutcome::between(&id(0), Some(&before), &after).unwrap())
        .unwrap();
    let new_source = record.batch.payloads[0].store_id.clone();
    pages.fold(&record).unwrap();
    dirty.insert(id(0));
    let calls = Cell::new(0usize);
    let keep = index
        .protected_segments_indexed(&log, &pages, &commit, &position, &dirty, &|| {
            calls.set(calls.get() + 1);
            Ok(())
        })
        .unwrap();
    assert!(calls.get() > 8192);
    assert!(keep.contains(&new_source));
    assert_eq!(
        keep,
        index
            .protected_segments(&log, &pages, &commit, &position, &|| Ok(()))
            .unwrap()
    );
    assert!(index
        .protected_segments_indexed(&log, &pages, &commit, &position, &dirty, &|| Err(
            io::Error::new(io::ErrorKind::Interrupted, "cancelled protection")
        ),)
        .is_err());
    let mut changed = commit.clone();
    changed.epoch += 1;
    assert!(index
        .protected_segments_indexed(&log, &pages, &changed, &position, &BTreeSet::new(), &|| Ok(
            ()
        ),)
        .is_err());
}

#[test]
fn checkpoint_index_metadata_walk_keeps_legacy_and_unpublished_validation() {
    use crate::durable_payload::outcome_segments::CompactionSegment;
    use std::cell::Cell;
    let dir = Dir::new();
    let log = dir.open();
    let selected = [CompactionSegment {
        number: 0,
        store_id: [0; 16],
        length: MIN_SEGMENT_BYTES,
    }];
    let mut pages = PageCatalog::default();
    let cut = seed(&log, &mut pages, 512);
    let mut index = None;
    publish(&log, &pages, &mut index, &mut BTreeSet::new(), cut, 1);
    let index = index.as_mut().unwrap();
    assert!(!index.planner.has_metadata(&selected[0].store_id));
    let key = id(7);
    let (value, records) = {
        let node = index.root.as_mut().unwrap().find_mut(&key);
        (node.value.clone(), node.extent_records.clone())
    };
    index.put_with_extents(key, value, records).unwrap();
    assert!(!index.ordinary_coverage());
    let mut dirty = BTreeSet::new();
    let visits = Cell::new(0usize);
    index
        .evacuate_metadata(&log, &selected, &mut dirty, &|| {
            visits.set(visits.get() + 1);
            Ok(())
        })
        .unwrap();
    assert!(visits.get() > 512);
    assert!(dirty.is_empty());
    // An invalid legacy catalog must still be decoded and rejected.
    let mut legacy = IndexState::default();
    legacy.put(
        id(0),
        Value {
            page: None,
            catalog: vec![1],
        },
    );
    assert!(!legacy.ordinary_coverage());
    assert!(!legacy.planner.has_metadata(&selected[0].store_id));
    assert!(legacy
        .evacuate_metadata(&log, &selected, &mut dirty, &|| Ok(()))
        .is_err());
}
