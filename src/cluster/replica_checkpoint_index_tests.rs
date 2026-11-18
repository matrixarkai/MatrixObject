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
    prepare(
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
    .publish(log)
    .unwrap()
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
    let mut index = None;
    publish(
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
        index.as_mut().unwrap().prune(&candidates);
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
