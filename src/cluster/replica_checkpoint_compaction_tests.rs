// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::super::replica_checkpoint_spans::{CatalogPage, CatalogPageMetadata};
use super::*;
use crate::{ChunkId, PageState};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "checkpoint-compaction-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn log(&self) -> SegmentedOutcomeLog {
        SegmentedOutcomeLog::open(
            self.0.join("outcomes"),
            crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES,
        )
        .unwrap()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn id(page: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: page,
    }
}
fn state(data: Vec<u8>) -> PageState {
    PageState {
        logical_length: data.len() as u64,
        write_sequence: 7,
        checksum_fnv1a64: crate::fnv1a64(&data),
        data: Arc::new(data),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    }
}
fn payload(log: &SegmentedOutcomeLog, data: Vec<u8>) -> Arc<PayloadExtent> {
    let fragment = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![41; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data,
    };
    let record = log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])
        .unwrap();
    assert_eq!(record.batch.payloads.len(), 1);
    Arc::new(record.batch.payloads[0].clone())
}
fn verify(
    log: &SegmentedOutcomeLog,
    catalog: &PageCatalog,
    expected: &BTreeMap<PageId, PageState>,
) {
    for (id, state) in expected {
        assert_eq!(&catalog.pages[id].materialize(|extent,offset,length|log.read_payload_range(extent,offset,length)).unwrap(),state);
    }
}

#[test]
fn compaction_unions_shared_ranges_and_packs_sparse_pages_without_copying_dead_extent_bytes() {
    let dir = Dir::new();
    let log = dir.log();
    let data: Vec<_> = (0..1024 * 1024).map(|n| (n % 251 + 1) as u8).collect();
    let extent = payload(&log, data.clone());
    let selected = extent.store_id.as_slice().try_into().unwrap();
    let old_key = extent_key(&extent).unwrap();
    let mut catalog = PageCatalog::default();
    let mut expected = BTreeMap::new();
    for n in 0..100 {
        let mut bytes = vec![0; 300];
        bytes[..43].copy_from_slice(&data[17..60]);
        bytes[200..257].copy_from_slice(&data[32..89]);
        let page = state(bytes);
        let id = id(n);
        catalog.pages.insert(
            id.clone(),
            CatalogPage {
                metadata: CatalogPageMetadata::from_state(&id, &page).unwrap(),
                spans: vec![
                    PayloadSpan {
                        offset: 0,
                        length: 43,
                        extent: extent.clone(),
                        extent_offset: 17,
                    },
                    PayloadSpan {
                        offset: 200,
                        length: 57,
                        extent: extent.clone(),
                        extent_offset: 32,
                    },
                ],
            },
        );
        expected.insert(id, page);
    }
    let before = log.logical_length().unwrap();
    let mut dirty = BTreeSet::new();
    assert_eq!(
        relocate(
            &log,
            &mut catalog,
            selected,
            &mut dirty,
            COPY_BYTES,
            &|| Ok(())
        )
        .unwrap(),
        72
    );
    assert_eq!(dirty.len(), 100);
    assert!(
        log.logical_length().unwrap() - before < 64 * 1024,
        "tiny live ranges were not packed together"
    );
    let moved: BTreeSet<_> = catalog
        .pages
        .values()
        .flat_map(|page| &page.spans)
        .map(|span| extent_key(&span.extent).unwrap())
        .collect();
    assert_eq!(moved.len(), 1);
    assert!(!moved.contains(&old_key));
    verify(&log, &catalog, &expected);
    drop(log);
    let log = dir.log();
    verify(&log, &catalog, &expected);
}

#[test]
fn compaction_reuses_sector_storage_without_mixing_distinct_extents() {
    let dir = Dir::new();
    let log = dir.log();
    let mut catalog = PageCatalog::default();
    let mut expected = BTreeMap::new();
    let mut selected = None;
    for n in 0..3 {
        let data: Vec<_> = (0..9000).map(|at| ((at + n * 37) % 251) as u8).collect();
        let extent = payload(&log, data.clone());
        let segment: [u8; 16] = extent.store_id.as_slice().try_into().unwrap();
        if let Some(selected) = selected {
            assert_eq!(segment, selected);
        }
        selected = Some(segment);
        let mut bytes = vec![0; 96];
        bytes[4..17].copy_from_slice(&data[17..30]);
        bytes[40..51].copy_from_slice(&data[4093..4104]);
        let page = state(bytes);
        let id = id(n as u64);
        catalog.pages.insert(
            id.clone(),
            CatalogPage {
                metadata: CatalogPageMetadata::from_state(&id, &page).unwrap(),
                spans: vec![
                    PayloadSpan {
                        offset: 4,
                        length: 13,
                        extent: extent.clone(),
                        extent_offset: 17,
                    },
                    PayloadSpan {
                        offset: 40,
                        length: 11,
                        extent,
                        extent_offset: 4093,
                    },
                ],
            },
        );
        expected.insert(id, page);
    }
    let mut dirty = BTreeSet::new();
    assert_eq!(
        relocate(
            &log,
            &mut catalog,
            selected.unwrap(),
            &mut dirty,
            COPY_BYTES,
            &|| Ok(())
        )
        .unwrap(),
        72
    );
    assert_eq!(dirty.len(), 3);
    verify(&log, &catalog, &expected);
    drop(log);
    verify(&dir.log(), &catalog, &expected);
}

#[test]
fn compaction_reused_fragment_buffer_handles_unaligned_ranges_across_flushes() {
    let dir = Dir::new();
    let log = dir.log();
    let mut catalog = PageCatalog::default();
    let mut expected = BTreeMap::new();
    let mut selected = None;
    for n in 0..3 {
        let data: Vec<_> = (0..CheckpointFragment::MAX_DATA_BYTES)
            .map(|at| ((at + n * 37) % 251) as u8)
            .collect();
        let extent = payload(&log, data.clone());
        let segment: [u8; 16] = extent.store_id.as_slice().try_into().unwrap();
        if let Some(selected) = selected {
            assert_eq!(selected, segment);
        }
        selected = Some(segment);
        let bytes = data[19..data.len() - 18].to_vec();
        let page = state(bytes);
        let id = id(n as u64);
        catalog.pages.insert(
            id.clone(),
            CatalogPage {
                metadata: CatalogPageMetadata::from_state(&id, &page).unwrap(),
                spans: vec![PayloadSpan {
                    offset: 0,
                    length: page.logical_length,
                    extent,
                    extent_offset: 19,
                }],
            },
        );
        expected.insert(id, page);
    }
    let mut dirty = BTreeSet::new();
    assert_eq!(
        relocate(
            &log,
            &mut catalog,
            selected.unwrap(),
            &mut dirty,
            COPY_BYTES,
            &|| Ok(())
        )
        .unwrap(),
        3 * (CheckpointFragment::MAX_DATA_BYTES as u64 - 37)
    );
    assert_eq!(dirty.len(), 3);
    verify(&log, &catalog, &expected);
    drop(log);
    verify(&dir.log(), &catalog, &expected);
}

#[test]
fn compaction_budget_preserves_unmoved_spans_and_resumes_a_shared_extent() {
    let dir = Dir::new();
    let log = dir.log();
    let bytes: Vec<_> = (0..128 * 1024).map(|n| (n % 251 + 1) as u8).collect();
    let extent = payload(&log, bytes.clone());
    let selected = extent.store_id.as_slice().try_into().unwrap();
    let original = extent_key(&extent).unwrap();
    let filler = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![61; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![23; CheckpointFragment::MAX_DATA_BYTES],
    };
    while log.segment_count().unwrap() < 2 {
        log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&filler)])
            .unwrap();
    }
    let mut catalog = PageCatalog::default();
    let mut expected = BTreeMap::new();
    for n in 0..3 {
        let page = state(bytes.clone());
        let id = id(n);
        catalog.pages.insert(
            id.clone(),
            CatalogPage {
                metadata: CatalogPageMetadata::from_state(&id, &page).unwrap(),
                spans: vec![PayloadSpan {
                    offset: 0,
                    length: bytes.len() as u64,
                    extent: extent.clone(),
                    extent_offset: 0,
                }],
            },
        );
        expected.insert(id, page);
    }
    let mut dirty = BTreeSet::new();
    assert_eq!(
        relocate(&log, &mut catalog, selected, &mut dirty, 0, &|| Ok(())).unwrap(),
        0
    );
    assert!(dirty.is_empty());
    assert_eq!(
        relocate(&log, &mut catalog, selected, &mut dirty, 17 * 1024, &|| Ok(
            ()
        ))
        .unwrap(),
        17 * 1024
    );
    for page in catalog.pages.values() {
        assert!(page
            .spans
            .iter()
            .any(|span| extent_key(&span.extent).unwrap() == original));
        assert!(page
            .spans
            .iter()
            .any(|span| extent_key(&span.extent).unwrap() != original));
    }
    verify(&log, &catalog, &expected);
    let mut total = 17 * 1024;
    for _ in 0..8 {
        let copied = relocate(&log, &mut catalog, selected, &mut dirty, 17 * 1024, &|| {
            Ok(())
        })
        .unwrap();
        total += copied;
        verify(&log, &catalog, &expected);
        if copied == 0 {
            break;
        }
    }
    assert_eq!(total, 128 * 1024);
    assert!(catalog
        .pages
        .values()
        .flat_map(|page| &page.spans)
        .all(|span| extent_key(&span.extent).unwrap() != original));
}

#[test]
fn compaction_refuses_damaged_source_before_rebinding_pages() {
    use std::os::unix::fs::FileExt;
    let dir = Dir::new();
    let log = dir.log();
    let bytes = vec![19; 8192];
    let extent = payload(&log, bytes.clone());
    let selected = extent.store_id.as_slice().try_into().unwrap();
    let key = extent_key(&extent).unwrap();
    let id = id(0);
    let page = state(bytes);
    let mut catalog = PageCatalog::default();
    catalog.pages.insert(
        id.clone(),
        CatalogPage {
            metadata: CatalogPageMetadata::from_state(&id, &page).unwrap(),
            spans: vec![PayloadSpan {
                offset: 0,
                length: 8192,
                extent: extent.clone(),
                extent_offset: 0,
            }],
        },
    );
    let path = dir.0.join("outcomes.segment-0000000000000000");
    let mut header = vec![0; 4096];
    std::fs::File::open(&path)
        .unwrap()
        .read_exact_at(&mut header, 0)
        .unwrap();
    assert!(header
        .windows(extent.store_id.len())
        .any(|part| part == extent.store_id));
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&[0x71], extent.offset + 3).unwrap();
    file.sync_data().unwrap();
    let mut dirty = BTreeSet::new();
    assert!(relocate(
        &log,
        &mut catalog,
        selected,
        &mut dirty,
        COPY_BYTES,
        &|| Ok(())
    )
    .is_err());
    assert!(dirty.is_empty());
    assert_eq!(
        extent_key(&catalog.pages[&id].spans[0].extent).unwrap(),
        key
    );
}

#[test]
fn compaction_checks_cancellation_before_writing_or_changing_the_catalog() {
    let dir = Dir::new();
    let log = dir.log();
    let extent = payload(&log, vec![11; 64]);
    let selected = extent.store_id.as_slice().try_into().unwrap();
    let id = id(0);
    let page = state(vec![11; 64]);
    let mut catalog = PageCatalog::default();
    catalog.pages.insert(
        id.clone(),
        CatalogPage {
            metadata: CatalogPageMetadata::from_state(&id, &page).unwrap(),
            spans: vec![PayloadSpan {
                offset: 0,
                length: 64,
                extent,
                extent_offset: 0,
            }],
        },
    );
    let before = log.logical_length().unwrap();
    let mut dirty = BTreeSet::new();
    let error = relocate(
        &log,
        &mut catalog,
        selected,
        &mut dirty,
        COPY_BYTES,
        &|| Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled")),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(dirty.is_empty());
    assert_eq!(log.logical_length().unwrap(), before);
}

#[test]
fn compaction_cancelled_after_copy_keeps_the_published_root_recoverable() {
    use super::super::{replica_checkpoint_catalog as catalog, replica_checkpoint_index as index};
    let dir = Dir::new();
    let log = dir.log();
    let bytes = vec![0x47; 1024 * 1024];
    let extent = payload(&log, bytes.clone());
    let selected = extent.store_id.as_slice().try_into().unwrap();
    let original = extent_key(&extent).unwrap();
    let page_id = id(0);
    let page_state = state(bytes);
    let mut pages = PageCatalog::default();
    pages.pages.insert(
        page_id.clone(),
        CatalogPage {
            metadata: CatalogPageMetadata::from_state(&page_id, &page_state).unwrap(),
            spans: vec![PayloadSpan {
                offset: 0,
                length: extent.length,
                extent,
                extent_offset: 0,
            }],
        },
    );
    let cut = log
        .metadata_cursor_after(None)
        .unwrap()
        .next_record()
        .unwrap()
        .unwrap()
        .position;
    let mut saved = None;
    let mut dirty = BTreeSet::new();
    let (commit, _) = index::prepare(
        &log,
        &pages,
        &mut saved,
        &mut dirty,
        b"scalars".to_vec(),
        Some(cut),
        1,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let before = log.logical_length().unwrap();
    let error = relocate(&log, &mut pages, selected, &mut dirty, COPY_BYTES, &|| {
        if log.logical_length()? > before {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "superseded after copy",
            ))
        } else {
            Ok(())
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(
        log.logical_length().unwrap() > before,
        "the copy must be durable before cancellation"
    );
    assert!(dirty.is_empty());
    assert_eq!(
        extent_key(&pages.pages[&page_id].spans[0].extent).unwrap(),
        original
    );
    drop(saved);
    drop(pages);
    drop(log);
    let log = dir.log();
    let loaded = catalog::latest(&log, 1).unwrap().unwrap();
    assert_eq!(
        loaded.commit, commit,
        "an unpublished copy replaced the committed root"
    );
    assert_eq!(
        loaded.catalog.pages[&page_id]
            .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
            .unwrap(),
        page_state
    );
}

#[test]
fn compaction_range_union_handles_aliases_adjacency_and_invalid_bounds() {
    let mut ranges = vec![(11, 19), (0, 4), (3, 9), (9, 11), (0, 4), (24, 31)];
    assert_eq!(normalize(&mut ranges).unwrap(), 26);
    assert_eq!(ranges, vec![(0, 19), (24, 31)]);
    assert!(normalize(&mut vec![(2, 1)]).is_err());
    assert!(normalize(&mut vec![(1, 1)]).is_err());
}

#[test]
fn compaction_cached_plan_matches_complete_scan_with_bounded_work() {
    use super::super::{replica_checkpoint_index as index, replica_checkpoint_recovery};
    use crate::outcome::PageOutcome;
    use std::cell::Cell;
    let dir = Dir::new();
    let log = dir.log();
    let mut managed = CheckpointState::new(replica_checkpoint_recovery::scalar_template()).unwrap();
    let mut cut = None;
    let initial = state(vec![0x31; 16]);
    for start in (0..4096u64).step_by(64) {
        let outcomes: Vec<_> = (start..start + 64)
            .map(|n| PageOutcome::between(&id(n), None, &initial).unwrap())
            .collect();
        let decisions: Vec<_> = outcomes.iter().map(DecidedMutation::Page).collect();
        let record = log.append_decided_batch(&decisions).unwrap();
        managed.catalog.fold(&record).unwrap();
        cut = Some(record.position);
    }
    let metadata = managed.metadata_bytes().unwrap();
    let (_, mut cut) = index::prepare(
        &log,
        &managed.catalog,
        &mut managed.index,
        &mut managed.dirty_pages,
        metadata,
        cut,
        1,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let mut filler = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![29; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x61; CheckpointFragment::MAX_DATA_BYTES],
    };
    let maximum = crate::durable_payload::outcome_segments::MIN_SEGMENT_BYTES
        / CheckpointFragment::MAX_DATA_BYTES as u64
        + 2;
    for _ in 0..maximum {
        if !log
            .compaction_candidates(&cut, None, SAMPLE_SEGMENTS)
            .unwrap()
            .is_empty()
        {
            break;
        }
        cut = log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&filler)])
            .unwrap()
            .position;
        filler.ordinal += 1;
    }
    assert!(!log
        .compaction_candidates(&cut, None, SAMPLE_SEGMENTS)
        .unwrap()
        .is_empty());
    let cursor = managed.compaction_cursor;
    let cached_checks = Cell::new(0usize);
    let cached = plan_with_cache(&log, &mut managed, &cut, 1, true, &|| {
        cached_checks.set(cached_checks.get() + 1);
        Ok(())
    })
    .unwrap();
    managed.compaction_cursor = cursor;
    let scanned_checks = Cell::new(0usize);
    let scanned = plan_with_cache(&log, &mut managed, &cut, 1, false, &|| {
        scanned_checks.set(scanned_checks.get() + 1);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        cached.selected.as_ref().map(|value| value.store_id),
        scanned.selected.as_ref().map(|value| value.store_id)
    );
    assert_eq!(cached.unreferenced, scanned.unreferenced);
    assert!(
        cached_checks.get() <= SAMPLE_SEGMENTS + 2,
        "planner visited {} items",
        cached_checks.get()
    );
    assert!(scanned_checks.get() > 4096);
    let before = log.logical_length().unwrap();
    assert!(plan(&log, &mut managed, &cut, 1, &|| Err(io::Error::new(
        io::ErrorKind::Interrupted,
        "cancelled"
    )))
    .is_err());
    assert_eq!(log.logical_length().unwrap(), before);
}

#[test]
fn compaction_indexed_owners_bound_partial_copy_work_and_release_the_source_after_restart() {
    use super::super::{
        replica_checkpoint_catalog as catalog, replica_checkpoint_index as index,
        replica_checkpoint_recovery,
    };
    use std::cell::Cell;
    let dir = Dir::new();
    let log = dir.log();
    let bytes: Vec<_> = (0..128 * 1024).map(|n| (n % 251 + 1) as u8).collect();
    let extent = payload(&log, bytes.clone());
    let old_key = extent_key(&extent).unwrap();
    let selected_id = old_key.0;
    // Seal the real source file before creating unrelated page payloads.
    let mut filler = CheckpointFragment {
        version: 1,
        checkpoint_id: vec![29; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal: 0,
        data: vec![0x61; CheckpointFragment::MAX_DATA_BYTES],
    };
    let mut cut;
    loop {
        let record = log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&filler)])
            .unwrap();
        cut = record.position;
        if cut.store_id != selected_id {
            break;
        }
        filler.ordinal += 1;
        assert!(filler.ordinal < 32, "source file did not seal");
    }
    let unrelated = payload(&log, vec![0x22; 128]);
    assert_ne!(extent_key(&unrelated).unwrap().0, selected_id);
    let selected_keys: Vec<_> = [0, 2048, 4095].into_iter().map(id).collect();
    let mut managed = CheckpointState::new(replica_checkpoint_recovery::scalar_template()).unwrap();
    let mut expected = BTreeMap::new();
    for n in 0..4096 {
        let key = id(n);
        let selected = selected_keys.contains(&key);
        let page = state(if selected {
            bytes.clone()
        } else {
            vec![0x22; 128]
        });
        managed.catalog.pages.insert(
            key.clone(),
            CatalogPage {
                metadata: CatalogPageMetadata::from_state(&key, &page).unwrap(),
                spans: vec![PayloadSpan {
                    offset: 0,
                    length: page.logical_length,
                    extent: if selected {
                        extent.clone()
                    } else {
                        unrelated.clone()
                    },
                    extent_offset: 0,
                }],
            },
        );
        expected.insert(key, page);
    }
    drop(extent);
    drop(unrelated);
    let metadata = managed.metadata_bytes().unwrap();
    let (_, published) = index::prepare(
        &log,
        &managed.catalog,
        &mut managed.index,
        &mut managed.dirty_pages,
        metadata,
        Some(cut.clone()),
        1,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let owners = managed
        .index
        .as_ref()
        .unwrap()
        .planned_payload_pages(&selected_id, 4096)
        .unwrap();
    assert_eq!(owners, selected_keys);
    let count = Cell::new(0usize);
    let mut scanned = PageCatalog {
        pages: managed.catalog.pages.clone(),
    };
    let mut scanned_dirty = BTreeSet::new();
    let (scanned_bytes, scanned_moves) = relocate_with_movements(
        &log,
        &mut scanned,
        selected_id,
        &mut scanned_dirty,
        17 * 1024,
        &|| {
            count.set(count.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert!(count.get() >= 8192);
    count.set(0);
    let (copied, movements) = relocate_pages_with_movements(
        &log,
        &mut managed.catalog,
        selected_id,
        &mut managed.dirty_pages,
        Some(&owners),
        17 * 1024,
        &|| {
            count.set(count.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(copied, 17 * 1024);
    assert_eq!(copied, scanned_bytes);
    assert!(count.get() < 32, "visited unrelated pages: {}", count.get());
    assert_eq!(managed.dirty_pages, scanned_dirty);
    assert_eq!(
        movements.keys().collect::<Vec<_>>(),
        scanned_moves.keys().collect::<Vec<_>>()
    );
    for (key, movement) in &movements {
        assert_eq!(movement.source, scanned_moves[key].source);
        assert_eq!(
            movement
                .ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>(),
            scanned_moves[key]
                .ranges
                .iter()
                .map(|r| (r.start, r.end))
                .collect::<Vec<_>>()
        );
    }
    verify(&log, &managed.catalog, &expected);
    verify(&log, &scanned, &expected);
    assert!(managed.catalog.pages[&id(0)]
        .spans
        .iter()
        .any(|s| extent_key(&s.extent).unwrap() == old_key));
    // Oracle snapshots must not keep descriptor owners alive during pruning.
    drop(scanned);
    drop(scanned_moves);
    drop(movements);
    let before = log.logical_length().unwrap();
    let error = relocate_pages_with_movements(
        &log,
        &mut managed.catalog,
        selected_id,
        &mut managed.dirty_pages,
        Some(&owners),
        COPY_BYTES,
        &|| Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled")),
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(log.logical_length().unwrap(), before);
    assert!(relocate_pages_with_movements(
        &log,
        &mut managed.catalog,
        selected_id,
        &mut managed.dirty_pages,
        Some(&[id(9999)]),
        COPY_BYTES,
        &|| Ok(())
    )
    .is_err());
    assert_eq!(log.logical_length().unwrap(), before);
    // A dirty owner added after planning must join the same relocation pass.
    let key = id(5000);
    let page = expected[&id(0)].clone();
    let mut added = managed.catalog.pages[&id(0)].clone();
    added.metadata = CatalogPageMetadata::from_state(&key, &page).unwrap();
    managed.catalog.pages.insert(key.clone(), added);
    managed.dirty_pages.insert(key.clone());
    expected.insert(key, page);
    let selected = log
        .compaction_candidates(&published, None, SAMPLE_SEGMENTS)
        .unwrap()
        .into_iter()
        .find(|segment| segment.store_id == selected_id)
        .unwrap();
    let error = managed
        .index
        .as_mut()
        .unwrap()
        .evacuate_metadata(
            &log,
            std::slice::from_ref(&selected),
            &mut managed.dirty_pages,
            &|| Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled")),
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    count.set(0);
    let moved = apply(
        &log,
        &mut managed,
        Plan {
            selected: Some(selected),
            unreferenced: 0,
        },
        &|| {
            count.set(count.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert!(
        count.get() < 64,
        "payload-only compaction visited {} items",
        count.get()
    );
    assert_eq!(moved.keys().copied().collect::<Vec<_>>(), vec![old_key]);
    verify(&log, &managed.catalog, &expected);
    assert!(managed
        .catalog
        .pages
        .values()
        .flat_map(|p| &p.spans)
        .all(|s| extent_key(&s.extent).unwrap() != old_key));
    let metadata = managed.metadata_bytes().unwrap();
    let (commit, position) = index::prepare(
        &log,
        &managed.catalog,
        &mut managed.index,
        &mut managed.dirty_pages,
        metadata,
        Some(published),
        2,
        false,
        &|| Ok(()),
    )
    .unwrap()
    .publish_record(&log)
    .unwrap();
    let current = managed.index.as_ref().unwrap();
    assert!(current
        .planned_payload_pages(&selected_id, 4097)
        .unwrap()
        .is_empty());
    let keep = current
        .protected_segments_indexed(
            &log,
            &managed.catalog,
            &commit,
            &position,
            &managed.dirty_pages,
            &|| Ok(()),
        )
        .unwrap();
    assert!(
        !keep.contains(selected_id.as_slice()),
        "copied descriptors pinned the source"
    );
    let retired = log
        .prepare_retention(&keep, commit.through.as_ref().unwrap(), &|| Ok(()))
        .unwrap()
        .unwrap()
        .commit()
        .unwrap();
    assert!(retired.finish().unwrap().removed > 0);
    drop(managed);
    drop(log);
    let log = dir.log();
    let loaded = catalog::latest(&log, 4097).unwrap().unwrap();
    verify(&log, &loaded.catalog, &expected);
    assert!(loaded
        .index
        .as_ref()
        .unwrap()
        .planned_payload_pages(&selected_id, 4097)
        .unwrap()
        .is_empty());
}

#[path = "replica_checkpoint_payload_budget_tests.rs"]
mod payload_budget_tests;
