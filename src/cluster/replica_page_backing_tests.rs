// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::super::replica_checkpoint_recovery::{capture_scalars, encode_scalars, scalar_template};
use super::*;
use crate::durable_payload::{
    outcome_log::{CheckpointFragment, DecidedMutation},
    outcome_segments::MIN_SEGMENT_BYTES,
};
use crate::{outcome::PageOutcome, ChunkId, MatrixReplicaService};
use prost::Message;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replica-page-backing-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> Arc<SegmentedOutcomeLog> {
        Arc::new(SegmentedOutcomeLog::open(self.0.join("journal"), MIN_SEGMENT_BYTES).unwrap())
    }
}
impl Drop for Directory {
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
fn service() -> MatrixReplicaService {
    let mut service = MatrixReplicaService::new();
    service.set_keep_ec_sidecar(false);
    service
}
fn append(
    log: &SegmentedOutcomeLog,
    service: &mut MatrixReplicaService,
    id: PageId,
    offset: u64,
    data: &[u8],
) -> StoredRecordMetadata {
    let before = service.read_chunk_state(&id).ok();
    let counters = PageOutcome::counters(service);
    service.write_chunk(id.clone(), offset, data).unwrap();
    let after = service.read_chunk_state(&id).unwrap();
    let outcome = PageOutcome::between(&id, before.as_ref(), &after)
        .unwrap()
        .with_counters(counters, PageOutcome::counters(service));
    let metadata = encode_scalars(capture_scalars(service, &scalar_template())).unwrap();
    log.append_decided_batch_with_metadata(&[DecidedMutation::Page(&outcome)], &metadata)
        .unwrap()
}
fn fragment(log: &SegmentedOutcomeLog, ordinal: u64, data: Vec<u8>) -> StoredRecordMetadata {
    log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&CheckpointFragment {
        version: 1,
        checkpoint_id: vec![9; 16],
        kind: CheckpointFragment::PAYLOAD,
        ordinal,
        data,
    })])
    .unwrap()
}

#[test]
fn live_backing_reads_exact_ranges_with_only_metadata_and_owned_file_handles() {
    let dir = Directory::new();
    let log = dir.open();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut service = service();
    for n in 0..32 {
        backing
            .fold(&append(
                &log,
                &mut service,
                id(n),
                0,
                &vec![n as u8; 16 * 1024],
            ))
            .unwrap();
    }
    drop(service);
    assert_eq!(backing.totals().unwrap(), (32, 32 * 16 * 1024));
    let holes = backing.capture_page(&id(0)).unwrap().unwrap();
    assert!(holes.readers.is_empty());
    assert_eq!(holes.read().unwrap(), vec![0; 16 * 1024]);
    let plan = backing.capture_range(&id(17), 5, 7).unwrap().unwrap();
    assert_eq!(plan.page.spans.len(), 1);
    assert_eq!(plan.readers.len(), 1);
    assert_eq!(plan.metadata().write_sequence, 1);
    assert_eq!(plan.read().unwrap(), vec![17; 7]);
    assert!(plan.materialize().is_err());
    assert!(!log.retention_readers_active());
    let empty = backing
        .capture_range(&id(17), 16 * 1024, 0)
        .unwrap()
        .unwrap();
    assert!(empty.readers.is_empty());
    assert!(empty.read().unwrap().is_empty());
    assert!(backing.capture_range(&id(17), 16 * 1024, 1).is_err());
    assert!(backing.capture_range(&id(17), u64::MAX, 1).is_err());
    assert!(backing.capture_page(&id(99)).unwrap().is_none());
    let whole = backing.capture_page(&id(17)).unwrap().unwrap();
    drop(backing);
    drop(log);
    assert_eq!(
        whole.materialize().unwrap().data.as_slice(),
        &[17; 16 * 1024]
    );
}

#[test]
fn live_backing_rebases_untouched_ranges_in_a_newer_partial_mutation() {
    let dir = Directory::new();
    let log = dir.open();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut service = service();
    let original: Vec<u8> = (0..64).collect();
    let first = append(&log, &mut service, id(0), 0, &original);
    let source = Arc::new(first.batch.payloads[0].clone());
    backing.fold(&first).unwrap();
    let older = backing.capture_page(&id(0)).unwrap().unwrap();
    let update = append(&log, &mut service, id(0), 24, &[77; 16]);
    let patch = Arc::new(update.batch.payloads[0].clone());
    backing.fold(&update).unwrap();
    // Use a real rollover so the reverse ownership index must change stores.
    let mut rolled = false;
    for ordinal in 0..32 {
        let record = fragment(&log, ordinal, vec![0; 1024 * 1024]);
        if record.batch.payloads[0].store_id != source.store_id {
            rolled = true;
            break;
        }
    }
    assert!(rolled);
    let copied = fragment(
        &log,
        64,
        log.read_payload_range(&source, 0, source.length).unwrap(),
    );
    let destination = Arc::new(copied.batch.payloads[0].clone());
    let moved = BTreeMap::from([(
        extent_key(&source).unwrap(),
        PayloadRelocation {
            source: source.as_ref().clone(),
            ranges: vec![PayloadMovement {
                start: 0,
                end: source.length,
                extent: destination.clone(),
                offset: 0,
            }],
        },
    )]);
    let destination_owners = Arc::strong_count(&destination);
    assert_eq!(backing.rebase(&moved).unwrap(), 1);
    assert_eq!(
        Arc::strong_count(&destination),
        destination_owners,
        "backing must not retain a checkpoint worker's destination handle"
    );
    let expected = service.read_chunk_state(&id(0)).unwrap();
    let current = backing.capture_page(&id(0)).unwrap().unwrap();
    assert_eq!(current.metadata().write_sequence, 2);
    assert_eq!(current.materialize().unwrap(), expected);
    assert_eq!(older.read().unwrap(), original);
    assert!(
        backing
            .protected_segments()
            .unwrap()
            .contains(&source.store_id),
        "the newer patch still owns the old segment"
    );
    let copied_patch = fragment(&log, 65, vec![77; 16]);
    let patch_destination = Arc::new(copied_patch.batch.payloads[0].clone());
    assert_eq!(
        backing
            .rebase(&BTreeMap::from([(
                extent_key(&patch).unwrap(),
                PayloadRelocation {
                    source: patch.as_ref().clone(),
                    ranges: vec![PayloadMovement {
                        start: 0,
                        end: 16,
                        extent: patch_destination,
                        offset: 0
                    }],
                }
            )]))
            .unwrap(),
        1
    );
    assert!(!backing
        .protected_segments()
        .unwrap()
        .contains(&source.store_id));
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap(),
        expected
    );
    assert_eq!(backing.totals().unwrap(), (1, 64));
    assert!(!log.retention_readers_active());
}

#[test]
fn live_backing_fences_reads_after_invalid_foreground_metadata() {
    let dir = Directory::new();
    let log = dir.open();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut service = service();
    backing
        .fold(&append(&log, &mut service, id(0), 0, &[1; 64]))
        .unwrap();
    let mut invalid = append(&log, &mut service, id(0), 8, &[2; 8]);
    invalid.batch.decisions[0]
        .page
        .as_mut()
        .unwrap()
        .before_checksum = "invalid".into();
    assert!(backing.fold(&invalid).is_err());
    assert!(backing.metadata(&id(0)).is_err());
    assert!(backing.capture_page(&id(0)).is_err());
    assert!(backing.totals().is_err());
}

#[test]
fn live_backing_refuses_a_changed_descriptor_for_the_same_relocation_key() {
    let dir = Directory::new();
    let log = dir.open();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut service = service();
    let record = append(&log, &mut service, id(0), 0, &[1; 64]);
    let source = &record.batch.payloads[0];
    backing.fold(&record).unwrap();
    let copied = fragment(&log, 0, vec![1; 64]);
    let mut changed = source.clone();
    changed.length += 1;
    let error = backing
        .rebase(&BTreeMap::from([(
            extent_key(source).unwrap(),
            PayloadRelocation {
                source: changed,
                ranges: vec![PayloadMovement {
                    start: 0,
                    end: 64,
                    extent: Arc::new(copied.batch.payloads[0].clone()),
                    offset: 0,
                }],
            },
        )]))
        .unwrap_err();
    assert!(error.to_string().contains("descriptor differs"));
    assert!(backing.capture_page(&id(0)).is_err());
}

#[test]
fn live_backing_keeps_checkpoint_descriptor_lifetimes_separate_and_interns_aliases() {
    let dir = Directory::new();
    let log = dir.open();
    let mut service = service();
    let record = append(&log, &mut service, id(0), 0, &[1; 64]);
    let mut worker = PageCatalog::default();
    worker.fold(&record).unwrap();
    let state = service.read_chunk_state(&id(0)).unwrap();
    worker.pages.insert(
        id(1),
        CatalogPage {
            metadata: CatalogPageMetadata::from_state(&id(1), &state).unwrap(),
            spans: worker.pages[&id(0)].spans.clone(),
        },
    );
    let descriptor = Arc::downgrade(&worker.pages[&id(0)].spans[0].extent);
    let snapshot = PageCatalog {
        pages: worker
            .pages
            .iter()
            .map(|(id, page)| (id.clone(), page.clone()))
            .collect(),
    };
    let backing = LivePageBacking::new(log, snapshot).unwrap();
    drop(worker);
    assert!(
        descriptor.upgrade().is_none(),
        "backing must not pin a descriptor that the checkpoint worker can prune"
    );
    let first = backing.capture_page(&id(0)).unwrap().unwrap();
    let alias = backing.capture_page(&id(1)).unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &first.page.spans[0].extent,
        &alias.page.spans[0].extent
    ));
    assert_eq!(first.read().unwrap(), vec![1; 64]);
    assert_eq!(alias.read().unwrap(), vec![1; 64]);
}

#[test]
fn live_backing_refuses_mismatched_page_keys_on_open() {
    let dir = Directory::new();
    let log = dir.open();
    let mut service = service();
    let record = append(&log, &mut service, id(0), 0, &[1; 64]);
    let mut catalog = PageCatalog::default();
    catalog.fold(&record).unwrap();
    let page = catalog.pages.remove(&id(0)).unwrap();
    catalog.pages.insert(id(99), page);
    let error = LivePageBacking::new(log, catalog).err().unwrap();
    assert!(error.to_string().contains("identity differs"));
}

fn append_group(
    log: &SegmentedOutcomeLog,
    service: &mut MatrixReplicaService,
    writes: &[(PageId, u64, Vec<u8>)],
) -> StoredRecordMetadata {
    let mut outcomes = Vec::new();
    for (page, offset, data) in writes {
        let before = service.read_chunk_state(page).ok();
        let counters = PageOutcome::counters(service);
        service.write_chunk(page.clone(), *offset, data).unwrap();
        let after = service.read_chunk_state(page).unwrap();
        outcomes.push(
            PageOutcome::between(page, before.as_ref(), &after)
                .unwrap()
                .with_counters(counters, PageOutcome::counters(service)),
        );
    }
    let metadata = encode_scalars(capture_scalars(service, &scalar_template())).unwrap();
    let mutations: Vec<_> = outcomes.iter().map(DecidedMutation::Page).collect();
    log.append_decided_batch_with_metadata(&mutations, &metadata)
        .unwrap()
}

#[test]
fn append_span_deltas_reuse_worker_and_backing_prefix_allocations() {
    let dir = Directory::new();
    let log = dir.open();
    let mut live = service();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut catalog = PageCatalog::default();
    for n in 0..48 {
        let record = append(&log, &mut live, id(0), n * 8, &[n as u8 + 1; 8]);
        catalog.fold(&record).unwrap();
        backing.fold(&record).unwrap();
    }
    assert_eq!(catalog.pages[&id(0)].spans.len(), 48);
    let worker_pointer = {
        let page = catalog.pages.get_mut(&id(0)).unwrap();
        page.spans.reserve(64);
        page.spans.as_ptr() as usize
    };
    let backing_pointer = {
        let mut state = backing.lock().unwrap();
        let page = state.catalog.pages.get_mut(&id(0)).unwrap();
        page.spans.reserve(64);
        page.spans.as_ptr() as usize
    };
    let older = backing.capture_page(&id(0)).unwrap().unwrap();
    let record = append(&log, &mut live, id(0), 48 * 8 + 32, b"tail");
    catalog.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    assert_eq!(
        catalog.pages[&id(0)].spans.as_ptr() as usize,
        worker_pointer
    );
    {
        let state = backing.lock().unwrap();
        let page = &state.catalog.pages[&id(0)];
        assert_eq!(page.spans.as_ptr() as usize, backing_pointer);
        assert_eq!(page.spans.len(), 49);
        assert!(!Arc::ptr_eq(
            &page.spans[0].extent,
            &catalog.pages[&id(0)].spans[0].extent
        ));
        let sources = segments(page).unwrap();
        assert_eq!(sources, state.by_segment.keys().cloned().collect());
    }
    assert_eq!(backing.totals().unwrap(), (1, 420));
    assert_eq!(
        backing
            .capture_range(&id(0), 384, 36)
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        [vec![0; 32], b"tail".to_vec()].concat()
    );
    assert_eq!(older.read().unwrap().len(), 384);
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap(),
        live.read_chunk_state(&id(0)).unwrap()
    );
}

#[test]
fn append_span_deltas_coalesce_same_record_spans_and_count_repeated_pages_once() {
    let dir = Directory::new();
    let log = dir.open();
    let mut live = service();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut catalog = PageCatalog::default();
    for (page, bytes) in [(id(0), &b"prefix"[..]), (id(1), &b"other"[..])] {
        let record = append(&log, &mut live, page, 0, bytes);
        catalog.fold(&record).unwrap();
        backing.fold(&record).unwrap();
    }
    let pointer = {
        let mut state = backing.lock().unwrap();
        let page = state.catalog.pages.get_mut(&id(0)).unwrap();
        page.spans.reserve(16);
        page.spans.as_ptr() as usize
    };
    let record = append_group(
        &log,
        &mut live,
        &[(id(0), 6, b"A".to_vec()), (id(0), 7, b"BC".to_vec())],
    );
    catalog.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    assert_eq!(catalog.pages[&id(0)].spans.len(), 2);
    {
        let state = backing.lock().unwrap();
        let page = &state.catalog.pages[&id(0)];
        assert_eq!(page.spans.len(), 2);
        assert_eq!(page.spans[1].length, 3);
        assert_eq!(page.spans.as_ptr() as usize, pointer);
    }
    assert_eq!(backing.totals().unwrap(), (2, 14));
    let record = append_group(
        &log,
        &mut live,
        &[(id(0), 9, b"D".to_vec()), (id(1), 5, b"E".to_vec())],
    );
    catalog.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    assert_eq!(backing.totals().unwrap(), (2, 16));
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"prefixABCD"
    );
    assert_eq!(
        backing
            .capture_page(&id(1))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"otherE"
    );
    // A mixed append/overwrite batch uses the ordinary replacement path.
    let record = append_group(
        &log,
        &mut live,
        &[(id(0), 10, b"F".to_vec()), (id(0), 2, b"X".to_vec())],
    );
    catalog.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"prXfixABCDF"
    );
    assert_eq!(backing.totals().unwrap(), (2, 17));
    for page in [id(0), id(1)] {
        assert_eq!(
            backing
                .capture_page(&page)
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap(),
            live.read_chunk_state(&page).unwrap()
        );
    }
}

#[test]
fn append_span_deltas_fence_a_partly_folded_invalid_batch_until_recovery() {
    let dir = Directory::new();
    let log = dir.open();
    let mut live = service();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    backing
        .fold(&append(&log, &mut live, id(0), 0, b"base"))
        .unwrap();
    let older = backing.capture_page(&id(0)).unwrap().unwrap();
    let mut record = append_group(
        &log,
        &mut live,
        &[(id(0), 4, b"A".to_vec()), (id(0), 5, b"B".to_vec())],
    );
    record.batch.decisions[1]
        .page
        .as_mut()
        .unwrap()
        .before_checksum = "changed".into();
    assert!(backing.fold(&record).is_err());
    assert!(backing.totals().is_err());
    assert!(backing.metadata(&id(0)).is_err());
    assert!(backing.capture_page(&id(0)).is_err());
    assert!(backing.protected_segments().is_err());
    assert_eq!(older.read().unwrap(), b"base");
    let mut catalog = PageCatalog::default();
    let mut cursor = log.metadata_cursor_after(None).unwrap();
    while let Some(record) = cursor.next_record().unwrap() {
        catalog.fold(&record).unwrap();
    }
    drop(cursor);
    let recovered = LivePageBacking::new(log, catalog).unwrap();
    assert_eq!(
        recovered
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"baseAB"
    );
    assert_eq!(recovered.totals().unwrap(), (1, 6));
}

#[test]
fn append_span_deltas_keep_full_validation_for_truncation_and_suffix_bounds() {
    let dir = Directory::new();
    let log = dir.open();
    let mut live = service();
    let mut catalog = PageCatalog::default();
    let record = append(&log, &mut live, id(0), 0, b"abcdefgh");
    catalog.fold(&record).unwrap();
    let page = catalog.pages.get_mut(&id(0)).unwrap();
    assert!(page.validate_span_suffix(page.spans.len() + 1).is_err());
    page.metadata.data_length = 4;
    assert!(page.validate_span_suffix(page.spans.len()).is_err());
    assert!(page.validate_spans().is_err());
    page.metadata.data_length = 8;
    let record = append(&log, &mut live, id(0), 8, b"tail");
    catalog.fold(&record).unwrap();
    catalog.pages[&id(0)].validate_spans().unwrap();
    assert_eq!(
        catalog.pages[&id(0)]
            .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
            .unwrap(),
        live.read_chunk_state(&id(0)).unwrap()
    );
    let backing = LivePageBacking::new(
        log.clone(),
        PageCatalog {
            pages: catalog.pages.clone(),
        },
    )
    .unwrap();
    let before = live.read_chunk_state(&id(0)).unwrap();
    let counters = PageOutcome::counters(&live);
    let mut shortened = before.clone();
    shortened.data = Arc::new(before.data[..4].to_vec());
    shortened.checksum_fnv1a64 = crate::fnv1a64(&shortened.data);
    shortened.write_sequence += 1;
    shortened.logical_length = 4;
    shortened.discarded_ranges.clear();
    live.install_chunk_state(id(0), shortened.clone()).unwrap();
    let outcome = PageOutcome::between(&id(0), Some(&before), &shortened)
        .unwrap()
        .with_counters(counters, PageOutcome::counters(&live));
    let metadata = encode_scalars(capture_scalars(&live, &scalar_template())).unwrap();
    let record = log
        .append_decided_batch_with_metadata(&[DecidedMutation::Page(&outcome)], &metadata)
        .unwrap();
    catalog.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    assert_eq!(backing.totals().unwrap(), (1, 4));
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap(),
        shortened
    );
    // Appending after truncation preserves the shortened physical spans.
    let record = append(&log, &mut live, id(0), 4, b"xy");
    catalog.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    assert_eq!(backing.totals().unwrap(), (1, 6));
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .read()
            .unwrap(),
        b"abcdxy"
    );
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap(),
        live.read_chunk_state(&id(0)).unwrap()
    );
}

#[test]
fn single_span_catalog_pages_share_packed_sources_with_one_retained_slot() {
    let dir = Directory::new();
    let log = dir.open();
    let mut live = service();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut worker = PageCatalog::default();
    let writes: Vec<_> = (0..64u64)
        .map(|n| (id(n), 0, vec![n as u8 + 1; 128]))
        .collect();
    let record = append_group(&log, &mut live, &writes);
    worker.fold(&record).unwrap();
    backing.fold(&record).unwrap();
    for page in worker.pages.values() {
        assert_eq!(page.spans.len(), 1);
        assert_eq!(page.spans.capacity(), 1);
    }
    {
        let state = backing.state.lock().unwrap();
        for page in state.catalog.pages.values() {
            assert_eq!(page.spans.len(), 1);
            assert_eq!(page.spans.capacity(), 1);
        }
    }
    for (id, _, expected) in &writes {
        assert_eq!(
            backing.capture_page(id).unwrap().unwrap().read().unwrap(),
            *expected
        );
    }
    let mut recovered = PageCatalog::default();
    let mut cursor = log.metadata_cursor_after(None).unwrap();
    while let Some(record) = cursor.next_record().unwrap() {
        recovered.fold(&record).unwrap();
    }
    assert_eq!(recovered.pages.len(), 64);
    for page in recovered.pages.values() {
        assert_eq!(page.spans.len(), 1);
        assert_eq!(page.spans.capacity(), 1);
    }
}

#[test]
fn live_backing_relocation_scans_shared_source_segment_once() {
    let dir = Directory::new();
    let log = dir.open();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut service = service();
    let mut sources = Vec::new();
    for n in 0..128 {
        let record = append(&log, &mut service, id(n), 0, &[(n + 1) as u8; 64]);
        sources.push(record.batch.payloads[0].clone());
        backing.fold(&record).unwrap();
    }
    let source_id = sources[0].store_id.clone();
    assert!(sources.iter().all(|source| source.store_id == source_id));
    let older = backing.capture_page(&id(127)).unwrap().unwrap();
    // Include a newer partial mutation that must survive relocation unchanged.
    backing
        .fold(&append(&log, &mut service, id(63), 16, &[211; 16]))
        .unwrap();
    let mut moved = BTreeMap::new();
    for (n, source) in sources.iter().enumerate() {
        let copied = fragment(&log, n as u64, vec![(n + 1) as u8; 64]);
        moved.insert(
            extent_key(source).unwrap(),
            PayloadRelocation {
                source: source.clone(),
                ranges: vec![PayloadMovement {
                    start: 0,
                    end: 64,
                    extent: Arc::new(copied.batch.payloads[0].clone()),
                    offset: 0,
                }],
            },
        );
    }
    assert_eq!(relocation_segments(&moved).count(), 1);
    assert_eq!(backing.rebase(&moved).unwrap(), 128);
    assert_eq!(backing.totals().unwrap(), (128, 128 * 64));
    for n in 0..128 {
        assert_eq!(
            backing
                .capture_page(&id(n))
                .unwrap()
                .unwrap()
                .materialize()
                .unwrap(),
            service.read_chunk_state(&id(n)).unwrap()
        );
    }
    assert_eq!(older.read().unwrap(), vec![128; 64]);
    // Every movement is validated, including later keys in the same segment.
    moved.last_entry().unwrap().get_mut().ranges[0].end = 65;
    assert!(backing
        .rebase(&moved)
        .unwrap_err()
        .to_string()
        .contains("bounds"));
    assert!(backing.capture_page(&id(0)).is_err());
    assert!(backing.protected_segments().is_err());
}

#[test]
fn retained_catalog_headers_and_read_plans_survive_append_overwrite_and_discard() {
    let dir = Directory::new();
    let log = dir.open();
    let backing = LivePageBacking::new(log.clone(), PageCatalog::default()).unwrap();
    let mut service = service();
    backing
        .fold(&append(&log, &mut service, id(0), 0, b"abcdefghijkl"))
        .unwrap();
    let discard = |service: &mut MatrixReplicaService, offset, length| {
        let before = service.read_chunk_state(&id(0)).unwrap();
        let counters = PageOutcome::counters(service);
        service.discard_chunk(&id(0), offset, length).unwrap();
        let after = service.read_chunk_state(&id(0)).unwrap();
        let outcome = PageOutcome::between(&id(0), Some(&before), &after)
            .unwrap()
            .with_counters(counters, PageOutcome::counters(service));
        let metadata = encode_scalars(capture_scalars(service, &scalar_template())).unwrap();
        log.append_decided_batch_with_metadata(&[DecidedMutation::Page(&outcome)], &metadata)
            .unwrap()
    };
    backing.fold(&discard(&mut service, 2, 2)).unwrap();
    let before = service.read_chunk_state(&id(0)).unwrap();
    let retained = backing.metadata(&id(0)).unwrap().unwrap();
    let snapshot = backing.catalog_snapshot().unwrap();
    let plan = backing.capture_page(&id(0)).unwrap().unwrap();
    let encoded = retained.encode_to_vec();
    assert_eq!(retained.write_sequence, 2);
    assert_eq!(retained.discarded_ranges.len(), 1);
    backing
        .fold(&append(&log, &mut service, id(0), 12, b"tail"))
        .unwrap();
    backing
        .fold(&append(&log, &mut service, id(0), 0, b"XY"))
        .unwrap();
    backing.fold(&discard(&mut service, 4, 2)).unwrap();
    let current = backing.metadata(&id(0)).unwrap().unwrap();
    assert_eq!(current.write_sequence, 5);
    assert_eq!(current.data_length, 16);
    assert_ne!(current.state_checksum, retained.state_checksum);
    assert_eq!(retained.encode_to_vec(), encoded);
    assert_eq!(snapshot.pages[&id(0)].metadata.encode_to_vec(), encoded);
    assert_eq!(plan.materialize().unwrap(), before);
    assert_eq!(
        backing
            .capture_page(&id(0))
            .unwrap()
            .unwrap()
            .materialize()
            .unwrap(),
        service.read_chunk_state(&id(0)).unwrap()
    );
}
