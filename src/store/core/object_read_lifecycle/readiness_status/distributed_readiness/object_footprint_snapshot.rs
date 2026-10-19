#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DistributedObjectFootprintSnapshot {
    live_object_count: u64,
    deleted_object_count: u64,
    page_count: u64,
    live_bytes: u64,
    stale_bytes: u64,
    pending_discard_bytes: u64,
    discarded_bytes: u64,
    physical_bytes: u64,
    live_bytes_ppm: u64,
    stale_bytes_ppm: u64,
    pending_discard_bytes_ppm: u64,
    discarded_bytes_ppm: u64,
    logical_write_amplification_ppm: u64,
    space_amplification_ppm: u64,
    discard_backlog_ppm: u64,
    discard_candidate_bytes: u64,
    discard_candidate_slabs: u64,
    pending_discard_range_count: u64,
    merged_discard_range_count: u64,
    fragmented_range_count: u64,
    compaction_candidate_slabs: u64,
    compaction_candidate_bytes: u64,
    compaction_candidate_range_count: u64,
    compaction_recommended: bool,
}

impl DistributedObjectFootprintSnapshot {
    fn capture(store: &MatrixObjectStore, page: &ChunkStorageReadiness) -> Self {
        let live_object_count = store
            .objects
            .values()
            .filter(|object| !object.deleted)
            .count() as u64;
        let deleted_object_count = store
            .objects
            .values()
            .filter(|object| object.deleted)
            .count() as u64;
        let live_bytes = store
            .objects
            .values()
            .filter(|object| !object.deleted)
            .fold(0u64, |total, object| total.saturating_add(object.length));
        let gc = store.gc_report();
        let pending_discard_bytes = gc.stale_bytes.saturating_sub(gc.discarded_bytes);
        let compaction_candidate_slabs = gc.compaction_candidate_count;

        Self {
            live_object_count,
            deleted_object_count,
            page_count: page
                .replica_readiness
                .iter()
                .map(|replica| replica.page_count)
                .max()
                .unwrap_or(0),
            live_bytes,
            stale_bytes: gc.stale_bytes,
            pending_discard_bytes,
            discarded_bytes: gc.discarded_bytes,
            physical_bytes: page.total_physical_bytes,
            live_bytes_ppm: ratio_ppm(live_bytes, gc.logical_bytes),
            stale_bytes_ppm: ratio_ppm(gc.stale_bytes, gc.logical_bytes),
            pending_discard_bytes_ppm: ratio_ppm(pending_discard_bytes, gc.logical_bytes),
            discarded_bytes_ppm: ratio_ppm(gc.discarded_bytes, gc.logical_bytes),
            logical_write_amplification_ppm: gc.logical_write_amplification_ppm,
            space_amplification_ppm: gc.space_amplification_ppm,
            discard_backlog_ppm: ratio_ppm(pending_discard_bytes, gc.stale_bytes),
            discard_candidate_bytes: gc.discard_candidate_bytes,
            discard_candidate_slabs: gc.candidate_count,
            pending_discard_range_count: gc.pending_discard_range_count,
            merged_discard_range_count: gc.merged_discard_range_count,
            fragmented_range_count: gc.fragmented_range_count,
            compaction_candidate_slabs,
            compaction_candidate_bytes: gc.compaction_candidate_bytes,
            compaction_candidate_range_count: gc.compaction_candidate_range_count,
            compaction_recommended: compaction_candidate_slabs > 0,
        }
    }
}
