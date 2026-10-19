// SPDX-License-Identifier: Apache-2.0
fn populate_lifecycle_module_status(
    status: &mut ObjectStoreModuleStatus,
    readiness: &ObjectStoreDistributedReadiness,
) {
    status.live_bytes = readiness.live_bytes;
    status.stale_bytes = readiness.stale_bytes;
    status.pending_discard_bytes = readiness.pending_discard_bytes;
    status.discarded_bytes = readiness.discarded_bytes;
    status.physical_bytes = readiness.physical_bytes;
    status.live_bytes_ppm = readiness.live_bytes_ppm;
    status.stale_bytes_ppm = readiness.stale_bytes_ppm;
    status.pending_discard_bytes_ppm = readiness.pending_discard_bytes_ppm;
    status.discarded_bytes_ppm = readiness.discarded_bytes_ppm;
    status.logical_write_amplification_ppm = readiness.logical_write_amplification_ppm;
    status.storage_write_amplification_ppm = readiness.storage_write_amplification_ppm;
    status.space_amplification_ppm = readiness.space_amplification_ppm;
    status.discard_backlog_ppm = readiness.discard_backlog_ppm;
    status.discard_candidate_bytes = readiness.discard_candidate_bytes;
    status.discard_candidate_chunks = readiness.discard_candidate_chunks;
    status.pending_discard_range_count = readiness.pending_discard_range_count;
    status.merged_discard_range_count = readiness.merged_discard_range_count;
    status.fragmented_range_count = readiness.fragmented_range_count;
    status.compaction_candidate_chunks = readiness.compaction_candidate_chunks;
    status.compaction_candidate_bytes = readiness.compaction_candidate_bytes;
    status.compaction_candidate_range_count = readiness.compaction_candidate_range_count;
    status.compaction_recommended = readiness.compaction_recommended;
    status.bucket_count = readiness.bucket_count;
    status.object_count = readiness.object_count;
    status.live_object_count = readiness.live_object_count;
    status.deleted_object_count = readiness.deleted_object_count;
    status.chunk_count = readiness.chunk_count;
    status.page_count = readiness.page_count;
    status.logical_written_bytes = readiness.logical_written_bytes;
    status.physical_written_bytes = readiness.physical_written_bytes;
    status.chunk_read_cache_max_bytes_per_replica =
        readiness.chunk_read_cache_max_bytes_per_replica;
    status.chunk_read_cache_max_cacheable_read_bytes_per_replica =
        readiness.chunk_read_cache_max_cacheable_read_bytes_per_replica;
    status.metadata_state_fingerprint = readiness.metadata_state_fingerprint.clone();
    status.root_state_fingerprint = readiness.root_state_fingerprint.clone();
    status.page_state_fingerprint = readiness.page_state_fingerprint.clone();
    status.chunk_state_fingerprint = readiness.chunk_state_fingerprint.clone();
    status.distributed_readiness_checksum = readiness.control_checksum.clone();
}
