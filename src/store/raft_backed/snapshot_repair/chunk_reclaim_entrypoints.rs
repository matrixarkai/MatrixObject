// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_reclaim_chunk_replica_prepared(
        &mut self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_reclaim_chunk_replica_raft_entry(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
        )?;
        self.propose_prepared_reclaim_chunk_replica(replica_index, prepared)
    }

    pub fn propose_reclaim_chunk_replica_prepared_bounded(
        &mut self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_reclaim_chunk_replica_raft_entry_bounded(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
        )?;
        self.propose_prepared_reclaim_chunk_replica(replica_index, prepared)
    }

    pub fn propose_reclaim_chunk_replica_prepared_bounded_fenced(
        &mut self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let prepared = self.prepare_reclaim_chunk_replica_raft_entry_bounded_fenced(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        self.propose_prepared_reclaim_chunk_replica_fenced(
            replica_index,
            prepared,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_reclaim_chunk_replica_prepared_bounded_fenced_with_status(
        &mut self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkReclaimApplyReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let prepared = self.prepare_reclaim_chunk_replica_raft_entry_bounded_fenced(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        self.propose_prepared_reclaim_chunk_replica_fenced_with_status(
            replica_index,
            prepared,
            expected_page_route_control_checksum,
        )
    }
}
