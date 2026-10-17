impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_reclaim_chunk_replica_raft_entry(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        self.store.prepare_chunk_reclaim_raft_entry(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_reclaim_chunk_replica_raft_entry_bounded(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        self.store.prepare_chunk_reclaim_raft_entry_bounded(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_reclaim_chunk_replica_raft_entry_bounded_fenced(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "chunk replica reclaim prepare",
            expected_page_route_control_checksum,
        )?;
        self.store.prepare_chunk_reclaim_raft_entry_bounded_fenced(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            self.store.last_applied_index(),
            Some(expected_page_route_control_checksum),
        )
    }

    pub fn prepare_reclaim_chunk_replica_raft_entry_bounded_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedChunkReplicaReclaim>, ObjectError> {
        self.prepare_reclaim_chunk_replica_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_reclaim_chunk_replica_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkReplicaReclaim>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "chunk replica reclaim prepare",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
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
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_reclaim_chunk_replica_envelope_linearizable_fenced(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkReclaimEnvelope, ObjectError> {
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
        Ok(make_prepared_chunk_reclaim_envelope(&prepared))
    }

    pub fn prepare_reclaim_chunk_replica_envelope_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPreparedChunkReclaimEnvelope, ObjectError> {
        self.prepare_reclaim_chunk_replica_envelope_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_reclaim_chunk_replica_envelope_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenPreparedChunkReclaimEnvelope, ObjectError> {
        let proven_prepared = self
            .prepare_reclaim_chunk_replica_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
                replica_index,
                max_chunks,
                max_rewrite_bytes,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
                now_ms,
                expected_page_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenPreparedChunkReclaimEnvelope {
            envelope: make_prepared_chunk_reclaim_envelope(&proven_prepared.value),
            proof: proven_prepared.proof,
        })
    }

}
