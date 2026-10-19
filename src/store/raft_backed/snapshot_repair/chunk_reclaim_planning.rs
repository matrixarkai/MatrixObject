impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_reclaim_chunk_replicas(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_reclaim_chunk_replicas_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            "",
        )
    }

    pub fn propose_reclaim_chunk_replicas_fenced(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "chunk replica reclaim",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::ReclaimChunkReplicas {
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
        })
    }

    pub fn propose_reclaim_chunk_replicas_linearizable_fenced(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "chunk replica reclaim",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_reclaim_chunk_replicas_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_reclaim_chunk_replicas_with_limits(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_reclaim_chunk_replicas_with_limits_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            "",
        )
    }

    pub fn propose_reclaim_chunk_replicas_with_limits_fenced(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "bounded chunk replica reclaim",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::ReclaimChunkReplicasWithLimits {
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms: now_millis(),
        })
    }

    pub fn propose_reclaim_chunk_replicas_with_limits_linearizable_fenced(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "bounded chunk replica reclaim",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_reclaim_chunk_replicas_with_limits_fenced(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            expected_page_route_control_checksum,
        )
    }

    pub fn plan_chunk_replica_reclaim_linearizable(
        &self,
        marker: Option<PageId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> Result<ChunkReplicaReclaimPlan, ObjectError> {
        self.plan_chunk_replica_reclaim_linearizable_fenced(
            marker,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            "",
        )
    }

    pub fn plan_chunk_replica_reclaim_linearizable_fenced(
        &self,
        marker: Option<PageId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ChunkReplicaReclaimPlan, ObjectError> {
        self.ensure_page_route_control_ready(
            "chunk replica reclaim plan",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store.plan_chunk_replica_reclaim(
            marker.as_ref(),
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        )
    }

    pub fn plan_chunk_replica_reclaim_with_read_proof_linearizable_fenced(
        &self,
        marker: Option<PageId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<ChunkReplicaReclaimPlan>, ObjectError> {
        self.plan_chunk_replica_reclaim_with_read_proof_linearizable_fenced_committed_at(
            marker,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_chunk_replica_reclaim_with_read_proof_linearizable_fenced_committed_at(
        &self,
        marker: Option<PageId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<ChunkReplicaReclaimPlan>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "chunk replica reclaim plan",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let value = self.plan_chunk_replica_reclaim_linearizable_fenced(
            marker,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead { value, proof })
    }

}
