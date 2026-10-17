impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_configure_chunk_trim_flow_raft_entry(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.store.prepare_chunk_trim_flow_raft_entry(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_configure_chunk_replica_trim_flow_raft_entry(
        &self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        let admitted_route_checksum = self
            .store
            .page_service
            .route_control_proof()
            .state_checksum;
        self.store
            .prepare_chunk_replica_trim_flow_raft_entry_fenced(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
                self.store.last_applied_index(),
                Some(&admitted_route_checksum),
            )
    }

    pub fn prepare_configure_chunk_trim_flow_raft_entry_linearizable(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        self.prepare_configure_chunk_trim_flow_raft_entry_linearizable_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            None,
        )
    }

    pub fn prepare_configure_chunk_trim_flow_raft_entry_linearizable_fenced(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk trim flow",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_chunk_trim_flow_raft_entry_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_chunk_trim_flow_raft_entry_full_linearizable_fenced(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk trim flow",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_chunk_trim_flow_raft_entry_full_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            rate_ops_per_sec,
            burst_ops,
            now_ms,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_chunk_replica_trim_flow_raft_entry_linearizable_fenced(
        &self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica trim flow",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store
            .prepare_chunk_replica_trim_flow_raft_entry_fenced(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
                self.store.last_applied_index(),
                Some(route_checksum),
            )
    }

    pub fn prepare_configure_chunk_replica_trim_flow_raft_entry_full_linearizable_fenced(
        &self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkTrimFlowConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica trim flow",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store
            .prepare_chunk_replica_trim_flow_raft_entry_full_fenced(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                rate_ops_per_sec,
                burst_ops,
                now_ms,
                self.store.last_applied_index(),
                Some(route_checksum),
            )
    }

    pub fn prepare_configure_chunk_trim_flow_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkTrimFlowConfig>, ObjectError> {
        self.prepare_configure_chunk_trim_flow_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_chunk_replica_trim_flow_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkTrimFlowConfig>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk replica trim flow",
            expected_route_control_checksum,
            requested_read_index,
            0,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self
            .store
            .prepare_chunk_replica_trim_flow_raft_entry_fenced(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
                self.store.last_applied_index(),
                Some(route_checksum),
            )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, 0);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_configure_chunk_trim_flow_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkTrimFlowConfig>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk trim flow",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self.store.prepare_chunk_trim_flow_raft_entry_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            self.store.last_applied_index(),
            Some(route_checksum),
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }


}
