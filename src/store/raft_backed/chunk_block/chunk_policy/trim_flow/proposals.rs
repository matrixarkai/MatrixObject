impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_trim_flow(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_trim_flow_fenced(rate_bytes_per_sec, burst_bytes, now_ms, None)
    }

    pub fn propose_configure_chunk_trim_flow_fenced(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkTrimFlow {
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
        })
    }

    pub fn propose_configure_chunk_trim_flow_linearizable_fenced(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk trim flow",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_trim_flow_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            expected_route_control_checksum,
        )
    }

    pub fn propose_configure_chunk_replica_trim_flow(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_trim_flow_fenced(
            replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_trim_flow_fenced(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigureChunkReplicaTrimFlow {
            replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
        })
    }

    pub fn propose_configure_chunk_replica_trim_flow_linearizable_fenced(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure chunk replica trim flow",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_chunk_replica_trim_flow_fenced(
            replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            expected_route_control_checksum,
        )
    }


}
