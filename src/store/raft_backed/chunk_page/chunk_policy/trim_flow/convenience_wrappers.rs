// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_trim_flow_prepared(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_trim_flow_raft_entry(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
        )?;
        self.propose_prepared_chunk_trim_flow(prepared)
    }

    pub fn propose_configure_chunk_replica_trim_flow_prepared(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_replica_trim_flow_raft_entry(
            replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
        )?;
        self.propose_prepared_chunk_trim_flow(prepared)
    }

    pub fn propose_configure_chunk_trim_flow_prepared_linearizable(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_trim_flow_prepared_linearizable_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            None,
        )
    }

    pub fn propose_configure_chunk_trim_flow_prepared_linearizable_fenced(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_trim_flow_raft_entry_linearizable_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            expected_route_control_checksum,
        )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk trim flow prepared linearizable",
        )?;
        self.propose_prepared_chunk_trim_flow_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_trim_flow_prepared_full_linearizable_fenced(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_trim_flow_raft_entry_full_linearizable_fenced(
            rate_bytes_per_sec,
            burst_bytes,
            rate_ops_per_sec,
            burst_ops,
            now_ms,
            expected_route_control_checksum,
        )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk trim flow prepared full linearizable",
        )?;
        self.propose_prepared_chunk_trim_flow_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_replica_trim_flow_prepared_linearizable(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_trim_flow_prepared_linearizable_fenced(
            replica_index,
            rate_bytes_per_sec,
            burst_bytes,
            now_ms,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_trim_flow_prepared_linearizable_fenced(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_trim_flow_raft_entry_linearizable_fenced(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                now_ms,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica trim flow prepared linearizable",
        )?;
        self.propose_prepared_chunk_trim_flow_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_replica_trim_flow_prepared_full_linearizable_fenced(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_trim_flow_raft_entry_full_linearizable_fenced(
                replica_index,
                rate_bytes_per_sec,
                burst_bytes,
                rate_ops_per_sec,
                burst_ops,
                now_ms,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica trim flow prepared full linearizable",
        )?;
        self.propose_prepared_chunk_trim_flow_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }
}
