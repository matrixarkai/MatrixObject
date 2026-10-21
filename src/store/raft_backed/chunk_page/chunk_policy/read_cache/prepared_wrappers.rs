// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_chunk_read_cache_prepared(
        &mut self,
        max_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_read_cache_raft_entry(max_bytes_per_replica)?;
        self.propose_prepared_chunk_read_cache(prepared)
    }

    pub fn propose_configure_chunk_replica_read_cache_prepared(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_replica_read_cache_raft_entry(
            replica_index,
            max_bytes_per_replica,
        )?;
        self.propose_prepared_chunk_read_cache(prepared)
    }

    pub fn propose_configure_chunk_read_cache_prepared_linearizable(
        &mut self,
        max_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_read_cache_prepared_linearizable_fenced(
            max_bytes_per_replica,
            None,
        )
    }

    pub fn propose_configure_chunk_read_cache_prepared_linearizable_fenced(
        &mut self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_read_cache_raft_entry_linearizable_fenced(
            max_bytes_per_replica,
            expected_route_control_checksum,
        )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk read-cache prepared linearizable",
        )?;
        self.propose_prepared_chunk_read_cache_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_replica_read_cache_prepared_linearizable(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_read_cache_prepared_linearizable_fenced(
            replica_index,
            max_bytes_per_replica,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_read_cache_prepared_linearizable_fenced(
        &mut self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_read_cache_raft_entry_linearizable_fenced(
                replica_index,
                max_bytes_per_replica,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica read-cache prepared linearizable",
        )?;
        self.propose_prepared_chunk_read_cache_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }
}
