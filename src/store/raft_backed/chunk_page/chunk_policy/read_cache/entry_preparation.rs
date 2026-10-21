// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_configure_chunk_read_cache_raft_entry(
        &self,
        max_bytes_per_replica: u64,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.prepare_configure_chunk_read_cache_raft_entry_with_stream_bypass(
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
        )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_with_stream_bypass(
        &self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.store
            .prepare_chunk_read_cache_raft_entry_with_stream_bypass(
                max_bytes_per_replica,
                max_cacheable_read_bytes_per_replica,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_configure_chunk_replica_read_cache_raft_entry(
        &self,
        replica_index: usize,
        max_bytes_per_replica: u64,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        let admitted_route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.store
            .prepare_chunk_replica_read_cache_raft_entry_fenced(
                replica_index,
                max_bytes_per_replica,
                self.store.last_applied_index(),
                Some(&admitted_route_checksum),
            )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_linearizable(
        &self,
        max_bytes_per_replica: u64,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.prepare_configure_chunk_read_cache_raft_entry_linearizable_fenced_with_stream_bypass(
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            None,
        )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_linearizable_fenced(
        &self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.prepare_configure_chunk_read_cache_raft_entry_linearizable_fenced_with_stream_bypass(
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_linearizable_fenced_with_stream_bypass(
        &self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk read-cache",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store.prepare_chunk_read_cache_raft_entry_fenced(
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            self.store.last_applied_index(),
            Some(route_checksum),
        )
    }

    pub fn prepare_configure_chunk_replica_read_cache_raft_entry_linearizable_fenced(
        &self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        let proof = self.ensure_storage_control_read_index(
            "prepare chunk replica read-cache",
            expected_route_control_checksum,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        self.store
            .prepare_chunk_replica_read_cache_raft_entry_fenced(
                replica_index,
                max_bytes_per_replica,
                self.store.last_applied_index(),
                Some(route_checksum),
            )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkReadCacheConfig>, ObjectError> {
        self.prepare_configure_chunk_read_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            max_bytes_per_replica,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_chunk_replica_read_cache_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedChunkReadCacheConfig>, ObjectError> {
        self.prepare_configure_chunk_replica_read_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            replica_index,
            max_bytes_per_replica,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkReadCacheConfig>, ObjectError> {
        self.prepare_configure_chunk_read_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at_with_stream_bypass(
            max_bytes_per_replica,
            self.store
                .chunk_read_cache_max_cacheable_read_bytes_per_replica(),
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn prepare_configure_chunk_read_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at_with_stream_bypass(
        &self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkReadCacheConfig>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk read-cache",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self.store.prepare_chunk_read_cache_raft_entry_fenced(
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            self.store.last_applied_index(),
            Some(route_checksum),
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_configure_chunk_replica_read_cache_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedChunkReadCacheConfig>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        let proof = self.ensure_storage_control_read_index_committed_at(
            "prepare chunk replica read-cache",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_checksum =
            expected_route_control_checksum.unwrap_or(proof.state_checksum.as_str());
        let prepared = self
            .store
            .prepare_chunk_replica_read_cache_raft_entry_fenced(
                replica_index,
                max_bytes_per_replica,
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
