// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn prepare_chunk_read_cache_raft_entry(
        &self,
        max_bytes_per_replica: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.prepare_chunk_read_cache_raft_entry_with_stream_bypass(
            max_bytes_per_replica,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_chunk_read_cache_raft_entry_with_stream_bypass(
        &self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_chunk_read_cache_raft_entry_fenced(
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_read_cache_raft_entry_fenced(
        &self,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.prepare_chunk_read_cache_raft_entry_targeted_fenced(
            None,
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_chunk_replica_read_cache_raft_entry_fenced(
        &self,
        replica_index: usize,
        max_bytes_per_replica: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.prepare_chunk_read_cache_raft_entry_targeted_fenced(
            Some(replica_index),
            max_bytes_per_replica,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    fn prepare_chunk_read_cache_raft_entry_targeted_fenced(
        &self,
        target_replica_index: Option<usize>,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReadCacheConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_read_cache_control_image_fingerprint(&self.chunk_service);
        let projected = self.chunk_service.clone();
        if let Some(replica_index) = target_replica_index {
            projected.set_read_cache_max_bytes_for_replica(replica_index, max_bytes_per_replica)?;
            projected.set_read_cache_max_cacheable_read_bytes_for_replica(
                replica_index,
                max_cacheable_read_bytes_per_replica,
            )?;
        } else {
            projected.set_read_cache_max_bytes_per_replica(max_bytes_per_replica);
            projected.set_read_cache_max_cacheable_read_bytes_per_replica(
                max_cacheable_read_bytes_per_replica,
            );
        }
        let after_page_service_fingerprint =
            page_read_cache_control_image_fingerprint(&projected);
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let (chunk_read_cache_prepared_aggregate_checksum, chunk_prepared_replica_count) =
            projected.chunk_read_cache_prepared_aggregate_checksum(
                target_replica_index,
                max_bytes_per_replica,
                max_cacheable_read_bytes_per_replica,
                expected_previous_commit_index,
                prepared_commit_index,
            )?;
        let route_control_proof = self.chunk_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedChunkReadCacheConfig {
            target_replica_index,
            max_bytes_per_replica,
            max_cacheable_read_bytes_per_replica,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            chunk_read_cache_prepared_aggregate_checksum,
            chunk_prepared_replica_count,
            expected_previous_commit_index,
            prepared_commit_index,
            route_control_proof,
            expected_route_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_read_cache_config_checksum(&prepared);
        Ok(prepared)
    }
}
