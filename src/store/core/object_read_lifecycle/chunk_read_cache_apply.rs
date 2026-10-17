impl MatrixObjectStore {
    pub fn apply_prepared_chunk_read_cache(
        &mut self,
        prepared: PreparedChunkReadCacheConfig,
    ) -> Result<(), ObjectError> {
        let expected_checksum =
            prepared_chunk_read_cache_config_checksum(&PreparedChunkReadCacheConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared read-cache raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache commit index must be adjacent".to_string(),
            ));
        }
        let projected = self.page_service.clone();
        if let Some(replica_index) = prepared.target_replica_index {
            projected.set_read_cache_max_bytes_for_replica(
                replica_index,
                prepared.max_bytes_per_replica,
            )?;
            projected.set_read_cache_max_cacheable_read_bytes_for_replica(
                replica_index,
                prepared.max_cacheable_read_bytes_per_replica,
            )?;
        } else {
            projected.set_read_cache_max_bytes_per_replica(prepared.max_bytes_per_replica);
            projected.set_read_cache_max_cacheable_read_bytes_per_replica(
                prepared.max_cacheable_read_bytes_per_replica,
            );
        }
        let (expected_aggregate_checksum, expected_replica_count) = projected
            .chunk_read_cache_prepared_aggregate_checksum(
                prepared.target_replica_index,
                prepared.max_bytes_per_replica,
                prepared.max_cacheable_read_bytes_per_replica,
                prepared.expected_previous_commit_index,
                prepared.prepared_commit_index,
            )?;
        if prepared.chunk_prepared_replica_count != expected_replica_count {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache replica-count fence mismatch".to_string(),
            ));
        }
        if prepared.chunk_read_cache_prepared_aggregate_checksum != expected_aggregate_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache aggregate checksum mismatch".to_string(),
            ));
        }
        let current_fingerprint = page_read_cache_control_image_fingerprint(&self.page_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            if prepared.target_replica_index.is_none() {
                self.chunk_read_cache_max_bytes_per_replica = prepared.max_bytes_per_replica;
                self.chunk_read_cache_max_cacheable_read_bytes_per_replica =
                    prepared.max_cacheable_read_bytes_per_replica;
            }
            return Ok(());
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache previous commit fence mismatch".to_string(),
            ));
        }
        self.page_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared read-cache",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared read-cache source fingerprint mismatch".to_string(),
            ));
        }
        if let Some(replica_index) = prepared.target_replica_index {
            self.page_service.set_read_cache_max_bytes_for_replica(
                replica_index,
                prepared.max_bytes_per_replica,
            )?;
            self.page_service
                .set_read_cache_max_cacheable_read_bytes_for_replica(
                    replica_index,
                    prepared.max_cacheable_read_bytes_per_replica,
                )?;
        } else {
            self.chunk_read_cache_max_bytes_per_replica = prepared.max_bytes_per_replica;
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica =
                prepared.max_cacheable_read_bytes_per_replica;
            self.page_service
                .set_read_cache_max_bytes_per_replica(prepared.max_bytes_per_replica);
            self.page_service
                .set_read_cache_max_cacheable_read_bytes_per_replica(
                    prepared.max_cacheable_read_bytes_per_replica,
                );
        }
        if page_read_cache_control_image_fingerprint(&self.page_service)
            != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "chunk prepared read-cache deterministic apply mismatch".to_string(),
            ));
        }
        Ok(())
    }

    pub fn chunk_read_cache_max_bytes_per_replica(&self) -> u64 {
        self.chunk_read_cache_max_bytes_per_replica
    }

    pub fn chunk_read_cache_max_cacheable_read_bytes_per_replica(&self) -> u64 {
        self.chunk_read_cache_max_cacheable_read_bytes_per_replica
    }
}
