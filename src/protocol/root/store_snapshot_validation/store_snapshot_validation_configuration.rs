// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl StoreSnapshot {
    fn validate_configuration(&self) -> Result<(), ObjectError> {
        if self.options.chunk_size == 0
            || self.options.max_extent_bytes == 0
            || self.options.max_extent_bytes > self.options.chunk_size
            || self.options.page_size == 0
            || self.options.page_size > self.options.chunk_size
            || self.options.volume_count == 0
            || self.options.replica_count == 0
            || self.options.write_quorum == 0
            || self.options.write_quorum > self.options.replica_count
            || self.options.metadata_route_cache_entries == 0
            || self.options.metadata_route_cache_ttl_ms == 0
            || self.options.page_read_parallelism == 0
            || self.options.page_read_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.page_write_parallelism == 0
            || self.options.page_write_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.page_batch_read_parallelism == 0
            || self.options.page_batch_read_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.page_batch_write_parallelism == 0
            || self.options.page_batch_write_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.page_batch_discard_parallelism == 0
            || self.options.page_batch_discard_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.page_repair_parallelism == 0
            || self.options.page_repair_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.replica_fanout_parallelism == 0
            || self.options.replica_fanout_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.local_replica_store_parallelism == 0
            || self.options.local_replica_store_parallelism > MAX_STORAGE_PARALLELISM
        {
            return Err(ObjectError::Corruption(
                "snapshot store options are invalid".to_string(),
            ));
        }
        if self.next_chunk_id == 0 {
            return Err(ObjectError::Corruption(
                "snapshot next chunk id is invalid".to_string(),
            ));
        }
        if self.next_upload_sequence == 0 {
            return Err(ObjectError::Corruption(
                "snapshot next upload sequence is invalid".to_string(),
            ));
        }
        if self.next_object_change_sequence == 0 {
            return Err(ObjectError::Corruption(
                "snapshot next object change sequence is invalid".to_string(),
            ));
        }
        validate_page_chunk_endpoint_topology(
            &self.replica_endpoints,
            &self.page_disabled_chunk_endpoints,
            self.page_write_quorum,
            self.page_read_quorum,
            self.options.replica_count,
        )
        .map_err(|err| {
            ObjectError::Corruption(format!(
                "snapshot page endpoint topology is invalid: {err}"
            ))
        })?;
        Ok(())
    }
}
