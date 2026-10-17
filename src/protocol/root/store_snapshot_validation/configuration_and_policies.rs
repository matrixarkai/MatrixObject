impl StoreSnapshot {
    fn validate_configuration_and_policies(&self) -> Result<(), ObjectError> {
        if self.options.slab_size == 0
            || self.options.max_extent_bytes == 0
            || self.options.max_extent_bytes > self.options.slab_size
            || self.options.chunk_size == 0
            || self.options.chunk_size > self.options.slab_size
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
            || self.options.page_chunk_fanout_parallelism == 0
            || self.options.page_chunk_fanout_parallelism > MAX_STORAGE_PARALLELISM
            || self.options.local_replica_store_parallelism == 0
            || self.options.local_replica_store_parallelism > MAX_STORAGE_PARALLELISM
        {
            return Err(ObjectError::Corruption(
                "snapshot store options are invalid".to_string(),
            ));
        }
        if self.next_slab_id == 0 {
            return Err(ObjectError::Corruption(
                "snapshot next segment id is invalid".to_string(),
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
            &self.page_chunk_endpoints,
            &self.page_disabled_chunk_endpoints,
            self.page_write_quorum,
            self.page_read_quorum,
            self.options.replica_count,
        )
        .map_err(|err| {
            ObjectError::Corruption(format!(
                "snapshot block endpoint topology is invalid: {err}"
            ))
        })?;
        self.meta_snapshot.validate()?;
        self.root_snapshot.validate()?;
        for (bucket, retain_for_ms) in &self.bucket_retention {
            validate_bucket(bucket)?;
            if *retain_for_ms == 0 {
                return Err(ObjectError::Corruption(
                    "snapshot bucket retention stores zero duration".to_string(),
                ));
            }
        }
        for (bucket, quota) in &self.bucket_quotas {
            validate_bucket(bucket)?;
            if quota.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket quota stores disabled policy".to_string(),
                ));
            }
        }
        for (bucket, policy) in &self.bucket_lifecycle {
            validate_bucket(bucket)?;
            validate_lifecycle_policy(policy)?;
            if policy.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket lifecycle stores disabled policy".to_string(),
                ));
            }
        }
        for (bucket, policy) in &self.bucket_policy_documents {
            validate_bucket(bucket)?;
            validate_bucket_policy_document(policy)?;
            if policy.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket policy document stores disabled policy".to_string(),
                ));
            }
        }
        for (bucket, tag_set) in &self.bucket_tags {
            validate_bucket(bucket)?;
            validate_tags(&tag_set.tags)?;
            if tag_set.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket tag set stores disabled tag set".to_string(),
                ));
            }
        }
        for (bucket, policy) in &self.bucket_cors {
            validate_bucket(bucket)?;
            validate_cors_policy(policy)?;
            if policy.disabled() {
                return Err(ObjectError::Corruption(
                    "snapshot bucket cors stores disabled policy".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.bucket_policy_receipts {
            validate_bucket_policy_receipt(receipt)?;
            if idempotency_key != &receipt.idempotency_key {
                return Err(ObjectError::Corruption(
                    "snapshot bucket policy receipt key mismatch".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.bucket_cors_receipts {
            validate_bucket_cors_receipt(receipt)?;
            if idempotency_key != &receipt.idempotency_key {
                return Err(ObjectError::Corruption(
                    "snapshot bucket cors receipt key mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }
}
