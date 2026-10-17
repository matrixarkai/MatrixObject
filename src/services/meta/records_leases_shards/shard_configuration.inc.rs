impl MatrixMetaService {
    pub fn configure_shard(&mut self, shard: MetadataShard) -> Result<(), ObjectError> {
        validate_meta_key(&shard.shard_id)?;
        self.admit_metadata_qos(
            1,
            shard
                .shard_id
                .len()
                .saturating_add(shard.start_key.len())
                .saturating_add(shard.end_key.len())
                .saturating_add(shard.owner_node_id.len()) as u64,
            now_millis(),
        )?;
        if !self.shards.contains_key(&shard.shard_id) {
            self.check_shard_admission(1)?;
        }
        self.validate_shard_image(&shard)?;
        self.validate_shard_owner_session(&shard, 0)?;
        for existing in self.shards.values() {
            if existing.shard_id == shard.shard_id {
                continue;
            }
            let no_overlap = (!shard.end_key.is_empty() && existing.start_key >= shard.end_key)
                || (!existing.end_key.is_empty() && shard.start_key >= existing.end_key);
            if !no_overlap {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard ranges overlap".to_string(),
                ));
            }
        }
        self.shards.insert(shard.shard_id.clone(), shard);
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(())
    }

    pub fn configure_shard_idempotent(
        &mut self,
        shard: MetadataShard,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "configure";
        let request_checksum = metadata_shard_request_checksum(operation, &[shard.clone()], &[]);
        if let Some(existing) = self.shard_receipts.get(idempotency_key) {
            return self.checked_shard_receipt_retry(existing, operation, &request_checksum);
        }
        self.configure_shard(shard.clone())?;
        let receipt = MetadataShardReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            shards: vec![shard],
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: 0,
            deduped: false,
        };
        self.shard_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }
}
