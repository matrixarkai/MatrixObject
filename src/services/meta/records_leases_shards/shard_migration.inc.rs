impl MatrixMetaService {
    pub fn migrate_shard(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<MetadataShard, ObjectError> {
        validate_meta_key(shard_id)?;
        validate_meta_key(new_owner_node_id)?;
        let mut shard = self
            .shards
            .get(shard_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata shard missing".to_string()))?;
        if shard.epoch != expected_epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard migration epoch fence mismatch".to_string(),
            ));
        }
        if new_epoch <= shard.epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard migration epoch must advance".to_string(),
            ));
        }
        shard.owner_node_id = new_owner_node_id.to_string();
        shard.epoch = new_epoch;
        shard.draining = draining;
        self.validate_shard_image(&shard)?;
        self.validate_shard_owner_session(&shard, now_ms)?;
        self.shards.insert(shard.shard_id.clone(), shard.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(shard)
    }

    pub fn migrate_shard_idempotent(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "migrate";
        let request_fields = vec![
            shard_id.to_string(),
            new_owner_node_id.to_string(),
            expected_epoch.to_string(),
            new_epoch.to_string(),
            (draining as u64).to_string(),
            now_ms.to_string(),
        ];
        let request_checksum = metadata_shard_request_checksum(
            operation,
            &[],
            request_fields
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice(),
        );
        if let Some(existing) = self.shard_receipts.get(idempotency_key) {
            return self.checked_shard_receipt_retry(existing, operation, &request_checksum);
        }
        let shard = self.migrate_shard(
            shard_id,
            new_owner_node_id,
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
        )?;
        let receipt = MetadataShardReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            shards: vec![shard],
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.shard_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }
}
