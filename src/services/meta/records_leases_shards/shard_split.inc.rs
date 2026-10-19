// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn split_shard(
        &mut self,
        source_shard_id: &str,
        split_key: &str,
        left_shard_id: &str,
        right_shard_id: &str,
        left_owner_node_id: &str,
        right_owner_node_id: &str,
        epoch: u64,
        left_draining: bool,
        right_draining: bool,
        now_ms: u64,
    ) -> Result<MetadataShardSplit, ObjectError> {
        validate_meta_key(source_shard_id)?;
        validate_meta_key(left_shard_id)?;
        validate_meta_key(right_shard_id)?;
        validate_meta_key(split_key)?;
        if left_shard_id == right_shard_id {
            return Err(ObjectError::InvalidArgument(
                "metadata split shard ids must be distinct".to_string(),
            ));
        }
        if epoch == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata split epoch must be positive".to_string(),
            ));
        }
        let source =
            self.shards.get(source_shard_id).cloned().ok_or_else(|| {
                ObjectError::NotFound("metadata source shard missing".to_string())
            })?;
        if split_key <= source.start_key.as_str()
            || (!source.end_key.is_empty() && split_key >= source.end_key.as_str())
        {
            return Err(ObjectError::InvalidArgument(
                "metadata split key must be inside source shard".to_string(),
            ));
        }
        if epoch <= source.epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata split epoch must advance source epoch".to_string(),
            ));
        }
        self.check_shard_admission(1)?;
        let left = MetadataShard {
            shard_id: left_shard_id.to_string(),
            start_key: source.start_key.clone(),
            end_key: split_key.to_string(),
            owner_node_id: left_owner_node_id.to_string(),
            epoch,
            draining: left_draining,
        };
        let right = MetadataShard {
            shard_id: right_shard_id.to_string(),
            start_key: split_key.to_string(),
            end_key: source.end_key.clone(),
            owner_node_id: right_owner_node_id.to_string(),
            epoch,
            draining: right_draining,
        };
        self.validate_shard_image(&left)?;
        self.validate_shard_image(&right)?;
        self.validate_shard_owner_session(&left, now_ms)?;
        self.validate_shard_owner_session(&right, now_ms)?;
        self.shards.remove(source_shard_id);
        for shard in [&left, &right] {
            for existing in self.shards.values() {
                let no_overlap = (!shard.end_key.is_empty() && existing.start_key >= shard.end_key)
                    || (!existing.end_key.is_empty() && shard.start_key >= existing.end_key);
                if !no_overlap {
                    self.shards.insert(source.shard_id.clone(), source);
                    return Err(ObjectError::InvalidArgument(
                        "metadata split shard ranges overlap".to_string(),
                    ));
                }
            }
        }
        self.shards.insert(left.shard_id.clone(), left.clone());
        self.shards.insert(right.shard_id.clone(), right.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(MetadataShardSplit { left, right })
    }

    pub fn split_shard_idempotent(
        &mut self,
        source_shard_id: &str,
        split_key: &str,
        left_shard_id: &str,
        right_shard_id: &str,
        left_owner_node_id: &str,
        right_owner_node_id: &str,
        epoch: u64,
        left_draining: bool,
        right_draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "split";
        let request_fields = vec![
            source_shard_id.to_string(),
            split_key.to_string(),
            left_shard_id.to_string(),
            right_shard_id.to_string(),
            left_owner_node_id.to_string(),
            right_owner_node_id.to_string(),
            epoch.to_string(),
            (left_draining as u64).to_string(),
            (right_draining as u64).to_string(),
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
        let split = self.split_shard(
            source_shard_id,
            split_key,
            left_shard_id,
            right_shard_id,
            left_owner_node_id,
            right_owner_node_id,
            epoch,
            left_draining,
            right_draining,
            now_ms,
        )?;
        let receipt = MetadataShardReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            shards: vec![split.left, split.right],
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.shard_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }
}
