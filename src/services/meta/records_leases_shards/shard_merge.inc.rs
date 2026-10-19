// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn merge_shards(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<MetadataShard, ObjectError> {
        validate_meta_key(left_shard_id)?;
        validate_meta_key(right_shard_id)?;
        validate_meta_key(merged_shard_id)?;
        if left_shard_id == right_shard_id {
            return Err(ObjectError::InvalidArgument(
                "metadata merge shard ids must be distinct".to_string(),
            ));
        }
        let left = self
            .shards
            .get(left_shard_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata left shard missing".to_string()))?;
        let right = self
            .shards
            .get(right_shard_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata right shard missing".to_string()))?;
        if left.end_key != right.start_key {
            return Err(ObjectError::InvalidArgument(
                "metadata merge shards must be adjacent".to_string(),
            ));
        }
        if epoch <= left.epoch.max(right.epoch) {
            return Err(ObjectError::PreconditionFailed(
                "metadata merge epoch must advance source epochs".to_string(),
            ));
        }
        let merged = MetadataShard {
            shard_id: merged_shard_id.to_string(),
            start_key: left.start_key.clone(),
            end_key: right.end_key.clone(),
            owner_node_id: owner_node_id.to_string(),
            epoch,
            draining,
        };
        self.validate_shard_image(&merged)?;
        self.validate_shard_owner_session(&merged, now_ms)?;
        self.shards.remove(left_shard_id);
        self.shards.remove(right_shard_id);
        for existing in self.shards.values() {
            let no_overlap = (!merged.end_key.is_empty() && existing.start_key >= merged.end_key)
                || (!existing.end_key.is_empty() && merged.start_key >= existing.end_key);
            if !no_overlap {
                self.shards.insert(left.shard_id.clone(), left);
                self.shards.insert(right.shard_id.clone(), right);
                return Err(ObjectError::InvalidArgument(
                    "metadata merged shard range overlaps".to_string(),
                ));
            }
        }
        self.shards.insert(merged.shard_id.clone(), merged.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(merged)
    }

    pub fn merge_shards_idempotent(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "merge";
        let request_fields = vec![
            left_shard_id.to_string(),
            right_shard_id.to_string(),
            merged_shard_id.to_string(),
            owner_node_id.to_string(),
            epoch.to_string(),
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
        let merged = self.merge_shards(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
        )?;
        let receipt = MetadataShardReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            shards: vec![merged],
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.shard_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }
}
