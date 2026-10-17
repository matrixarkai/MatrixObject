impl MatrixMetaService {
    fn checked_shard_receipt_retry(
        &self,
        receipt: &MetadataShardReceipt,
        operation: &str,
        request_checksum: &str,
    ) -> Result<MetadataShardReceipt, ObjectError> {
        if receipt.operation != operation || receipt.request_checksum != request_checksum {
            return Err(ObjectError::PreconditionFailed(
                "metadata shard idempotency key reused with different request".to_string(),
            ));
        }
        let mut receipt = receipt.clone();
        receipt.deduped = true;
        Ok(receipt)
    }

    fn metadata_log_has_applied(&self, prepared_commit_index: u64) -> bool {
        self.next_commit_index.saturating_sub(1) >= prepared_commit_index
    }

    fn shard_log_has_applied(&self, prepared_commit_index: u64) -> bool {
        self.metadata_log_has_applied(prepared_commit_index)
    }

    fn prepared_shard_split_already_applied(&self, prepared: &PreparedMetadataShardSplit) -> bool {
        self.shard_log_has_applied(prepared.prepared_commit_index)
            && !self.shards.contains_key(&prepared.source.shard_id)
            && self.shards.get(&prepared.left.shard_id) == Some(&prepared.left)
            && self.shards.get(&prepared.right.shard_id) == Some(&prepared.right)
    }

    fn prepared_shard_merge_already_applied(&self, prepared: &PreparedMetadataShardMerge) -> bool {
        self.shard_log_has_applied(prepared.prepared_commit_index)
            && !self.shards.contains_key(&prepared.left.shard_id)
            && !self.shards.contains_key(&prepared.right.shard_id)
            && self.shards.get(&prepared.merged.shard_id) == Some(&prepared.merged)
    }

    fn prepared_shard_migration_already_applied(
        &self,
        prepared: &PreparedMetadataShardMigration,
    ) -> bool {
        self.shard_log_has_applied(prepared.prepared_commit_index)
            && self.shards.get(&prepared.after.shard_id) == Some(&prepared.after)
    }

    fn prepared_shard_migration_plan_already_applied(
        &self,
        prepared: &PreparedMetadataShardMigrationPlan,
    ) -> bool {
        self.shard_log_has_applied(prepared.prepared_commit_index)
            && prepared.report.actions.iter().all(|action| {
                self.shards.get(&action.shard_id).is_some_and(|shard| {
                    shard.owner_node_id == action.target_owner_node_id
                        && shard.epoch == action.new_epoch
                        && shard.draining == action.draining
                })
            })
    }
}
