// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn prepare_shard_migration_plan_raft_entry(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMigrationPlan, ObjectError> {
        self.prepare_shard_migration_plan_raft_entry_fenced(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn prepare_shard_migration_plan_raft_entry_fenced(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<PreparedMetadataShardMigrationPlan, ObjectError> {
        let mut report = match operation {
            "failover" => self.plan_shard_failover(
                source_owner_node_id,
                target_owner_node_id,
                max_shards,
                now_ms,
            )?,
            "expired_failover" => self.plan_expired_shard_owner_failover(
                source_owner_node_id,
                target_owner_node_id,
                max_shards,
                now_ms,
            )?,
            "rebalance" => self.plan_shard_rebalance(
                source_owner_node_id,
                target_owner_node_id,
                max_shards,
                now_ms,
            )?,
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard migration prepare operation must be failover, expired_failover, or rebalance"
                        .to_string(),
                ))
            }
        };
        if !expected_plan_checksum.is_empty() && report.plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration plan checksum fence mismatch".to_string(),
            ));
        }
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        let request_checksum = metadata_shard_migration_plan_request_checksum(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
        );
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataShardMigrationPlan {
            operation: operation.to_string(),
            source_owner_node_id: source_owner_node_id.to_string(),
            target_owner_node_id: target_owner_node_id.to_string(),
            max_shards,
            now_ms,
            report,
            expected_plan_checksum: expected_plan_checksum.to_string(),
            before_state_fingerprint: self.metadata_state_fingerprint(),
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index: self.next_commit_index.saturating_sub(1),
            prepared_commit_index: self.next_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_shard_migration_plan_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_shard_migration_plan(
        &mut self,
        prepared: PreparedMetadataShardMigrationPlan,
    ) -> Result<MetadataShardMigrationReport, ObjectError> {
        let expected_checksum =
            prepared_metadata_shard_migration_plan_checksum(&PreparedMetadataShardMigrationPlan {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared shard migration raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_shard_migration_plan_request_checksum(
                &prepared.operation,
                &prepared.source_owner_node_id,
                &prepared.target_owner_node_id,
                prepared.max_shards,
                prepared.now_ms,
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared shard migration request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration commit index must be adjacent".to_string(),
            ));
        }
        if self.prepared_shard_migration_plan_already_applied(&prepared) {
            let mut report = prepared.report;
            report.migrated_shards = report.actions.len() as u64;
            return Ok(report);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration state changed before apply".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.now_ms,
        )?;
        let mut planned = match prepared.operation.as_str() {
            "failover" => self.plan_shard_failover(
                &prepared.source_owner_node_id,
                &prepared.target_owner_node_id,
                prepared.max_shards,
                prepared.now_ms,
            )?,
            "expired_failover" => self.plan_expired_shard_owner_failover(
                &prepared.source_owner_node_id,
                &prepared.target_owner_node_id,
                prepared.max_shards,
                prepared.now_ms,
            )?,
            "rebalance" => self.plan_shard_rebalance(
                &prepared.source_owner_node_id,
                &prepared.target_owner_node_id,
                prepared.max_shards,
                prepared.now_ms,
            )?,
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared shard migration operation must be failover, expired_failover, or rebalance"
                        .to_string(),
                ))
            }
        };
        planned.expected_plan_checksum = prepared.expected_plan_checksum.clone();
        if planned != prepared.report {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration plan changed before apply".to_string(),
            ));
        }
        if !prepared.expected_plan_checksum.is_empty()
            && prepared.report.plan_checksum != prepared.expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration expected plan checksum mismatch".to_string(),
            ));
        }
        let mut report = prepared.report;
        let mut updated_shards = Vec::new();
        for action in &report.actions {
            let mut shard = self
                .shards
                .get(&action.shard_id)
                .cloned()
                .ok_or_else(|| ObjectError::NotFound("metadata shard missing".to_string()))?;
            if shard.owner_node_id != action.current_owner_node_id
                || shard.epoch != action.expected_epoch
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared shard migration action fence mismatch".to_string(),
                ));
            }
            if action.new_epoch <= shard.epoch {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared shard migration action epoch must advance".to_string(),
                ));
            }
            shard.owner_node_id = action.target_owner_node_id.clone();
            shard.epoch = action.new_epoch;
            shard.draining = action.draining;
            self.validate_shard_image(&shard)?;
            self.validate_shard_owner_session(&shard, prepared.now_ms)?;
            updated_shards.push(shard);
        }
        for shard in updated_shards {
            self.shards.insert(shard.shard_id.clone(), shard);
            report.migrated_shards = report.migrated_shards.saturating_add(1);
        }
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(report)
    }
}
