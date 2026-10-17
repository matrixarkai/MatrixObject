impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_meta_shard_migration_plan_raft_entry(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMigrationPlan, ObjectError> {
        self.prepare_meta_shard_migration_plan_raft_entry_fenced(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            "",
        )
    }

    pub fn prepare_meta_shard_migration_plan_raft_entry_fenced(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<PreparedMetadataShardMigrationPlan, ObjectError> {
        self.store
            .prepare_metadata_shard_migration_plan_raft_entry_fenced(
                operation,
                source_owner_node_id,
                target_owner_node_id,
                max_shards,
                now_ms,
                expected_plan_checksum,
            )
    }

    pub fn prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMigrationPlan>, ObjectError> {
        self.prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            "",
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMigrationPlan>, ObjectError> {
        self.prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        operation: &str,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
        expected_plan_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMigrationPlan>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata shard migration plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_meta_shard_migration_plan_raft_entry_fenced(
            operation,
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
            expected_plan_checksum,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn plan_meta_shard_failover_with_read_proof_linearizable(
        &self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMigrationPlan>, ObjectError> {
        self.prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable(
            "failover",
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )
    }

    pub fn plan_expired_meta_shard_owner_failover_with_read_proof_linearizable(
        &self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMigrationPlan>, ObjectError> {
        self.prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable(
            "expired_failover",
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )
    }

    pub fn plan_meta_shard_rebalance_with_read_proof_linearizable(
        &self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMigrationPlan>, ObjectError> {
        self.prepare_meta_shard_migration_plan_raft_entry_with_read_proof_linearizable(
            "rebalance",
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
        )
    }

    pub fn propose_prepared_meta_shard_migration_plan(
        &mut self,
        prepared: PreparedMetadataShardMigrationPlan,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index != self.store.last_applied_index()
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata shard migration requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaShardMigrationPlan {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaShardMigrationPlan { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared metadata shard migration raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_meta_shard_migration_plan_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataShardMigrationPlan>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata shard migration proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        if !expected_plan_checksum.is_empty()
            && proven_prepared.value.report.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard migration proven plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, proven_prepared.value.report.plan_checksum
            )));
        }
        self.propose_prepared_meta_shard_migration_plan(proven_prepared.value)
    }

    pub fn propose_meta_shard_failover_prepared(
        &mut self,
        failed_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_shard_migration_plan_raft_entry(
            "failover",
            failed_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )?;
        self.propose_prepared_meta_shard_migration_plan(prepared)
    }

    pub fn propose_expired_meta_shard_owner_failover_prepared(
        &mut self,
        expired_owner_node_id: &str,
        replacement_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_shard_migration_plan_raft_entry(
            "expired_failover",
            expired_owner_node_id,
            replacement_owner_node_id,
            max_shards,
            now_ms,
        )?;
        self.propose_prepared_meta_shard_migration_plan(prepared)
    }

    pub fn propose_meta_shard_rebalance_prepared(
        &mut self,
        source_owner_node_id: &str,
        target_owner_node_id: &str,
        max_shards: usize,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_shard_migration_plan_raft_entry(
            "rebalance",
            source_owner_node_id,
            target_owner_node_id,
            max_shards,
            now_ms,
        )?;
        self.propose_prepared_meta_shard_migration_plan(prepared)
    }

}
