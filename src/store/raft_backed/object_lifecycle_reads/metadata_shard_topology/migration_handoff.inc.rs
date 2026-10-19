// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_meta_shard_migration_raft_entry(
        &self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_owner_node_id: Option<&str>,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMigration, ObjectError> {
        self.store.prepare_metadata_shard_migration_raft_entry(
            shard_id,
            new_owner_node_id,
            expected_owner_node_id,
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
        )
    }

    pub fn propose_prepared_meta_shard_migration(
        &mut self,
        prepared: PreparedMetadataShardMigration,
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
            .apply_command(ObjectCommand::ApplyPreparedMetaShardMigration {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaShardMigration { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared metadata shard migration raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_meta_shard_migration_prepared(
        &mut self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_owner_node_id: Option<&str>,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_shard_migration_raft_entry(
            shard_id,
            new_owner_node_id,
            expected_owner_node_id,
            expected_epoch,
            new_epoch,
            draining,
            now_ms,
        )?;
        self.propose_prepared_meta_shard_migration(prepared)
    }

    pub fn prepare_meta_shard_record_handoff_raft_entry(
        &self,
        shard_id: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        include_deleted: bool,
        source_node_id: &str,
        expected_shard_owner: Option<&str>,
        expected_shard_epoch: Option<u64>,
        handoff_token: Option<&str>,
        apply_mutation_id: &str,
    ) -> Result<PreparedMetadataShardRecordHandoff, ObjectError> {
        self.store.prepare_metadata_shard_record_handoff_raft_entry(
            shard_id,
            marker,
            limit,
            max_payload_bytes,
            include_deleted,
            source_node_id,
            expected_shard_owner,
            expected_shard_epoch,
            handoff_token,
            apply_mutation_id,
        )
    }

    pub fn prepare_meta_shard_record_handoff_raft_entry_linearizable(
        &self,
        shard_id: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        include_deleted: bool,
        source_node_id: &str,
        expected_shard_owner: Option<&str>,
        expected_shard_epoch: Option<u64>,
        handoff_token: Option<&str>,
        apply_mutation_id: &str,
        now_ms: u64,
    ) -> Result<(PreparedMetadataShardRecordHandoff, MetadataRaftReadiness), ObjectError> {
        let commit_index = self.raft.commit_index();
        let readiness = self.metadata_raft_readiness(commit_index, now_ms);
        if self.store.last_applied_index() < commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata shard record handoff read index is not satisfied: applied={} required={}",
                self.store.last_applied_index(),
                commit_index
            )));
        }
        let prepared = self.prepare_meta_shard_record_handoff_raft_entry(
            shard_id,
            marker,
            limit,
            max_payload_bytes,
            include_deleted,
            source_node_id,
            expected_shard_owner,
            expected_shard_epoch,
            handoff_token,
            apply_mutation_id,
        )?;
        Ok((prepared, readiness))
    }

    pub fn propose_prepared_meta_shard_record_handoff(
        &mut self,
        prepared: PreparedMetadataShardRecordHandoff,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_meta_shard_record_handoff_apply(prepared)
    }

    fn propose_prepared_meta_shard_record_handoff_apply(
        &mut self,
        prepared: PreparedMetadataShardRecordHandoff,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index != self.store.last_metadata_commit_index()
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata shard record handoff requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaShardRecordHandoff {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaShardRecordHandoff { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared metadata shard record handoff raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    fn metadata_shard_record_handoff_apply_report(
        &self,
        prepared: &PreparedMetadataShardRecordHandoff,
        metadata: ObjectMetadata,
    ) -> Result<MetadataShardRecordHandoffApplyReport, ObjectError> {
        let receipt = self
            .store
            .meta_service
            .batch_request_receipts
            .get(&prepared.apply_mutation_id)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "prepared metadata shard record handoff receipt missing after apply"
                        .to_string(),
                )
            })?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(MetadataShardRecordHandoffApplyReport {
            mutation_id: prepared.apply_mutation_id.clone(),
            mutation_request_checksum: metadata_shard_record_handoff_request_checksum(prepared),
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: receipt.deduped,
            source_node_id: prepared.source_node_id.clone(),
            handoff_token: prepared.handoff_token.clone(),
            handoff_batch_checksum: prepared.handoff_batch_checksum.clone(),
            raft_entry_checksum: prepared.raft_entry_checksum.clone(),
            source_commit_index: prepared.source_commit_index,
            source_read_index: prepared.source_read_index,
            source_shard_topology_epoch: prepared.source_shard_topology_epoch,
            source_shard_topology_checksum: prepared.source_shard_topology_checksum.clone(),
            record_count: prepared.records.len() as u64,
            scanned_records: prepared.scanned_records,
            payload_bytes: prepared.payload_bytes,
            truncated: prepared.truncated,
            marker: prepared.marker.clone(),
            next_marker: prepared.next_marker.clone(),
            shard: prepared.shard.clone(),
            receipt,
            metadata,
        })
    }

    pub fn propose_prepared_meta_shard_record_handoff_with_status(
        &mut self,
        prepared: PreparedMetadataShardRecordHandoff,
    ) -> Result<MetadataShardRecordHandoffApplyReport, ObjectError> {
        let report_source = prepared.clone();
        let metadata = self.propose_prepared_meta_shard_record_handoff_apply(prepared)?;
        self.metadata_shard_record_handoff_apply_report(&report_source, metadata)
    }

    pub fn propose_meta_shard_record_handoff_prepared(
        &mut self,
        shard_id: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        include_deleted: bool,
        source_node_id: &str,
        expected_shard_owner: Option<&str>,
        expected_shard_epoch: Option<u64>,
        handoff_token: Option<&str>,
        apply_mutation_id: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_shard_record_handoff_raft_entry(
            shard_id,
            marker,
            limit,
            max_payload_bytes,
            include_deleted,
            source_node_id,
            expected_shard_owner,
            expected_shard_epoch,
            handoff_token,
            apply_mutation_id,
        )?;
        self.propose_prepared_meta_shard_record_handoff(prepared)
    }

}
