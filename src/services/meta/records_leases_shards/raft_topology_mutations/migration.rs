// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn prepare_shard_migration_raft_entry(
        &self,
        shard_id: &str,
        new_owner_node_id: &str,
        expected_owner_node_id: Option<&str>,
        expected_epoch: u64,
        new_epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMigration, ObjectError> {
        validate_meta_key(shard_id)?;
        validate_meta_key(new_owner_node_id)?;
        if let Some(expected_owner_node_id) = expected_owner_node_id {
            validate_node_id(expected_owner_node_id)?;
        }
        let before = self
            .shards
            .get(shard_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata shard missing".to_string()))?;
        if expected_owner_node_id.is_some_and(|owner| before.owner_node_id != owner) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration owner fence mismatch".to_string(),
            ));
        }
        if before.epoch != expected_epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration epoch fence mismatch".to_string(),
            ));
        }
        if new_epoch <= before.epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration epoch must advance".to_string(),
            ));
        }
        let mut after = before.clone();
        after.owner_node_id = new_owner_node_id.to_string();
        after.epoch = new_epoch;
        after.draining = draining;
        self.validate_shard_image(&after)?;
        self.validate_shard_owner_session(&after, now_ms)?;
        let request_checksum = metadata_shard_request_checksum(
            "prepared-migration",
            &[before.clone(), after.clone()],
            &[&now_ms.to_string()],
        );
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataShardMigration {
            before,
            after,
            now_ms,
            before_state_fingerprint: self.metadata_state_fingerprint(),
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index: self.next_commit_index.saturating_sub(1),
            prepared_commit_index: self.next_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_shard_migration_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_shard_migration(
        &mut self,
        prepared: PreparedMetadataShardMigration,
    ) -> Result<MetadataShard, ObjectError> {
        let expected_checksum =
            prepared_metadata_shard_migration_checksum(&PreparedMetadataShardMigration {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared shard migration raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_shard_request_checksum(
                "prepared-migration",
                &[prepared.before.clone(), prepared.after.clone()],
                &[&prepared.now_ms.to_string()],
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
        if self.prepared_shard_migration_already_applied(&prepared) {
            return Ok(prepared.after);
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
        if self.shards.get(&prepared.before.shard_id) != Some(&prepared.before) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration before image mismatch".to_string(),
            ));
        }
        if prepared.after.shard_id != prepared.before.shard_id
            || prepared.after.start_key != prepared.before.start_key
            || prepared.after.end_key != prepared.before.end_key
            || prepared.after.epoch <= prepared.before.epoch
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard migration after image mismatch".to_string(),
            ));
        }
        self.migrate_shard(
            &prepared.before.shard_id,
            &prepared.after.owner_node_id,
            prepared.before.epoch,
            prepared.after.epoch,
            prepared.after.draining,
            prepared.now_ms,
        )
    }
}
