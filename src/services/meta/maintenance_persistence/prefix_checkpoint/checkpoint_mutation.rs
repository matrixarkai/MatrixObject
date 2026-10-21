// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn commit_change_checkpoint(
        &mut self,
        checkpoint: MetadataChangeCheckpoint,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        validate_meta_key(&checkpoint.consumer_id)?;
        validate_meta_key(&checkpoint.prefix)?;
        if checkpoint.owner.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata checkpoint owner must be set".to_string(),
            ));
        }
        if checkpoint.after_change_sequence >= self.next_change_sequence {
            return Err(ObjectError::PreconditionFailed(
                "metadata checkpoint change sequence is ahead of applied changes".to_string(),
            ));
        }
        if checkpoint.after_commit_index > self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata checkpoint commit index is ahead of metaserver".to_string(),
            ));
        }
        let checkpoint_key = change_checkpoint_key(&checkpoint.consumer_id, &checkpoint.prefix);
        if let Some(existing) = self.change_checkpoints.get(&checkpoint_key) {
            if checkpoint.after_change_sequence < existing.after_change_sequence
                || checkpoint.after_commit_index < existing.after_commit_index
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata checkpoint must not move backward".to_string(),
                ));
            }
        }
        self.change_checkpoints
            .insert(checkpoint_key, checkpoint.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(checkpoint)
    }

    pub fn prepare_change_checkpoint_raft_entry(
        &self,
        checkpoint: MetadataChangeCheckpoint,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataChangeCheckpoint, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        validate_meta_key(&checkpoint.consumer_id)?;
        validate_meta_key(&checkpoint.prefix)?;
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata checkpoint prepare previous commit fence mismatch".to_string(),
            ));
        }
        if checkpoint.owner.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata checkpoint owner must be set".to_string(),
            ));
        }
        if checkpoint.after_change_sequence >= self.next_change_sequence {
            return Err(ObjectError::PreconditionFailed(
                "metadata checkpoint change sequence is ahead of applied changes".to_string(),
            ));
        }
        if checkpoint.after_commit_index > self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata checkpoint commit index is ahead of metaserver".to_string(),
            ));
        }
        let checkpoint_key = change_checkpoint_key(&checkpoint.consumer_id, &checkpoint.prefix);
        if let Some(existing) = self.change_checkpoints.get(&checkpoint_key) {
            if checkpoint.after_change_sequence < existing.after_change_sequence
                || checkpoint.after_commit_index < existing.after_commit_index
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata checkpoint must not move backward".to_string(),
                ));
            }
        }
        let request_checksum = metadata_change_checkpoint_request_checksum(&checkpoint);
        let metadata_control_proof_now_ms = checkpoint.update_time_ms;
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(metadata_control_proof_now_ms);
        let mut prepared = PreparedMetadataChangeCheckpoint {
            checkpoint,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            metadata_control_proof_now_ms,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_change_checkpoint_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_change_checkpoint(
        &mut self,
        prepared: PreparedMetadataChangeCheckpoint,
    ) -> Result<MetadataChangeCheckpoint, ObjectError> {
        validate_idempotency_key(&prepared.idempotency_key)?;
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared checkpoint previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared checkpoint commit index must be contiguous".to_string(),
            ));
        }
        let expected_checksum =
            prepared_metadata_change_checkpoint_checksum(&PreparedMetadataChangeCheckpoint {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared checkpoint raft entry checksum mismatch".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.metadata_control_proof_now_ms,
        )?;
        let request_checksum = metadata_change_checkpoint_request_checksum(&prepared.checkpoint);
        if request_checksum != prepared.request_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared checkpoint request checksum mismatch".to_string(),
            ));
        }
        let checkpoint = prepared.checkpoint;
        validate_meta_key(&checkpoint.consumer_id)?;
        validate_meta_key(&checkpoint.prefix)?;
        if checkpoint.owner.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata checkpoint owner must be set".to_string(),
            ));
        }
        if checkpoint.after_change_sequence >= self.next_change_sequence {
            return Err(ObjectError::PreconditionFailed(
                "metadata checkpoint change sequence is ahead of applied changes".to_string(),
            ));
        }
        if checkpoint.after_commit_index > prepared.prepared_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared checkpoint after_commit_index exceeds apply commit".to_string(),
            ));
        }
        let checkpoint_key = change_checkpoint_key(&checkpoint.consumer_id, &checkpoint.prefix);
        if let Some(existing) = self.change_checkpoints.get(&checkpoint_key) {
            if checkpoint.after_change_sequence < existing.after_change_sequence
                || checkpoint.after_commit_index < existing.after_commit_index
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata checkpoint must not move backward".to_string(),
                ));
            }
        }
        self.change_checkpoints
            .insert(checkpoint_key, checkpoint.clone());
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        Ok(checkpoint)
    }

}
