impl MatrixMetaService {
    pub fn prepare_shard_split_raft_entry(
        &self,
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
    ) -> Result<PreparedMetadataShardSplit, ObjectError> {
        validate_meta_key(source_shard_id)?;
        validate_meta_key(split_key)?;
        validate_meta_key(left_shard_id)?;
        validate_meta_key(right_shard_id)?;
        let source =
            self.shards.get(source_shard_id).cloned().ok_or_else(|| {
                ObjectError::NotFound("metadata source shard missing".to_string())
            })?;
        if split_key <= source.start_key.as_str()
            || (!source.end_key.is_empty() && split_key >= source.end_key.as_str())
        {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared split key must be inside source shard".to_string(),
            ));
        }
        if epoch <= source.epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared split epoch must advance source epoch".to_string(),
            ));
        }
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
        let request_checksum = metadata_shard_request_checksum(
            "prepared-split",
            &[source.clone(), left.clone(), right.clone()],
            &[split_key, &now_ms.to_string()],
        );
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataShardSplit {
            source,
            left,
            right,
            split_key: split_key.to_string(),
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
        prepared.raft_entry_checksum = prepared_metadata_shard_split_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_shard_split(
        &mut self,
        prepared: PreparedMetadataShardSplit,
    ) -> Result<MetadataShardSplit, ObjectError> {
        let expected_checksum =
            prepared_metadata_shard_split_checksum(&PreparedMetadataShardSplit {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared shard split raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_shard_request_checksum(
                "prepared-split",
                &[
                    prepared.source.clone(),
                    prepared.left.clone(),
                    prepared.right.clone(),
                ],
                &[&prepared.split_key, &prepared.now_ms.to_string()],
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared shard split request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard split commit index must be adjacent".to_string(),
            ));
        }
        if self.prepared_shard_split_already_applied(&prepared) {
            return Ok(MetadataShardSplit {
                left: prepared.left,
                right: prepared.right,
            });
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard split previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard split state changed before apply".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.now_ms,
        )?;
        let current = self.shards.get(&prepared.source.shard_id).ok_or_else(|| {
            ObjectError::NotFound("metadata prepared split source missing".to_string())
        })?;
        if current != &prepared.source {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard split source image mismatch".to_string(),
            ));
        }
        if prepared.left.start_key != prepared.source.start_key
            || prepared.left.end_key != prepared.split_key
            || prepared.right.start_key != prepared.split_key
            || prepared.right.end_key != prepared.source.end_key
            || prepared.left.epoch <= prepared.source.epoch
            || prepared.right.epoch <= prepared.source.epoch
            || prepared.left.epoch != prepared.right.epoch
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard split after image mismatch".to_string(),
            ));
        }
        self.split_shard(
            &prepared.source.shard_id,
            &prepared.split_key,
            &prepared.left.shard_id,
            &prepared.right.shard_id,
            &prepared.left.owner_node_id,
            &prepared.right.owner_node_id,
            prepared.left.epoch.max(prepared.right.epoch),
            prepared.left.draining,
            prepared.right.draining,
            prepared.now_ms,
        )
    }

}
