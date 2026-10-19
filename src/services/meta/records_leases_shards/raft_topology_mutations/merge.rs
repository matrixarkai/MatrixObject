// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn prepare_shard_merge_raft_entry(
        &self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMerge, ObjectError> {
        validate_meta_key(left_shard_id)?;
        validate_meta_key(right_shard_id)?;
        validate_meta_key(merged_shard_id)?;
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
                "metadata prepared merge shards must be adjacent".to_string(),
            ));
        }
        if epoch <= left.epoch.max(right.epoch) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared merge epoch must advance source epochs".to_string(),
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
        let request_checksum = metadata_shard_request_checksum(
            "prepared-merge",
            &[left.clone(), right.clone(), merged.clone()],
            &[&now_ms.to_string()],
        );
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataShardMerge {
            left,
            right,
            merged,
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
        prepared.raft_entry_checksum = prepared_metadata_shard_merge_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_shard_merge(
        &mut self,
        prepared: PreparedMetadataShardMerge,
    ) -> Result<MetadataShard, ObjectError> {
        let expected_checksum =
            prepared_metadata_shard_merge_checksum(&PreparedMetadataShardMerge {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared shard merge raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_shard_request_checksum(
                "prepared-merge",
                &[
                    prepared.left.clone(),
                    prepared.right.clone(),
                    prepared.merged.clone(),
                ],
                &[&prepared.now_ms.to_string()],
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared shard merge request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard merge commit index must be adjacent".to_string(),
            ));
        }
        if self.prepared_shard_merge_already_applied(&prepared) {
            return Ok(prepared.merged);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard merge previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard merge state changed before apply".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.now_ms,
        )?;
        if self.shards.get(&prepared.left.shard_id) != Some(&prepared.left)
            || self.shards.get(&prepared.right.shard_id) != Some(&prepared.right)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard merge source image mismatch".to_string(),
            ));
        }
        if prepared.left.end_key != prepared.right.start_key
            || prepared.merged.start_key != prepared.left.start_key
            || prepared.merged.end_key != prepared.right.end_key
            || prepared.merged.epoch <= prepared.left.epoch.max(prepared.right.epoch)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared shard merge after image mismatch".to_string(),
            ));
        }
        self.merge_shards(
            &prepared.left.shard_id,
            &prepared.right.shard_id,
            &prepared.merged.shard_id,
            &prepared.merged.owner_node_id,
            prepared.merged.epoch,
            prepared.merged.draining,
            prepared.now_ms,
        )
    }

}
