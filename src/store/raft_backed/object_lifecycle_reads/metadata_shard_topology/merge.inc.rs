impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_meta_shard_merge_raft_entry(
        &self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<PreparedMetadataShardMerge, ObjectError> {
        self.store.prepare_metadata_shard_merge_raft_entry(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
        )
    }

    pub fn prepare_meta_shard_merge_raft_entry_with_read_proof_linearizable(
        &self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMerge>, ObjectError> {
        self.prepare_meta_shard_merge_raft_entry_with_read_proof_linearizable_committed_at(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_meta_shard_merge_raft_entry_with_read_proof_linearizable_committed_at(
        &self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataShardMerge>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata shard merge",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_meta_shard_merge_raft_entry(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_meta_shard_merge(
        &mut self,
        prepared: PreparedMetadataShardMerge,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index != self.store.last_applied_index()
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata shard merge requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaShardMerge {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaShardMerge { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared metadata shard merge raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_meta_shard_merge_linearizable(
        &mut self,
        prepared: PreparedMetadataShardMerge,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_metadata_read_index("propose prepared metadata shard merge", prepared.now_ms)?;
        if prepared.metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "prepared metadata shard merge requires a metadata control proof checksum"
                    .to_string(),
            ));
        }
        self.propose_prepared_meta_shard_merge(prepared)
    }

    pub fn propose_proven_meta_shard_merge_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataShardMerge>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata shard merge requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata shard merge control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata shard merge requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata shard merge proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_meta_shard_merge_linearizable(proven_prepared.value)
    }

    pub fn propose_meta_shard_merge_prepared(
        &mut self,
        left_shard_id: &str,
        right_shard_id: &str,
        merged_shard_id: &str,
        owner_node_id: &str,
        epoch: u64,
        draining: bool,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_meta_shard_merge_raft_entry(
            left_shard_id,
            right_shard_id,
            merged_shard_id,
            owner_node_id,
            epoch,
            draining,
            now_ms,
        )?;
        self.propose_prepared_meta_shard_merge(prepared)
    }

}
