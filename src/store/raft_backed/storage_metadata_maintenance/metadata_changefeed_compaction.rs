// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_compact_meta_changefeed_raft_entry(
        &self,
        prefix: &str,
        max_changes: usize,
    ) -> Result<PreparedMetadataChangefeedCompaction, ObjectError> {
        self.store
            .prepare_metadata_changefeed_compaction_raft_entry(
                prefix,
                max_changes,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_compact_meta_changefeed_raft_entry_with_read_proof_linearizable(
        &self,
        prefix: &str,
        max_changes: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataChangefeedCompaction>, ObjectError> {
        self.prepare_compact_meta_changefeed_raft_entry_with_read_proof_linearizable_committed_at(
            prefix,
            max_changes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_compact_meta_changefeed_raft_entry_with_read_proof_linearizable_committed_at(
        &self,
        prefix: &str,
        max_changes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataChangefeedCompaction>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata changefeed compaction",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_compact_meta_changefeed_raft_entry(prefix, max_changes)?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_meta_changefeed_compaction(
        &mut self,
        prepared: PreparedMetadataChangefeedCompaction,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata changefeed compaction requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaChangefeedCompaction {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaChangefeedCompaction { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata changefeed compaction raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_meta_changefeed_compaction_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataChangefeedCompaction>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata changefeed compaction requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata changefeed compaction proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_meta_changefeed_compaction(proven_prepared.value)
    }

    pub fn propose_compact_meta_changefeed_prepared(
        &mut self,
        prefix: &str,
        max_changes: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_compact_meta_changefeed_raft_entry(prefix, max_changes)?;
        self.propose_prepared_meta_changefeed_compaction(prepared)
    }

}
