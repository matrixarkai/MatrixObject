impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_compact_metadata_tombstones_raft_entry(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<PreparedMetadataTombstoneCompaction, ObjectError> {
        self.store.prepare_metadata_tombstone_compaction_raft_entry(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
            self.store.last_metadata_commit_index(),
        )
    }

    pub fn prepare_compact_metadata_tombstones_raft_entry_with_read_proof_linearizable(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTombstoneCompaction>, ObjectError> {
        self.prepare_compact_metadata_tombstones_raft_entry_with_read_proof_linearizable_committed_at(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_compact_metadata_tombstones_raft_entry_with_read_proof_linearizable_committed_at(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataTombstoneCompaction>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata tombstone compaction",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_compact_metadata_tombstones_raft_entry(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_metadata_tombstone_compaction(
        &mut self,
        prepared: PreparedMetadataTombstoneCompaction,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata tombstone compaction requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaTombstoneCompaction {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaTombstoneCompaction { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata tombstone compaction raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_metadata_tombstone_compaction_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataTombstoneCompaction>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata tombstone compaction requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata tombstone compaction proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_metadata_tombstone_compaction(proven_prepared.value)
    }

    pub fn propose_compact_metadata_tombstones_prepared(
        &mut self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_compact_metadata_tombstones_raft_entry(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
        )?;
        self.propose_prepared_metadata_tombstone_compaction(prepared)
    }

}
