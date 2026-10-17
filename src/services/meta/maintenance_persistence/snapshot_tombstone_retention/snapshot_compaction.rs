impl MatrixMetaService {
    pub fn prepare_snapshot_compaction_raft_entry(
        &self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataSnapshotCompaction, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata snapshot compaction expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let before_state_fingerprint = self.metadata_state_fingerprint();
        let mut projected = self.clone();
        let report = projected.compact_raft_snapshot_retention(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
        )?;
        let snapshot = projected.export_snapshot();
        let request_checksum = metadata_snapshot_compaction_request_checksum(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
        );
        let mutation_id = metadata_snapshot_compaction_mutation_id(
            expected_previous_commit_index,
            expected_previous_commit_index.saturating_add(1),
            &request_checksum,
        );
        let mut prepared = PreparedMetadataSnapshotCompaction {
            mutation_id,
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
            report,
            before_state_fingerprint,
            snapshot,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_snapshot_compaction_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_snapshot_compaction(
        &mut self,
        prepared: PreparedMetadataSnapshotCompaction,
    ) -> Result<MetadataSnapshotCompactionReport, ObjectError> {
        let expected_checksum =
            prepared_metadata_snapshot_compaction_checksum(&PreparedMetadataSnapshotCompaction {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata snapshot compaction checksum mismatch".to_string(),
            ));
        }
        validate_idempotency_key(&prepared.mutation_id)?;
        if prepared.request_checksum
            != metadata_snapshot_compaction_request_checksum(
                prepared.snapshot_commit_index,
                prepared.max_change_records,
                prepared.max_batch_receipts,
            )
        {
            return Err(ObjectError::Corruption(
                "prepared metadata snapshot compaction request checksum mismatch".to_string(),
            ));
        }
        prepared.snapshot.validate()?;
        let prepared_snapshot_fingerprint = {
            let mut snapshot_service = MatrixMetaService::default();
            snapshot_service.install_snapshot(prepared.snapshot.clone())?;
            snapshot_service.metadata_state_fingerprint()
        };
        if prepared.prepared_commit_index <= self.next_commit_index.saturating_sub(1)
            && self.metadata_state_fingerprint() == prepared_snapshot_fingerprint
        {
            return Ok(prepared.report);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata snapshot compaction expected previous commit {} does not match current {}",
                prepared.expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        if prepared.before_state_fingerprint != self.metadata_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata snapshot compaction source state changed".to_string(),
            ));
        }
        if prepared.snapshot.next_commit_index != prepared.prepared_commit_index.saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared metadata snapshot compaction post-state commit index is invalid"
                    .to_string(),
            ));
        }
        if prepared.snapshot.raft_snapshot_floor_commit_index != prepared.snapshot_commit_index {
            return Err(ObjectError::Corruption(
                "prepared metadata snapshot compaction floor mismatch".to_string(),
            ));
        }
        let mut projected = self.clone();
        let current_report = projected.compact_raft_snapshot_retention(
            prepared.snapshot_commit_index,
            prepared.max_change_records,
            prepared.max_batch_receipts,
        )?;
        if current_report != prepared.report
            || projected.metadata_state_fingerprint() != prepared_snapshot_fingerprint
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata snapshot compaction no longer matches current state".to_string(),
            ));
        }
        self.install_snapshot(prepared.snapshot.clone())?;
        Ok(prepared.report)
    }
}
