impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn watch_metadata_changes_linearizable(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MetadataWatchPage, MetadataRaftReadiness), ObjectError> {
        self.watch_metadata_changes_linearizable_committed_at(
            prefix,
            after_sequence,
            max_changes,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn watch_metadata_changes_linearizable_committed_at(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<(MetadataWatchPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata watch",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let page = self.store.watch_metadata_changes(
            prefix,
            after_sequence,
            max_changes,
            max_payload_bytes,
        )?;
        Ok((page, readiness))
    }

    pub fn watch_metadata_changes_with_read_proof_linearizable(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetadataWatchPage>, ObjectError> {
        self.watch_metadata_changes_with_read_proof_linearizable_committed_at(
            prefix,
            after_sequence,
            max_changes,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn watch_metadata_changes_with_read_proof_linearizable_committed_at(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataWatchPage>, ObjectError> {
        let (page, readiness) = self.watch_metadata_changes_linearizable_committed_at(
            prefix,
            after_sequence,
            max_changes,
            max_payload_bytes,
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

    pub fn propose_compact_meta_changefeed(
        &mut self,
        prefix: &str,
        max_changes: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CompactMetaChangefeed {
            prefix: prefix.to_string(),
            max_changes,
        })
    }

    pub fn propose_compact_meta_snapshot(
        &mut self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CompactMetaSnapshot {
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
        })
    }

    pub fn prepare_compact_meta_snapshot_raft_entry(
        &self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
    ) -> Result<PreparedMetadataSnapshotCompaction, ObjectError> {
        self.store.prepare_metadata_snapshot_compaction_raft_entry(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
            self.store.last_metadata_commit_index(),
        )
    }

    pub fn prepare_compact_meta_snapshot_raft_entry_with_read_proof_linearizable(
        &self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataSnapshotCompaction>, ObjectError> {
        self.prepare_compact_meta_snapshot_raft_entry_with_read_proof_linearizable_committed_at(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }
}
