// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn install_snapshot(&mut self, snapshot: MetaSnapshot) -> Result<(), ObjectError> {
        snapshot.validate()?;
        self.metadata_snapshot_assembler.reset();
        *self.snapshot_export_cache.borrow_mut() = None;
        // The snapshot's pairs become records ordered by their own key. A record already
        // names the key it was filed under, so the pair's key is dropped rather than kept.
        self.records = snapshot
            .records
            .into_values()
            .map(KeyedRecord::new)
            .collect();
        self.leases = snapshot.leases;
        self.lease_receipts = snapshot.lease_receipts;
        self.coordinator_config = snapshot.coordinator_config;
        self.coordinator_config_receipts = snapshot.coordinator_config_receipts;
        self.coordinator_sessions = snapshot.coordinator_sessions;
        self.coordinator_session_receipts = snapshot.coordinator_session_receipts;
        self.shards = snapshot.shards;
        self.shard_receipts = snapshot.shard_receipts;
        self.batch_request_receipts = snapshot.batch_request_receipts;
        self.transactions = snapshot.transactions;
        self.transaction_receipts = snapshot.transaction_receipts;
        self.change_checkpoints = snapshot.change_checkpoints;
        self.changes = snapshot.changes;
        self.next_commit_index = snapshot.next_commit_index;
        self.next_change_sequence = snapshot.next_change_sequence;
        self.compacted_tombstone_count = snapshot.compacted_tombstone_count;
        self.last_tombstone_compaction_commit_index =
            snapshot.last_tombstone_compaction_commit_index;
        self.raft_snapshot_floor_commit_index = snapshot.raft_snapshot_floor_commit_index;
        self.raft_snapshot_compacted_change_count = snapshot.raft_snapshot_compacted_change_count;
        self.raft_snapshot_compacted_batch_receipt_count =
            snapshot.raft_snapshot_compacted_batch_receipt_count;
        self.change_log_retention_limit = snapshot.change_log_retention_limit;
        self.mutation_receipt_retention_limit = snapshot.mutation_receipt_retention_limit;
        self.admission_policy = snapshot.admission_policy;
        self.qos_config = snapshot.qos_config;
        self.qos_state = snapshot.qos_state;
        self.trim_meta_retention();
        Ok(())
    }

    pub fn install_raft_snapshot_bytes(
        &mut self,
        bytes: &[u8],
        min_commit_index: u64,
    ) -> Result<MetaSnapshot, ObjectError> {
        let snapshot = MetaSnapshot::from_bytes(bytes)?;
        if snapshot.next_commit_index.saturating_sub(1) < min_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata snapshot commit index is behind requested fence".to_string(),
            ));
        }
        self.install_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub fn install_raft_snapshot_bytes_report(
        &mut self,
        bytes: &[u8],
        min_commit_index: u64,
    ) -> Result<MetadataSnapshotInstallReport, ObjectError> {
        let snapshot = MetaSnapshot::from_bytes(bytes)?;
        if snapshot.next_commit_index.saturating_sub(1) < min_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata snapshot commit index is behind requested fence".to_string(),
            ));
        }
        let report = metadata_snapshot_install_report(
            &snapshot,
            bytes.len() as u64,
            etag(bytes),
            true,
            true,
        );
        self.install_snapshot(snapshot)?;
        Ok(report)
    }

    pub fn install_raft_snapshot_chunks<I>(
        &mut self,
        pages: I,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = MetaSnapshotChunk>,
    {
        self.install_raft_snapshot_chunks_report(pages, min_commit_index)
            .map(|report| report.install_progress)
    }

    pub fn install_raft_snapshot_chunks_report<I>(
        &mut self,
        pages: I,
        min_commit_index: u64,
    ) -> Result<MetadataSnapshotInstallReport, ObjectError>
    where
        I: IntoIterator<Item = MetaSnapshotChunk>,
    {
        let mut progress = self.metadata_snapshot_assembler.install_progress();
        for chunk in pages {
            if let Some(assembled) = self
                .metadata_snapshot_assembler
                .push_chunk_with_checksum(chunk)?
            {
                let mut report =
                    self.install_raft_snapshot_bytes_report(&assembled.bytes, min_commit_index)?;
                report.snapshot_checksum = assembled.full_checksum.clone();
                report.install_progress = snapshot_install_progress(
                    true,
                    false,
                    assembled.bytes.len() as u64,
                    assembled.bytes.len() as u64,
                    assembled.full_checksum,
                    report.last_applied_index,
                    0,
                );
                return Ok(report);
            }
            progress = self.metadata_snapshot_assembler.install_progress();
        }
        if progress.stream_active {
            return Ok(MetadataSnapshotInstallReport {
                installed: false,
                validated: false,
                raft_snapshot_install: true,
                snapshot_bytes: progress.next_offset,
                snapshot_checksum: progress.full_checksum.clone(),
                last_applied_index: progress.last_applied_index,
                install_progress: progress,
                ..MetadataSnapshotInstallReport::default()
            });
        }
        Err(ObjectError::InvalidArgument(
            "metadata snapshot chunk stream is empty".to_string(),
        ))
    }

    pub fn metadata_snapshot_install_progress(&self) -> SnapshotInstallProgress {
        self.metadata_snapshot_assembler.install_progress()
    }

    pub fn abort_metadata_snapshot_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.metadata_snapshot_assembler
            .abort_install(expected_stream_id)
    }
}
