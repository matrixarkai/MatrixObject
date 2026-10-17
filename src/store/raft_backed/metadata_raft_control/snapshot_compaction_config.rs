impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_compact_meta_snapshot_raft_entry_with_read_proof_linearizable_committed_at(
        &self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataSnapshotCompaction>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata snapshot compaction",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_compact_meta_snapshot_raft_entry(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
        )?;
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_meta_snapshot_compaction(
        &mut self,
        prepared: PreparedMetadataSnapshotCompaction,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata snapshot compaction requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaSnapshotCompaction {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaSnapshotCompaction { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata snapshot compaction raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_meta_snapshot_compaction_with_status(
        &mut self,
        prepared: PreparedMetadataSnapshotCompaction,
    ) -> Result<MetadataSnapshotCompactionApplyReport, ObjectError> {
        let compaction = prepared.report.clone();
        let mutation_id = prepared.mutation_id.clone();
        let mutation_request_checksum = prepared.request_checksum.clone();
        let mut snapshot_service = MatrixMetaService::default();
        snapshot_service.install_snapshot(prepared.snapshot.clone())?;
        let prepared_snapshot_fingerprint = snapshot_service.metadata_state_fingerprint();
        let deduped = self.store.last_metadata_commit_index() >= prepared.prepared_commit_index
            && self.store.meta_service.metadata_state_fingerprint()
                == prepared_snapshot_fingerprint;
        let metadata = self.propose_prepared_meta_snapshot_compaction(prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(MetadataSnapshotCompactionApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped,
            metadata: Some(metadata),
            compaction,
        })
    }

    pub fn propose_proven_meta_snapshot_compaction_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataSnapshotCompaction>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata snapshot compaction requires expected raft entry checksum"
                    .to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata snapshot compaction proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_meta_snapshot_compaction(proven_prepared.value)
    }

    pub fn propose_compact_meta_snapshot_prepared(
        &mut self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_compact_meta_snapshot_raft_entry(
            snapshot_commit_index,
            max_change_records,
            max_batch_receipts,
        )?;
        self.propose_prepared_meta_snapshot_compaction(prepared)
    }

    pub fn propose_configure_meta_retention(
        &mut self,
        change_log_retention_limit: usize,
        mutation_receipt_retention_limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaRetention {
            change_log_retention_limit,
            mutation_receipt_retention_limit,
        })
    }

    pub fn propose_configure_meta_admission_policy(
        &mut self,
        policy: MetadataAdmissionPolicy,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaAdmissionPolicy { policy })
    }

    pub fn propose_configure_meta_qos(
        &mut self,
        config: MetadataQosConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaQos { config })
    }

    pub fn propose_configure_meta_route_cache(
        &mut self,
        config: MetadataRouteCacheConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaRouteCache { config })
    }

}
