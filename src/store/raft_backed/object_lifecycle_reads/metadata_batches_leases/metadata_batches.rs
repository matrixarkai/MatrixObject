// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_check_metadata_read_set(
        &mut self,
        conditions: Vec<MetadataReadCondition>,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CheckMetaReadSet { conditions, now_ms })
    }

    pub fn propose_commit_metadata_batch_idempotent(
        &mut self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitMetaBatchIdempotent {
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn prepare_commit_metadata_batch_raft_entry(
        &self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataBatch, ObjectError> {
        self.store.prepare_metadata_batch_raft_entry(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_commit_metadata_batch_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataBatch>, ObjectError> {
        self.prepare_commit_metadata_batch_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_commit_metadata_batch_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataBatch>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata batch",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let expected_metadata_control_proof_checksum =
            if expected_metadata_control_proof_checksum.is_empty() {
                readiness.metadata_control_proof_checksum.as_str()
            } else {
                expected_metadata_control_proof_checksum
            };
        let prepared = self.prepare_commit_metadata_batch_raft_entry(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata batch control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_with_read_proof_linearizable_fenced(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedPageBatchWrite>, ObjectError> {
        self.prepare_page_batch_write_raft_entry_bounded_from_with_read_proof_linearizable_fenced(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            idempotency_key,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_from_with_read_proof_linearizable_fenced(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedPageBatchWrite>, ObjectError> {
        self.prepare_page_batch_write_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
            idempotency_key,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedPageBatchWrite>, ObjectError> {
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_storage_control_read_index_committed_at(
            "prepare block batch write",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self
            .store
            .prepare_page_batch_write_raft_entry_bounded_fenced_from(
                writes,
                max_write_bytes,
                max_chunk_write_count,
                write_start_range_index,
                idempotency_key,
                self.store.last_applied_index(),
                expected_route_control_checksum,
            )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_commit_metadata_batch_raft_entry_bounded(
        &self,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataBatch, ObjectError> {
        self.store.prepare_metadata_batch_raft_entry_bounded(
            mutations,
            max_mutations,
            max_payload_bytes,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_commit_metadata_batch_raft_entry_bounded_with_read_proof_linearizable_fenced(
        &self,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataBatch>, ObjectError> {
        self.prepare_commit_metadata_batch_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            mutations,
            max_mutations,
            max_payload_bytes,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_commit_metadata_batch_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
        &self,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataBatch>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven bounded metadata batch",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let expected_metadata_control_proof_checksum =
            if expected_metadata_control_proof_checksum.is_empty() {
                readiness.metadata_control_proof_checksum.as_str()
            } else {
                expected_metadata_control_proof_checksum
            };
        let prepared = self.prepare_commit_metadata_batch_raft_entry_bounded(
            mutations,
            max_mutations,
            max_payload_bytes,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared bounded metadata batch control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_metadata_batch(
        &mut self,
        prepared: PreparedMetadataBatch,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata batch requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaBatch {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaBatch { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata batch raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_metadata_batch_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataBatch>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata batch requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata batch control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata batch requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata batch proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_metadata_batch(proven_prepared.value)
    }

}
