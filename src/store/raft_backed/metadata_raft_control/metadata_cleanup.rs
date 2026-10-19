// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_compact_metadata_tombstones(
        &mut self,
        prefix: &str,
        after_key: Option<String>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CompactMetaTombstones {
            prefix: prefix.to_string(),
            after_key,
            max_records,
            max_compact_bytes,
        })
    }

    pub fn propose_delete_meta_by_prefix(
        &mut self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::DeleteMetaByPrefix {
            prefix: prefix.to_string(),
            max_records,
            lease_owner,
            lease_epoch,
            now_ms,
        })
    }

    pub fn prepare_delete_meta_by_prefix_raft_entry(
        &self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataPrefixDelete, ObjectError> {
        self.store.prepare_metadata_prefix_delete_raft_entry(
            prefix,
            max_records,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_delete_meta_by_prefix_raft_entry_bounded(
        &self,
        prefix: &str,
        max_records: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<PreparedMetadataPrefixDelete, ObjectError> {
        self.store
            .prepare_metadata_prefix_delete_raft_entry_bounded(
                prefix,
                max_records,
                max_payload_bytes,
                lease_owner,
                lease_epoch,
                now_ms,
                idempotency_key,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_delete_meta_by_prefix_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataPrefixDelete>, ObjectError> {
        self.prepare_delete_meta_by_prefix_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            prefix,
            max_records,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_metadata_control_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_delete_meta_by_prefix_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataPrefixDelete>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven metadata prefix delete",
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
        let prepared = self.prepare_delete_meta_by_prefix_raft_entry(
            prefix,
            max_records,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata prefix delete control proof checksum mismatch".to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn prepare_delete_meta_by_prefix_raft_entry_bounded_with_read_proof_linearizable_fenced(
        &self,
        prefix: &str,
        max_records: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
    ) -> Result<ProvenMetadataRead<PreparedMetadataPrefixDelete>, ObjectError> {
        self.prepare_delete_meta_by_prefix_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
            prefix,
            max_records,
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

    pub fn prepare_delete_meta_by_prefix_raft_entry_bounded_with_read_proof_linearizable_fenced_committed_at(
        &self,
        prefix: &str,
        max_records: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_metadata_control_proof_checksum: &str,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<PreparedMetadataPrefixDelete>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "prepare proven bounded metadata prefix delete",
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
        let prepared = self.prepare_delete_meta_by_prefix_raft_entry_bounded(
            prefix,
            max_records,
            max_payload_bytes,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        if prepared.metadata_control_proof_checksum != expected_metadata_control_proof_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared bounded metadata prefix delete control proof checksum mismatch"
                    .to_string(),
            ));
        }
        Ok(proven_metadata_read(prepared, &readiness))
    }

    pub fn propose_prepared_delete_meta_by_prefix(
        &mut self,
        prepared: PreparedMetadataPrefixDelete,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata prefix delete requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaPrefixDelete {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaPrefixDelete { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata prefix delete raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_delete_meta_by_prefix_fenced(
        &mut self,
        proven_prepared: ProvenMetadataRead<PreparedMetadataPrefixDelete>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata prefix delete requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata prefix delete control proof checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata prefix delete requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_metadata_commit_index() < proven_prepared.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata prefix delete proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_prepared.proof.metadata_commit_index
            )));
        }
        self.propose_prepared_delete_meta_by_prefix(proven_prepared.value)
    }

    pub fn propose_delete_meta_by_prefix_prepared(
        &mut self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_delete_meta_by_prefix_raft_entry(
            prefix,
            max_records,
            lease_owner.as_deref(),
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_delete_meta_by_prefix(prepared)
    }

    pub fn propose_delete_meta_by_prefix_prepared_bounded(
        &mut self,
        prefix: &str,
        max_records: usize,
        max_payload_bytes: u64,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_delete_meta_by_prefix_raft_entry_bounded(
            prefix,
            max_records,
            max_payload_bytes,
            lease_owner.as_deref(),
            lease_epoch,
            now_ms,
            idempotency_key,
        )?;
        self.propose_prepared_delete_meta_by_prefix(prepared)
    }
}
