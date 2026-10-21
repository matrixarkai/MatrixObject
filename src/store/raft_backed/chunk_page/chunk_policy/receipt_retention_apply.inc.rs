// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_chunk_receipt_retention(
        &mut self,
        prepared: PreparedChunkReceiptRetentionConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk receipt retention requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedChunkReceiptRetention {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedChunkReceiptRetention { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk receipt retention raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_chunk_receipt_retention_linearizable_fenced(
        &mut self,
        prepared: PreparedChunkReceiptRetentionConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum
            .filter(|checksum| !checksum.is_empty())
            .ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "prepared chunk receipt retention requires expected route control checksum"
                        .to_string(),
                )
            })?;
        self.ensure_storage_control_read_index(
            "propose prepared chunk receipt retention",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk receipt retention requires prepared route control checksum"
                    .to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk receipt retention route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_chunk_receipt_retention(prepared)
    }

    pub fn propose_proven_chunk_receipt_retention_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedChunkReceiptRetentionConfig>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk receipt retention requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk receipt retention route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk receipt retention requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk receipt retention storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_chunk_receipt_retention_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
        )
    }

    pub fn propose_configure_chunk_receipt_retention_prepared(
        &mut self,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_receipt_retention_raft_entry(retention)?;
        self.propose_prepared_chunk_receipt_retention(prepared)
    }

    pub fn propose_configure_chunk_replica_receipt_retention_prepared(
        &mut self,
        replica_index: usize,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_replica_receipt_retention_raft_entry(
            replica_index,
            retention,
        )?;
        self.propose_prepared_chunk_receipt_retention(prepared)
    }

    pub fn propose_configure_chunk_receipt_retention_prepared_linearizable_fenced(
        &mut self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_receipt_retention_raft_entry_linearizable_fenced(
                retention,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk receipt retention prepared linearizable",
        )?;
        self.propose_prepared_chunk_receipt_retention_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_receipt_retention_prepared_linearizable(
        &mut self,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_receipt_retention_prepared_linearizable_fenced(retention, None)
    }

    pub fn propose_configure_chunk_replica_receipt_retention_prepared_linearizable_fenced(
        &mut self,
        replica_index: usize,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_receipt_retention_raft_entry_linearizable_fenced(
                replica_index,
                retention,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica receipt retention prepared linearizable",
        )?;
        self.propose_prepared_chunk_receipt_retention_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_chunk_replica_receipt_retention_prepared_linearizable(
        &mut self,
        replica_index: usize,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_receipt_retention_prepared_linearizable_fenced(
            replica_index,
            retention,
            None,
        )
    }
}
