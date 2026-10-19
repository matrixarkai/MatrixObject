// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_page_receipt_retention(
        &mut self,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_page_receipt_retention_fenced(retention, None)
    }

    pub fn propose_configure_page_receipt_retention_fenced(
        &mut self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store
            .chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        self.propose(ObjectCommand::ConfigurePageReceiptRetention { retention })
    }

    pub fn propose_configure_page_receipt_retention_linearizable_fenced(
        &mut self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_storage_control_read_index_committed_at(
            "configure block receipt retention",
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.propose_configure_page_receipt_retention_fenced(
            retention,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_configure_page_receipt_retention_raft_entry(
        &self,
        retention: usize,
    ) -> Result<PreparedPageReceiptRetentionConfig, ObjectError> {
        self.store
            .prepare_page_receipt_retention_raft_entry(retention, self.store.last_applied_index())
    }

    pub fn prepare_configure_page_receipt_retention_raft_entry_linearizable_fenced(
        &self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageReceiptRetentionConfig, ObjectError> {
        self.ensure_storage_control_read_index(
            "prepare block receipt retention",
            expected_route_control_checksum,
        )?;
        self.store
            .prepare_page_receipt_retention_raft_entry_fenced(
                retention,
                self.store.last_applied_index(),
                expected_route_control_checksum,
            )
    }

    pub fn prepare_configure_page_receipt_retention_raft_entry_linearizable(
        &self,
        retention: usize,
    ) -> Result<PreparedPageReceiptRetentionConfig, ObjectError> {
        self.prepare_configure_page_receipt_retention_raft_entry_linearizable_fenced(
            retention, None,
        )
    }

    pub fn prepare_configure_page_receipt_retention_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ProvenStorageRead<PreparedPageReceiptRetentionConfig>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let prepared = self
            .prepare_configure_page_receipt_retention_raft_entry_linearizable_fenced(
                retention,
                expected_route_control_checksum,
            )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn propose_prepared_page_receipt_retention(
        &mut self,
        prepared: PreparedPageReceiptRetentionConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page receipt retention requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedPageReceiptRetention {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedPageReceiptRetention { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared page receipt retention raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_page_receipt_retention_linearizable_fenced(
        &mut self,
        prepared: PreparedPageReceiptRetentionConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum
            .filter(|checksum| !checksum.is_empty())
            .ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "prepared page receipt retention requires expected route control checksum"
                        .to_string(),
                )
            })?;
        self.ensure_storage_control_read_index(
            "propose prepared block receipt retention",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared page receipt retention requires prepared route control checksum"
                    .to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared page receipt retention route checksum does not match caller fence"
                    .to_string(),
            ));
        }
        self.propose_prepared_page_receipt_retention(prepared)
    }

    pub fn propose_proven_page_receipt_retention_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedPageReceiptRetentionConfig>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page receipt retention requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page receipt retention route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven page receipt retention requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven page receipt retention storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_page_receipt_retention_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
        )
    }

    pub fn propose_configure_page_receipt_retention_prepared(
        &mut self,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_page_receipt_retention_raft_entry(retention)?;
        self.propose_prepared_page_receipt_retention(prepared)
    }

    pub fn propose_configure_page_receipt_retention_prepared_linearizable_fenced(
        &mut self,
        retention: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_page_receipt_retention_raft_entry_linearizable_fenced(
                retention,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure block receipt retention prepared linearizable",
        )?;
        self.propose_prepared_page_receipt_retention_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_page_receipt_retention_prepared_linearizable(
        &mut self,
        retention: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_page_receipt_retention_prepared_linearizable_fenced(retention, None)
    }

}
