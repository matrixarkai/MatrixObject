impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_meta_maintenance(
        &mut self,
        prepared: PreparedMetadataMaintenance,
    ) -> Result<ObjectMetadata, ObjectError> {
        if prepared.metadata_read_index != 0 || prepared.metadata_raft_term != 0 {
            let readiness = self.ensure_metadata_read_index(
                "propose prepared metadata maintenance",
                prepared.now_ms,
            )?;
            if (prepared.metadata_read_index != 0
                && readiness.raft_log_commit_index != prepared.metadata_read_index)
                || (prepared.metadata_raft_term != 0
                    && readiness.raft_term != prepared.metadata_raft_term)
            {
                return Err(ObjectError::PreconditionFailed(
                    "prepared metadata maintenance read-index fence mismatch".to_string(),
                ));
            }
        }
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata maintenance requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaMaintenance {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaMaintenance { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata maintenance raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_proven_meta_maintenance_envelope_fenced(
        &mut self,
        proven_envelope: ProvenMetadataRead<PreparedMetadataMaintenanceEnvelope>,
        expected_metadata_control_proof_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_metadata_control_proof_checksum =
            expected_metadata_control_proof_checksum.as_ref();
        if expected_metadata_control_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata maintenance envelope requires expected metadata control proof checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.value.metadata_control_proof_checksum
            != expected_metadata_control_proof_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata maintenance envelope control proof checksum mismatch".to_string(),
            ));
        }
        if self.store.last_metadata_commit_index() < proven_envelope.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata maintenance envelope proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_envelope.proof.metadata_commit_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.value.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata maintenance envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.value.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata maintenance envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = if proven_envelope.value.max_payload_bytes == 0 {
            self.prepare_run_meta_maintenance_raft_entry(
                proven_envelope.value.now_ms,
                &proven_envelope.value.lease_prefix,
                proven_envelope.value.lease_limit as usize,
                proven_envelope.value.session_limit as usize,
                proven_envelope.value.transaction_limit as usize,
            )?
        } else {
            self.prepare_run_meta_maintenance_raft_entry_with_payload_budget(
                proven_envelope.value.now_ms,
                &proven_envelope.value.lease_prefix,
                proven_envelope.value.lease_limit as usize,
                proven_envelope.value.session_limit as usize,
                proven_envelope.value.transaction_limit as usize,
                proven_envelope.value.max_payload_bytes,
            )?
        };
        let actual_envelope = prepared_metadata_maintenance_envelope(&prepared);
        if actual_envelope != proven_envelope.value {
            return Err(ObjectError::PreconditionFailed(
                "proven metadata maintenance prepared envelope changed".to_string(),
            ));
        }
        self.propose_prepared_meta_maintenance(prepared)
    }

    pub fn propose_run_meta_maintenance_prepared(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_run_meta_maintenance_raft_entry(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
        )?;
        self.propose_prepared_meta_maintenance(prepared)
    }

    pub fn propose_run_meta_maintenance_fenced(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_plan_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_run_meta_maintenance_raft_entry_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            self.raft.commit_index(),
            0,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "fenced metadata maintenance proposal",
            now_ms,
            self.raft.commit_index(),
            0,
        )?;
        let actual_plan_checksum =
            metadata_maintenance_plan_from_prepared(prepared.clone(), readiness)
                .actual_plan_checksum;
        if !expected_plan_checksum.is_empty() && actual_plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, actual_plan_checksum
            )));
        }
        self.propose_prepared_meta_maintenance(prepared)
    }

    pub fn propose_run_meta_maintenance_fenced_with_payload_budget(
        &mut self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
        expected_plan_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_run_meta_maintenance_raft_entry_with_payload_budget_committed_at(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                max_payload_bytes,
                self.raft.commit_index(),
                0,
            )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "fenced bounded metadata maintenance proposal",
            now_ms,
            self.raft.commit_index(),
            0,
        )?;
        let actual_plan_checksum =
            metadata_maintenance_plan_from_prepared(prepared.clone(), readiness)
                .actual_plan_checksum;
        if !expected_plan_checksum.is_empty() && actual_plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, actual_plan_checksum
            )));
        }
        self.propose_prepared_meta_maintenance(prepared)
    }

    pub fn propose_run_proven_meta_maintenance_fenced(
        &mut self,
        proven_plan: ProvenMetadataRead<MetadataMaintenancePlan>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if !expected_plan_checksum.is_empty()
            && proven_plan.value.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata maintenance proven plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, proven_plan.value.plan_checksum
            )));
        }
        if self.store.last_metadata_commit_index() < proven_plan.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata maintenance proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_plan.proof.metadata_commit_index
            )));
        }
        if !proven_plan.value.can_apply {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven metadata maintenance plan cannot apply: {}",
                proven_plan.value.reason
            )));
        }
        self.propose_prepared_meta_maintenance(proven_plan.value.prepared)
    }

    pub fn plan_run_meta_integrity_maintenance_fenced(
        &self,
        now_ms: u64,
        tombstone_prefix: &str,
        max_sample_count: usize,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_integrity_plan_checksum: &str,
    ) -> Result<(MetadataIntegrityPlan, MetadataMaintenancePlan), ObjectError> {
        if expected_integrity_plan_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metaserver integrity maintenance requires an expected plan checksum".to_string(),
            ));
        }
        let integrity =
            self.plan_metadata_integrity_linearizable(now_ms, tombstone_prefix, max_sample_count)?;
        if integrity.plan_checksum != expected_integrity_plan_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "metaserver integrity plan checksum mismatch: expected {} actual {}",
                expected_integrity_plan_checksum, integrity.plan_checksum
            )));
        }
        let maintenance = self.plan_run_meta_maintenance(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
        )?;
        Ok((integrity, maintenance))
    }

    pub fn propose_run_meta_integrity_maintenance_fenced(
        &mut self,
        now_ms: u64,
        tombstone_prefix: &str,
        max_sample_count: usize,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_integrity_plan_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        let (integrity, _) = self.plan_run_meta_integrity_maintenance_fenced(
            now_ms,
            tombstone_prefix,
            max_sample_count,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            expected_integrity_plan_checksum,
        )?;
        let prepared = self.prepare_run_meta_maintenance_raft_entry(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
        )?;
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "integrity-fenced metadata maintenance requires raft/store commit alignment"
                    .to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyIntegrityFencedMetaMaintenance {
                prepared: prepared.clone(),
                expected_integrity_plan_checksum: expected_integrity_plan_checksum.to_string(),
                actual_integrity_plan_checksum: integrity.plan_checksum.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyIntegrityFencedMetaMaintenance {
                prepared,
                expected_integrity_plan_checksum: expected_integrity_plan_checksum.to_string(),
                actual_integrity_plan_checksum: integrity.plan_checksum,
            })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "integrity-fenced metadata maintenance raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }
}
