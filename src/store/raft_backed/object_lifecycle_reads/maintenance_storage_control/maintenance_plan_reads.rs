// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn plan_run_meta_maintenance(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        self.plan_run_meta_maintenance_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_run_meta_maintenance_committed_at(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        let prepared = self.prepare_run_meta_maintenance_raft_entry_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            min_read_index,
            min_commit_index,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "plan metadata maintenance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        Ok(metadata_maintenance_plan_from_prepared(prepared, readiness))
    }

    pub fn plan_run_meta_maintenance_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        self.plan_run_meta_maintenance_with_payload_budget_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            max_payload_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_run_meta_maintenance_with_payload_budget_committed_at(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
        let prepared = self
            .prepare_run_meta_maintenance_raft_entry_with_payload_budget_committed_at(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                max_payload_bytes,
                min_read_index,
                min_commit_index,
            )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "plan bounded metadata maintenance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        Ok(metadata_maintenance_plan_from_prepared(prepared, readiness))
    }

    pub fn plan_run_meta_maintenance_with_read_proof_linearizable(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
    ) -> Result<ProvenMetadataRead<MetadataMaintenancePlan>, ObjectError> {
        self.plan_run_meta_maintenance_with_payload_budget_and_read_proof_linearizable_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            0,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_run_meta_maintenance_with_payload_budget_and_read_proof_linearizable(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ProvenMetadataRead<MetadataMaintenancePlan>, ObjectError> {
        self.plan_run_meta_maintenance_with_payload_budget_and_read_proof_linearizable_committed_at(
            now_ms,
            lease_prefix,
            lease_limit,
            session_limit,
            transaction_limit,
            max_payload_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_run_meta_maintenance_with_payload_budget_and_read_proof_linearizable_committed_at(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetadataMaintenancePlan>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metadata maintenance plan",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = if max_payload_bytes == 0 {
            self.prepare_run_meta_maintenance_raft_entry_committed_at(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                min_read_index,
                min_commit_index,
            )?
        } else {
            self.prepare_run_meta_maintenance_raft_entry_with_payload_budget_committed_at(
                now_ms,
                lease_prefix,
                lease_limit,
                session_limit,
                transaction_limit,
                max_payload_bytes,
                min_read_index,
                min_commit_index,
            )?
        };
        let plan = metadata_maintenance_plan_from_prepared(prepared, readiness.clone());
        Ok(proven_metadata_read(plan, &readiness))
    }

    pub fn plan_run_meta_maintenance_fenced(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        expected_plan_checksum: &str,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
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
            "fenced metadata maintenance plan",
            now_ms,
            self.raft.commit_index(),
            0,
        )?;
        let plan = metadata_maintenance_plan_from_prepared_fenced(
            prepared,
            readiness,
            expected_plan_checksum,
        );
        if !expected_plan_checksum.is_empty() && plan.actual_plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, plan.actual_plan_checksum
            )));
        }
        Ok(plan)
    }

    pub fn plan_run_meta_maintenance_fenced_with_payload_budget(
        &self,
        now_ms: u64,
        lease_prefix: &str,
        lease_limit: usize,
        session_limit: usize,
        transaction_limit: usize,
        max_payload_bytes: u64,
        expected_plan_checksum: &str,
    ) -> Result<MetadataMaintenancePlan, ObjectError> {
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
            "fenced bounded metadata maintenance plan",
            now_ms,
            self.raft.commit_index(),
            0,
        )?;
        let plan = metadata_maintenance_plan_from_prepared_fenced(
            prepared,
            readiness,
            expected_plan_checksum,
        );
        if !expected_plan_checksum.is_empty() && plan.actual_plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata maintenance plan checksum mismatch: expected {} actual {}",
                expected_plan_checksum, plan.actual_plan_checksum
            )));
        }
        Ok(plan)
    }
}
