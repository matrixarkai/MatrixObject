// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_rebalance_object_capacity_fenced(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectCapacityRebalanceRunReport, ObjectError> {
        self.propose_rebalance_object_capacity_fenced_with_plan_checksum(
            max_objects,
            min_free_bytes,
            marker,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_rebalance_object_capacity_fenced_with_plan_checksum(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectCapacityRebalanceRunReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object capacity rebalance",
            expected_page_route_control_checksum,
        )?;
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if expected_plan_checksum.is_empty() {
            let mut report =
                self.propose_rebalance_object_capacity(max_objects, min_free_bytes, marker)?;
            report.expected_route_control_checksum =
                expected_page_route_control_checksum.to_string();
            return report.require_expected_fences(
                expected_plan_checksum,
                expected_page_route_control_checksum,
            );
        }
        let plan =
            self.store
                .plan_object_capacity_rebalance(max_objects, min_free_bytes, marker.as_ref());
        plan.verify_expected_plan_checksum(expected_plan_checksum)?;
        let mut report = self.propose_rebalance_object_capacity_plan(plan);
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        report.require_expected_fences(
            expected_plan_checksum,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_rebalance_object_capacity_fenced_with_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.propose_rebalance_object_capacity_fenced_with_read_proof_linearizable_committed_at(
            max_objects,
            min_free_bytes,
            marker,
            expected_page_route_control_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_rebalance_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.propose_rebalance_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            max_objects,
            min_free_bytes,
            marker,
            expected_page_route_control_checksum,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_rebalance_object_capacity_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.propose_rebalance_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            max_objects,
            min_free_bytes,
            marker,
            expected_page_route_control_checksum,
            "",
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_rebalance_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "object capacity rebalance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_rebalance_object_capacity_fenced_with_plan_checksum(
            max_objects,
            min_free_bytes,
            marker,
            expected_page_route_control_checksum,
            expected_plan_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "object capacity rebalance post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_rebalance_proven_object_capacity_fenced(
        &mut self,
        proven_plan: ProvenRootRead<ObjectCapacityRebalanceReport>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectCapacityRebalanceRunReport, ObjectError> {
        self.propose_rebalance_proven_object_capacity_fenced_with_plan_checksum(
            proven_plan,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_rebalance_proven_object_capacity_fenced_with_plan_checksum(
        &mut self,
        proven_plan: ProvenRootRead<ObjectCapacityRebalanceReport>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectCapacityRebalanceRunReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "proven object capacity rebalance",
            expected_page_route_control_checksum,
        )?;
        if self.store.last_applied_index() < proven_plan.proof.root_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven object capacity rebalance root proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_plan.proof.root_applied_index
            )));
        }
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        proven_plan
            .value
            .verify_expected_plan_checksum(expected_plan_checksum)?;
        let mut report = self.propose_rebalance_object_capacity_plan(proven_plan.value);
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        report.require_expected_fences(
            expected_plan_checksum,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_rebalance_proven_object_capacity_fenced_with_read_proof_linearizable(
        &mut self,
        proven_plan: ProvenRootRead<ObjectCapacityRebalanceReport>,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.propose_rebalance_proven_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable(
            proven_plan,
            expected_page_route_control_checksum,
            "",
            now_ms,
        )
    }

    pub fn propose_rebalance_proven_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable(
        &mut self,
        proven_plan: ProvenRootRead<ObjectCapacityRebalanceReport>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.propose_rebalance_proven_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            expected_page_route_control_checksum,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_rebalance_proven_object_capacity_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        proven_plan: ProvenRootRead<ObjectCapacityRebalanceReport>,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.propose_rebalance_proven_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            expected_page_route_control_checksum,
            "",
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_rebalance_proven_object_capacity_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
        &mut self,
        proven_plan: ProvenRootRead<ObjectCapacityRebalanceReport>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectCapacityRebalanceRunReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "proven object capacity rebalance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_rebalance_proven_object_capacity_fenced_with_plan_checksum(
            proven_plan,
            expected_page_route_control_checksum,
            expected_plan_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "proven object capacity rebalance post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }
}
