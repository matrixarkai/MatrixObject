// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_repair_planned_object_replicas_fenced(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        self.propose_repair_planned_object_replicas_fenced_with_plan_checksum(
            max_objects,
            marker,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_repair_planned_object_replicas_fenced_with_plan_checksum(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object replica repair",
            expected_page_route_control_checksum,
        )?;
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        let plan = self
            .store
            .plan_object_replica_repairs(max_objects, marker.as_ref())?;
        plan.verify_expected_plan_checksum(expected_plan_checksum)?;
        let mut report =
            self.propose_repair_object_replica_plan(plan, max_repair_chunks_per_object)?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        report.require_expected_fences(
            expected_plan_checksum,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_repair_planned_object_replicas_fenced_with_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.propose_repair_planned_object_replicas_fenced_with_read_proof_linearizable_committed_at(
            max_objects,
            marker,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_repair_planned_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.propose_repair_planned_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            max_objects,
            marker,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_repair_planned_object_replicas_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.propose_repair_planned_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            max_objects,
            marker,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_repair_planned_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "object replica repair",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_repair_planned_object_replicas_fenced_with_plan_checksum(
            max_objects,
            marker,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            expected_plan_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "object replica repair post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

}
