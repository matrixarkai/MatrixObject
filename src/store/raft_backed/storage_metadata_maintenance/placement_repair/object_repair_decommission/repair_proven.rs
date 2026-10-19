// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_repair_proven_object_replica_plan_fenced(
        &mut self,
        proven_plan: ProvenMetadataRead<ObjectReplicaRepairPlan>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        self.propose_repair_proven_object_replica_plan_fenced_with_plan_checksum(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_repair_proven_object_replica_plan_fenced_with_plan_checksum(
        &mut self,
        proven_plan: ProvenMetadataRead<ObjectReplicaRepairPlan>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "proven object replica repair",
            expected_page_route_control_checksum,
        )?;
        if self.store.last_metadata_commit_index() < proven_plan.proof.metadata_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven object replica repair metadata proof is not applied: applied={} required={}",
                self.store.last_metadata_commit_index(),
                proven_plan.proof.metadata_commit_index
            )));
        }
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        proven_plan
            .value
            .verify_expected_plan_checksum(expected_plan_checksum)?;
        let mut report = self
            .propose_repair_object_replica_plan(proven_plan.value, max_repair_chunks_per_object)?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.expected_plan_checksum = expected_plan_checksum.to_string();
        Ok(report)
    }

    pub fn propose_repair_proven_object_replica_plan_fenced_with_read_proof_linearizable(
        &mut self,
        proven_plan: ProvenMetadataRead<ObjectReplicaRepairPlan>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.propose_repair_proven_object_replica_plan_fenced_with_plan_checksum_and_read_proof_linearizable(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
            now_ms,
        )
    }

    pub fn propose_repair_proven_object_replica_plan_fenced_with_plan_checksum_and_read_proof_linearizable(
        &mut self,
        proven_plan: ProvenMetadataRead<ObjectReplicaRepairPlan>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.propose_repair_proven_object_replica_plan_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_repair_proven_object_replica_plan_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        proven_plan: ProvenMetadataRead<ObjectReplicaRepairPlan>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.propose_repair_proven_object_replica_plan_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_repair_proven_object_replica_plan_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
        &mut self,
        proven_plan: ProvenMetadataRead<ObjectReplicaRepairPlan>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairRunReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "proven object replica repair",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self.propose_repair_proven_object_replica_plan_fenced_with_plan_checksum(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            expected_plan_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "proven object replica repair post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }

}
