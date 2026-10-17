impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_decommission_proven_draining_object_replicas_fenced(
        &mut self,
        proven_plan: ProvenRootRead<RootPlacementRepairReport>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectReplicaDecommissionReport, ObjectError> {
        self.propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum(
        &mut self,
        proven_plan: ProvenRootRead<RootPlacementRepairReport>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
    ) -> Result<ObjectReplicaDecommissionReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "proven object replica decommission",
            expected_page_route_control_checksum,
        )?;
        if self.store.last_applied_index() < proven_plan.proof.root_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven object replica decommission root proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_plan.proof.root_applied_index
            )));
        }
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        let mut report = self.propose_decommission_draining_object_replica_plan(
            proven_plan.value,
            max_repair_chunks_per_object,
            expected_plan_checksum,
        )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.require_expected_fences(
            expected_plan_checksum,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_decommission_proven_draining_object_replicas_fenced_with_read_proof_linearizable(
        &mut self,
        proven_plan: ProvenRootRead<RootPlacementRepairReport>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaDecommissionReport>, ObjectError> {
        self.propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable(
        &mut self,
        proven_plan: ProvenRootRead<RootPlacementRepairReport>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaDecommissionReport>, ObjectError> {
        self.propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            expected_plan_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_decommission_proven_draining_object_replicas_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        proven_plan: ProvenRootRead<RootPlacementRepairReport>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaDecommissionReport>, ObjectError> {
        self.propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
            proven_plan,
            max_repair_chunks_per_object,
            expected_page_route_control_checksum,
            "",
            now_ms,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum_and_read_proof_linearizable_committed_at(
        &mut self,
        proven_plan: ProvenRootRead<RootPlacementRepairReport>,
        max_repair_chunks_per_object: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaDecommissionReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "proven object replica decommission",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let report = self
            .propose_decommission_proven_draining_object_replicas_fenced_with_plan_checksum(
                proven_plan,
                max_repair_chunks_per_object,
                expected_page_route_control_checksum,
                expected_plan_checksum,
            )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "proven object replica decommission post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(report, &readiness))
    }
}
