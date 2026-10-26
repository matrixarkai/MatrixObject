// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_root_capacity_rebalance(
        &mut self,
        prepared: PreparedPlacementCapacityRebalance,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index()
            || prepared.expected_previous_commit_index
                != self.store.meta_service.placement_commit_index.saturating_sub(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared root capacity rebalance requires raft/root commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedRootCapacityRebalance {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedRootCapacityRebalance { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::Corruption(
                "prepared root capacity rebalance raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_prepared_root_capacity_rebalance_linearizable(
        &mut self,
        prepared: PreparedPlacementCapacityRebalance,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_root_read_index("propose prepared root capacity rebalance")?;
        self.propose_prepared_root_capacity_rebalance(prepared)
    }

    pub fn propose_prepared_root_capacity_rebalance_with_read_proof_linearizable(
        &mut self,
        prepared: PreparedPlacementCapacityRebalance,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_prepared_root_capacity_rebalance_with_read_proof_linearizable_committed_at(
            prepared,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_prepared_root_capacity_rebalance_with_read_proof_linearizable_committed_at(
        &mut self,
        prepared: PreparedPlacementCapacityRebalance,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose prepared root capacity rebalance",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_prepared_root_capacity_rebalance_linearizable(prepared)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose prepared root capacity rebalance post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_prepared_root_capacity_rebalance_with_route_proof_linearizable_fenced(
        &mut self,
        prepared: PreparedPlacementCapacityRebalance,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_prepared_root_capacity_rebalance_with_route_proof_linearizable_fenced_committed_at(
            prepared,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_prepared_root_capacity_rebalance_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        prepared: PreparedPlacementCapacityRebalance,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "prepared root capacity rebalance route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_prepared_root_capacity_rebalance_with_read_proof_linearizable_committed_at(
                prepared,
                now_ms,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            value: proven.value,
            proof: proven.proof,
            route_control_proof,
            expected_route_control_checksum: expected_checksum,
        })
    }

}
