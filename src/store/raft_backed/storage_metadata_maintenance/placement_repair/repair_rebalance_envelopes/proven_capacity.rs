// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_proven_placement_capacity_rebalance_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedPlacementCapacityRebalanceEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.store.last_applied_index() < proven_envelope.proof.placement_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven root capacity rebalance proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.placement_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root capacity rebalance envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root capacity rebalance raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = self.prepare_placement_capacity_rebalance_raft_entry_linearizable(
            proven_envelope.envelope.max_objects as usize,
            proven_envelope.envelope.min_free_bytes,
        )?;
        let actual_envelope = prepared_placement_capacity_rebalance_envelope(&prepared);
        if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven root capacity rebalance envelope changed before proposal".to_string(),
            ));
        }
        prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        self.propose_prepared_placement_capacity_rebalance_linearizable(prepared)
    }

    pub fn propose_proven_placement_capacity_rebalance_envelope_fenced_with_read_proof_linearizable(
        &mut self,
        proven_envelope: ProvenPreparedPlacementCapacityRebalanceEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_proven_placement_capacity_rebalance_envelope_fenced_with_read_proof_linearizable_committed_at(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_proven_placement_capacity_rebalance_envelope_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        proven_envelope: ProvenPreparedPlacementCapacityRebalanceEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose proven root capacity rebalance envelope",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_proven_placement_capacity_rebalance_envelope_fenced(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose proven root capacity rebalance envelope post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_proven_placement_capacity_rebalance_envelope_fenced_with_route_proof_linearizable(
        &mut self,
        proven_envelope: ProvenPreparedPlacementCapacityRebalanceEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_proven_placement_capacity_rebalance_envelope_fenced_with_route_proof_linearizable_committed_at(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
            now_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_proven_placement_capacity_rebalance_envelope_fenced_with_route_proof_linearizable_committed_at(
        &mut self,
        proven_envelope: ProvenPreparedPlacementCapacityRebalanceEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "proven root capacity rebalance route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_proven_placement_capacity_rebalance_envelope_fenced_with_read_proof_linearizable_committed_at(
                proven_envelope,
                expected_envelope_checksum,
                expected_raft_entry_checksum,
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
