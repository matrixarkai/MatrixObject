// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_reclaim_chunk_replica(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_reclaim_chunk_replica_apply(replica_index, prepared)
            .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_reclaim_chunk_replica_apply(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
    ) -> Result<(ObjectMetadata, ReplicaPartialReclaimReport), ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk reclaim requires raft/store commit alignment".to_string(),
            ));
        }
        let report = self
            .store
            .clone()
            .apply_prepared_chunk_reclaim(replica_index, prepared.clone())?;
        let entry = self.raft.append(ObjectCommand::ApplyPreparedChunkReclaim {
            replica_index,
            prepared,
        })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk reclaim raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        Ok((metadata, report))
    }

    pub fn propose_prepared_reclaim_chunk_replica_with_status(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
    ) -> Result<PreparedChunkReclaimApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report) =
            self.propose_prepared_reclaim_chunk_replica_apply(replica_index, prepared)?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedChunkReclaimApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: false,
            replica_index,
            report,
            metadata,
        })
    }

    pub fn propose_prepared_reclaim_chunk_replica_fenced(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_prepared_reclaim_chunk_replica_fenced_apply(
            replica_index,
            prepared,
            expected_page_route_control_checksum,
        )
        .map(|(metadata, _)| metadata)
    }

    fn propose_prepared_reclaim_chunk_replica_fenced_apply(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(ObjectMetadata, ReplicaPartialReclaimReport), ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "prepared chunk replica reclaim",
            expected_page_route_control_checksum,
        )?;
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk reclaim requires expected route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk reclaim requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk reclaim route checksum does not match caller fence".to_string(),
            ));
        }
        self.propose_prepared_reclaim_chunk_replica_apply(replica_index, prepared)
    }

    pub fn propose_prepared_reclaim_chunk_replica_fenced_with_status(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkReclaimApplyReport, ObjectError> {
        let mutation_id = prepared.raft_entry_checksum.clone();
        let mutation_request_checksum = prepared.raft_entry_checksum.clone();
        let (metadata, report) = self.propose_prepared_reclaim_chunk_replica_fenced_apply(
            replica_index,
            prepared,
            expected_page_route_control_checksum,
        )?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(PreparedChunkReclaimApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: false,
            replica_index,
            report,
            metadata,
        })
    }

    pub fn propose_proven_reclaim_chunk_replica_fenced(
        &mut self,
        replica_index: usize,
        proven_prepared: ProvenStorageRead<PreparedChunkReplicaReclaim>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk reclaim route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk reclaim storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_reclaim_chunk_replica_fenced(
            replica_index,
            proven_prepared.value,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_proven_reclaim_chunk_replica_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedChunkReclaimEnvelope,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
        expected_plan_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim envelope requires expected route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.envelope.expected_route_control_checksum
                != expected_page_route_control_checksum
            || proven_envelope.envelope.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk reclaim envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim envelope checksum mismatch".to_string(),
            ));
        }
        let expected_plan_checksum = expected_plan_checksum.as_ref();
        if expected_plan_checksum.is_empty()
            || proven_envelope.envelope.plan_checksum != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim envelope plan checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim envelope raft entry checksum mismatch".to_string(),
            ));
        }
        let prepared = self.prepare_reclaim_chunk_replica_raft_entry_bounded_fenced(
            proven_envelope.envelope.replica_index,
            proven_envelope.envelope.max_chunks as usize,
            proven_envelope.envelope.max_rewrite_bytes,
            proven_envelope.envelope.min_discarded_bytes,
            proven_envelope.envelope.min_discard_ratio_ppm,
            proven_envelope.envelope.max_discard_ranges as usize,
            proven_envelope.envelope.max_trim_bytes,
            proven_envelope.envelope.now_ms,
            expected_page_route_control_checksum,
        )?;
        let actual_envelope = make_prepared_chunk_reclaim_envelope(&prepared);
        if actual_envelope != proven_envelope.envelope {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk reclaim envelope changed before proposal".to_string(),
            ));
        }
        self.propose_prepared_reclaim_chunk_replica_fenced(
            proven_envelope.envelope.replica_index,
            prepared,
            expected_page_route_control_checksum,
        )
    }

}
