impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_chunk_state_handoffs_linearizable_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        source_replica_index: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<Vec<ReplicaStateHandoff>, ObjectError> {
        self.ensure_page_route_control_ready(
            "chunk state handoff prepare",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store.prepare_chunk_state_handoffs(
            chunk_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
        )
    }

    pub fn prepare_chunk_state_handoffs_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        source_replica_index: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<Vec<ReplicaStateHandoff>>, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let handoffs = self.prepare_chunk_state_handoffs_linearizable_fenced(
            chunk_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenStorageRead {
            value: handoffs,
            proof,
        })
    }

    pub fn prepare_chunk_state_handoff_envelope_linearizable_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        source_replica_index: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedChunkStateHandoffEnvelope, ObjectError> {
        let expected_page_route_control_checksum =
            expected_page_route_control_checksum.as_ref().to_string();
        let handoffs = self.prepare_chunk_state_handoffs_linearizable_fenced(
            chunk_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
            &expected_page_route_control_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        if !expected_page_route_control_checksum.is_empty()
            && route_control_proof.state_checksum != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk state handoff envelope route control checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, route_control_proof.state_checksum
            )));
        }
        Ok(make_prepared_chunk_state_handoff_envelope(
            chunk_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
            &handoffs,
            &route_control_proof,
        ))
    }

    pub fn prepare_chunk_state_handoff_envelope_with_read_proof_linearizable_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        source_replica_index: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenPreparedChunkStateHandoffEnvelope, ObjectError> {
        let requested_read_index = self.raft.commit_index();
        let envelope = self.prepare_chunk_state_handoff_envelope_linearizable_fenced(
            chunk_id,
            offset,
            length,
            source_replica_index,
            max_payload_bytes,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof(requested_read_index);
        Ok(ProvenPreparedChunkStateHandoffEnvelope { envelope, proof })
    }

    pub fn propose_chunk_state_handoffs_linearizable_fenced(
        &mut self,
        target_replica_index: usize,
        handoffs: Vec<ReplicaStateHandoff>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_chunk_state_handoffs_linearizable_fenced_apply(
            target_replica_index,
            handoffs,
            expected_page_route_control_checksum,
        )
        .map(|(metadata, _)| metadata)
    }

    fn propose_chunk_state_handoffs_linearizable_fenced_apply(
        &mut self,
        target_replica_index: usize,
        handoffs: Vec<ReplicaStateHandoff>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(ObjectMetadata, RepairStats), ObjectError> {
        let expected_page_route_control_checksum =
            expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "chunk state handoff apply",
            &expected_page_route_control_checksum,
        )?;
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk state handoff requires a block route control checksum".to_string(),
            ));
        }
        let command = ObjectCommand::ApplyChunkStateHandoffs {
            target_replica_index,
            handoffs,
            expected_page_route_control_checksum,
        };
        let entry = self.raft.append(command)?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "chunk state handoff raft index is not contiguous".to_string(),
            ));
        }
        let metadata = self.store.apply_raft_entry(&entry)?;
        let stats = RepairStats {
            checked_chunks: metadata.version,
            repaired_replicas: metadata.length,
            failed_repairs: metadata.update_time_ms,
            degraded_chunks: metadata.length,
        };
        Ok((metadata, stats))
    }

    pub fn propose_chunk_state_handoffs_linearizable_fenced_with_status(
        &mut self,
        target_replica_index: usize,
        handoffs: Vec<ReplicaStateHandoff>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ReplicaStateHandoffApplyReport, ObjectError> {
        let handoff_count = handoffs.len() as u64;
        let payload_bytes = handoffs.iter().fold(0u64, |sum, handoff| {
            sum.saturating_add(handoff.state.data.len() as u64)
        });
        let payload_checksum = chunk_state_handoff_payload_checksum(&handoffs);
        let mutation_id = payload_checksum.clone();
        let mutation_request_checksum = payload_checksum.clone();
        let (metadata, stats) = self.propose_chunk_state_handoffs_linearizable_fenced_apply(
            target_replica_index,
            handoffs,
            expected_page_route_control_checksum,
        )?;
        let mutation_commit_index = self.raft.commit_index();
        Ok(ReplicaStateHandoffApplyReport {
            mutation_id,
            mutation_request_checksum,
            mutation_commit_index,
            next_log_index: mutation_commit_index.saturating_add(1),
            committed: true,
            prepared: false,
            external_raft_required: false,
            deduped: false,
            target_replica_index,
            handoff_count,
            payload_bytes,
            payload_checksum,
            stats,
            metadata,
        })
    }

    pub fn propose_proven_chunk_state_handoffs_linearizable_fenced(
        &mut self,
        target_replica_index: usize,
        proven_handoffs: ProvenStorageRead<Vec<ReplicaStateHandoff>>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "proven chunk state handoff requires a block route control checksum".to_string(),
            ));
        }
        if proven_handoffs.proof.control_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk state handoff route checksum mismatch: expected {} actual {}",
                expected_page_route_control_checksum, proven_handoffs.proof.control_checksum
            )));
        }
        if self.store.last_applied_index() < proven_handoffs.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk state handoff storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_handoffs.proof.storage_applied_index
            )));
        }
        self.propose_chunk_state_handoffs_linearizable_fenced(
            target_replica_index,
            proven_handoffs.value,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_proven_chunk_state_handoff_envelope_linearizable_fenced(
        &mut self,
        target_replica_index: usize,
        proven_envelope: ProvenPreparedChunkStateHandoffEnvelope,
        handoffs: Vec<ReplicaStateHandoff>,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_envelope_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        if expected_page_route_control_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "proven chunk state handoff envelope requires a block route control checksum"
                    .to_string(),
            ));
        }
        if proven_envelope.proof.control_checksum != expected_page_route_control_checksum
            || proven_envelope.envelope.route_control_proof_checksum
                != expected_page_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk state handoff envelope route checksum mismatch".to_string(),
            ));
        }
        if self.store.last_applied_index() < proven_envelope.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk state handoff envelope storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.storage_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk state handoff envelope checksum mismatch".to_string(),
            ));
        }
        let route_control_proof = self.store.chunk_service.route_control_proof();
        if route_control_proof.state_checksum != expected_page_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk state handoff envelope current route checksum mismatch".to_string(),
            ));
        }
        let actual_envelope = make_prepared_chunk_state_handoff_envelope(
            &proven_envelope.envelope.chunk_id,
            proven_envelope.envelope.offset,
            proven_envelope.envelope.length,
            proven_envelope.envelope.source_replica_index,
            proven_envelope.envelope.max_payload_bytes,
            &handoffs,
            &route_control_proof,
        );
        if actual_envelope != proven_envelope.envelope {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk state handoff envelope payload mismatch".to_string(),
            ));
        }
        self.propose_chunk_state_handoffs_linearizable_fenced(
            target_replica_index,
            handoffs,
            expected_page_route_control_checksum,
        )
    }

}
