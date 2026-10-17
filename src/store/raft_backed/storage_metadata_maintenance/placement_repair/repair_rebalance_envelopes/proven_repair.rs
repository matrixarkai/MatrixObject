impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_proven_root_placement_repair_envelope_fenced(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementRepairEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.store.last_applied_index() < proven_envelope.proof.root_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven root placement repair proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_envelope.proof.root_applied_index
            )));
        }
        let expected_envelope_checksum = expected_envelope_checksum.as_ref();
        if expected_envelope_checksum.is_empty()
            || proven_envelope.envelope.envelope_checksum != expected_envelope_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root placement repair envelope checksum mismatch".to_string(),
            ));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty()
            || proven_envelope.envelope.raft_entry_checksum != expected_raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "proven root placement repair raft entry checksum mismatch".to_string(),
            ));
        }
        match proven_envelope.envelope.repair_kind.as_str() {
            "draining" => {
                let prepared = self
                    .prepare_draining_root_placement_repair_raft_entry_linearizable(
                        proven_envelope.envelope.max_objects as usize,
                    )?;
                let actual_envelope = prepared_draining_root_placement_repair_envelope(&prepared);
                if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
                    return Err(ObjectError::PreconditionFailed(
                        "proven draining root placement repair envelope changed before proposal"
                            .to_string(),
                    ));
                }
                prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
                self.propose_prepared_draining_root_placement_repair_linearizable(prepared)
            }
            "unhealthy" => {
                let prepared = self
                    .prepare_unhealthy_root_placement_repair_raft_entry_linearizable(
                        proven_envelope.envelope.marker.as_ref(),
                        proven_envelope.envelope.max_objects as usize,
                        proven_envelope.envelope.expected_replica_count as usize,
                        proven_envelope.envelope.min_free_bytes,
                        proven_envelope.envelope.max_payload_bytes,
                    )?;
                let actual_envelope = prepared_unhealthy_root_placement_repair_envelope(&prepared);
                if actual_envelope.envelope_checksum != proven_envelope.envelope.envelope_checksum {
                    return Err(ObjectError::PreconditionFailed(
                        "proven unhealthy root placement repair envelope changed before proposal"
                            .to_string(),
                    ));
                }
                prepared.verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
                self.propose_prepared_unhealthy_root_placement_repair_linearizable(prepared)
            }
            other => Err(ObjectError::InvalidArgument(format!(
                "unsupported root placement repair envelope kind {}",
                other
            ))),
        }
    }

    pub fn propose_proven_root_placement_repair_envelope_fenced_with_read_proof_linearizable(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementRepairEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_proven_root_placement_repair_envelope_fenced_with_read_proof_linearizable_committed_at(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_proven_root_placement_repair_envelope_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        proven_envelope: ProvenPreparedRootPlacementRepairEnvelope,
        expected_envelope_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "propose proven root placement repair envelope",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_proven_root_placement_repair_envelope_fenced(
            proven_envelope,
            expected_envelope_checksum,
            expected_raft_entry_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "propose proven root placement repair envelope post-commit",
            now_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

}
