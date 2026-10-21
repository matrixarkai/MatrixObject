// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_prepared_chunk_capacity(
        &mut self,
        prepared: PreparedChunkCapacityConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk capacity requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedChunkCapacity {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedChunkCapacity { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk capacity raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    fn prepared_or_caller_route_checksum(
        expected_route_control_checksum: Option<&str>,
        prepared_route_control_checksum: &str,
        context: &str,
    ) -> Result<String, ObjectError> {
        if let Some(checksum) = expected_route_control_checksum {
            if checksum.is_empty() {
                return Err(ObjectError::PreconditionFailed(format!(
                    "{} requires non-empty expected route control checksum",
                    context
                )));
            }
            return Ok(checksum.to_string());
        }
        if prepared_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} requires prepared route control checksum",
                context
            )));
        }
        Ok(prepared_route_control_checksum.to_string())
    }

    pub fn propose_prepared_chunk_capacity_linearizable_fenced(
        &mut self,
        prepared: PreparedChunkCapacityConfig,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum
            .filter(|checksum| !checksum.is_empty())
            .ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "prepared chunk capacity requires expected route control checksum".to_string(),
                )
            })?;
        self.ensure_storage_control_read_index(
            "propose prepared chunk replica capacity",
            Some(expected_route_control_checksum),
        )?;
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk capacity requires prepared route control checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared chunk capacity route checksum does not match caller fence".to_string(),
            ));
        }
        self.propose_prepared_chunk_capacity(prepared)
    }

    pub fn propose_proven_chunk_capacity_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedChunkCapacityConfig>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk capacity requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk capacity route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk capacity requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk capacity storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_chunk_capacity_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
        )
    }

    pub fn propose_configure_chunk_replica_capacity_prepared(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_capacity_raft_entry(replica_index, capacity_bytes)?;
        self.propose_prepared_chunk_capacity(prepared)
    }

    pub fn propose_configure_all_eligible_chunk_capacity_prepared(
        &mut self,
        capacity_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared =
            self.prepare_configure_all_eligible_chunk_capacity_raft_entry(capacity_bytes)?;
        self.propose_prepared_chunk_capacity(prepared)
    }

    pub fn propose_configure_chunk_replica_capacity_prepared_linearizable(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_capacity_prepared_linearizable_fenced(
            replica_index,
            capacity_bytes,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_capacity_prepared_linearizable_fenced(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_capacity_raft_entry_linearizable_fenced(
                replica_index,
                capacity_bytes,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica capacity prepared linearizable",
        )?;
        self.propose_prepared_chunk_capacity_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_all_eligible_chunk_capacity_prepared_linearizable(
        &mut self,
        capacity_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_all_eligible_chunk_capacity_prepared_linearizable_fenced(
            capacity_bytes,
            None,
        )
    }

    pub fn propose_configure_all_eligible_chunk_capacity_prepared_linearizable_fenced(
        &mut self,
        capacity_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_all_eligible_chunk_capacity_raft_entry_linearizable_fenced(
                capacity_bytes,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure all-eligible chunk capacity prepared linearizable",
        )?;
        self.propose_prepared_chunk_capacity_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }
}
