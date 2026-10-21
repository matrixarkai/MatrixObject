// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_proven_replica_service_mode_fenced(
        &mut self,
        proven_prepared: ProvenStorageRead<PreparedReplicaServiceModeConfig>,
        expected_route_control_checksum: impl AsRef<str>,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let expected_route_control_checksum = expected_route_control_checksum.as_ref();
        if expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk service-mode requires expected route control checksum".to_string(),
            ));
        }
        if proven_prepared.proof.control_checksum != expected_route_control_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk service-mode route checksum mismatch: expected {} actual {}",
                expected_route_control_checksum, proven_prepared.proof.control_checksum
            )));
        }
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "proven chunk service-mode requires expected raft entry checksum".to_string(),
            ));
        }
        proven_prepared
            .value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        if self.store.last_applied_index() < proven_prepared.proof.storage_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "proven chunk service-mode storage proof is not applied: applied={} required={}",
                self.store.last_applied_index(),
                proven_prepared.proof.storage_applied_index
            )));
        }
        self.propose_prepared_replica_service_mode_linearizable_fenced(
            proven_prepared.value,
            Some(expected_route_control_checksum),
        )
    }

    pub fn propose_configure_chunk_replica_service_mode_prepared(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_chunk_replica_service_mode_raft_entry(
            replica_index,
            accepting_writes,
            draining,
            frozen,
        )?;
        self.propose_prepared_replica_service_mode(prepared)
    }

    pub fn propose_configure_all_eligible_replica_service_mode_prepared(
        &mut self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_configure_all_eligible_replica_service_mode_raft_entry(
            accepting_writes,
            draining,
            frozen,
        )?;
        self.propose_prepared_replica_service_mode(prepared)
    }

    pub fn propose_configure_chunk_replica_service_mode_prepared_linearizable(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_chunk_replica_service_mode_prepared_linearizable_fenced(
            replica_index,
            accepting_writes,
            draining,
            frozen,
            None,
        )
    }

    pub fn propose_configure_chunk_replica_service_mode_prepared_linearizable_fenced(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_chunk_replica_service_mode_raft_entry_linearizable_fenced(
                replica_index,
                accepting_writes,
                draining,
                frozen,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure chunk replica service-mode prepared linearizable",
        )?;
        self.propose_prepared_replica_service_mode_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }

    pub fn propose_configure_all_eligible_replica_service_mode_prepared_linearizable(
        &mut self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_configure_all_eligible_replica_service_mode_prepared_linearizable_fenced(
            accepting_writes,
            draining,
            frozen,
            None,
        )
    }

    pub fn propose_configure_all_eligible_replica_service_mode_prepared_linearizable_fenced(
        &mut self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self
            .prepare_configure_all_eligible_replica_service_mode_raft_entry_linearizable_fenced(
                accepting_writes,
                draining,
                frozen,
                expected_route_control_checksum,
            )?;
        let route_checksum = Self::prepared_or_caller_route_checksum(
            expected_route_control_checksum,
            &prepared.expected_route_control_checksum,
            "configure all-eligible chunk service-mode prepared linearizable",
        )?;
        self.propose_prepared_replica_service_mode_linearizable_fenced(
            prepared,
            Some(route_checksum.as_str()),
        )
    }
}
