// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn set_chunk_replica_capacity(
        &mut self,
        replica_index: usize,
        capacity_bytes: u64,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get_mut(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.set_capacity_bytes(capacity_bytes)
    }

    pub fn configure_all_eligible_chunk_replica_capacity(
        &mut self,
        capacity_bytes: u64,
    ) -> Result<u64, ObjectError> {
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        let mut applied = 0_u64;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica.set_capacity_bytes(capacity_bytes)?;
            applied = applied.saturating_add(1);
        }
        if applied == 0 {
            return Err(ObjectError::InvalidArgument(
                "pageserver has no eligible chunk replicas for capacity fanout".to_string(),
            ));
        }
        Ok(applied)
    }

    pub fn configure_chunk_replica_service_mode(
        &mut self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get_mut(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.configure_service_mode(accepting_writes, draining, frozen);
        Ok(())
    }

    pub fn configure_all_eligible_chunk_replica_service_mode(
        &mut self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) -> Result<u64, ObjectError> {
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        let mut applied = 0_u64;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica.configure_service_mode(accepting_writes, draining, frozen);
            applied = applied.saturating_add(1);
        }
        Ok(applied)
    }

    pub fn set_chunk_replica_capacities(&mut self, capacity_bytes: u64) -> Result<(), ObjectError> {
        for replica in &mut self.replicas {
            replica.set_capacity_bytes(capacity_bytes)?;
        }
        Ok(())
    }
}
