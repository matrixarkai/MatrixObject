// SPDX-License-Identifier: Apache-2.0
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
                "blockserver has no eligible chunk replicas for capacity fanout".to_string(),
            ));
        }
        Ok(applied)
    }

    pub(crate) fn chunk_capacity_prepared_aggregate_checksum(
        &self,
        replica_index: usize,
        capacity_bytes: u64,
        expected_previous_commit_index: u64,
        prepared_commit_index: u64,
    ) -> Result<(String, u64), ObjectError> {
        let mut material = Vec::with_capacity(
            encoded_string_len("MOS-RUST-BLOCK-CHUNK-CAPACITY-PREPARED1")
                .saturating_add(std::mem::size_of::<u64>() * 4)
                .saturating_add(self.replicas.len().saturating_mul(256)),
        );
        append_string(&mut material, "MOS-RUST-BLOCK-CHUNK-CAPACITY-PREPARED1");
        append_u64(&mut material, expected_previous_commit_index);
        append_u64(&mut material, prepared_commit_index);
        append_u64(&mut material, replica_index as u64);
        append_u64(&mut material, capacity_bytes);
        if replica_index == PAGE_CHUNK_CAPACITY_ALL_ELIGIBLE_TARGET {
            let quarantined = &self.quarantined_replicas;
            let disabled = &self.disabled_chunk_endpoints;
            let mut replica_count = 0_u64;
            for (index, replica) in self.replicas.iter().enumerate() {
                if quarantined.contains(&index) || disabled.contains(&index) {
                    continue;
                }
                replica_count = replica_count.saturating_add(1);
                append_u64(&mut material, index as u64);
                append_replica_service_control_proof(&mut material, &replica.control_proof());
            }
            if replica_count == 0 {
                return Err(ObjectError::InvalidArgument(
                    "blockserver has no eligible chunk replicas for capacity prepare".to_string(),
                ));
            }
            return Ok((etag(&material), replica_count));
        }
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        append_replica_service_control_proof(&mut material, &replica.control_proof());
        Ok((etag(&material), 1))
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

    pub(crate) fn replica_service_mode_prepared_aggregate_checksum(
        &self,
        replica_index: usize,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
        expected_previous_commit_index: u64,
        prepared_commit_index: u64,
    ) -> Result<(String, u64), ObjectError> {
        let mut material = Vec::with_capacity(
            encoded_string_len("MOS-RUST-BLOCK-CHUNK-SERVICE-MODE-PREPARED1")
                .saturating_add(std::mem::size_of::<u64>() * 6)
                .saturating_add(self.replicas.len().saturating_mul(256)),
        );
        append_string(&mut material, "MOS-RUST-BLOCK-CHUNK-SERVICE-MODE-PREPARED1");
        append_u64(&mut material, expected_previous_commit_index);
        append_u64(&mut material, prepared_commit_index);
        append_u64(&mut material, replica_index as u64);
        append_u64(
            &mut material,
            accepting_writes.map_or(2, |value| value as u64),
        );
        append_u64(&mut material, draining.map_or(2, |value| value as u64));
        append_u64(&mut material, frozen.map_or(2, |value| value as u64));
        if replica_index == PAGE_REPLICA_SERVICE_MODE_ALL_ELIGIBLE_TARGET {
            let quarantined = &self.quarantined_replicas;
            let disabled = &self.disabled_chunk_endpoints;
            let mut replica_count = 0_u64;
            for (index, replica) in self.replicas.iter().enumerate() {
                if quarantined.contains(&index) || disabled.contains(&index) {
                    continue;
                }
                replica_count = replica_count.saturating_add(1);
                append_u64(&mut material, index as u64);
                append_replica_service_control_proof(&mut material, &replica.control_proof());
            }
            return Ok((etag(&material), replica_count));
        }
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        append_replica_service_control_proof(&mut material, &replica.control_proof());
        Ok((etag(&material), 1))
    }

    pub fn set_chunk_replica_capacities(&mut self, capacity_bytes: u64) -> Result<(), ObjectError> {
        for replica in &mut self.replicas {
            replica.set_capacity_bytes(capacity_bytes)?;
        }
        Ok(())
    }
}
