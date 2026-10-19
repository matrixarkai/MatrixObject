impl MatrixPageService {
    fn cache_control_replica_indexes(
        &self,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
    ) -> Result<Vec<usize>, ObjectError> {
        if let Some(index) = target_replica_index {
            if index >= self.replicas.len() {
                return Err(ObjectError::InvalidArgument(
                    "chunk cache target replica index out of range".to_string(),
                ));
            }
            if include_disabled_replicas
                || (!self.quarantined_replicas.contains(&index)
                    && !self.disabled_chunk_endpoints.contains(&index))
            {
                return Ok(vec![index]);
            }
            return Ok(Vec::new());
        }
        Ok(self
            .replicas
            .iter()
            .enumerate()
            .filter_map(|(index, _)| {
                if include_disabled_replicas
                    || (!self.quarantined_replicas.contains(&index)
                        && !self.disabled_chunk_endpoints.contains(&index))
                {
                    Some(index)
                } else {
                    None
                }
            })
            .collect())
    }

    pub fn set_read_cache_max_bytes_per_replica(&self, max_bytes: u64) {
        for replica in &self.replicas {
            replica.set_read_cache_max_bytes(max_bytes);
        }
    }

    pub fn set_read_cache_max_cacheable_read_bytes_per_replica(&self, max_bytes: u64) {
        for replica in &self.replicas {
            replica.set_read_cache_max_cacheable_read_bytes(max_bytes);
        }
    }

    pub fn set_read_cache_max_bytes_for_replica(
        &self,
        replica_index: usize,
        max_bytes: u64,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.set_read_cache_max_bytes(max_bytes);
        Ok(())
    }

    pub fn set_read_cache_max_cacheable_read_bytes_for_replica(
        &self,
        replica_index: usize,
        max_bytes: u64,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.set_read_cache_max_cacheable_read_bytes(max_bytes);
        Ok(())
    }

    pub(crate) fn chunk_read_cache_prepared_aggregate_checksum(
        &self,
        target_replica_index: Option<usize>,
        max_bytes_per_replica: u64,
        max_cacheable_read_bytes_per_replica: u64,
        expected_previous_commit_index: u64,
        prepared_commit_index: u64,
    ) -> Result<(String, u64), ObjectError> {
        let mut material = Vec::with_capacity(176 + self.replicas.len().saturating_mul(640));
        append_string(&mut material, "MOS-RUST-BLOCK-CHUNK-READ-CACHE-PREPARED1");
        append_u64(&mut material, expected_previous_commit_index);
        append_u64(&mut material, prepared_commit_index);
        append_u64(&mut material, target_replica_index.map_or(0, |_| 1));
        append_u64(
            &mut material,
            target_replica_index.map_or(0, |index| index as u64),
        );
        append_u64(&mut material, max_bytes_per_replica);
        append_u64(&mut material, max_cacheable_read_bytes_per_replica);
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        let mut replica_count = 0_u64;
        for (index, replica) in self.replicas.iter().enumerate() {
            if let Some(target) = target_replica_index {
                if index != target {
                    continue;
                }
            } else if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica_count = replica_count.saturating_add(1);
            append_u64(&mut material, index as u64);
            append_replica_service_control_proof(&mut material, &replica.control_proof());
        }
        if target_replica_index.is_some() && replica_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk replica read-cache target is out of range".to_string(),
            ));
        }
        Ok((etag(&material), replica_count))
    }

}
