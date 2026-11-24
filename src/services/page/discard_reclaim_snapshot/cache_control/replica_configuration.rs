// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
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

}
