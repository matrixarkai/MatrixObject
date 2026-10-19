// SPDX-License-Identifier: Apache-2.0
impl MatrixChunkService {
    pub fn configure_chunk_trim_flow(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica.configure_trim_flow(rate_bytes_per_sec, burst_bytes, now_ms)?;
        }
        Ok(())
    }

    pub fn configure_chunk_trim_flow_ops(
        &mut self,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) {
                continue;
            }
            replica.configure_trim_flow_ops(rate_ops_per_sec, burst_ops, now_ms)?;
        }
        Ok(())
    }

    pub fn configure_chunk_replica_trim_flow(
        &mut self,
        replica_index: usize,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get_mut(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.configure_trim_flow(rate_bytes_per_sec, burst_bytes, now_ms)
    }

    pub fn configure_chunk_replica_trim_flow_ops(
        &mut self,
        replica_index: usize,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get_mut(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index out of range".to_string())
        })?;
        replica.configure_trim_flow_ops(rate_ops_per_sec, burst_ops, now_ms)
    }

    pub(crate) fn chunk_trim_flow_prepared_aggregate_checksum(
        &self,
        target_replica_index: Option<usize>,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        prepared_commit_index: u64,
    ) -> Result<(String, u64), ObjectError> {
        let target_count = target_replica_index.map_or_else(
            || {
                self.replicas
                    .len()
                    .saturating_sub(self.quarantined_replicas.len())
                    .saturating_sub(self.disabled_chunk_endpoints.len())
            },
            |_| 1,
        );
        let mut material = Vec::with_capacity(
            encoded_string_len("MOS-RUST-BLOCK-CHUNK-TRIM-FLOW-PREPARED1")
                + std::mem::size_of::<u64>() * 9
                + target_count.saturating_mul(384),
        );
        append_string(&mut material, "MOS-RUST-BLOCK-CHUNK-TRIM-FLOW-PREPARED1");
        append_u64(&mut material, expected_previous_commit_index);
        append_u64(&mut material, prepared_commit_index);
        append_u64(&mut material, target_replica_index.map_or(0, |_| 1));
        append_u64(
            &mut material,
            target_replica_index.map_or(0, |index| index as u64),
        );
        append_u64(&mut material, rate_bytes_per_sec);
        append_u64(&mut material, burst_bytes);
        append_u64(&mut material, rate_ops_per_sec);
        append_u64(&mut material, burst_ops);
        append_u64(&mut material, now_ms);
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
                "chunk replica trim-flow target is out of range".to_string(),
            ));
        }
        Ok((etag(&material), replica_count))
    }
}
