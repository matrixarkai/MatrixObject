// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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

}
