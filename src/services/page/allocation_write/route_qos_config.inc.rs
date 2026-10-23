// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn configure_chunk_endpoints(
        &mut self,
        endpoints: Vec<String>,
        disabled_endpoints: BTreeSet<usize>,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<(), ObjectError> {
        validate_page_chunk_endpoint_topology(
            &endpoints,
            &disabled_endpoints,
            write_quorum,
            read_quorum,
            self.replicas.len(),
        )?;
        // A REPLICA TAKEN OUT OF THE ENDPOINT SET stops receiving writes just as surely
        // as a quarantined one. One bump covers the call however many it disables: the
        // epoch counts membership changes, not replicas.
        let newly_disabled: BTreeSet<usize> = disabled_endpoints
            .iter()
            .filter(|index| !self.disabled_chunk_endpoints.contains(index))
            .copied()
            .collect();
        if !newly_disabled.is_empty() {
            self.record_replica_membership_change(&newly_disabled);
        }
        self.chunk_endpoints = endpoints;
        self.disabled_chunk_endpoints = disabled_endpoints;
        self.write_quorum = write_quorum;
        self.read_quorum = read_quorum;
        Ok(())
    }

    pub fn configure_route_qos(
        &mut self,
        config: ChunkRouteQosConfig,
    ) -> Result<ChunkRouteQosConfig, ObjectError> {
        let commit_index = self
            .chunk_route_control_next_log_index
            .max(self.chunk_route_control_commit_index.saturating_add(1));
        self.configure_route_qos_at_commit(config, commit_index)
    }

    pub(crate) fn configure_route_qos_at_commit(
        &mut self,
        config: ChunkRouteQosConfig,
        commit_index: u64,
    ) -> Result<ChunkRouteQosConfig, ObjectError> {
        validate_page_route_qos_config(&config)?;
        if commit_index == 0 {
            return Err(ObjectError::InvalidArgument(
                "page route control commit index must be positive".to_string(),
            ));
        }
        if commit_index <= self.chunk_route_control_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "page route control commit index is stale".to_string(),
            ));
        }
        self.route_qos_config = config.clone();
        *self.route_qos_state.borrow_mut() = ChunkRouteQosState::default();
        self.chunk_route_control_commit_index = commit_index;
        self.chunk_route_control_next_log_index = commit_index.saturating_add(1);
        Ok(config)
    }

    pub fn route_qos_config(&self) -> ChunkRouteQosConfig {
        self.route_qos_config.clone()
    }

    pub fn route_qos_state(&self) -> ChunkRouteQosState {
        self.route_qos_state.borrow().clone()
    }

    pub fn chunk_endpoints(&self) -> &[String] {
        &self.chunk_endpoints
    }

    pub fn disabled_chunk_endpoint_indices(&self) -> Vec<usize> {
        self.disabled_chunk_endpoints.iter().copied().collect()
    }

    fn replica_endpoint_disabled(&self, replica_index: usize) -> bool {
        self.disabled_chunk_endpoints.contains(&replica_index)
    }

    fn chunk_route_qos_enabled(&self) -> bool {
        self.route_qos_config.max_read_ops_per_window > 0
            || self.route_qos_config.max_read_bytes_per_window > 0
            || self.route_qos_config.max_write_ops_per_window > 0
            || self.route_qos_config.max_write_bytes_per_window > 0
            || self.route_qos_config.max_discard_ops_per_window > 0
            || self.route_qos_config.max_discard_bytes_per_window > 0
    }

    fn chunk_route_qos_pressure(&self) -> bool {
        let state = self.route_qos_state.borrow();
        self.chunk_route_qos_enabled()
            && ((self.route_qos_config.max_read_ops_per_window > 0
                && state.read_ops_in_window >= self.route_qos_config.max_read_ops_per_window)
                || (self.route_qos_config.max_read_bytes_per_window > 0
                    && state.read_bytes_in_window
                        >= self.route_qos_config.max_read_bytes_per_window)
                || (self.route_qos_config.max_write_ops_per_window > 0
                    && state.write_ops_in_window >= self.route_qos_config.max_write_ops_per_window)
                || (self.route_qos_config.max_write_bytes_per_window > 0
                    && state.write_bytes_in_window
                        >= self.route_qos_config.max_write_bytes_per_window)
                || (self.route_qos_config.max_discard_ops_per_window > 0
                    && state.discard_ops_in_window
                        >= self.route_qos_config.max_discard_ops_per_window)
                || (self.route_qos_config.max_discard_bytes_per_window > 0
                    && state.discard_bytes_in_window
                        >= self.route_qos_config.max_discard_bytes_per_window))
    }

    fn admit_page_route_qos(
        &self,
        read_ops: u64,
        read_bytes: u64,
        write_ops: u64,
        write_bytes: u64,
        discard_ops: u64,
        discard_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if !self.chunk_route_qos_enabled() {
            return Ok(());
        }
        let window_ms = self.route_qos_config.window_ms;
        let mut state = self.route_qos_state.borrow_mut();
        if state.window_start_ms == 0
            || now_ms < state.window_start_ms
            || now_ms.saturating_sub(state.window_start_ms) >= window_ms
        {
            state.window_start_ms = now_ms;
            state.read_ops_in_window = 0;
            state.read_bytes_in_window = 0;
            state.write_ops_in_window = 0;
            state.write_bytes_in_window = 0;
            state.discard_ops_in_window = 0;
            state.discard_bytes_in_window = 0;
        }
        let next_read_ops = state
            .read_ops_in_window
            .checked_add(read_ops)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("page route qos op overflow".to_string())
            })?;
        let next_read_bytes = state
            .read_bytes_in_window
            .checked_add(read_bytes)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("page route qos byte overflow".to_string())
            })?;
        let next_write_ops = state
            .write_ops_in_window
            .checked_add(write_ops)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("page route qos op overflow".to_string())
            })?;
        let next_write_bytes = state
            .write_bytes_in_window
            .checked_add(write_bytes)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("page route qos byte overflow".to_string())
            })?;
        let next_discard_ops = state
            .discard_ops_in_window
            .checked_add(discard_ops)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("page route qos op overflow".to_string())
            })?;
        let next_discard_bytes = state
            .discard_bytes_in_window
            .checked_add(discard_bytes)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("page route qos byte overflow".to_string())
            })?;
        if (self.route_qos_config.max_read_ops_per_window > 0
            && next_read_ops > self.route_qos_config.max_read_ops_per_window)
            || (self.route_qos_config.max_write_ops_per_window > 0
                && next_write_ops > self.route_qos_config.max_write_ops_per_window)
            || (self.route_qos_config.max_discard_ops_per_window > 0
                && next_discard_ops > self.route_qos_config.max_discard_ops_per_window)
        {
            state.throttled_ops = state.throttled_ops.saturating_add(
                read_ops
                    .saturating_add(write_ops)
                    .saturating_add(discard_ops),
            );
            return Err(ObjectError::PreconditionFailed(
                "page route qos throttled: operation window exceeded".to_string(),
            ));
        }
        if (self.route_qos_config.max_read_bytes_per_window > 0
            && next_read_bytes > self.route_qos_config.max_read_bytes_per_window)
            || (self.route_qos_config.max_write_bytes_per_window > 0
                && next_write_bytes > self.route_qos_config.max_write_bytes_per_window)
            || (self.route_qos_config.max_discard_bytes_per_window > 0
                && next_discard_bytes > self.route_qos_config.max_discard_bytes_per_window)
        {
            state.throttled_bytes = state.throttled_bytes.saturating_add(
                read_bytes
                    .saturating_add(write_bytes)
                    .saturating_add(discard_bytes),
            );
            return Err(ObjectError::PreconditionFailed(
                "page route qos throttled: byte window exceeded".to_string(),
            ));
        }
        state.read_ops_in_window = next_read_ops;
        state.read_bytes_in_window = next_read_bytes;
        state.write_ops_in_window = next_write_ops;
        state.write_bytes_in_window = next_write_bytes;
        state.discard_ops_in_window = next_discard_ops;
        state.discard_bytes_in_window = next_discard_bytes;
        Ok(())
    }
}
