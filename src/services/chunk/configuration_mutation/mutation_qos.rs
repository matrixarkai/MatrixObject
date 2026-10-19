// SPDX-License-Identifier: Apache-2.0
impl MatrixReplicaService {
    pub fn configure_mutation_qos(
        &mut self,
        config: ReplicaMutationQosConfig,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        let commit_index = self
            .chunk_mutation_control_next_log_index
            .max(self.chunk_mutation_control_commit_index.saturating_add(1));
        self.configure_mutation_qos_at_commit(config, commit_index)
    }

    pub(crate) fn configure_mutation_qos_at_commit(
        &mut self,
        config: ReplicaMutationQosConfig,
        commit_index: u64,
    ) -> Result<ReplicaMutationQosConfig, ObjectError> {
        validate_chunk_mutation_qos_config(&config)?;
        if commit_index == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk mutation control commit index must be positive".to_string(),
            ));
        }
        if commit_index <= self.chunk_mutation_control_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "chunk mutation control commit index is stale".to_string(),
            ));
        }
        self.mutation_qos_config = config.clone();
        self.mutation_qos_state = ReplicaMutationQosState::default();
        self.chunk_mutation_control_commit_index = commit_index;
        self.chunk_mutation_control_next_log_index = commit_index.saturating_add(1);
        Ok(config)
    }

    pub fn mutation_qos_config(&self) -> ReplicaMutationQosConfig {
        self.mutation_qos_config.clone()
    }

    pub fn mutation_qos_state(&self) -> ReplicaMutationQosState {
        self.mutation_qos_state.clone()
    }

    fn mutation_qos_enabled(&self) -> bool {
        self.mutation_qos_config.max_write_ops_per_window > 0
            || self.mutation_qos_config.max_write_bytes_per_window > 0
            || self.mutation_qos_config.max_discard_ops_per_window > 0
            || self.mutation_qos_config.max_discard_bytes_per_window > 0
    }

    fn mutation_qos_pressure(&self) -> bool {
        self.mutation_qos_enabled()
            && ((self.mutation_qos_config.max_write_ops_per_window > 0
                && self.mutation_qos_state.write_ops_in_window
                    >= self.mutation_qos_config.max_write_ops_per_window)
                || (self.mutation_qos_config.max_write_bytes_per_window > 0
                    && self.mutation_qos_state.write_bytes_in_window
                        >= self.mutation_qos_config.max_write_bytes_per_window)
                || (self.mutation_qos_config.max_discard_ops_per_window > 0
                    && self.mutation_qos_state.discard_ops_in_window
                        >= self.mutation_qos_config.max_discard_ops_per_window)
                || (self.mutation_qos_config.max_discard_bytes_per_window > 0
                    && self.mutation_qos_state.discard_bytes_in_window
                        >= self.mutation_qos_config.max_discard_bytes_per_window))
    }

    fn mutation_qos_would_limit(
        &self,
        write_ops: u64,
        write_bytes: u64,
        discard_ops: u64,
        discard_bytes: u64,
        now_ms: u64,
    ) -> bool {
        if !self.mutation_qos_enabled()
            || (write_ops == 0 && write_bytes == 0 && discard_ops == 0 && discard_bytes == 0)
        {
            return false;
        }
        let mut write_ops_in_window = self.mutation_qos_state.write_ops_in_window;
        let mut write_bytes_in_window = self.mutation_qos_state.write_bytes_in_window;
        let mut discard_ops_in_window = self.mutation_qos_state.discard_ops_in_window;
        let mut discard_bytes_in_window = self.mutation_qos_state.discard_bytes_in_window;
        if self.mutation_qos_state.window_start_ms == 0
            || now_ms < self.mutation_qos_state.window_start_ms
            || now_ms.saturating_sub(self.mutation_qos_state.window_start_ms)
                >= self.mutation_qos_config.window_ms
        {
            write_ops_in_window = 0;
            write_bytes_in_window = 0;
            discard_ops_in_window = 0;
            discard_bytes_in_window = 0;
        }
        let Some(next_write_ops) = write_ops_in_window.checked_add(write_ops) else {
            return true;
        };
        let Some(next_write_bytes) = write_bytes_in_window.checked_add(write_bytes) else {
            return true;
        };
        let Some(next_discard_ops) = discard_ops_in_window.checked_add(discard_ops) else {
            return true;
        };
        let Some(next_discard_bytes) = discard_bytes_in_window.checked_add(discard_bytes) else {
            return true;
        };
        (self.mutation_qos_config.max_write_ops_per_window > 0
            && next_write_ops > self.mutation_qos_config.max_write_ops_per_window)
            || (self.mutation_qos_config.max_write_bytes_per_window > 0
                && next_write_bytes > self.mutation_qos_config.max_write_bytes_per_window)
            || (self.mutation_qos_config.max_discard_ops_per_window > 0
                && next_discard_ops > self.mutation_qos_config.max_discard_ops_per_window)
            || (self.mutation_qos_config.max_discard_bytes_per_window > 0
                && next_discard_bytes > self.mutation_qos_config.max_discard_bytes_per_window)
    }

    fn admit_mutation_qos(
        &mut self,
        write_ops: u64,
        write_bytes: u64,
        discard_ops: u64,
        discard_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if !self.mutation_qos_enabled()
            || (write_ops == 0 && write_bytes == 0 && discard_ops == 0 && discard_bytes == 0)
        {
            return Ok(());
        }
        let window_ms = self.mutation_qos_config.window_ms;
        if self.mutation_qos_state.window_start_ms == 0
            || now_ms < self.mutation_qos_state.window_start_ms
            || now_ms.saturating_sub(self.mutation_qos_state.window_start_ms) >= window_ms
        {
            self.mutation_qos_state.window_start_ms = now_ms;
            self.mutation_qos_state.write_ops_in_window = 0;
            self.mutation_qos_state.write_bytes_in_window = 0;
            self.mutation_qos_state.discard_ops_in_window = 0;
            self.mutation_qos_state.discard_bytes_in_window = 0;
        }
        let next_write_ops = self
            .mutation_qos_state
            .write_ops_in_window
            .checked_add(write_ops)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("chunk mutation qos op overflow".to_string())
            })?;
        let next_write_bytes = self
            .mutation_qos_state
            .write_bytes_in_window
            .checked_add(write_bytes)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("chunk mutation qos byte overflow".to_string())
            })?;
        let next_discard_ops = self
            .mutation_qos_state
            .discard_ops_in_window
            .checked_add(discard_ops)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("chunk mutation qos op overflow".to_string())
            })?;
        let next_discard_bytes = self
            .mutation_qos_state
            .discard_bytes_in_window
            .checked_add(discard_bytes)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("chunk mutation qos byte overflow".to_string())
            })?;
        if (self.mutation_qos_config.max_write_ops_per_window > 0
            && next_write_ops > self.mutation_qos_config.max_write_ops_per_window)
            || (self.mutation_qos_config.max_discard_ops_per_window > 0
                && next_discard_ops > self.mutation_qos_config.max_discard_ops_per_window)
        {
            self.mutation_qos_state.throttled_ops = self
                .mutation_qos_state
                .throttled_ops
                .saturating_add(write_ops.saturating_add(discard_ops));
            return Err(ObjectError::PreconditionFailed(
                "chunk mutation qos throttled: operation window exceeded".to_string(),
            ));
        }
        if (self.mutation_qos_config.max_write_bytes_per_window > 0
            && next_write_bytes > self.mutation_qos_config.max_write_bytes_per_window)
            || (self.mutation_qos_config.max_discard_bytes_per_window > 0
                && next_discard_bytes > self.mutation_qos_config.max_discard_bytes_per_window)
        {
            self.mutation_qos_state.throttled_bytes = self
                .mutation_qos_state
                .throttled_bytes
                .saturating_add(write_bytes.saturating_add(discard_bytes));
            return Err(ObjectError::PreconditionFailed(
                "chunk mutation qos throttled: byte window exceeded".to_string(),
            ));
        }
        self.mutation_qos_state.write_ops_in_window = next_write_ops;
        self.mutation_qos_state.write_bytes_in_window = next_write_bytes;
        self.mutation_qos_state.discard_ops_in_window = next_discard_ops;
        self.mutation_qos_state.discard_bytes_in_window = next_discard_bytes;
        Ok(())
    }

}
