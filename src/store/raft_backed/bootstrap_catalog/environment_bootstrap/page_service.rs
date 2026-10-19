impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn bootstrap_page_route_control_from_env(
        &mut self,
    ) -> Result<Option<ChunkRouteControlBootstrapReport>, ObjectError> {
        let Some(target_commit_index) =
            first_non_empty_env_u64(&MATRIX_PAGESERVER_ROUTE_CONTROL_COMMIT_INDEX_ENV_NAMES)?
        else {
            return Ok(None);
        };
        if target_commit_index == 0 {
            return Err(ObjectError::InvalidArgument(
                "block route-control bootstrap commit index must be positive".to_string(),
            ));
        }
        let max_steps = first_non_empty_env_u64(
            &MATRIX_PAGESERVER_ROUTE_CONTROL_BOOTSTRAP_MAX_STEPS_ENV_NAMES,
        )?
        .unwrap_or(65_536);
        let before_proof = self.store.chunk_service.route_control_proof();
        if before_proof.chunk_route_control_commit_index > target_commit_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "block route-control bootstrap target is behind local state: target={} local={}",
                target_commit_index, before_proof.chunk_route_control_commit_index
            )));
        }
        let mut applied_steps = 0;
        let mut last_metadata = None;
        while self
            .store
            .chunk_service
            .route_control_proof()
            .chunk_route_control_commit_index
            < target_commit_index
        {
            if applied_steps >= max_steps {
                return Err(ObjectError::PreconditionFailed(
                    "block route-control bootstrap exceeded max steps before target commit index"
                        .to_string(),
                ));
            }
            if self.raft.commit_index() != self.store.last_applied_index() {
                return Err(ObjectError::PreconditionFailed(
                    "block route-control bootstrap requires raft/store commit alignment"
                        .to_string(),
                ));
            }
            let next_commit_index = self.store.last_applied_index().saturating_add(1);
            if next_commit_index > target_commit_index {
                return Err(ObjectError::PreconditionFailed(format!(
                    "block route-control bootstrap would overshoot target: next={} target={}",
                    next_commit_index, target_commit_index
                )));
            }
            let route_checksum = self
                .store
                .chunk_service
                .route_control_proof()
                .state_checksum;
            let config = self.store.chunk_route_qos_config();
            let metadata = self.propose_configure_page_route_qos_prepared_linearizable_fenced(
                config,
                Some(route_checksum.as_str()),
            )?;
            last_metadata = Some(metadata);
            applied_steps += 1;
        }
        let after_proof = self.store.chunk_service.route_control_proof();
        let report = ChunkRouteControlBootstrapReport {
            target_commit_index,
            max_steps,
            applied_steps,
            before_proof,
            after_proof,
            last_metadata,
        };
        if let Some(expected) = first_non_empty_env(
            &MATRIX_PAGESERVER_ROUTE_CONTROL_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES,
        )? {
            report.verify_expected_report_checksum(expected.as_str())?;
        }
        Ok(Some(report))
    }

    pub fn bootstrap_page_service_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<ChunkServiceBootstrapReport, ObjectError> {
        let before_readiness = self.store.page_storage_readiness(0, 0, now_ms);
        let route_report = self.bootstrap_page_route_control_from_env()?;
        self.bootstrap_page_chunk_trim_flow_from_env(now_ms)?;
        let after_readiness = self.store.page_storage_readiness(0, 0, now_ms);
        let report = ChunkServiceBootstrapReport {
            route_report,
            before_readiness,
            after_readiness,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_PAGESERVER_SERVICE_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(report)
    }

    fn bootstrap_page_chunk_trim_flow_from_env(
        &mut self,
        now_ms: u64,
    ) -> Result<bool, ObjectError> {
        let requested_rate = first_non_empty_env_u64(
            &MATRIX_PAGESERVER_CHUNK_TRIM_FLOW_RATE_BYTES_PER_SEC_ENV_NAMES,
        )?;
        let requested_burst =
            first_non_empty_env_u64(&MATRIX_PAGESERVER_CHUNK_TRIM_FLOW_BURST_BYTES_ENV_NAMES)?;
        let requested_ops_rate = first_non_empty_env_u64(
            &MATRIX_PAGESERVER_CHUNK_TRIM_FLOW_RATE_OPS_PER_SEC_ENV_NAMES,
        )?;
        let requested_ops_burst =
            first_non_empty_env_u64(&MATRIX_PAGESERVER_CHUNK_TRIM_FLOW_BURST_OPS_ENV_NAMES)?;
        if requested_rate.is_none()
            && requested_burst.is_none()
            && requested_ops_rate.is_none()
            && requested_ops_burst.is_none()
        {
            return Ok(false);
        }
        let include_disabled = first_non_empty_env_bool(
            &MATRIX_PAGESERVER_CHUNK_TRIM_FLOW_INCLUDE_DISABLED_ENV_NAMES,
        )?
        .unwrap_or(false);
        let mut rate = requested_rate.unwrap_or(0);
        let mut burst = requested_burst.unwrap_or(0);
        let mut ops_rate = requested_ops_rate.unwrap_or(0);
        let mut ops_burst = requested_ops_burst.unwrap_or(0);
        if rate > 0 && burst == 0 && requested_burst.is_none() {
            burst = rate;
        }
        if burst > 0 && rate == 0 && requested_rate.is_none() {
            rate = burst;
        }
        if ops_rate > 0 && ops_burst == 0 && requested_ops_burst.is_none() {
            ops_burst = ops_rate;
        }
        if ops_burst > 0 && ops_rate == 0 && requested_ops_rate.is_none() {
            ops_rate = ops_burst;
        }
        if requested_rate.is_some()
            || requested_burst.is_some()
            || requested_ops_rate.is_some()
            || requested_ops_burst.is_some()
        {
            let route_checksum = self
                .store
                .chunk_service
                .route_control_proof()
                .state_checksum;
            self.propose_configure_chunk_trim_flow_prepared_full_linearizable_fenced(
                rate,
                burst,
                ops_rate,
                ops_burst,
                now_ms,
                Some(route_checksum.as_str()),
            )?;
        }
        if include_disabled {
            let disabled_indices = self.store.chunk_service.disabled_chunk_endpoint_indices();
            for replica_index in disabled_indices {
                if requested_rate.is_some()
                    || requested_burst.is_some()
                    || requested_ops_rate.is_some()
                    || requested_ops_burst.is_some()
                {
                    let route_checksum = self
                        .store
                        .chunk_service
                        .route_control_proof()
                        .state_checksum;
                    self.propose_configure_chunk_replica_trim_flow_prepared_full_linearizable_fenced(
                        replica_index,
                        rate,
                        burst,
                        ops_rate,
                        ops_burst,
                        now_ms,
                        Some(route_checksum.as_str()),
                    )?;
                }
            }
        }
        Ok(true)
    }
}
