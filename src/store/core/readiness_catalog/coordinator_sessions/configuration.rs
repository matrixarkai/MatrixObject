impl MatrixObjectStore {
    pub fn register_meta_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<CoordinatorSession, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let session = self
            .meta_service
            .register_coordinator_session(session_id, owner, endpoint, ttl_ms, now_ms)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_meta_coordinator_session_register_journal_record(
            session_id, owner, endpoint, ttl_ms, now_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(session)
    }

    pub fn metadata_coordinator_config(&self) -> MetadataCoordinatorConfig {
        self.meta_service.coordinator_config()
    }

    pub fn metadata_coordinator_policy_control_fields(
        &self,
        field_prefix: &str,
    ) -> BTreeMap<String, String> {
        self.metadata_coordinator_config()
            .to_control_fields(field_prefix)
    }

    pub fn configure_metadata_coordinator_config(
        &mut self,
        config: MetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfig, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let config = self.meta_service.configure_coordinator_config(config)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_metadata_coordinator_config_journal_record(&config) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(config)
    }

    pub fn configure_metadata_coordinator_config_from_env(
        &mut self,
    ) -> Result<Option<MetadataCoordinatorConfig>, ObjectError> {
        let Some(config) = MetadataCoordinatorConfig::from_env()? else {
            return Ok(None);
        };
        self.configure_metadata_coordinator_config(config).map(Some)
    }

    pub fn configure_metadata_coordinator_config_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<MetadataCoordinatorBootstrapReport>, ObjectError> {
        let Some(config) = MetadataCoordinatorConfig::from_env()? else {
            return Ok(None);
        };
        let before_readiness = self.metadata_raft_readiness(0, now_ms);
        let applied_config = self.configure_metadata_coordinator_config(config.clone())?;
        let after_readiness = self.metadata_raft_readiness(0, now_ms);
        let report = MetadataCoordinatorBootstrapReport {
            config,
            before_readiness,
            after_readiness,
            applied_config,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_METASERVER_COORDINATOR_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(Some(report))
    }

    pub fn bootstrap_metadata_raft_membership_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<MetadataRaftBootstrapReport>, ObjectError> {
        let Some(voter_text) = first_non_empty_env(&MATRIX_METASERVER_RAFT_VOTER_ENV_NAMES)? else {
            return Ok(None);
        };
        let target_voter_node_ids = parse_raft_voter_csv(&voter_text)?;
        let target_healthy_peer_count =
            first_non_empty_env_u64(&MATRIX_METASERVER_RAFT_HEALTHY_PEER_ENV_NAMES)?
                .unwrap_or(target_voter_node_ids.len() as u64);
        let mutation_id =
            first_non_empty_env(&MATRIX_METASERVER_RAFT_BOOTSTRAP_MUTATION_ID_ENV_NAMES)?
                .unwrap_or_else(|| {
                    format!(
                        "metaserver-raft-bootstrap-{}",
                        raft_voter_checksum(&target_voter_node_ids)
                    )
                });
        let before_readiness = self.metadata_raft_readiness(0, now_ms);
        let membership_receipt = self.apply_raft_membership_change(
            mutation_id.clone(),
            target_voter_node_ids.clone(),
            target_healthy_peer_count,
            self.last_applied_index(),
        )?;
        let after_readiness = self.metadata_raft_readiness(0, now_ms);
        let report = MetadataRaftBootstrapReport {
            mutation_id,
            target_voter_node_ids,
            target_healthy_peer_count,
            before_readiness,
            after_readiness,
            membership_receipt: Some(membership_receipt),
            last_metadata: None,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_METASERVER_RAFT_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(Some(report))
    }

    pub fn bootstrap_metadata_service_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<MetadataServiceBootstrapReport, ObjectError> {
        let before_readiness = self.metadata_raft_readiness(0, now_ms);
        let raft_report = self.bootstrap_metadata_raft_membership_from_env_report(now_ms)?;
        let coordinator_report =
            self.configure_metadata_coordinator_config_from_env_report(now_ms)?;
        let after_readiness = self.metadata_raft_readiness(0, now_ms);
        let report = MetadataServiceBootstrapReport {
            raft_report,
            coordinator_report,
            before_readiness,
            after_readiness,
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_METASERVER_SERVICE_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(report)
    }

}
