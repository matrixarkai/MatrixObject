// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_configure_meta_coordinator(
        &mut self,
        config: MetadataCoordinatorConfig,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ConfigureMetaCoordinator { config })
    }

    pub fn propose_configure_meta_coordinator_from_env(
        &mut self,
    ) -> Result<Option<ObjectMetadata>, ObjectError> {
        let Some(config) = MetadataCoordinatorConfig::from_env()? else {
            return Ok(None);
        };
        self.propose_configure_meta_coordinator(config).map(Some)
    }

    pub fn propose_configure_meta_coordinator_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<MetadataCoordinatorBootstrapReport>, ObjectError> {
        let Some(config) = MetadataCoordinatorConfig::from_env()? else {
            return Ok(None);
        };
        let before_readiness = self.store.metadata_raft_readiness(0, now_ms);
        self.propose_configure_meta_coordinator(config.clone())?;
        let after_readiness = self.store.metadata_raft_readiness(0, now_ms);
        let applied_config = self.store.metadata_coordinator_config();
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

    pub fn propose_metadata_raft_membership_from_env_report(
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
        let before_readiness = self.store.metadata_raft_readiness(0, now_ms);
        let metadata = self.propose_apply_raft_membership_prepared(
            &mutation_id,
            target_voter_node_ids.clone(),
            target_healthy_peer_count,
        )?;
        let after_readiness = self.store.metadata_raft_readiness(0, now_ms);
        let report = MetadataRaftBootstrapReport {
            mutation_id,
            target_voter_node_ids,
            target_healthy_peer_count,
            before_readiness,
            after_readiness,
            membership_receipt: None,
            last_metadata: Some(metadata),
        };
        if let Some(expected) =
            first_non_empty_env(&MATRIX_METASERVER_RAFT_BOOTSTRAP_REPORT_CHECKSUM_ENV_NAMES)?
        {
            report.verify_expected_report_checksum(expected)?;
        }
        Ok(Some(report))
    }

    pub fn propose_metadata_service_from_env_report(
        &mut self,
        now_ms: u64,
    ) -> Result<MetadataServiceBootstrapReport, ObjectError> {
        let before_readiness = self.store.metadata_raft_readiness(0, now_ms);
        let raft_report = self.propose_metadata_raft_membership_from_env_report(now_ms)?;
        let coordinator_report = self.propose_configure_meta_coordinator_from_env_report(now_ms)?;
        let after_readiness = self.store.metadata_raft_readiness(0, now_ms);
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
