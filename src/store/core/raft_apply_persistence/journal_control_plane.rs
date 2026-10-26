// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    fn encoded_metadata_coordinator_config_len(config: &MetadataCoordinatorConfig) -> usize {
        std::mem::size_of::<u64>() * 4
            + encoded_string_len(&config.backend)
            + config
                .endpoints
                .iter()
                .map(|endpoint| encoded_string_len(endpoint))
                .sum::<usize>()
            + encoded_string_len(&config.namespace_path)
    }

    fn encoded_optional_journal_string_len(value: Option<&str>) -> usize {
        std::mem::size_of::<u64>() + value.map(encoded_string_len).unwrap_or_default()
    }

    fn append_embedded_control_plane_bootstrap_journal_record(
        &self,
        metaserver_node_id: &str,
        embedded_root_node_id: Option<&str>,
        failure_domain: &str,
        metaserver_endpoint: &str,
        now_ms: u64,
        root_lease_ttl_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + encoded_string_len(metaserver_node_id)
                + Self::encoded_optional_journal_string_len(embedded_root_node_id)
                + encoded_string_len(failure_domain)
                + encoded_string_len(metaserver_endpoint),
        );
        append_u64(&mut payload, 34);
        append_string(&mut payload, metaserver_node_id);
        match embedded_root_node_id {
            Some(node_id) => {
                append_u64(&mut payload, 1);
                append_string(&mut payload, node_id);
            }
            None => append_u64(&mut payload, 0),
        }
        append_string(&mut payload, failure_domain);
        append_string(&mut payload, metaserver_endpoint);
        append_u64(&mut payload, now_ms);
        append_u64(&mut payload, root_lease_ttl_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_control_plane_mode_journal_record(&self, mode: &str) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() + encoded_string_len(mode));
        append_u64(&mut payload, 35);
        append_string(&mut payload, mode);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_metaserver_catalog_endpoints_journal_record(
        &self,
        endpoints: &str,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload =
            Vec::with_capacity(std::mem::size_of::<u64>() + encoded_string_len(endpoints));
        append_u64(&mut payload, 36);
        append_string(&mut payload, endpoints);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    // Record 37, the root catalog endpoint, has no writer any more: nothing can configure
    // one. Replay still READS a 37 from an existing journal and refuses it, by way of the
    // configure function -- see `journal_replay::catalog_operations`.

    fn append_metadata_coordinator_config_journal_record(
        &self,
        config: &MetadataCoordinatorConfig,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() + Self::encoded_metadata_coordinator_config_len(config),
        );
        append_u64(&mut payload, 38);
        append_metadata_coordinator_config(&mut payload, config);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_placement_coordinator_config_journal_record(
        &self,
        config: &MetadataCoordinatorConfig,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() + Self::encoded_metadata_coordinator_config_len(config),
        );
        append_u64(&mut payload, 39);
        append_metadata_coordinator_config(&mut payload, config);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_meta_coordinator_session_register_journal_record(
        &self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3
                + encoded_string_len(session_id)
                + encoded_string_len(owner)
                + encoded_string_len(endpoint),
        );
        append_u64(&mut payload, 40);
        append_string(&mut payload, session_id);
        append_string(&mut payload, owner);
        append_string(&mut payload, endpoint);
        append_u64(&mut payload, ttl_ms);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_meta_coordinator_session_renew_journal_record(
        &self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 4
                + encoded_string_len(session_id)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 41);
        append_string(&mut payload, session_id);
        append_string(&mut payload, owner);
        append_u64(&mut payload, epoch);
        append_u64(&mut payload, ttl_ms);
        append_u64(&mut payload, now_ms);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_meta_coordinator_session_release_journal_record(
        &self,
        session_id: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(
            std::mem::size_of::<u64>() * 2
                + encoded_string_len(session_id)
                + encoded_string_len(owner),
        );
        append_u64(&mut payload, 42);
        append_string(&mut payload, session_id);
        append_string(&mut payload, owner);
        append_u64(&mut payload, epoch);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

    fn append_meta_coordinator_session_expiry_journal_record(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<(), ObjectError> {
        if self.persistent_journal_path.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(std::mem::size_of::<u64>() * 4);
        append_u64(&mut payload, 43);
        append_u64(&mut payload, now_ms);
        append_u64(&mut payload, limit as u64);
        append_u64(&mut payload, include_fenced as u64);
        append_persistent_journal_frame(&self.persistent_journal_path, &payload)
    }

}
