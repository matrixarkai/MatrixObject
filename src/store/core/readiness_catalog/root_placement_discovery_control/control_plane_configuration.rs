// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn configured_control_plane_mode(&self) -> ControlPlaneMode {
        self.options.control_plane_mode.clone()
    }

    pub fn configure_control_plane_mode(
        &mut self,
        mode: impl AsRef<str>,
        metaserver_endpoint: Option<&str>,
        rootserver_endpoint: Option<&str>,
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        let mode = ControlPlaneMode::parse(mode.as_ref())?;
        let metaserver_endpoint =
            metaserver_endpoint.or_else(|| self.configured_metaserver_catalog_endpoints());
        let rootserver_endpoint =
            rootserver_endpoint.or_else(|| self.configured_rootserver_fallback_endpoint());
        let endpoint_set =
            mode.select_catalog_endpoint_set(metaserver_endpoint, rootserver_endpoint)?;
        let previous_mode = self.options.control_plane_mode.clone();
        self.options.control_plane_mode = mode;
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                self.options.control_plane_mode = previous_mode;
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_control_plane_mode_journal_record(self.options.control_plane_mode.as_str())
        {
            self.options.control_plane_mode = previous_mode;
            return Err(err);
        }
        Ok(endpoint_set)
    }

    pub fn configured_metaserver_catalog_endpoints(&self) -> Option<&str> {
        non_empty_endpoint(Some(self.options.metaserver_catalog_endpoints.as_str()))
    }

    pub fn configure_metaserver_catalog_endpoints(
        &mut self,
        metaserver_endpoints: impl AsRef<str>,
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        let parsed =
            parse_catalog_endpoint_list(non_empty_endpoint(Some(metaserver_endpoints.as_ref())))?;
        if parsed.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metaserver catalog endpoints must not be empty".to_string(),
            ));
        }
        let previous_endpoints = self.options.metaserver_catalog_endpoints.clone();
        self.options.metaserver_catalog_endpoints = parsed.join(",");
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                self.options.metaserver_catalog_endpoints = previous_endpoints;
                return Err(err);
            }
        }
        if let Err(err) = self.append_metaserver_catalog_endpoints_journal_record(
            &self.options.metaserver_catalog_endpoints,
        ) {
            self.options.metaserver_catalog_endpoints = previous_endpoints;
            return Err(err);
        }
        self.select_catalog_endpoint_set(None, None)
    }

    pub fn configured_rootserver_fallback_endpoint(&self) -> Option<&str> {
        non_empty_endpoint(Some(self.options.rootserver_catalog_endpoint.as_str()))
    }

    /// Refused: the control plane is the metaserver, so nothing reads a root catalog.
    ///
    /// # WHY THIS IS A REFUSAL AND NOT A NO-OP
    ///
    /// Accepting the endpoint and ignoring it would leave a store answering from the
    /// metaserver while its own configuration said otherwise -- it would have taken the
    /// setting and not honoured it.
    ///
    /// This is also the JOURNAL REPLAY path: `catalog_operations` replays a recorded
    /// endpoint by calling this function, so a journal carrying one is refused on replay by
    /// the same check, rather than quietly restoring a setting this build cannot serve.
    pub fn configure_rootserver_fallback_endpoint(
        &mut self,
        rootserver_endpoint: impl AsRef<str>,
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        Err(ObjectError::InvalidArgument(format!(
            "a root catalog endpoint ({:?}) cannot be configured: this store's control plane \
             is the metaserver, so nothing would read it",
            rootserver_endpoint.as_ref()
        )))
    }

    pub fn select_catalog_endpoint(
        &self,
        metaserver_endpoint: Option<&str>,
        rootserver_endpoint: Option<&str>,
    ) -> Result<Option<ControlPlaneCatalogSelection>, ObjectError> {
        let configured_metaserver_endpoint = if metaserver_endpoint.is_none() {
            parse_catalog_endpoint_list(self.configured_metaserver_catalog_endpoints())?
                .into_iter()
                .next()
        } else {
            None
        };
        let metaserver_endpoint = metaserver_endpoint.or(configured_metaserver_endpoint.as_deref());
        let rootserver_endpoint =
            rootserver_endpoint.or_else(|| self.configured_rootserver_fallback_endpoint());
        self.options
            .control_plane_mode
            .select_catalog_endpoint(metaserver_endpoint, rootserver_endpoint)
    }

    pub fn select_catalog_endpoint_set(
        &self,
        metaserver_endpoints: Option<&str>,
        rootserver_endpoint: Option<&str>,
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        let metaserver_endpoints =
            metaserver_endpoints.or_else(|| self.configured_metaserver_catalog_endpoints());
        let rootserver_endpoint =
            rootserver_endpoint.or_else(|| self.configured_rootserver_fallback_endpoint());
        self.options
            .control_plane_mode
            .select_catalog_endpoint_set(metaserver_endpoints, rootserver_endpoint)
    }
}
