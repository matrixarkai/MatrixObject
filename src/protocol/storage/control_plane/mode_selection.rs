// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlPlaneMode {
    Auto,
    MetaServer,
}

impl Default for ControlPlaneMode {
    fn default() -> Self {
        Self::Auto
    }
}

impl ControlPlaneMode {
    pub fn parse_from_env() -> Result<Self, ObjectError> {
        match std::env::var(MATRIX_OBJECTSTORE_CONTROL_PLANE_ENV) {
            Ok(value) => Self::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::Auto),
            Err(std::env::VarError::NotUnicode(_)) => Err(ObjectError::InvalidArgument(format!(
                "{MATRIX_OBJECTSTORE_CONTROL_PLANE_ENV} must be valid unicode"
            ))),
        }
    }

    pub fn parse(value: &str) -> Result<Self, ObjectError> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" | "auto" => Ok(Self::Auto),
            "metaserver" | "meta" | "embedded" => Ok(Self::MetaServer),
            // THE RETIRED MODES, refused by name rather than falling into the catch-all, so
            // the error says what happened to them instead of only what is allowed.
            "globalcatalog" | "global-catalog" | "catalog" | "rootserver" | "root" => {
                Err(ObjectError::InvalidArgument(format!(
                    "control plane mode {normalized:?} is retired: this store's control plane \
                     is the metaserver, so the modes are auto and metaserver"
                )))
            }
            _ => Err(ObjectError::InvalidArgument(
                "control plane mode must be auto or metaserver".to_string(),
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::MetaServer => "metaserver",
        }
    }

    pub fn snapshot_code(&self) -> u64 {
        match self {
            Self::Auto => 0,
            Self::MetaServer => 1,
        }
    }

    /// The mode a snapshot's code names, given the root catalog endpoint it carried.
    ///
    /// # WHY THIS TAKES THE ENDPOINT
    ///
    /// Codes 2 and 3 are the retired `globalcatalog` and `rootserver` modes, and the two
    /// cannot be treated alike. A store in `rootserver` mode with NO endpoint configured
    /// selected the embedded metaserver -- it WAS a metaserver, and its snapshot loads as
    /// one. With an endpoint it was pointed at a root catalog this build cannot reach, and
    /// `globalcatalog` always required one, so those are refused rather than silently
    /// repointed at the metaserver: changing where a store's catalog comes from is not a
    /// thing to do quietly on load.
    ///
    /// It replaces `from_snapshot_code`, rather than defaulting the endpoint, so that every
    /// caller has to say what it saw. A snapshot older than version 17 carries no endpoint
    /// field at all, which is an empty one and is correct: it could not have had a root
    /// catalog.
    pub fn from_snapshot_code_with_endpoint(
        code: u64,
        rootserver_endpoint: &str,
    ) -> Result<Self, ObjectError> {
        match code {
            0 => Ok(Self::Auto),
            1 => Ok(Self::MetaServer),
            2 => Err(ObjectError::InvalidArgument(
                "this store snapshot was written in the retired globalcatalog control-plane \
                 mode, which always required a global catalog endpoint this build cannot \
                 reach"
                    .to_string(),
            )),
            3 if rootserver_endpoint.trim().is_empty() => Ok(Self::MetaServer),
            3 => Err(ObjectError::InvalidArgument(format!(
                "this store snapshot was written in the retired rootserver control-plane mode \
                 with the catalog endpoint {rootserver_endpoint:?}, which this build cannot \
                 reach; a rootserver-mode snapshot with no endpoint loads as a metaserver \
                 because that is what it was"
            ))),
            _ => Err(ObjectError::Corruption(
                "store snapshot control plane mode is invalid".to_string(),
            )),
        }
    }

    pub fn select_catalog_endpoint(
        &self,
        metaserver_endpoint: Option<&str>,
        rootserver_endpoint: Option<&str>,
    ) -> Result<Option<ControlPlaneCatalogSelection>, ObjectError> {
        let metaserver_endpoint = non_empty_endpoint(metaserver_endpoint);
        let rootserver_endpoint = non_empty_endpoint(rootserver_endpoint);
        let selected = match self {
            Self::MetaServer => {
                let endpoint = metaserver_endpoint.unwrap_or(EMBEDDED_METASERVER_CATALOG_ENDPOINT);
                Some(("metaserver", endpoint))
            }
            Self::Auto => metaserver_endpoint
                .map(|endpoint| ("metaserver", endpoint))
                .or(Some(("metaserver", EMBEDDED_METASERVER_CATALOG_ENDPOINT))),
        };
        Ok(selected.map(|(source, endpoint)| {
            let mut selection = ControlPlaneCatalogSelection {
                configured_mode: self.as_str().to_string(),
                // BOTH REMAINING MODES SELECT THE METASERVER, so there is nothing to decide
                // here any more. The branch that produced "rootserver" also tested for a
                // `rootserver-fallback` source that nothing ever added -- it was dead before
                // this change.
                effective_mode: "metaserver".to_string(),
                catalog_source: source.to_string(),
                catalog_endpoint: endpoint.to_string(),
                // Kept as a reported field, and now always false: no mode requires a root
                // catalog. `rootserver_endpoint_configured` below still reports whether one
                // was configured, which is how an operator sees that a setting is being
                // ignored.
                placement_required: false,
                metaserver_endpoint_configured: metaserver_endpoint.is_some(),
                rootserver_endpoint_configured: rootserver_endpoint.is_some(),
                catalog_route_checksum_format: CONTROL_PLANE_CATALOG_SELECTION_CHECKSUM_FORMAT
                    .to_string(),
                catalog_route_checksum: String::new(),
            };
            selection.catalog_route_checksum = selection.compute_catalog_route_checksum();
            selection
        }))
    }

    pub fn select_catalog_endpoint_set(
        &self,
        metaserver_endpoints: Option<&str>,
        rootserver_endpoint: Option<&str>,
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        let metaserver_endpoint_list = parse_catalog_endpoint_list(metaserver_endpoints)?;
        let rootserver_endpoint = non_empty_endpoint(rootserver_endpoint).map(str::to_string);
        let mut endpoints = Vec::new();
        endpoints.reserve(metaserver_endpoint_list.len().max(1));
        let mut seen = BTreeSet::new();
        let mut add_endpoint = |source: &str, endpoint: String| {
            if seen.insert((source.to_string(), endpoint.clone())) {
                endpoints.push(ControlPlaneCatalogRoute {
                    catalog_source: source.to_string(),
                    catalog_endpoint: endpoint,
                });
            }
        };

        match self {
            Self::MetaServer => {
                if metaserver_endpoint_list.is_empty() {
                    add_endpoint(
                        "metaserver",
                        EMBEDDED_METASERVER_CATALOG_ENDPOINT.to_string(),
                    );
                } else {
                    for endpoint in metaserver_endpoint_list.iter().cloned() {
                        add_endpoint("metaserver", endpoint);
                    }
                }
            }
            Self::Auto => {
                if !metaserver_endpoint_list.is_empty() {
                    for endpoint in metaserver_endpoint_list.iter().cloned() {
                        add_endpoint("metaserver", endpoint);
                    }
                }
                if metaserver_endpoint_list.is_empty() {
                    add_endpoint(
                        "metaserver",
                        EMBEDDED_METASERVER_CATALOG_ENDPOINT.to_string(),
                    );
                }
            }
        }

        let selected = endpoints.first().cloned();
        let mut endpoint_set = ControlPlaneCatalogEndpointSet {
            configured_mode: self.as_str().to_string(),
            // "none" is still reachable: a mode can select no route at all.
            effective_mode: if selected.is_some() {
                "metaserver".to_string()
            } else {
                "none".to_string()
            },
            placement_required: false,
            metaserver_endpoint_configured: !metaserver_endpoint_list.is_empty(),
            rootserver_endpoint_configured: rootserver_endpoint.is_some(),
            selected,
            endpoints,
            catalog_route_checksum_format: CONTROL_PLANE_CATALOG_ENDPOINT_SET_CHECKSUM_FORMAT
                .to_string(),
            catalog_route_checksum: String::new(),
        };
        endpoint_set.catalog_route_checksum = endpoint_set.compute_catalog_route_checksum();
        Ok(endpoint_set)
    }

    pub fn select_catalog_endpoint_set_from_env(
    ) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
        let mode = Self::parse_from_env()?;
        let metaserver_endpoints =
            first_non_empty_env(&MATRIX_OBJECTSTORE_METASERVER_CATALOG_ENV_NAMES)?;
        // REFUSED RATHER THAN IGNORED. No mode reads a root catalog endpoint now, so a store
        // that started with one set would be answering from the metaserver while its
        // configuration said otherwise -- taking the setting and not honouring it.
        let rootserver_endpoint =
            first_non_empty_env(&MATRIX_OBJECTSTORE_GLOBAL_CATALOG_ENV_NAMES)?;
        if let Some(endpoint) = rootserver_endpoint.as_deref() {
            if !endpoint.trim().is_empty() {
                return Err(ObjectError::InvalidArgument(format!(
                    "a root catalog endpoint is set in the environment ({endpoint:?}) and this \
                     store's control plane is the metaserver, so nothing would read it"
                )));
            }
        }
        let endpoint_set = mode.select_catalog_endpoint_set(
            metaserver_endpoints.as_deref(),
            rootserver_endpoint.as_deref(),
        )?;
        require_control_plane_catalog_endpoint_set_env_checksums(endpoint_set)
    }
}
