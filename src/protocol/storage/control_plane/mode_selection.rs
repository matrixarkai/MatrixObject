#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlPlaneMode {
    Auto,
    MetaServer,
    GlobalCatalog,
    RootServer,
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
            "globalcatalog" | "global-catalog" | "catalog" => Ok(Self::GlobalCatalog),
            "rootserver" | "root" => Ok(Self::RootServer),
            _ => Err(ObjectError::InvalidArgument(
                "control plane mode must be auto, metaserver, globalcatalog, or rootserver"
                    .to_string(),
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::MetaServer => "metaserver",
            Self::GlobalCatalog => "globalcatalog",
            Self::RootServer => "rootserver",
        }
    }

    pub fn snapshot_code(&self) -> u64 {
        match self {
            Self::Auto => 0,
            Self::MetaServer => 1,
            Self::GlobalCatalog => 2,
            Self::RootServer => 3,
        }
    }

    pub fn from_snapshot_code(code: u64) -> Result<Self, ObjectError> {
        match code {
            0 => Ok(Self::Auto),
            1 => Ok(Self::MetaServer),
            2 => Ok(Self::GlobalCatalog),
            3 => Ok(Self::RootServer),
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
            Self::GlobalCatalog => {
                let endpoint = rootserver_endpoint.ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "globalcatalog control-plane mode requires a global catalog endpoint"
                            .to_string(),
                    )
                })?;
                Some(("globalcatalog", endpoint))
            }
            Self::RootServer => {
                if let Some(endpoint) = rootserver_endpoint {
                    Some(("rootserver", endpoint))
                } else {
                    let endpoint =
                        metaserver_endpoint.unwrap_or(EMBEDDED_METASERVER_CATALOG_ENDPOINT);
                    Some(("metaserver", endpoint))
                }
            }
            Self::Auto => metaserver_endpoint
                .map(|endpoint| ("metaserver", endpoint))
                .or(Some(("metaserver", EMBEDDED_METASERVER_CATALOG_ENDPOINT))),
        };
        Ok(selected.map(|(source, endpoint)| {
            let mut selection = ControlPlaneCatalogSelection {
                configured_mode: self.as_str().to_string(),
                effective_mode: if (matches!(self, Self::RootServer) && source == "rootserver")
                    || source == "rootserver-fallback"
                {
                    "rootserver".to_string()
                } else if source == "globalcatalog" {
                    "globalcatalog".to_string()
                } else {
                    "metaserver".to_string()
                },
                catalog_source: source.to_string(),
                catalog_endpoint: endpoint.to_string(),
                rootserver_required: matches!(self, Self::GlobalCatalog)
                    || (matches!(self, Self::RootServer) && source == "rootserver")
                    || (matches!(self, Self::Auto) && source == "rootserver-fallback"),
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
            Self::GlobalCatalog => {
                let endpoint = rootserver_endpoint.clone().ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "globalcatalog control-plane mode requires a global catalog endpoint"
                            .to_string(),
                    )
                })?;
                add_endpoint("globalcatalog", endpoint);
            }
            Self::RootServer => {
                if let Some(endpoint) = rootserver_endpoint.clone() {
                    add_endpoint("rootserver", endpoint);
                } else if metaserver_endpoint_list.is_empty() {
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
            effective_mode: if (matches!(self, Self::RootServer)
                && selected
                    .as_ref()
                    .map(|route| route.catalog_source.as_str() == "rootserver")
                    .unwrap_or(false))
                || selected
                    .as_ref()
                    .map(|route| route.catalog_source.as_str() == "rootserver-fallback")
                    .unwrap_or(false)
            {
                "rootserver".to_string()
            } else if selected
                .as_ref()
                .map(|route| route.catalog_source.as_str() == "globalcatalog")
                .unwrap_or(false)
            {
                "globalcatalog".to_string()
            } else if selected.is_some() {
                "metaserver".to_string()
            } else {
                "none".to_string()
            },
            rootserver_required: matches!(self, Self::GlobalCatalog)
                || (matches!(self, Self::RootServer)
                    && selected
                        .as_ref()
                        .map(|route| route.catalog_source.as_str() == "rootserver")
                        .unwrap_or(false)),
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
        let rootserver_endpoint =
            first_non_empty_env(&MATRIX_OBJECTSTORE_GLOBAL_CATALOG_ENV_NAMES)?;
        let endpoint_set = mode.select_catalog_endpoint_set(
            metaserver_endpoints.as_deref(),
            rootserver_endpoint.as_deref(),
        )?;
        require_control_plane_catalog_endpoint_set_env_checksums(endpoint_set)
    }
}
