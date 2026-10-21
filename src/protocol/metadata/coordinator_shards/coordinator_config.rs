// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataCoordinatorConfig {
    pub backend: String,
    pub endpoints: Vec<String>,
    pub namespace_path: String,
    pub session_ttl_ms: u64,
    pub revision: u64,
}

impl Default for MetadataCoordinatorConfig {
    fn default() -> Self {
        Self {
            backend: "embedded-raft".to_string(),
            endpoints: Vec::new(),
            namespace_path: "/matrixobjectstore/metaserver".to_string(),
            session_ttl_ms: 10_000,
            revision: 1,
        }
    }
}

impl MetadataCoordinatorConfig {
    pub fn from_env() -> Result<Option<Self>, ObjectError> {
        let configured = env_any_present(&[
            &MATRIX_METASERVER_COORDINATOR_BACKEND_ENV_NAMES[..],
            &MATRIX_METASERVER_COORDINATOR_NAMESPACE_ENV_NAMES[..],
            &MATRIX_METASERVER_COORDINATOR_TTL_ENV_NAMES[..],
            &MATRIX_METASERVER_COORDINATOR_ENDPOINT_ENV_NAMES[..],
            &MATRIX_METASERVER_ETCD_ENDPOINT_ENV_NAMES[..],
            &MATRIX_METASERVER_ZOOKEEPER_ENDPOINT_ENV_NAMES[..],
        ])?;
        if !configured {
            return Ok(None);
        }

        let generic_endpoints =
            first_non_empty_env(&MATRIX_METASERVER_COORDINATOR_ENDPOINT_ENV_NAMES)?;
        let etcd_endpoints = first_non_empty_env(&MATRIX_METASERVER_ETCD_ENDPOINT_ENV_NAMES)?;
        let zookeeper_endpoints =
            first_non_empty_env(&MATRIX_METASERVER_ZOOKEEPER_ENDPOINT_ENV_NAMES)?;
        let explicit_backend =
            first_non_empty_env(&MATRIX_METASERVER_COORDINATOR_BACKEND_ENV_NAMES)?;
        let mut backend = explicit_backend
            .clone()
            .unwrap_or_else(|| "embedded-raft".to_string());
        backend = normalize_coordinator_backend(&backend);
        if explicit_backend.is_none() && (etcd_endpoints.is_some() || zookeeper_endpoints.is_some())
        {
            return Err(ObjectError::InvalidArgument(
                "MetaServer uses embedded-raft as the production coordinator; set MATRIX_METASERVER_COORDINATOR_BACKEND=etcd or zookeeper explicitly to enable external compatibility mode".to_string(),
            ));
        }
        if backend == "embedded-raft" && generic_endpoints.is_some() {
            return Err(ObjectError::InvalidArgument(
                "embedded-raft MetaServer coordinator must not define external endpoints"
                    .to_string(),
            ));
        }
        if backend == "etcd" && zookeeper_endpoints.is_some() {
            return Err(ObjectError::InvalidArgument(
                "etcd MetaServer coordinator bootstrap must not define zookeeper endpoints"
                    .to_string(),
            ));
        }
        if backend == "zookeeper" && etcd_endpoints.is_some() {
            return Err(ObjectError::InvalidArgument(
                "zookeeper MetaServer coordinator bootstrap must not define etcd endpoints"
                    .to_string(),
            ));
        }

        let namespace_path =
            first_non_empty_env(&MATRIX_METASERVER_COORDINATOR_NAMESPACE_ENV_NAMES)?
                .unwrap_or_else(|| MetadataCoordinatorConfig::default().namespace_path);
        let ttl_text = first_non_empty_env(&MATRIX_METASERVER_COORDINATOR_TTL_ENV_NAMES)?
            .unwrap_or_else(|| {
                MetadataCoordinatorConfig::default()
                    .session_ttl_ms
                    .to_string()
            });
        let session_ttl_ms = ttl_text.parse::<u64>().map_err(|_| {
            ObjectError::InvalidArgument(
                "metadata coordinator env session ttl must be a positive integer".to_string(),
            )
        })?;

        let endpoint_text = generic_endpoints.or_else(|| {
            if backend == "etcd" {
                etcd_endpoints
            } else if backend == "zookeeper" {
                zookeeper_endpoints
            } else {
                None
            }
        });
        let endpoints = endpoint_text
            .as_deref()
            .map(parse_coordinator_endpoint_csv)
            .transpose()?
            .unwrap_or_default();
        let mut config = MetadataCoordinatorConfig {
            backend,
            endpoints,
            namespace_path,
            session_ttl_ms,
            revision: 1,
        };
        config.endpoints.sort();
        config.endpoints.dedup();
        validate_metadata_coordinator_config(&config)?;
        Ok(Some(config))
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}backend"), self.backend.clone());
        fields.insert(
            format!("{field_prefix}normalized_backend"),
            self.normalized_backend(),
        );
        fields.insert(
            format!("{field_prefix}endpoint_count"),
            self.endpoints.len().to_string(),
        );
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            fields.insert(format!("{field_prefix}endpoint.{index}"), endpoint.clone());
        }
        fields.insert(
            format!("{field_prefix}namespace_path"),
            self.namespace_path.clone(),
        );
        fields.insert(
            format!("{field_prefix}session_ttl_ms"),
            self.session_ttl_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}revision"), self.revision.to_string());
        fields.insert(
            format!("{field_prefix}policy_format"),
            self.policy_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}policy_checksum"),
            self.policy_checksum(),
        );
        fields.insert(
            format!("{field_prefix}preferred_backend"),
            self.preferred_backend().to_string(),
        );
        fields.insert(
            format!("{field_prefix}normalized_backend"),
            self.normalized_backend(),
        );
        fields.insert(
            format!("{field_prefix}preferred_backend_active"),
            self.preferred_backend_active().to_string(),
        );
        fields.insert(
            format!("{field_prefix}external_compatibility_mode"),
            self.external_compatibility_mode().to_string(),
        );
        fields.insert(
            format!("{field_prefix}requires_external_coordinator"),
            self.requires_external_coordinator().to_string(),
        );
        fields.insert(
            format!("{field_prefix}production_ready"),
            self.production_ready().to_string(),
        );
        fields.insert(
            format!("{field_prefix}external_dependency"),
            self.external_dependency(),
        );
        fields
    }

    pub fn normalized_backend(&self) -> String {
        normalize_coordinator_backend(&self.backend)
    }

    pub fn policy_format(&self) -> &'static str {
        coordinator_policy_format()
    }

    pub fn policy_checksum(&self) -> String {
        metadata_coordinator_policy_checksum(self)
    }

    pub fn preferred_backend(&self) -> &'static str {
        preferred_production_coordinator_backend()
    }

    pub fn preferred_backend_active(&self) -> bool {
        coordinator_preferred_backend_active(&self.backend)
    }

    pub fn external_compatibility_mode(&self) -> bool {
        external_coordinator_compatibility_mode(&self.backend)
    }

    pub fn requires_external_coordinator(&self) -> bool {
        self.external_compatibility_mode()
    }

    pub fn production_ready(&self) -> bool {
        production_coordinator_config(&self.backend, &self.endpoints)
    }

    pub fn external_dependency(&self) -> String {
        external_coordinator_dependency(&self.backend)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataCoordinatorConfig {
    pub config: MetadataCoordinatorConfig,
    pub idempotency_key: String,
    pub request_checksum: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub metadata_control_proof_now_ms: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}
