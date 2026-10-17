impl RootCatalogLivenessReceipt {
    pub fn require_checksum(&self, expected_checksum: &str) -> Result<(), ObjectError> {
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "root catalog liveness receipt checksum must be set".to_string(),
            ));
        }
        if self.checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog liveness receipt checksum mismatch: expected {expected_checksum}, got {}",
                self.checksum
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}format"), self.format.clone());
        fields.insert(format!("{field_prefix}checksum"), self.checksum.clone());
        fields.insert(format!("{field_prefix}method"), self.method.clone());
        fields.insert(format!("{field_prefix}node_id"), self.node_id.clone());
        fields.insert(
            format!("{field_prefix}service_kind"),
            self.service_kind.clone(),
        );
        for (name, value) in [
            ("heartbeat_ms", self.heartbeat_ms),
            ("term", self.term),
            ("commit_index", self.commit_index),
            ("read_index", self.read_index),
            ("node_count", self.node_count),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }

    pub fn to_native_liveness_receipt_fields(&self) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            "root_catalog_liveness_receipt_format".to_string(),
            self.format.clone(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_checksum".to_string(),
            self.checksum.clone(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_method".to_string(),
            self.method.clone(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_node_id".to_string(),
            self.node_id.clone(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_service_kind".to_string(),
            self.service_kind.clone(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_heartbeat_ms".to_string(),
            self.heartbeat_ms.to_string(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_term".to_string(),
            self.term.to_string(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_commit_index".to_string(),
            self.commit_index.to_string(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_read_index".to_string(),
            self.read_index.to_string(),
        );
        fields.insert(
            "root_catalog_liveness_receipt_node_count".to_string(),
            self.node_count.to_string(),
        );
        fields
    }

    pub fn from_native_liveness_receipt_fields(
        fields: &BTreeMap<String, String>,
        expected_method: &str,
        expected_node_id: &str,
        expected_service_kind: &str,
    ) -> Result<Self, ObjectError> {
        let format = require_liveness_field(fields, "root_catalog_liveness_receipt_format")?;
        if format != ROOT_CATALOG_LIVENESS_RECEIPT_FORMAT {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog liveness receipt format mismatch: expected {}, got {}",
                ROOT_CATALOG_LIVENESS_RECEIPT_FORMAT, format
            )));
        }
        let method = require_liveness_field(fields, "root_catalog_liveness_receipt_method")?;
        if method != expected_method {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog liveness receipt method mismatch: expected {expected_method}, got {method}"
            )));
        }
        let node_id = require_liveness_field(fields, "root_catalog_liveness_receipt_node_id")?;
        if node_id != expected_node_id {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog liveness receipt node mismatch: expected {expected_node_id}, got {node_id}"
            )));
        }
        let service_kind =
            require_liveness_field(fields, "root_catalog_liveness_receipt_service_kind")?;
        if service_kind != expected_service_kind {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog liveness receipt service mismatch: expected {expected_service_kind}, got {service_kind}"
            )));
        }
        let heartbeat_ms =
            require_liveness_u64(fields, "root_catalog_liveness_receipt_heartbeat_ms")?;
        let term = optional_liveness_u64(fields, "root_catalog_liveness_receipt_term")?
            .unwrap_or(require_liveness_u64(fields, "raft_term")?);
        let commit_index =
            require_liveness_u64(fields, "root_catalog_liveness_receipt_commit_index")?;
        let read_index = require_liveness_u64(fields, "root_catalog_liveness_receipt_read_index")?;
        let node_count = optional_liveness_u64(fields, "root_catalog_liveness_receipt_node_count")?
            .unwrap_or(require_liveness_u64(fields, "node_count")?);
        let node = RootNode {
            node_id: node_id.to_string(),
            endpoint: require_liveness_field(fields, "endpoint")?.to_string(),
            service_kind: service_kind.to_string(),
            failure_domain: require_liveness_field(fields, "failure_domain")?.to_string(),
            metadata_eligible: require_liveness_bool(fields, "metadata_eligible")?,
            data_eligible: require_liveness_bool(fields, "data_eligible")?,
            gateway_eligible: require_liveness_bool(fields, "gateway_eligible")?,
            metadata_value_bytes: optional_liveness_u64(fields, "metadata_value_bytes")?
                .unwrap_or(0),
            capacity_bytes: require_liveness_u64(fields, "capacity_bytes")?,
            used_bytes: require_liveness_u64(fields, "used_bytes")?,
            committed_bytes: optional_liveness_u64(fields, "committed_bytes")?.unwrap_or(0),
            draining: require_liveness_bool(fields, "draining")?,
            online: require_liveness_bool(fields, "online")?,
            last_heartbeat_ms: heartbeat_ms,
            page_allocator_shard_index: require_liveness_u64(
                fields,
                "block_allocator_shard_index",
            )?,
            page_allocator_shard_count: require_liveness_u64(
                fields,
                "block_allocator_shard_count",
            )?,
            meta_health: RootMetaHealth::default(),
            page_health: RootPageHealth::default(),
            chunk_health: RootChunkHealth::default(),
        };
        let expected_checksum = root_catalog_liveness_receipt_checksum(
            method,
            &node,
            term,
            commit_index,
            read_index,
            node_count,
        );
        let checksum = require_liveness_field(fields, "root_catalog_liveness_receipt_checksum")?;
        if checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "root catalog liveness receipt checksum mismatch: expected {expected_checksum}, got {checksum}"
            )));
        }
        Ok(Self {
            format: format.to_string(),
            checksum: checksum.to_string(),
            method: method.to_string(),
            node_id: node_id.to_string(),
            service_kind: service_kind.to_string(),
            heartbeat_ms,
            term,
            commit_index,
            read_index,
            node_count,
        })
    }

    pub fn require_native_liveness_receipt_fields(
        fields: &BTreeMap<String, String>,
        expected_method: &str,
        expected_node_id: &str,
        expected_service_kind: &str,
    ) -> Result<Self, ObjectError> {
        Self::from_native_liveness_receipt_fields(
            fields,
            expected_method,
            expected_node_id,
            expected_service_kind,
        )
    }
}

pub(crate) fn require_liveness_field<'a>(
    fields: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, ObjectError> {
    let value = fields.get(name).map(String::as_str).unwrap_or("");
    if value.is_empty() {
        return Err(ObjectError::InvalidArgument(format!(
            "root catalog liveness receipt field {name} must be set"
        )));
    }
    Ok(value)
}

pub(crate) fn optional_liveness_u64(
    fields: &BTreeMap<String, String>,
    name: &str,
) -> Result<Option<u64>, ObjectError> {
    let Some(value) = fields.get(name) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    value.parse::<u64>().map(Some).map_err(|_| {
        ObjectError::InvalidArgument(format!(
            "root catalog liveness receipt field {name} must be a u64"
        ))
    })
}

pub(crate) fn require_liveness_u64(
    fields: &BTreeMap<String, String>,
    name: &str,
) -> Result<u64, ObjectError> {
    optional_liveness_u64(fields, name)?.ok_or_else(|| {
        ObjectError::InvalidArgument(format!(
            "root catalog liveness receipt field {name} must be set"
        ))
    })
}

pub(crate) fn require_liveness_bool(
    fields: &BTreeMap<String, String>,
    name: &str,
) -> Result<bool, ObjectError> {
    match require_liveness_field(fields, name)? {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        value => Err(ObjectError::InvalidArgument(format!(
            "root catalog liveness receipt field {name} must be bool, got {value}"
        ))),
    }
}

