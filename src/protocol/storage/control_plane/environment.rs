pub(crate) fn non_empty_endpoint(endpoint: Option<&str>) -> Option<&str> {
    endpoint.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

pub fn first_non_empty_env(names: &[&str]) -> Result<Option<String>, ObjectError> {
    for name in names {
        match std::env::var(name) {
            Ok(value) => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    return Ok(Some(trimmed.to_string()));
                }
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(ObjectError::InvalidArgument(format!(
                    "{name} must be valid unicode"
                )));
            }
        }
    }
    Ok(None)
}

pub fn first_non_empty_env_u64(names: &[&str]) -> Result<Option<u64>, ObjectError> {
    let Some(value) = first_non_empty_env(names)? else {
        return Ok(None);
    };
    value.parse::<u64>().map(Some).map_err(|_| {
        ObjectError::InvalidArgument(format!(
            "{} must be an unsigned integer",
            names.first().copied().unwrap_or("environment value")
        ))
    })
}

pub fn first_non_empty_env_bool(names: &[&str]) -> Result<Option<bool>, ObjectError> {
    let Some(value) = first_non_empty_env(names)? else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(ObjectError::InvalidArgument(format!(
            "{} must be a boolean",
            names.first().copied().unwrap_or("environment value")
        ))),
    }
}

pub(crate) fn require_control_plane_catalog_endpoint_set_env_checksums(
    endpoint_set: ControlPlaneCatalogEndpointSet,
) -> Result<ControlPlaneCatalogEndpointSet, ObjectError> {
    if let Some(expected_checksum) = first_non_empty_env(
        &MATRIX_OBJECTSTORE_CONTROL_PLANE_CATALOG_ENDPOINT_SET_CHECKSUM_ENV_NAMES,
    )? {
        endpoint_set.require_catalog_route_checksum(&expected_checksum)?;
    }
    if let Some(expected_checksum) = first_non_empty_env(
        &MATRIX_OBJECTSTORE_CONTROL_PLANE_SELECTED_CATALOG_ROUTE_CHECKSUM_ENV_NAMES,
    )? {
        let Some(selected) = endpoint_set.selected.as_ref() else {
            return Err(ObjectError::PreconditionFailed(
                "control-plane selected catalog route checksum requires a selected catalog route"
                    .to_string(),
            ));
        };
        let mut selection = ControlPlaneCatalogSelection {
            configured_mode: endpoint_set.configured_mode.clone(),
            effective_mode: endpoint_set.effective_mode.clone(),
            catalog_source: selected.catalog_source.clone(),
            catalog_endpoint: selected.catalog_endpoint.clone(),
            rootserver_required: endpoint_set.rootserver_required,
            metaserver_endpoint_configured: endpoint_set.metaserver_endpoint_configured,
            rootserver_endpoint_configured: endpoint_set.rootserver_endpoint_configured,
            catalog_route_checksum_format: CONTROL_PLANE_CATALOG_SELECTION_CHECKSUM_FORMAT
                .to_string(),
            catalog_route_checksum: String::new(),
        };
        selection.catalog_route_checksum = selection.compute_catalog_route_checksum();
        selection.require_catalog_route_checksum(&expected_checksum)?;
    }
    Ok(endpoint_set)
}

pub fn env_any_present(groups: &[&[&str]]) -> Result<bool, ObjectError> {
    for names in groups {
        if first_non_empty_env(names)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn parse_coordinator_endpoint_csv(endpoints: &str) -> Result<Vec<String>, ObjectError> {
    let mut out = Vec::new();
    for item in endpoints.split(',') {
        let endpoint = item.trim();
        if endpoint.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata coordinator endpoint list contains an empty endpoint".to_string(),
            ));
        }
        out.push(endpoint.to_string());
    }
    Ok(out)
}

pub fn parse_raft_voter_csv(voters: &str) -> Result<Vec<String>, ObjectError> {
    let mut out = Vec::new();
    for item in voters.split(',') {
        let node_id = item.trim();
        if node_id.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "raft voter list contains an empty node id".to_string(),
            ));
        }
        out.push(node_id.to_string());
    }
    canonical_raft_voters(out)
}

pub(crate) fn non_empty_str(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(crate) fn parse_catalog_endpoint_list(
    endpoints: Option<&str>,
) -> Result<Vec<String>, ObjectError> {
    let Some(endpoints) = non_empty_endpoint(endpoints) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    out.reserve(endpoints.matches(',').count().saturating_add(1));
    let mut seen = BTreeSet::new();
    for item in endpoints.split(',') {
        let endpoint = item.trim();
        if endpoint.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "catalog endpoint list contains an empty endpoint".to_string(),
            ));
        }
        if seen.insert(endpoint) {
            out.push(endpoint.to_string());
        }
    }
    Ok(out)
}
