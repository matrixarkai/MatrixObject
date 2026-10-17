pub(crate) fn preferred_production_coordinator_backend() -> &'static str {
    "embedded-raft"
}

pub(crate) fn coordinator_preferred_backend_active(backend: &str) -> bool {
    normalize_coordinator_backend(backend) == preferred_production_coordinator_backend()
}

pub(crate) fn external_coordinator_compatibility_mode(backend: &str) -> bool {
    matches!(
        normalize_coordinator_backend(backend).as_str(),
        "etcd" | "zookeeper"
    )
}

pub(crate) fn production_coordinator_config(backend: &str, endpoints: &[String]) -> bool {
    coordinator_preferred_backend_active(backend) && endpoints.is_empty()
}

pub(crate) fn external_coordinator_dependency(backend: &str) -> String {
    let normalized = normalize_coordinator_backend(backend);
    if external_coordinator_compatibility_mode(&normalized) {
        normalized
    } else {
        String::new()
    }
}

pub(crate) fn metadata_coordinator_policy_checksum(config: &MetadataCoordinatorConfig) -> String {
    let normalized_backend = normalize_coordinator_backend(&config.backend);
    let mut material = Vec::new();
    append_string(&mut material, coordinator_policy_format());
    append_string(&mut material, &normalized_backend);
    append_string(&mut material, preferred_production_coordinator_backend());
    append_u64(
        &mut material,
        coordinator_preferred_backend_active(&normalized_backend) as u64,
    );
    append_u64(
        &mut material,
        external_coordinator_compatibility_mode(&normalized_backend) as u64,
    );
    append_u64(
        &mut material,
        external_coordinator_compatibility_mode(&normalized_backend) as u64,
    );
    append_string(&mut material, &config.namespace_path);
    append_u64(&mut material, config.session_ttl_ms);
    append_u64(&mut material, config.endpoints.len() as u64);
    for endpoint in &config.endpoints {
        append_string(&mut material, endpoint);
    }
    etag(&material)
}

pub(crate) fn validate_metadata_coordinator_config(
    config: &MetadataCoordinatorConfig,
) -> Result<(), ObjectError> {
    if !is_supported_coordinator_backend(&config.backend) {
        return Err(ObjectError::InvalidArgument(
            "metadata coordinator backend must be embedded-raft/raft/matrix-raft, embedded, etcd, or zookeeper/zk"
                .to_string(),
        ));
    }
    if config.revision == 0 {
        return Err(ObjectError::InvalidArgument(
            "metadata coordinator config revision must be positive".to_string(),
        ));
    }
    if !(1_000..=86_400_000).contains(&config.session_ttl_ms) {
        return Err(ObjectError::InvalidArgument(
            "metadata coordinator session ttl must be 1000..86400000 ms".to_string(),
        ));
    }
    if config.namespace_path.is_empty()
        || config.namespace_path.len() > 4096
        || !config.namespace_path.starts_with('/')
        || config.namespace_path.contains('\0')
        || config.namespace_path.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "metadata coordinator namespace path must be absolute and 1..4096 bytes without control separators"
                .to_string(),
        ));
    }
    if is_embedded_coordinator_backend(&config.backend) && !config.endpoints.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "embedded-raft metadata coordinator must not define endpoints".to_string(),
        ));
    }
    if config.endpoints.len() > 64 {
        return Err(ObjectError::InvalidArgument(
            "metadata coordinator endpoint count must be <= 64".to_string(),
        ));
    }
    if !is_embedded_coordinator_backend(&config.backend) && config.endpoints.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "distributed metadata coordinator backends require endpoints".to_string(),
        ));
    }
    let mut previous = None::<&String>;
    for endpoint in &config.endpoints {
        validate_session_endpoint(endpoint)?;
        if previous.is_some_and(|prev| prev >= endpoint) {
            return Err(ObjectError::InvalidArgument(
                "metadata coordinator endpoints must be sorted and unique".to_string(),
            ));
        }
        previous = Some(endpoint);
    }
    Ok(())
}

pub(crate) fn validate_node_id(node_id: &str) -> Result<(), ObjectError> {
    if node_id.is_empty() || node_id.len() > 256 || node_id.contains('\0') || node_id.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "node id must be 1..256 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_service_kind(service_kind: &str) -> Result<(), ObjectError> {
    if service_kind.is_empty()
        || service_kind.len() > 64
        || service_kind.contains('\0')
        || service_kind.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "service kind must be 1..64 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

