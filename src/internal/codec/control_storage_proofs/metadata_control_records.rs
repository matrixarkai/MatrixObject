pub(crate) fn coordinator_session_metadata(
    session: &CoordinatorSession,
    content_type: &str,
) -> ObjectMetadata {
    ObjectMetadata {
        object_id: ObjectId::new("_metacoord", session.session_id.clone()),
        version: session.epoch,
        length: 0,
        etag: etag(
            format!(
                "{}\n{}\n{}\n{}\n{}",
                session.session_id,
                session.owner,
                session.endpoint,
                session.epoch,
                session.expire_time_ms
            )
            .as_bytes(),
        ).into(),
        content_type: content_type.to_string().into(),
        user_metadata: BTreeMap::new().into(),
        storage_class: k_standard_storage_class().to_string().into(),
        lock: ObjectLock::default(),
        tags: BTreeMap::new().into(),
        update_time_ms: session.last_renew_ms,
        extents: Vec::new().into(),
        deleted: false,
    }
}

pub(crate) fn metadata_coordinator_config_metadata(
    config: &MetadataCoordinatorConfig,
    content_type: &str,
    update_time_ms: u64,
) -> ObjectMetadata {
    let mut material = Vec::new();
    append_metadata_coordinator_config(&mut material, config);
    ObjectMetadata {
        object_id: ObjectId::new("_metacoord", "config"),
        version: config.revision,
        length: config.endpoints.len() as u64,
        etag: etag(&material).into(),
        content_type: content_type.to_string().into(),
        user_metadata: BTreeMap::new().into(),
        storage_class: k_standard_storage_class().to_string().into(),
        lock: ObjectLock::default(),
        tags: BTreeMap::new().into(),
        update_time_ms,
        extents: Vec::new().into(),
        deleted: false,
    }
}

pub(crate) fn root_coordinator_config_metadata(
    config: &MetadataCoordinatorConfig,
) -> ObjectMetadata {
    let mut material = Vec::new();
    append_metadata_coordinator_config(&mut material, config);
    ObjectMetadata {
        object_id: ObjectId::new("_rootcoord", "config"),
        version: config.revision,
        length: config.endpoints.len() as u64,
        etag: etag(&material).into(),
        content_type: "rootserver/coordinator-config".to_string().into(),
        user_metadata: BTreeMap::new().into(),
        storage_class: k_standard_storage_class().to_string().into(),
        lock: ObjectLock::default(),
        tags: BTreeMap::new().into(),
        update_time_ms: now_millis(),
        extents: Vec::new().into(),
        deleted: false,
    }
}

pub(crate) fn metadata_lease_metadata(lease: &MetadataLease, content_type: &str) -> ObjectMetadata {
    ObjectMetadata {
        object_id: ObjectId::new("_metalease", lease.namespace.clone()),
        version: lease.epoch,
        length: lease.ttl_ms,
        etag: etag(
            format!(
                "{}\n{}\n{}\n{}\n{}",
                lease.namespace, lease.owner, lease.epoch, lease.expire_time_ms, lease.ttl_ms
            )
            .as_bytes(),
        ).into(),
        content_type: content_type.to_string().into(),
        user_metadata: BTreeMap::new().into(),
        storage_class: k_standard_storage_class().to_string().into(),
        lock: ObjectLock::default(),
        tags: BTreeMap::new().into(),
        update_time_ms: lease.expire_time_ms.saturating_sub(lease.ttl_ms),
        extents: Vec::new().into(),
        deleted: content_type.starts_with("metaserver/lease-release"),
    }
}

pub(crate) fn root_leader_lease_metadata(
    lease: &RootLeaderLease,
    content_type: &str,
) -> ObjectMetadata {
    ObjectMetadata {
        object_id: ObjectId::new("_rootlease", lease.leader_node_id.clone()),
        version: lease.epoch,
        length: lease.ttl_ms,
        etag: etag(
            format!(
                "{}\n{}\n{}\n{}\n{}",
                lease.leader_node_id,
                lease.epoch,
                lease.acquire_time_ms,
                lease.expire_time_ms,
                lease.ttl_ms
            )
            .as_bytes(),
        ).into(),
        content_type: content_type.to_string().into(),
        user_metadata: BTreeMap::new().into(),
        storage_class: k_standard_storage_class().to_string().into(),
        lock: ObjectLock::default(),
        tags: BTreeMap::new().into(),
        update_time_ms: lease.expire_time_ms.saturating_sub(lease.ttl_ms),
        extents: Vec::new().into(),
        deleted: false,
    }
}

pub(crate) fn metadata_control_metadata(
    bucket: &str,
    key: &str,
    version: u64,
    length: u64,
    content_type: &str,
    update_time_ms: u64,
) -> ObjectMetadata {
    ObjectMetadata {
        object_id: ObjectId::new(bucket, key),
        version,
        length,
        etag: etag(format!("{bucket}\n{key}\n{version}\n{length}\n{content_type}").as_bytes()).into(),
        content_type: content_type.to_string().into(),
        user_metadata: BTreeMap::new().into(),
        storage_class: k_standard_storage_class().to_string().into(),
        lock: ObjectLock::default(),
        tags: BTreeMap::new().into(),
        update_time_ms,
        extents: Vec::new().into(),
        deleted: false,
    }
}

pub(crate) fn change_checkpoint_key(consumer_id: &str, prefix: &str) -> String {
    format!("{consumer_id}\n{prefix}")
}

