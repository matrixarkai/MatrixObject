pub(crate) fn root_node_usage(
    node: &RootNode,
    min_free_bytes: u64,
    min_reclaimable_bytes: u64,
    registered: bool,
    object_count: u64,
    referenced_bytes: u64,
) -> RootNodeUsage {
    let free_bytes = node.capacity_bytes.saturating_sub(node.committed_bytes);
    let reported_free_bytes = MatrixMetaService::node_effective_free_bytes(node);
    let committed_vs_reported_delta_bytes = node.committed_bytes.abs_diff(node.used_bytes);
    let reclaimable_bytes = node.used_bytes.saturating_sub(node.committed_bytes);
    let overcommitted_bytes = node.committed_bytes.saturating_sub(node.used_bytes);
    let data_service = MatrixMetaService::is_data_service_node(node);
    let chunk_reclaim_pressure = node.chunk_health.reclaimable_bytes > 0
        || node.chunk_health.trailing_trim_bytes > 0
        || node.chunk_health.discarded_range_count > 0
        || node.chunk_health.fragmented_chunk_count > 0
        || node.chunk_health.fragmented_discarded_bytes > 0;
    let placement_eligible = MatrixMetaService::node_accepts_new_placement(node)
        && node.committed_bytes <= MatrixMetaService::node_effective_capacity_bytes(node)
        && (min_free_bytes == 0 || reported_free_bytes >= min_free_bytes);
    let effective_reclaimable_bytes = reclaimable_bytes.max(node.chunk_health.reclaimable_bytes);
    let reclaim_pressure = data_service
        && (chunk_reclaim_pressure || effective_reclaimable_bytes > 0)
        && (min_reclaimable_bytes == 0 || effective_reclaimable_bytes >= min_reclaimable_bytes);
    RootNodeUsage {
        node: node.clone(),
        registered,
        object_count,
        referenced_bytes,
        free_bytes,
        reported_free_bytes,
        committed_vs_reported_delta_bytes,
        reclaimable_bytes: effective_reclaimable_bytes,
        overcommitted_bytes,
        capacity_pressure: min_free_bytes > 0 && reported_free_bytes < min_free_bytes,
        reclaim_pressure,
        placement_eligible,
    }
}

pub(crate) fn unregistered_root_node_usage(
    node_id: &str,
    object_count: u64,
    referenced_bytes: u64,
) -> RootNodeUsage {
    RootNodeUsage {
        node: RootNode {
            node_id: node_id.to_string(),
            endpoint: String::new(),
            service_kind: "unregistered".to_string(),
            failure_domain: String::new(),
            metadata_eligible: false,
            data_eligible: false,
            gateway_eligible: false,
            metadata_value_bytes: 0,
            capacity_bytes: 0,
            used_bytes: 0,
            committed_bytes: referenced_bytes,
            draining: true,
            online: false,
            last_heartbeat_ms: 0,
            page_allocator_shard_index: 0,
            page_allocator_shard_count: 0,
            meta_health: RootMetaHealth::default(),
            page_health: RootPageHealth::default(),
            chunk_health: RootChunkHealth::default(),
        },
        registered: false,
        object_count,
        referenced_bytes,
        free_bytes: 0,
        reported_free_bytes: 0,
        committed_vs_reported_delta_bytes: referenced_bytes,
        reclaimable_bytes: 0,
        overcommitted_bytes: referenced_bytes,
        capacity_pressure: referenced_bytes > 0,
        reclaim_pressure: false,
        placement_eligible: false,
    }
}

pub(crate) fn k_max_multipart_part_number() -> u32 {
    10_000
}

pub(crate) fn meta_bucket_key(bucket: &str) -> String {
    format!("bucket/{bucket}")
}

pub(crate) fn meta_lifecycle_apply_key() -> &'static str {
    "lifecycle/apply"
}

pub(crate) fn meta_lifecycle_transition_key() -> &'static str {
    "lifecycle/transition"
}

pub(crate) fn meta_object_key(object_id: &ObjectId) -> String {
    if object_id.key.is_empty() {
        meta_bucket_key(&object_id.bucket)
    } else {
        format!("object/{}/{}", object_id.bucket, object_id.key)
    }
}

pub(crate) fn metadata_projection_value(metadata: &ObjectMetadata) -> String {
    format!(
        "version={} length={} etag={} content_type={} metadata_count={} storage_class={} restore_until_ms={} deleted={} extent_count={} tag_count={} legal_hold={} retention_until_ms={}",
        metadata.version,
        metadata.length,
        metadata.etag,
        metadata.content_type,
        metadata.user_metadata.len(),
        metadata.storage_class,
        metadata.lock.restore_until_ms,
        metadata.deleted,
        metadata.extents.len(),
        metadata.tags.len(),
        metadata.lock.legal_hold,
        metadata.lock.retention_until_ms
    )
}

pub(crate) fn object_inventory_record_payload_bytes(record: &ObjectInventoryRecord) -> u64 {
    let mut bytes = metadata_projection_value(&record.metadata).len() as u64;
    bytes = bytes
        .saturating_add(record.metadata.object_id.bucket.len() as u64)
        .saturating_add(record.metadata.object_id.key.len() as u64)
        .saturating_add(record.metadata.etag.len() as u64)
        .saturating_add(record.metadata.content_type.len() as u64)
        .saturating_add(record.metadata.storage_class.len() as u64)
        .saturating_add((record.metadata.extents.len() as u64).saturating_mul(32));
    for (key, value) in &record.metadata.tags {
        bytes = bytes
            .saturating_add(key.len() as u64)
            .saturating_add(value.len() as u64);
    }
    if let Some(placement) = &record.placement {
        bytes = bytes
            .saturating_add(placement.object_id.bucket.len() as u64)
            .saturating_add(placement.object_id.key.len() as u64)
            .saturating_add((placement.node_ids.len() as u64).saturating_mul(16));
        for node_id in &placement.node_ids {
            bytes = bytes.saturating_add(node_id.len() as u64);
        }
    }
    bytes
}

pub(crate) fn retention_active(
    bucket_retention: &BTreeMap<String, u64>,
    metadata: &ObjectMetadata,
    now_ms: u64,
) -> bool {
    if metadata.lock.retention_until_ms > now_ms {
        return true;
    }
    bucket_retention
        .get(metadata.object_id.bucket.as_str())
        .map_or(false, |retain_for_ms| {
            *retain_for_ms > 0 && metadata.update_time_ms.saturating_add(*retain_for_ms) > now_ms
        })
}

