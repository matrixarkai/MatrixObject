// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn root_catalog_registration_fields(
    service_kind: &str,
    node: &PlacementNode,
    catalog_source: &str,
    catalog_endpoint: &str,
    catalog_endpoint_count: u64,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert("service_kind".to_string(), service_kind.to_string());
    fields.insert("node_id".to_string(), node.node_id.clone());
    fields.insert("endpoint".to_string(), node.endpoint.clone());
    fields.insert("online".to_string(), node.online.to_string());
    fields.insert("draining".to_string(), node.draining.to_string());
    fields.insert("catalog_source".to_string(), catalog_source.to_string());
    fields.insert("catalog_endpoint".to_string(), catalog_endpoint.to_string());
    fields.insert(
        "catalog_endpoint_count".to_string(),
        catalog_endpoint_count.to_string(),
    );
    fields.insert(
        "catalog_route_checksum_format".to_string(),
        CATALOG_ROUTE_CHECKSUM_FORMAT.to_string(),
    );
    fields.insert(
        "catalog_route_checksum".to_string(),
        root_catalog_route_checksum(
            service_kind,
            node,
            catalog_source,
            catalog_endpoint,
            catalog_endpoint_count,
        ),
    );
    fields
}

pub(crate) fn root_catalog_route_checksum(
    service_kind: &str,
    node: &PlacementNode,
    catalog_source: &str,
    catalog_endpoint: &str,
    catalog_endpoint_count: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, CATALOG_ROUTE_CHECKSUM_FORMAT);
    append_string(&mut material, service_kind);
    append_string(&mut material, &node.node_id);
    append_string(&mut material, &node.endpoint);
    append_string(&mut material, catalog_source);
    append_string(&mut material, catalog_endpoint);
    append_u64(&mut material, catalog_endpoint_count);
    etag(&material)
}

pub(crate) fn root_catalog_liveness_receipt(
    method: &str,
    node: &PlacementNode,
    term: u64,
    commit_index: u64,
    read_index: u64,
    node_count: u64,
) -> CatalogLivenessReceipt {
    CatalogLivenessReceipt {
        format: CATALOG_LIVENESS_RECEIPT_FORMAT.to_string(),
        checksum: root_catalog_liveness_receipt_checksum(
            method,
            node,
            term,
            commit_index,
            read_index,
            node_count,
        ),
        method: method.to_string(),
        node_id: node.node_id.clone(),
        service_kind: node.service_kind.clone(),
        heartbeat_ms: node.last_heartbeat_ms,
        term,
        commit_index,
        read_index,
        node_count,
    }
}

pub(crate) fn root_catalog_liveness_receipt_checksum(
    method: &str,
    node: &PlacementNode,
    term: u64,
    commit_index: u64,
    read_index: u64,
    node_count: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, CATALOG_LIVENESS_RECEIPT_FORMAT);
    append_string(&mut material, method);
    append_string(&mut material, &node.node_id);
    append_string(&mut material, &node.service_kind);
    append_string(&mut material, &node.endpoint);
    append_string(&mut material, &node.failure_domain);
    append_u64(&mut material, node.capacity_bytes);
    append_u64(&mut material, node.used_bytes);
    append_u64(&mut material, node.last_heartbeat_ms);
    append_u64(&mut material, if node.online { 1 } else { 0 });
    append_u64(&mut material, if node.draining { 1 } else { 0 });
    append_u64(&mut material, if node.metadata_eligible { 1 } else { 0 });
    append_u64(&mut material, if node.data_eligible { 1 } else { 0 });
    append_u64(&mut material, if node.gateway_eligible { 1 } else { 0 });
    append_u64(&mut material, node.page_allocator_shard_index);
    append_u64(&mut material, node.page_allocator_shard_count);
    append_u64(&mut material, term);
    append_u64(&mut material, commit_index);
    append_u64(&mut material, read_index);
    append_u64(&mut material, node_count);
    etag(&material)
}

pub(crate) fn require_root_catalog_route_checksum(
    service_kind: &str,
    actual_checksum: &str,
    expected_checksum: &str,
) -> Result<(), ObjectError> {
    if expected_checksum.is_empty() {
        return Err(ObjectError::InvalidArgument(format!(
            "{service_kind} catalog route checksum must be set"
        )));
    }
    if actual_checksum != expected_checksum {
        return Err(ObjectError::PreconditionFailed(format!(
            "{service_kind} catalog route checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
        )));
    }
    Ok(())
}

