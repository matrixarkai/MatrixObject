pub(crate) fn metadata_transaction_receipt_payload_bytes(
    receipt: &MetadataTransactionReceipt,
) -> u64 {
    let mut bytes = receipt
        .idempotency_key
        .len()
        .saturating_add(receipt.operation.len())
        .saturating_add(receipt.request_checksum.len())
        .saturating_add(receipt.transaction.transaction_id.len())
        .saturating_add(receipt.transaction.owner.len())
        .saturating_add(receipt.transaction.state.len())
        .saturating_add(96);
    for mutation in &receipt.transaction.mutations {
        bytes = bytes
            .saturating_add(mutation.key.len())
            .saturating_add(mutation.value.len())
            .saturating_add(32);
    }
    for key in receipt.transaction.read_versions.keys() {
        bytes = bytes.saturating_add(key.len()).saturating_add(16);
    }
    for record in &receipt.records {
        bytes = bytes
            .saturating_add(record.key.len())
            .saturating_add(record.value.len())
            .saturating_add(40);
    }
    bytes as u64
}

pub(crate) fn coordinator_session_receipt_payload_bytes(
    receipt: &CoordinatorSessionReceipt,
) -> u64 {
    receipt
        .idempotency_key
        .len()
        .saturating_add(receipt.operation.len())
        .saturating_add(receipt.request_checksum.len())
        .saturating_add(receipt.session.session_id.len())
        .saturating_add(receipt.session.owner.len())
        .saturating_add(receipt.session.endpoint.len())
        .saturating_add(96) as u64
}

pub(crate) fn metadata_batch_receipt_payload_bytes(receipt: &MetadataBatchReceipt) -> u64 {
    let mut bytes = receipt
        .idempotency_key
        .len()
        .saturating_add(receipt.request_checksum.len())
        .saturating_add(receipt.expected_owner.len())
        .saturating_add(receipt.lease_owner.as_ref().map_or(0, String::len))
        .saturating_add(104);
    for record in &receipt.records {
        bytes = bytes
            .saturating_add(record.key.len())
            .saturating_add(record.value.len())
            .saturating_add(40);
    }
    bytes as u64
}

pub(crate) fn metadata_mutation_receipt_payload_bytes(receipt: &MetadataMutationReceipt) -> u64 {
    let mut bytes = receipt
        .audit_key
        .len()
        .saturating_add(receipt.receipt_family.len())
        .saturating_add(receipt.idempotency_key.len())
        .saturating_add(receipt.method.len())
        .saturating_add(receipt.request_checksum.len())
        .saturating_add(receipt.expected_owner.len())
        .saturating_add(receipt.lease_owner.as_ref().map_or(0, String::len))
        .saturating_add(128);
    for key in &receipt.record_keys {
        bytes = bytes.saturating_add(key.len()).saturating_add(8);
    }
    bytes as u64
}

pub(crate) fn metadata_shard_route_payload_bytes(route: &MetadataShardRoute) -> u64 {
    let mut bytes = route
        .shard
        .shard_id
        .len()
        .saturating_add(route.shard.start_key.len())
        .saturating_add(route.shard.end_key.len())
        .saturating_add(route.shard.owner_node_id.len())
        .saturating_add(route.preferred_endpoint.len())
        .saturating_add(40);
    for endpoint in &route.fallback_endpoints {
        bytes = bytes.saturating_add(endpoint.len()).saturating_add(8);
    }
    if let Some(session) = &route.active_owner_session {
        bytes = bytes
            .saturating_add(session.session_id.len())
            .saturating_add(session.owner.len())
            .saturating_add(session.endpoint.len())
            .saturating_add(40);
    }
    bytes as u64
}

pub(crate) fn metadata_shard_inventory_item_payload_bytes(
    item: &MetadataShardInventoryItem,
) -> u64 {
    let mut bytes = item
        .shard
        .shard_id
        .len()
        .saturating_add(item.shard.start_key.len())
        .saturating_add(item.shard.end_key.len())
        .saturating_add(item.shard.owner_node_id.len())
        .saturating_add(48);
    if item.record_stats.is_some() {
        bytes = bytes.saturating_add(56);
    }
    bytes as u64
}

