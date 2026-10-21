// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn metadata_shard_contains_key(shard: &MetadataShard, key: &str) -> bool {
    shard.start_key.as_str() <= key && (shard.end_key.is_empty() || key < shard.end_key.as_str())
}

pub(crate) fn metadata_shard_overlaps_prefix(shard: &MetadataShard, prefix: &str) -> bool {
    if metadata_shard_contains_key(shard, prefix) || shard.start_key.starts_with(prefix) {
        return true;
    }
    !shard.end_key.is_empty() && shard.end_key.starts_with(prefix)
}

pub(crate) fn chunk_replica_audit_item_payload_bytes(item: &ChunkReplicaAuditItem) -> u64 {
    item.page_id
        .chunk_id
        .volume_id
        .to_string()
        .len()
        .saturating_add(item.page_id.chunk_id.id.to_string().len())
        .saturating_add(item.page_id.page_index.to_string().len())
        .saturating_add(item.status.len())
        .saturating_add(item.checksum.len())
        .saturating_add(72) as u64
}

pub(crate) fn root_placement_health_item_payload_bytes(item: &RootPlacementHealthItem) -> u64 {
    let mut bytes = item
        .placement
        .object_id
        .bucket
        .len()
        .saturating_add(item.placement.object_id.key.len())
        .saturating_add(64);
    for node_id in &item.placement.node_ids {
        bytes = bytes.saturating_add(node_id.len()).saturating_add(8);
    }
    for node_id in item
        .missing_node_ids
        .iter()
        .chain(item.offline_node_ids.iter())
        .chain(item.draining_node_ids.iter())
        .chain(item.capacity_pressure_node_ids.iter())
    {
        bytes = bytes.saturating_add(node_id.len()).saturating_add(8);
    }
    bytes as u64
}

