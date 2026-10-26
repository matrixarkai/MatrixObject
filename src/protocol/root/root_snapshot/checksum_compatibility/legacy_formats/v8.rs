// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PlacementSnapshot {
    fn v8_checksum_material_hash(&self) -> String {
        let mut material = Vec::new();
        append_u64(&mut material, self.next_commit_index);
        for (node_id, node) in &self.nodes {
            append_string(&mut material, node_id);
            append_string(&mut material, &node.service_kind);
            append_string(&mut material, &node.failure_domain);
            append_u64(&mut material, node.capacity_bytes);
            append_u64(&mut material, node.used_bytes);
            append_u64(&mut material, node.committed_bytes);
            append_u64(&mut material, node.draining as u64);
            append_u64(&mut material, node.online as u64);
            append_u64(&mut material, node.last_heartbeat_ms);
            append_u64(&mut material, node.page_allocator_shard_index);
            append_u64(&mut material, node.page_allocator_shard_count);
            append_placement_meta_health(&mut material, &node.meta_health);
        }
        for (object_id, placement) in &self.placements {
            append_object_id(&mut material, object_id);
            append_u64(&mut material, placement.version);
            append_u64(&mut material, placement.length);
            append_u64(&mut material, placement.update_time_ms);
            append_u64(&mut material, placement.node_ids.len() as u64);
            for node_id in &placement.node_ids {
                append_string(&mut material, node_id);
            }
        }
        if let Some(lease) = &self.leader_lease {
            append_u64(&mut material, 1);
            append_string(&mut material, &lease.leader_node_id);
            append_u64(&mut material, lease.epoch);
            append_u64(&mut material, lease.acquire_time_ms);
            append_u64(&mut material, lease.expire_time_ms);
            append_u64(&mut material, lease.ttl_ms);
        } else {
            append_u64(&mut material, 0);
        }
        append_u64(&mut material, self.leader_lease_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.leader_lease_receipts {
            append_string(&mut material, idempotency_key);
            append_string(&mut material, &receipt.operation);
            append_string(&mut material, &receipt.request_checksum);
            append_string(&mut material, &receipt.lease.leader_node_id);
            append_u64(&mut material, receipt.lease.epoch);
            append_u64(&mut material, receipt.lease.acquire_time_ms);
            append_u64(&mut material, receipt.lease.expire_time_ms);
            append_u64(&mut material, receipt.lease.ttl_ms);
        }
        etag(&material)
    }

}
