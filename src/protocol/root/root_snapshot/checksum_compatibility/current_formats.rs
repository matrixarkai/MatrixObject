// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl RootSnapshot {
    fn v26_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, true, true, true, true, false, false, false, false, false,
        )
    }

    fn v22_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, true, true, true, false, false, false, false, false, false,
        )
    }

    fn v21_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, true, true, false, false, false, false, false, false, false,
        )
    }

    fn v20_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, true, false, false, false, false, false, false, false, false,
        )
    }

    fn v19_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, false, false, false, false, false, false, false, false, false,
        )
    }

    fn v18_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, false, false, false, false, false, false, false, false, false, false,
        )
    }

    /// The format that was CURRENT until the page raft endpoint counts joined the material.
    ///
    /// A snapshot written by any build before that change stored this hash, so it has to stay
    /// in the accepted list -- otherwise closing the coverage hole would make every existing
    /// store fail to load, which is a break and not a fix.
    fn v32_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            true, true, true, true, true, true, true, true, true, true, false,
        )
    }

    fn v16_checksum_material_hash(&self) -> String {
        self.checksum_material_hash_with_chunk_control(
            false, false, false, false, false, false, false, false, false, false, false,
        )
    }

    pub(crate) fn checksum_material_hash_with_chunk_control(
        &self,
        include_chunk_control: bool,
        include_page_allocator_receipts: bool,
        include_node_endpoint: bool,
        include_coordinator_config: bool,
        include_chunk_reclaim_health: bool,
        include_page_mutation_receipts: bool,
        include_root_leader_lease_receipt_proof: bool,
        include_page_extended_service_health: bool,
        include_page_reclaim_health: bool,
        include_replica_service_health: bool,
        // THE ELEVENTH, AND IT CLOSES A HOLE RATHER THAN ADDING A FIELD. The two page-health
        // raft endpoint counts have been written into the snapshot BYTES since
        // `has_root_page_raft_endpoint_counts` arrived, and they appeared in NONE of the
        // fourteen checksum materials -- so a flipped byte in either decoded to a different
        // number and still validated. Guarded by its own flag so every older format keeps
        // producing exactly the bytes it always did.
        include_page_raft_endpoint_counts: bool,
    ) -> String {
        let mut material = Vec::with_capacity(self.encoded_size_hint());
        append_u64(&mut material, self.next_commit_index);
        for (node_id, node) in &self.nodes {
            append_string(&mut material, node_id);
            if include_node_endpoint {
                append_string(&mut material, &node.endpoint);
            }
            append_string(&mut material, &node.service_kind);
            append_string(&mut material, &node.failure_domain);
            append_u64(&mut material, node.metadata_eligible as u64);
            append_u64(&mut material, node.data_eligible as u64);
            append_u64(&mut material, node.gateway_eligible as u64);
            append_u64(&mut material, node.metadata_value_bytes);
            append_u64(&mut material, node.capacity_bytes);
            append_u64(&mut material, node.used_bytes);
            append_u64(&mut material, node.committed_bytes);
            append_u64(&mut material, node.draining as u64);
            append_u64(&mut material, node.online as u64);
            append_u64(&mut material, node.last_heartbeat_ms);
            append_u64(&mut material, node.page_allocator_shard_index);
            append_u64(&mut material, node.page_allocator_shard_count);
            append_root_meta_health(&mut material, &node.meta_health);
            append_u64(&mut material, node.page_health.ready as u64);
            append_u64(&mut material, node.page_health.read_ready as u64);
            append_string(&mut material, &node.page_health.route_control_checksum);
            append_u64(&mut material, node.page_health.route_control_commit_index);
            append_u64(&mut material, node.page_health.chunk_endpoint_count);
            append_u64(&mut material, node.page_health.write_quorum);
            append_u64(&mut material, node.page_health.read_quorum);
            append_u64(
                &mut material,
                node.page_health.writable_chunk_endpoint_count,
            );
            append_u64(
                &mut material,
                node.page_health.healthy_chunk_endpoint_count,
            );
            if include_page_raft_endpoint_counts {
                // THE SAME ORDER THE BYTES USE, so that the two encodings at least agree
                // about this pair. They cannot be collapsed into one codec: the material's
                // field order differs from the byte encoder's elsewhere, and reordering it
                // would change every stored checksum.
                append_u64(
                    &mut material,
                    node.page_health.raft_writable_chunk_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.raft_readable_chunk_endpoint_count,
                );
            }
            append_u64(
                &mut material,
                node.page_health
                    .capacity_ready_writable_chunk_endpoint_count,
            );
            append_u64(&mut material, node.page_health.quarantined_chunk_replicas);
            append_u64(&mut material, node.page_health.chunk_capacity_bytes);
            append_u64(&mut material, node.page_health.chunk_capacity_free_bytes);
            if include_page_extended_service_health {
                append_u64(
                    &mut material,
                    node.page_health.chunk_metadata_dirty_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_metadata_dirty_mutations,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_metadata_checkpoint_interval,
                );
                append_u64(
                    &mut material,
                    node.page_health
                        .deferred_metadata_checkpoint_endpoint_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.deferred_metadata_checkpoint_requests,
                );
                append_u64(
                    &mut material,
                    node.page_health.deferred_metadata_checkpoint_runs,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_capacity_rejected_writes,
                );
                append_u64(
                    &mut material,
                    node.page_health.replica_service_mode_rejected_mutations,
                );
                append_u64(&mut material, node.page_health.chunk_physical_write_count);
                append_u64(&mut material, node.page_health.chunk_physical_write_bytes);
                append_u64(
                    &mut material,
                    node.page_health.chunk_physical_discard_range_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.chunk_physical_discard_bytes,
                );
                if include_page_reclaim_health {
                    append_u64(&mut material, node.page_health.total_reclaimable_bytes);
                    append_u64(&mut material, node.page_health.total_trailing_trim_bytes);
                    append_u64(
                        &mut material,
                        node.page_health.total_fragmented_chunk_count,
                    );
                    append_u64(
                        &mut material,
                        node.page_health.total_fragmented_discarded_bytes,
                    );
                }
            }
            if include_page_allocator_receipts {
                append_u64(&mut material, node.page_health.allocation_receipt_count);
                append_u64(
                    &mut material,
                    node.page_health.batch_allocation_receipt_count,
                );
                append_u64(
                    &mut material,
                    node.page_health.release_allocation_receipt_count,
                );
                if include_page_mutation_receipts {
                    append_u64(&mut material, node.page_health.write_receipt_count);
                    append_u64(&mut material, node.page_health.batch_write_receipt_count);
                    append_u64(&mut material, node.page_health.discard_receipt_count);
                    append_u64(&mut material, node.page_health.batch_discard_receipt_count);
                }
            }
            append_root_chunk_health(
                &mut material,
                &node.chunk_health,
                include_chunk_control,
                include_chunk_reclaim_health,
                include_replica_service_health,
            );
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
            if include_root_leader_lease_receipt_proof {
                append_u64(&mut material, receipt.commit_index);
                append_u64(&mut material, receipt.update_time_ms);
                append_string(&mut material, &receipt.receipt_proof_checksum);
            }
        }
        if include_coordinator_config {
            append_metadata_coordinator_config(&mut material, &self.coordinator_config);
        }
        etag(&material)
    }

}
