// SPDX-License-Identifier: Apache-2.0
impl RootSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_size_hint());
        self.append_to_bytes(&mut out);
        Ok(out)
    }

    pub(crate) fn append_to_bytes(&self, mut out: &mut Vec<u8>) {
        let snapshot_start = out.len();
        const ROOT_SNAPSHOT_WITH_RECEIPT_PROOFS_VERSION: &str = "MOSRSROOT32";
        const ROOT_SNAPSHOT_BASE_VERSION: &str = "MOSRSROOT32";
        const ROOT_SNAPSHOT_RECEIPT_PROOF_PERSISTENCE_THRESHOLD: u64 = 27;
        let snapshot_version = if self.leader_lease_receipts.is_empty() {
            ROOT_SNAPSHOT_BASE_VERSION
        } else {
            ROOT_SNAPSHOT_WITH_RECEIPT_PROOFS_VERSION
        };
        append_string(&mut out, snapshot_version);
        append_u64(&mut out, self.next_commit_index);
        append_u64(&mut out, self.nodes.len() as u64);
        for (node_id, node) in &self.nodes {
            append_string(&mut out, node_id);
            append_string(&mut out, &node.node_id);
            append_string(&mut out, &node.endpoint);
            append_string(&mut out, &node.service_kind);
            append_string(&mut out, &node.failure_domain);
            append_u64(&mut out, node.metadata_eligible as u64);
            append_u64(&mut out, node.data_eligible as u64);
            append_u64(&mut out, node.gateway_eligible as u64);
            append_u64(&mut out, node.metadata_value_bytes);
            append_u64(&mut out, node.capacity_bytes);
            append_u64(&mut out, node.used_bytes);
            append_u64(&mut out, node.committed_bytes);
            append_u64(&mut out, node.draining as u64);
            append_u64(&mut out, node.online as u64);
            append_u64(&mut out, node.last_heartbeat_ms);
            append_u64(&mut out, node.page_allocator_shard_index);
            append_u64(&mut out, node.page_allocator_shard_count);
            append_root_meta_health(&mut out, &node.meta_health);
            append_u64(&mut out, node.page_health.ready as u64);
            append_u64(&mut out, node.page_health.read_ready as u64);
            append_string(&mut out, &node.page_health.route_control_checksum);
            append_u64(&mut out, node.page_health.route_control_commit_index);
            append_u64(&mut out, node.page_health.chunk_endpoint_count);
            append_u64(&mut out, node.page_health.write_quorum);
            append_u64(&mut out, node.page_health.read_quorum);
            append_u64(&mut out, node.page_health.writable_chunk_endpoint_count);
            append_u64(&mut out, node.page_health.healthy_chunk_endpoint_count);
            append_u64(
                &mut out,
                node.page_health.raft_writable_chunk_endpoint_count,
            );
            append_u64(
                &mut out,
                node.page_health.raft_readable_chunk_endpoint_count,
            );
            append_u64(
                &mut out,
                node.page_health
                    .capacity_ready_writable_chunk_endpoint_count,
            );
            append_u64(&mut out, node.page_health.quarantined_chunk_replicas);
            append_u64(&mut out, node.page_health.chunk_capacity_bytes);
            append_u64(&mut out, node.page_health.chunk_capacity_free_bytes);
            append_u64(
                &mut out,
                node.page_health.chunk_metadata_dirty_endpoint_count,
            );
            append_u64(&mut out, node.page_health.chunk_metadata_dirty_mutations);
            append_u64(
                &mut out,
                node.page_health.chunk_metadata_checkpoint_interval,
            );
            append_u64(
                &mut out,
                node.page_health
                    .deferred_metadata_checkpoint_endpoint_count,
            );
            append_u64(
                &mut out,
                node.page_health.deferred_metadata_checkpoint_requests,
            );
            append_u64(
                &mut out,
                node.page_health.deferred_metadata_checkpoint_runs,
            );
            append_u64(&mut out, node.page_health.chunk_capacity_rejected_writes);
            append_u64(
                &mut out,
                node.page_health.replica_service_mode_rejected_mutations,
            );
            append_u64(&mut out, node.page_health.chunk_physical_write_count);
            append_u64(&mut out, node.page_health.chunk_physical_write_bytes);
            append_u64(
                &mut out,
                node.page_health.chunk_physical_discard_range_count,
            );
            append_u64(&mut out, node.page_health.chunk_physical_discard_bytes);
            append_u64(&mut out, node.page_health.total_reclaimable_bytes);
            append_u64(&mut out, node.page_health.total_trailing_trim_bytes);
            append_u64(&mut out, node.page_health.total_fragmented_chunk_count);
            append_u64(&mut out, node.page_health.total_fragmented_discarded_bytes);
            append_u64(&mut out, node.page_health.allocation_receipt_count);
            append_u64(&mut out, node.page_health.batch_allocation_receipt_count);
            append_u64(&mut out, node.page_health.release_allocation_receipt_count);
            append_u64(&mut out, node.page_health.write_receipt_count);
            append_u64(&mut out, node.page_health.batch_write_receipt_count);
            append_u64(&mut out, node.page_health.discard_receipt_count);
            append_u64(&mut out, node.page_health.batch_discard_receipt_count);
            append_root_chunk_health(&mut out, &node.chunk_health, true, true, true);
        }
        append_u64(&mut out, self.placements.len() as u64);
        for (object_id, placement) in &self.placements {
            append_object_id(&mut out, object_id);
            append_object_id(&mut out, &placement.object_id);
            append_u64(&mut out, placement.version);
            append_u64(&mut out, placement.length);
            append_u64(&mut out, placement.update_time_ms);
            append_u64(&mut out, placement.node_ids.len() as u64);
            for node_id in &placement.node_ids {
                append_string(&mut out, node_id);
            }
        }
        if let Some(lease) = &self.leader_lease {
            append_u64(&mut out, 1);
            append_string(&mut out, &lease.leader_node_id);
            append_u64(&mut out, lease.epoch);
            append_u64(&mut out, lease.acquire_time_ms);
            append_u64(&mut out, lease.expire_time_ms);
            append_u64(&mut out, lease.ttl_ms);
        } else {
            append_u64(&mut out, 0);
        }
        append_u64(&mut out, self.leader_lease_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.leader_lease_receipts {
            append_string(&mut out, idempotency_key);
            append_string(&mut out, &receipt.idempotency_key);
            append_string(&mut out, &receipt.operation);
            append_string(&mut out, &receipt.request_checksum);
            append_string(&mut out, &receipt.lease.leader_node_id);
            append_u64(&mut out, receipt.lease.epoch);
            append_u64(&mut out, receipt.lease.acquire_time_ms);
            append_u64(&mut out, receipt.lease.expire_time_ms);
            append_u64(&mut out, receipt.lease.ttl_ms);
            if snapshot_version
                .strip_prefix("MOSRSROOT")
                .and_then(|suffix| suffix.parse::<u64>().ok())
                .unwrap_or(0)
                >= ROOT_SNAPSHOT_RECEIPT_PROOF_PERSISTENCE_THRESHOLD
            {
                append_u64(&mut out, receipt.commit_index);
                append_u64(&mut out, receipt.update_time_ms);
                append_string(&mut out, &receipt.receipt_proof_checksum);
            }
        }
        append_metadata_coordinator_config(&mut out, &self.coordinator_config);
        append_string(&mut out, &self.checksum);
        let envelope_checksum = etag(&out[snapshot_start..]);
        append_string(&mut out, &envelope_checksum);
    }

    pub(crate) fn encoded_size_hint(&self) -> usize {
        const U64: usize = std::mem::size_of::<u64>();
        const ENVELOPE_CHECKSUM_BYTES: usize = "fnv1a64:0000000000000000".len();
        fn object_id_size(object_id: &ObjectId) -> usize {
            encoded_string_len(&object_id.bucket).saturating_add(encoded_string_len(&object_id.key))
        }
        fn root_meta_health_size(health: &RootMetaHealth) -> usize {
            let proof = &health.control_proof;
            let effective_coordinator_backend = if proof.coordinator_backend.is_empty() {
                if proof.external_coordinator_required {
                    "external"
                } else {
                    "embedded-raft"
                }
            } else {
                &proof.coordinator_backend
            };
            U64.saturating_mul(63)
                .saturating_add(encoded_string_len(&proof.state_format))
                .saturating_add(encoded_string_len(&proof.state_checksum))
                .saturating_add(encoded_string_len(&proof.topology_checksum))
                .saturating_add(encoded_string_len(&proof.session_checksum))
                .saturating_add(encoded_string_len(&proof.state_fingerprint))
                .saturating_add(encoded_string_len(&proof.coordinator_config_checksum))
                .saturating_add(encoded_string_len(effective_coordinator_backend))
                .saturating_add(encoded_string_len(&health.route_cache.topology_checksum))
                .saturating_add(encoded_string_len(
                    &health.route_cache.shard_topology_checksum,
                ))
                .saturating_add(encoded_string_len(&health.route_cache.session_checksum))
        }
        fn root_page_health_size(health: &RootPageHealth) -> usize {
            U64.saturating_mul(37).saturating_add(encoded_string_len(
                &health.route_control_checksum,
            ))
        }
        fn root_chunk_health_size(health: &RootChunkHealth) -> usize {
            U64.saturating_mul(15)
                .saturating_add(encoded_string_len(&health.service_mode))
                .saturating_add(encoded_string_len(&health.control_proof_format))
                .saturating_add(encoded_string_len(&health.control_proof_checksum))
                .saturating_add(encoded_string_len(&health.service_health))
                .saturating_add(encoded_string_len(&health.service_health_reasons))
        }
        fn node_size(node_id: &str, node: &RootNode) -> usize {
            U64.saturating_mul(12)
                .saturating_add(encoded_string_len(node_id))
                .saturating_add(encoded_string_len(&node.node_id))
                .saturating_add(encoded_string_len(&node.endpoint))
                .saturating_add(encoded_string_len(&node.service_kind))
                .saturating_add(encoded_string_len(&node.failure_domain))
                .saturating_add(root_meta_health_size(&node.meta_health))
                .saturating_add(root_page_health_size(&node.page_health))
                .saturating_add(root_chunk_health_size(&node.chunk_health))
        }
        fn placement_size(object_id: &ObjectId, placement: &ObjectPlacement) -> usize {
            object_id_size(object_id)
                .saturating_add(object_id_size(&placement.object_id))
                .saturating_add(U64 * 4)
                .saturating_add(
                    placement
                        .node_ids
                        .iter()
                        .fold(0usize, |total, node_id| {
                            total.saturating_add(encoded_string_len(node_id))
                        }),
                )
        }
        fn leader_lease_payload_size(lease: &RootLeaderLease) -> usize {
            encoded_string_len(&lease.leader_node_id).saturating_add(U64 * 4)
        }
        fn leader_lease_size(lease: Option<&RootLeaderLease>) -> usize {
            U64.saturating_add(
                lease.map(leader_lease_payload_size)
                    .unwrap_or_default(),
            )
        }
        fn leader_lease_receipt_size(
            idempotency_key: &str,
            receipt: &RootLeaderLeaseReceipt,
        ) -> usize {
            encoded_string_len(idempotency_key)
                .saturating_add(encoded_string_len(&receipt.idempotency_key))
                .saturating_add(encoded_string_len(&receipt.operation))
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(leader_lease_payload_size(&receipt.lease))
                .saturating_add(U64 * 2)
                .saturating_add(encoded_string_len(&receipt.receipt_proof_checksum))
        }
        fn coordinator_config_size(config: &MetadataCoordinatorConfig) -> usize {
            encoded_string_len(&config.backend)
                .saturating_add(U64)
                .saturating_add(config.endpoints.iter().fold(0usize, |total, endpoint| {
                    total.saturating_add(encoded_string_len(endpoint))
                }))
                .saturating_add(encoded_string_len(&config.namespace_path))
                .saturating_add(U64 * 2)
        }

        let node_bytes = self.nodes.iter().fold(0usize, |total, (node_id, node)| {
            total.saturating_add(node_size(node_id, node))
        });
        let placement_bytes = self
            .placements
            .iter()
            .fold(0usize, |total, (object_id, placement)| {
                total.saturating_add(placement_size(object_id, placement))
            });
        let receipt_bytes =
            self.leader_lease_receipts
                .iter()
                .fold(0usize, |total, (idempotency_key, receipt)| {
                    total.saturating_add(leader_lease_receipt_size(idempotency_key, receipt))
                });
        encoded_string_len("MOSRSROOT32")
            .saturating_add(U64)
            .saturating_add(U64)
            .saturating_add(node_bytes)
            .saturating_add(U64)
            .saturating_add(placement_bytes)
            .saturating_add(leader_lease_size(self.leader_lease.as_ref()))
            .saturating_add(U64)
            .saturating_add(receipt_bytes)
            .saturating_add(coordinator_config_size(&self.coordinator_config))
            .saturating_add(encoded_string_len(&self.checksum))
            .saturating_add(U64 + ENVELOPE_CHECKSUM_BYTES)
    }
}
