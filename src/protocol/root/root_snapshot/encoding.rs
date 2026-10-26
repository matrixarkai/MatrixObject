// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PlacementSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_size_hint());
        self.append_to_bytes(&mut out);
        Ok(out)
    }

    pub(crate) fn append_to_bytes(&self, mut out: &mut Vec<u8>) {
        let snapshot_start = out.len();
        const PLACEMENT_SNAPSHOT_WITH_RECEIPT_PROOFS_VERSION: &str = "MOSRSROOT32";
        const PLACEMENT_SNAPSHOT_BASE_VERSION: &str = "MOSRSROOT32";
        const PLACEMENT_SNAPSHOT_RECEIPT_PROOF_PERSISTENCE_THRESHOLD: u64 = 27;
        let snapshot_version = if self.leader_lease_receipts.is_empty() {
            PLACEMENT_SNAPSHOT_BASE_VERSION
        } else {
            PLACEMENT_SNAPSHOT_WITH_RECEIPT_PROOFS_VERSION
        };
        append_string(&mut out, snapshot_version);
        append_u64(&mut out, self.next_commit_index);
        append_placement_roster(
            &mut out,
            &self.nodes,
            &self.placements,
            &self.leader_lease,
            &self.leader_lease_receipts,
            &self.coordinator_config,
            snapshot_version
                .strip_prefix("MOSRSROOT")
                .and_then(|suffix| suffix.parse::<u64>().ok())
                .unwrap_or(0)
                >= PLACEMENT_SNAPSHOT_RECEIPT_PROOF_PERSISTENCE_THRESHOLD,
        );
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
        fn placement_meta_health_size(health: &NodeMetaHealth) -> usize {
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
        fn placement_page_health_size(health: &NodePageHealth) -> usize {
            U64.saturating_mul(37).saturating_add(encoded_string_len(
                &health.route_control_checksum,
            ))
        }
        fn placement_chunk_health_size(health: &NodeChunkHealth) -> usize {
            U64.saturating_mul(15)
                .saturating_add(encoded_string_len(&health.service_mode))
                .saturating_add(encoded_string_len(&health.control_proof_format))
                .saturating_add(encoded_string_len(&health.control_proof_checksum))
                .saturating_add(encoded_string_len(&health.service_health))
                .saturating_add(encoded_string_len(&health.service_health_reasons))
        }
        fn node_size(node_id: &str, node: &PlacementNode) -> usize {
            U64.saturating_mul(12)
                .saturating_add(encoded_string_len(node_id))
                .saturating_add(encoded_string_len(&node.node_id))
                .saturating_add(encoded_string_len(&node.endpoint))
                .saturating_add(encoded_string_len(&node.service_kind))
                .saturating_add(encoded_string_len(&node.failure_domain))
                .saturating_add(placement_meta_health_size(&node.meta_health))
                .saturating_add(placement_page_health_size(&node.page_health))
                .saturating_add(placement_chunk_health_size(&node.chunk_health))
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
        fn leader_lease_payload_size(lease: &PlacementLeaderLease) -> usize {
            encoded_string_len(&lease.leader_node_id).saturating_add(U64 * 4)
        }
        fn leader_lease_size(lease: Option<&PlacementLeaderLease>) -> usize {
            U64.saturating_add(
                lease.map(leader_lease_payload_size)
                    .unwrap_or_default(),
            )
        }
        fn leader_lease_receipt_size(
            idempotency_key: &str,
            receipt: &PlacementLeaderLeaseReceipt,
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

/// The bytes a control-plane roster takes: its nodes, the placements they hold, the leader lease
/// and the lease receipts, and the coordinator configuration they were agreed under.
///
/// # WHY THIS IS SHARED
///
/// The roster is moving into the metadata snapshot, so these bytes are written from two
/// places. Writing them twice would be two encoders that have to agree forever, and the one
/// thing this project has measured repeatedly is that a rule written out twice diverges.
///
/// `with_receipt_proof` is a parameter rather than a stamp lookup because the second caller
/// has always written those three fields and should not have to name a version to say so.
/// The first caller still decides it from the stamp it is writing.
///
/// The bytes are unchanged for the existing caller, and that is pinned rather than asserted:
/// see `the_placement_snapshot_encoding_is_byte_for_byte_what_it_was`, whose length and digest
/// were captured from the encoder before any of this moved.
pub(crate) fn append_placement_roster(
    out: &mut Vec<u8>,
    nodes: &BTreeMap<String, PlacementNode>,
    placements: &BTreeMap<ObjectId, ObjectPlacement>,
    leader_lease: &Option<PlacementLeaderLease>,
    leader_lease_receipts: &BTreeMap<String, PlacementLeaderLeaseReceipt>,
    coordinator_config: &MetadataCoordinatorConfig,
    with_receipt_proof: bool,
) {
    append_u64(out, nodes.len() as u64);
    for (node_id, node) in nodes {
        append_string(out, node_id);
        append_string(out, &node.node_id);
        append_string(out, &node.endpoint);
        append_string(out, &node.service_kind);
        append_string(out, &node.failure_domain);
        append_u64(out, node.metadata_eligible as u64);
        append_u64(out, node.data_eligible as u64);
        append_u64(out, node.gateway_eligible as u64);
        append_u64(out, node.metadata_value_bytes);
        append_u64(out, node.capacity_bytes);
        append_u64(out, node.used_bytes);
        append_u64(out, node.committed_bytes);
        append_u64(out, node.draining as u64);
        append_u64(out, node.online as u64);
        append_u64(out, node.last_heartbeat_ms);
        append_u64(out, node.page_allocator_shard_index);
        append_u64(out, node.page_allocator_shard_count);
        append_placement_meta_health(out, &node.meta_health);
        append_placement_page_health(out, &node.page_health);
        append_placement_chunk_health(out, &node.chunk_health, true, true, true);
    }
    append_u64(out, placements.len() as u64);
    for (object_id, placement) in placements {
        append_object_id(out, object_id);
        append_object_id(out, &placement.object_id);
        append_u64(out, placement.version);
        append_u64(out, placement.length);
        append_u64(out, placement.update_time_ms);
        append_u64(out, placement.node_ids.len() as u64);
        for node_id in &placement.node_ids {
            append_string(out, node_id);
        }
    }
    if let Some(lease) = leader_lease {
        append_u64(out, 1);
        append_string(out, &lease.leader_node_id);
        append_u64(out, lease.epoch);
        append_u64(out, lease.acquire_time_ms);
        append_u64(out, lease.expire_time_ms);
        append_u64(out, lease.ttl_ms);
    } else {
        append_u64(out, 0);
    }
    append_u64(out, leader_lease_receipts.len() as u64);
    for (idempotency_key, receipt) in leader_lease_receipts {
        append_string(out, idempotency_key);
        append_string(out, &receipt.idempotency_key);
        append_string(out, &receipt.operation);
        append_string(out, &receipt.request_checksum);
        append_string(out, &receipt.lease.leader_node_id);
        append_u64(out, receipt.lease.epoch);
        append_u64(out, receipt.lease.acquire_time_ms);
        append_u64(out, receipt.lease.expire_time_ms);
        append_u64(out, receipt.lease.ttl_ms);
        if with_receipt_proof {
            append_u64(out, receipt.commit_index);
            append_u64(out, receipt.update_time_ms);
            append_string(out, &receipt.receipt_proof_checksum);
        }
    }
    append_metadata_coordinator_config(out, coordinator_config);
}
