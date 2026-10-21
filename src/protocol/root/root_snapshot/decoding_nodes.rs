// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Copy)]
struct RootSnapshotDecodeFeatures {
    has_leader_lease_receipts: bool,
    has_root_meta_health: bool,
    has_control2_meta_health: bool,
    has_control3_meta_health: bool,
    has_control4_meta_health: bool,
    has_control5_meta_health: bool,
    has_control6_meta_health: bool,
    has_control7_meta_health: bool,
    has_control8_meta_health: bool,
    has_control9_meta_health: bool,
    has_control10_meta_health: bool,
    has_root_leader_lease_receipt_proof: bool,
    has_page_allocator_shard: bool,
    has_node_eligibility: bool,
    has_metadata_value_bytes: bool,
    has_root_page_health: bool,
    has_root_page_route_control_commit_index: bool,
    has_root_page_allocator_receipts: bool,
    has_root_page_mutation_receipts: bool,
    has_root_page_raft_endpoint_counts: bool,
    has_root_page_extended_service_health: bool,
    has_root_page_reclaim_health: bool,
    has_root_page_quorum_policy: bool,
    has_node_endpoint: bool,
    has_root_coordinator_config: bool,
    has_root_chunk_reclaim_health: bool,
    has_root_chunk_fragmented_discarded_bytes: bool,
    has_root_chunk_health: bool,
    has_root_chunk_control_health: bool,
    has_root_replica_service_health: bool,
}

impl RootSnapshotDecodeFeatures {
    fn for_version(version: u64) -> Self {
        Self {
            has_leader_lease_receipts: version >= 2,
            has_root_meta_health: version >= 3,
            has_control2_meta_health: version >= 4,
            has_control3_meta_health: version >= 5,
            has_control4_meta_health: version >= 6,
            has_control5_meta_health: version >= 7,
            has_control6_meta_health: version >= 14,
            has_control7_meta_health: version >= 15,
            has_control8_meta_health: version >= 16,
            has_control9_meta_health: version >= 24,
            has_control10_meta_health: version >= 26,
            has_root_leader_lease_receipt_proof: version >= 27,
            has_page_allocator_shard: version >= 8,
            has_node_eligibility: version >= 9,
            has_metadata_value_bytes: version >= 10,
            has_root_page_health: version >= 11,
            has_root_page_route_control_commit_index: version >= 18,
            has_root_page_allocator_receipts: version >= 19,
            has_root_page_mutation_receipts: version >= 23,
            has_root_page_raft_endpoint_counts: version >= 25,
            has_root_page_extended_service_health: version >= 28,
            has_root_page_reclaim_health: version >= 30,
            has_root_page_quorum_policy: version >= 31,
            has_node_endpoint: version >= 20,
            has_root_coordinator_config: version >= 21,
            has_root_chunk_reclaim_health: version >= 22,
            has_root_chunk_fragmented_discarded_bytes: version >= 29,
            has_root_chunk_health: version >= 12,
            has_root_chunk_control_health: version >= 17,
            has_root_replica_service_health: version >= 32,
        }
    }
}

fn read_root_snapshot_nodes(
    mut cursor: &mut SnapshotByteCursor<'_>,
    features: RootSnapshotDecodeFeatures,
) -> Result<BTreeMap<String, RootNode>, ObjectError> {
    let RootSnapshotDecodeFeatures {
        has_root_meta_health,
        has_control2_meta_health,
        has_control3_meta_health,
        has_control4_meta_health,
        has_control5_meta_health,
        has_control6_meta_health,
        has_control7_meta_health,
        has_control8_meta_health,
        has_control9_meta_health,
        has_control10_meta_health,
        has_page_allocator_shard,
        has_node_eligibility,
        has_metadata_value_bytes,
        has_root_page_health,
        has_root_page_route_control_commit_index,
        has_root_page_allocator_receipts,
        has_root_page_mutation_receipts,
        has_root_page_raft_endpoint_counts,
        has_root_page_extended_service_health,
        has_root_page_reclaim_health,
        has_root_page_quorum_policy,
        has_node_endpoint,
        has_root_chunk_reclaim_health,
        has_root_chunk_fragmented_discarded_bytes,
        has_root_chunk_health,
        has_root_chunk_control_health,
        has_root_replica_service_health,
        ..
    } = features;
    let mut nodes = BTreeMap::new();
    for _ in 0..cursor.read_count("root snapshot node count")? {
        let map_node_id = cursor.read_string()?;
        let node = RootNode {
            node_id: cursor.read_string()?,
            endpoint: if has_node_endpoint {
                cursor.read_string()?
            } else {
                String::new()
            },
            service_kind: cursor.read_string()?,
            failure_domain: cursor.read_string()?,
            metadata_eligible: if has_node_eligibility {
                cursor.read_u64()? != 0
            } else {
                true
            },
            data_eligible: if has_node_eligibility {
                cursor.read_u64()? != 0
            } else {
                true
            },
            gateway_eligible: if has_node_eligibility {
                cursor.read_u64()? != 0
            } else {
                false
            },
            metadata_value_bytes: if has_metadata_value_bytes {
                cursor.read_u64()?
            } else {
                0
            },
            capacity_bytes: cursor.read_u64()?,
            used_bytes: cursor.read_u64()?,
            committed_bytes: cursor.read_u64()?,
            draining: cursor.read_u64()? != 0,
            online: cursor.read_u64()? != 0,
            last_heartbeat_ms: cursor.read_u64()?,
            page_allocator_shard_index: if has_page_allocator_shard {
                cursor.read_u64()?
            } else {
                0
            },
            page_allocator_shard_count: if has_page_allocator_shard {
                cursor.read_u64()?
            } else {
                1
            },
            meta_health: if has_root_meta_health {
                read_root_meta_health(
                    &mut cursor,
                    has_control2_meta_health,
                    has_control3_meta_health,
                    has_control4_meta_health,
                    has_control5_meta_health,
                    has_control6_meta_health,
                    has_control7_meta_health,
                    has_control8_meta_health,
                    has_control9_meta_health,
                    has_control10_meta_health,
                )?
            } else {
                RootMetaHealth::default()
            },
            page_health: if has_root_page_health {
                RootPageHealth {
                    ready: cursor.read_u64()? != 0,
                    read_ready: cursor.read_u64()? != 0,
                    route_control_checksum: cursor.read_string()?,
                    route_control_commit_index: if has_root_page_route_control_commit_index {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_endpoint_count: cursor.read_u64()?,
                    write_quorum: if has_root_page_quorum_policy {
                        cursor.read_u64()?
                    } else {
                        1
                    },
                    read_quorum: if has_root_page_quorum_policy {
                        cursor.read_u64()?
                    } else {
                        1
                    },
                    writable_chunk_endpoint_count: cursor.read_u64()?,
                    healthy_chunk_endpoint_count: cursor.read_u64()?,
                    raft_writable_chunk_endpoint_count: if has_root_page_raft_endpoint_counts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    raft_readable_chunk_endpoint_count: if has_root_page_raft_endpoint_counts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    capacity_ready_writable_chunk_endpoint_count: cursor.read_u64()?,
                    quarantined_chunk_replicas: cursor.read_u64()?,
                    chunk_capacity_bytes: cursor.read_u64()?,
                    chunk_capacity_free_bytes: cursor.read_u64()?,
                    chunk_metadata_dirty_endpoint_count: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_metadata_dirty_mutations: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_metadata_checkpoint_interval: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    deferred_metadata_checkpoint_endpoint_count:
                        if has_root_page_extended_service_health {
                            cursor.read_u64()?
                        } else {
                            0
                        },
                    deferred_metadata_checkpoint_requests: if has_root_page_extended_service_health
                    {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    deferred_metadata_checkpoint_runs: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_capacity_rejected_writes: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    replica_service_mode_rejected_mutations: if has_root_page_extended_service_health
                    {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_physical_write_count: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_physical_write_bytes: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_physical_discard_range_count: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    chunk_physical_discard_bytes: if has_root_page_extended_service_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    total_reclaimable_bytes: if has_root_page_reclaim_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    total_trailing_trim_bytes: if has_root_page_reclaim_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    total_fragmented_chunk_count: if has_root_page_reclaim_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    total_fragmented_discarded_bytes: if has_root_page_reclaim_health {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    allocation_receipt_count: if has_root_page_allocator_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    batch_allocation_receipt_count: if has_root_page_allocator_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    release_allocation_receipt_count: if has_root_page_allocator_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    write_receipt_count: if has_root_page_mutation_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    batch_write_receipt_count: if has_root_page_mutation_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    discard_receipt_count: if has_root_page_mutation_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                    batch_discard_receipt_count: if has_root_page_mutation_receipts {
                        cursor.read_u64()?
                    } else {
                        0
                    },
                }
            } else {
                RootPageHealth::default()
            },
            chunk_health: if has_root_chunk_health {
                read_root_chunk_health(
                    &mut cursor,
                    has_root_chunk_control_health,
                    has_root_chunk_reclaim_health,
                    has_root_chunk_fragmented_discarded_bytes,
                    has_root_replica_service_health,
                )?
            } else {
                RootChunkHealth::default()
            },
        };
        nodes.insert(map_node_id, node);
    }
    Ok(nodes)
}
