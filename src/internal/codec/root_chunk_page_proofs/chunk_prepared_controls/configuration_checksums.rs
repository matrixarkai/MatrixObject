// SPDX-License-Identifier: Apache-2.0
pub(crate) fn prepared_chunk_mutation_qos_config_checksum(
    prepared: &PreparedChunkMutationQosConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-MUTATION-QOS2");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(
        &mut material,
        prepared.target_replica_index.map_or(0, |_| 1),
    );
    append_u64(
        &mut material,
        prepared
            .target_replica_index
            .map_or(0, |index| index as u64),
    );
    append_chunk_mutation_qos_config(&mut material, &prepared.config);
    append_page_service_fingerprint(&mut material, &prepared.chunk_service);
    append_string(
        &mut material,
        &prepared.chunk_mutation_qos_prepared_aggregate_checksum,
    );
    append_u64(&mut material, prepared.chunk_prepared_replica_count);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_chunk_trim_flow_config_checksum(
    prepared: &PreparedChunkTrimFlowConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-TRIM-FLOW4");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(
        &mut material,
        prepared.target_replica_index.map_or(0, |_| 1),
    );
    append_u64(
        &mut material,
        prepared
            .target_replica_index
            .map_or(0, |index| index as u64),
    );
    append_u64(&mut material, prepared.rate_bytes_per_sec);
    append_u64(&mut material, prepared.burst_bytes);
    append_u64(&mut material, prepared.rate_ops_per_sec);
    append_u64(&mut material, prepared.burst_ops);
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_string(
        &mut material,
        &prepared.chunk_trim_flow_prepared_aggregate_checksum,
    );
    append_u64(&mut material, prepared.chunk_prepared_replica_count);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_chunk_read_cache_config_checksum(
    prepared: &PreparedChunkReadCacheConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-READ-CACHE3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(
        &mut material,
        prepared.target_replica_index.map_or(0, |_| 1),
    );
    append_u64(
        &mut material,
        prepared
            .target_replica_index
            .map_or(0, |index| index as u64),
    );
    append_u64(&mut material, prepared.max_bytes_per_replica);
    append_u64(&mut material, prepared.max_cacheable_read_bytes_per_replica);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_string(
        &mut material,
        &prepared.chunk_read_cache_prepared_aggregate_checksum,
    );
    append_u64(&mut material, prepared.chunk_prepared_replica_count);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_chunk_receipt_retention_config_checksum(
    prepared: &PreparedChunkReceiptRetentionConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-RECEIPT-RETENTION1");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(
        &mut material,
        prepared.target_replica_index.map_or(0, |_| 1),
    );
    append_u64(
        &mut material,
        prepared
            .target_replica_index
            .map_or(0, |index| index as u64),
    );
    append_u64(&mut material, prepared.retention as u64);
    append_u64(&mut material, prepared.trimmed_receipts);
    append_u64(&mut material, prepared.chunk_prepared_replica_count);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_chunk_capacity_config_checksum(
    prepared: &PreparedChunkCapacityConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-CAPACITY2");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.replica_index as u64);
    append_u64(&mut material, prepared.capacity_bytes);
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_string(
        &mut material,
        &prepared.chunk_capacity_prepared_aggregate_checksum,
    );
    append_u64(&mut material, prepared.chunk_prepared_replica_count);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn prepared_replica_service_mode_config_checksum(
    prepared: &PreparedReplicaServiceModeConfig,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-CHUNK-SERVICE-MODE3");
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_u64(&mut material, prepared.replica_index as u64);
    append_u64(
        &mut material,
        prepared.accepting_writes.map_or(2, |value| value as u64),
    );
    append_u64(
        &mut material,
        prepared.draining.map_or(2, |value| value as u64),
    );
    append_u64(
        &mut material,
        prepared.frozen.map_or(2, |value| value as u64),
    );
    append_string(&mut material, &prepared.before_page_service_fingerprint);
    append_string(&mut material, &prepared.after_page_service_fingerprint);
    append_string(
        &mut material,
        &prepared.replica_service_mode_prepared_aggregate_checksum,
    );
    append_u64(&mut material, prepared.chunk_prepared_replica_count);
    append_page_route_control_proof(&mut material, &prepared.route_control_proof);
    append_string(&mut material, &prepared.expected_route_control_checksum);
    etag(&material)
}
