pub(crate) fn metadata_shard_migration_plan_request_checksum(
    operation: &str,
    source_owner_node_id: &str,
    target_owner_node_id: &str,
    max_shards: usize,
    now_ms: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-SHARD-MIGRATION-PLAN-REQUEST1");
    append_string(&mut material, operation);
    append_string(&mut material, source_owner_node_id);
    append_string(&mut material, target_owner_node_id);
    append_u64(&mut material, max_shards as u64);
    append_u64(&mut material, now_ms);
    etag(&material)
}

pub(crate) fn prepared_metadata_shard_migration_plan_checksum(
    prepared: &PreparedMetadataShardMigrationPlan,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-META-SHARD-MIGRATION-PLAN3",
    );
    append_string(&mut material, &prepared.operation);
    append_string(&mut material, &prepared.source_owner_node_id);
    append_string(&mut material, &prepared.target_owner_node_id);
    append_u64(&mut material, prepared.max_shards as u64);
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.expected_plan_checksum);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_metadata_shard_migration_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn prepared_metadata_shard_split_checksum(
    prepared: &PreparedMetadataShardSplit,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-SHARD-SPLIT3");
    append_metadata_shard_image(&mut material, &prepared.source);
    append_metadata_shard_image(&mut material, &prepared.left);
    append_metadata_shard_image(&mut material, &prepared.right);
    append_string(&mut material, &prepared.split_key);
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

pub(crate) fn prepared_metadata_shard_merge_checksum(
    prepared: &PreparedMetadataShardMerge,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-SHARD-MERGE3");
    append_metadata_shard_image(&mut material, &prepared.left);
    append_metadata_shard_image(&mut material, &prepared.right);
    append_metadata_shard_image(&mut material, &prepared.merged);
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

pub(crate) fn prepared_metadata_shard_migration_checksum(
    prepared: &PreparedMetadataShardMigration,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-META-SHARD-MIGRATION3");
    append_metadata_shard_image(&mut material, &prepared.before);
    append_metadata_shard_image(&mut material, &prepared.after);
    append_u64(&mut material, prepared.now_ms);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.metadata_control_proof_format);
    append_string(&mut material, &prepared.metadata_control_proof_checksum);
    append_u64(&mut material, prepared.metadata_control_proof_bytes);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    etag(&material)
}

