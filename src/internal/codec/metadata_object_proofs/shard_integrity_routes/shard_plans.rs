// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_metadata_shard_image(out: &mut Vec<u8>, shard: &MetadataShard) {
    append_string(out, &shard.shard_id);
    append_string(out, &shard.start_key);
    append_string(out, &shard.end_key);
    append_string(out, &shard.owner_node_id);
    append_u64(out, shard.epoch);
    append_u64(out, shard.draining as u64);
}

pub(crate) fn metadata_shard_request_checksum(
    operation: &str,
    shards: &[MetadataShard],
    fields: &[&str],
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, operation);
    append_u64(&mut material, shards.len() as u64);
    for shard in shards {
        append_metadata_shard_image(&mut material, shard);
    }
    append_u64(&mut material, fields.len() as u64);
    for field in fields {
        append_string(&mut material, field);
    }
    etag(&material)
}

pub(crate) fn append_metadata_shard_migration_action(
    out: &mut Vec<u8>,
    action: &MetadataShardMigrationAction,
) {
    append_string(out, &action.shard_id);
    append_string(out, &action.current_owner_node_id);
    append_string(out, &action.target_owner_node_id);
    append_u64(out, action.expected_epoch);
    append_u64(out, action.new_epoch);
    append_u64(out, action.draining as u64);
    append_string(out, &action.reason);
}

pub(crate) fn append_metadata_shard_split_action(
    out: &mut Vec<u8>,
    action: &MetadataShardSplitAction,
) {
    append_string(out, &action.source_shard_id);
    append_string(out, &action.split_key);
    append_string(out, &action.left_shard_id);
    append_string(out, &action.right_shard_id);
    append_string(out, &action.owner_node_id);
    append_u64(out, action.expected_epoch);
    append_u64(out, action.new_epoch);
    append_u64(out, action.record_count);
    append_u64(out, action.payload_bytes);
    append_u64(out, action.left_record_count);
    append_u64(out, action.right_record_count);
    append_string(out, &action.reason);
}

pub(crate) fn metadata_shard_split_plan_checksum(report: &MetadataShardSplitReport) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-SHARD-SPLIT-PLAN1");
    append_u64(&mut material, report.scanned_shards);
    append_u64(&mut material, report.candidate_shards);
    append_u64(&mut material, report.planned_shards);
    append_u64(&mut material, report.skipped_shards);
    append_u64(&mut material, report.scanned_records);
    append_u64(&mut material, report.candidate_records);
    append_u64(&mut material, report.candidate_payload_bytes);
    append_u64(&mut material, report.actions.len() as u64);
    for action in &report.actions {
        append_metadata_shard_split_action(&mut material, action);
    }
    append_u64(&mut material, report.shard_plan_needed as u64);
    append_u64(&mut material, report.shard_plan_can_apply as u64);
    append_string(&mut material, &report.shard_plan_reason);
    append_u64(&mut material, report.metadata_shard_topology_epoch);
    append_string(&mut material, &report.metadata_shard_topology_checksum);
    append_u64(
        &mut material,
        report
            .expected_metadata_shard_topology_epoch
            .unwrap_or_default(),
    );
    append_string(
        &mut material,
        &report.expected_metadata_shard_topology_checksum,
    );
    etag(&material)
}

pub(crate) fn metadata_shard_migration_plan_checksum(
    report: &MetadataShardMigrationReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-META-SHARD-MIGRATION-PLAN1");
    append_u64(&mut material, report.scanned_shards);
    append_u64(&mut material, report.candidate_shards);
    append_u64(&mut material, report.planned_shards);
    append_u64(&mut material, report.skipped_shards);
    append_u64(&mut material, report.actions.len() as u64);
    for action in &report.actions {
        append_metadata_shard_migration_action(&mut material, action);
    }
    append_u64(&mut material, report.shard_plan_needed as u64);
    append_u64(&mut material, report.shard_plan_can_apply as u64);
    append_string(&mut material, &report.shard_plan_reason);
    append_u64(&mut material, report.metadata_shard_topology_epoch);
    append_string(&mut material, &report.metadata_shard_topology_checksum);
    append_string(&mut material, &report.metadata_coordinator_session_checksum);
    append_u64(
        &mut material,
        report
            .expected_metadata_shard_topology_epoch
            .unwrap_or_default(),
    );
    append_string(
        &mut material,
        &report.expected_metadata_shard_topology_checksum,
    );
    append_string(
        &mut material,
        &report.expected_metadata_coordinator_session_checksum,
    );
    etag(&material)
}

