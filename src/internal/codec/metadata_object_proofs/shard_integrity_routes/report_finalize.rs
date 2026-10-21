// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn finalize_metadata_shard_split_report(report: &mut MetadataShardSplitReport) {
    report.shard_plan_needed = report.planned_shards > 0;
    report.shard_plan_can_apply =
        report.shard_plan_needed && report.skipped_shards == 0 && report.errors.is_empty();
    report.shard_plan_reason = if !report.shard_plan_needed && report.candidate_shards == 0 {
        "metadata_shard_split_no_pressure"
    } else if !report.shard_plan_needed {
        "metadata_shard_split_no_splittable_candidate"
    } else if report.skipped_shards > 0 {
        "metadata_shard_split_partial"
    } else if !report.errors.is_empty() {
        "metadata_shard_split_errors"
    } else {
        "metadata_shard_split_ready"
    }
    .to_string();
    report.plan_checksum = metadata_shard_split_plan_checksum(report);
}

pub(crate) fn finalize_metadata_shard_migration_report(
    report: &mut MetadataShardMigrationReport,
    operation_reason: &str,
) {
    report.shard_plan_needed = report.planned_shards > 0;
    report.shard_plan_can_apply =
        report.shard_plan_needed && report.skipped_shards == 0 && report.errors.is_empty();
    let operation = match operation_reason {
        "expired_owner_failover" => "failover",
        "owner_failover" => "failover",
        "owner_rebalance" => "rebalance",
        _ => "migration",
    };
    report.shard_plan_reason = if !report.shard_plan_needed && report.candidate_shards == 0 {
        format!("metadata_shard_{operation}_no_action")
    } else if !report.shard_plan_needed {
        format!("metadata_shard_{operation}_no_migratable_candidate")
    } else if report.skipped_shards > 0 {
        format!("metadata_shard_{operation}_partial")
    } else if !report.errors.is_empty() {
        format!("metadata_shard_{operation}_errors")
    } else {
        format!("metadata_shard_{operation}_ready")
    };
    report.plan_checksum = metadata_shard_migration_plan_checksum(report);
}

pub(crate) fn append_metadata_shard_migration_report(
    out: &mut Vec<u8>,
    report: &MetadataShardMigrationReport,
) {
    append_u64(out, report.scanned_shards);
    append_u64(out, report.candidate_shards);
    append_u64(out, report.planned_shards);
    append_u64(out, report.migrated_shards);
    append_u64(out, report.skipped_shards);
    append_u64(out, report.failed_shards);
    append_u64(out, report.actions.len() as u64);
    for action in &report.actions {
        append_metadata_shard_migration_action(out, action);
    }
    append_u64(out, report.errors.len() as u64);
    for error in &report.errors {
        append_string(out, error);
    }
    append_u64(out, report.shard_plan_needed as u64);
    append_u64(out, report.shard_plan_can_apply as u64);
    append_string(out, &report.shard_plan_reason);
    append_string(out, &report.plan_checksum);
    append_string(out, &report.expected_plan_checksum);
    append_u64(out, report.metadata_shard_topology_epoch);
    append_string(out, &report.metadata_shard_topology_checksum);
    append_string(out, &report.metadata_coordinator_session_checksum);
    append_u64(
        out,
        report
            .expected_metadata_shard_topology_epoch
            .unwrap_or_default(),
    );
    append_string(out, &report.expected_metadata_shard_topology_checksum);
    append_string(out, &report.expected_metadata_coordinator_session_checksum);
}

