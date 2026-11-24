// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn append_placement_capacity_rebalance_item(
    out: &mut Vec<u8>,
    item: &PlacementCapacityRebalanceItem,
) {
    append_string(out, &item.pressure_node_id);
    append_placement_migration_plan(out, &item.plan);
}

pub(crate) fn append_placement_capacity_rebalance_report(
    out: &mut Vec<u8>,
    report: &PlacementCapacityRebalanceReport,
) {
    append_u64(out, report.scanned_objects);
    append_u64(out, report.pressure_node_count);
    append_u64(out, report.rebalance_objects);
    append_u64(out, report.rebalanced_objects);
    append_u64(out, report.skipped_objects);
    append_u64(out, report.failed_rebalances);
    append_u64(out, report.bytes_to_rebalance);
    append_u64(out, report.bytes_rebalanced);
    append_u64(out, report.items.len() as u64);
    for item in &report.items {
        append_placement_capacity_rebalance_item(out, item);
    }
    append_u64(out, report.errors.len() as u64);
    for error in &report.errors {
        append_string(out, error);
    }
}

pub(crate) fn placement_capacity_rebalance_request_checksum(
    max_objects: usize,
    min_free_bytes: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-ROOT-CAPACITY-REBALANCE-REQUEST1");
    append_u64(&mut material, max_objects as u64);
    append_u64(&mut material, min_free_bytes);
    etag(&material)
}

pub(crate) fn prepared_placement_capacity_rebalance_checksum(
    prepared: &PreparedPlacementCapacityRebalance,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-ROOT-CAPACITY-REBALANCE1");
    append_u64(&mut material, prepared.max_objects as u64);
    append_u64(&mut material, prepared.min_free_bytes);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_placement_capacity_rebalance_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn placement_leader_campaign_request_checksum(
    node_id: &str,
    ttl_ms: u64,
    now_ms: u64,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("campaign")
            + encoded_string_len(node_id)
            + std::mem::size_of::<u64>() * 2,
    );
    append_string(&mut material, "campaign");
    append_string(&mut material, node_id);
    append_u64(&mut material, ttl_ms);
    append_u64(&mut material, now_ms);
    etag(&material)
}

pub(crate) fn placement_leader_renew_request_checksum(
    node_id: &str,
    epoch: u64,
    ttl_ms: u64,
    now_ms: u64,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("renew")
            + encoded_string_len(node_id)
            + std::mem::size_of::<u64>() * 3,
    );
    append_string(&mut material, "renew");
    append_string(&mut material, node_id);
    append_u64(&mut material, epoch);
    append_u64(&mut material, ttl_ms);
    append_u64(&mut material, now_ms);
    etag(&material)
}
