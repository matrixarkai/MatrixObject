// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_root_capacity_rebalance_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedRootCapacityRebalanceEnvelope,
) {
    append_u64(out, envelope.max_objects);
    append_u64(out, envelope.min_free_bytes);
    append_u64(out, envelope.pressure_node_count);
    append_u64(out, envelope.has_more as u64);
    append_u64(out, envelope.scanned_objects);
    append_u64(out, envelope.rebalance_objects);
    append_u64(out, envelope.rebalanced_objects);
    append_u64(out, envelope.skipped_objects);
    append_u64(out, envelope.failed_rebalances);
    append_u64(out, envelope.bytes_to_rebalance);
    append_u64(out, envelope.bytes_rebalanced);
    append_u64(out, envelope.rebalance_needed as u64);
    append_u64(out, envelope.rebalance_can_apply as u64);
    append_string(out, &envelope.rebalance_reason);
    append_u64(out, envelope.planned_item_count);
    append_u64(out, envelope.first_object.is_some() as u64);
    if let Some(first_object) = &envelope.first_object {
        append_object_id(out, first_object);
    }
    append_string(out, &envelope.first_pressure_node_id);
    append_string(out, &envelope.first_target_node_id);
    append_string(out, &envelope.before_state_fingerprint);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_root_capacity_rebalance_envelope_checksum(
    envelope: &PreparedRootCapacityRebalanceEnvelope,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-ROOT-CAPACITY-REBALANCE-ENVELOPE1",
    );
    append_root_capacity_rebalance_envelope(&mut material, envelope);
    etag(&material)
}

pub(crate) fn prepared_root_capacity_rebalance_envelope(
    prepared: &PreparedRootCapacityRebalance,
) -> PreparedRootCapacityRebalanceEnvelope {
    let first_item = prepared.report.items.first();
    let mut envelope = PreparedRootCapacityRebalanceEnvelope {
        max_objects: prepared.max_objects as u64,
        min_free_bytes: prepared.min_free_bytes,
        pressure_node_count: prepared.report.pressure_node_count,
        has_more: prepared.report.has_more,
        scanned_objects: prepared.report.scanned_objects,
        rebalance_objects: prepared.report.rebalance_objects,
        rebalanced_objects: prepared.report.rebalanced_objects,
        skipped_objects: prepared.report.skipped_objects,
        failed_rebalances: prepared.report.failed_rebalances,
        bytes_to_rebalance: prepared.report.bytes_to_rebalance,
        bytes_rebalanced: prepared.report.bytes_rebalanced,
        rebalance_needed: prepared.report.rebalance_needed,
        rebalance_can_apply: prepared.report.rebalance_can_apply,
        rebalance_reason: prepared.report.rebalance_reason.clone(),
        planned_item_count: prepared.report.items.len() as u64,
        first_object: first_item.map(|item| item.plan.object_id.clone()),
        first_pressure_node_id: first_item
            .map(|item| item.pressure_node_id.clone())
            .unwrap_or_default(),
        first_target_node_id: first_item
            .and_then(|item| item.plan.after_node_ids.first().cloned())
            .unwrap_or_default(),
        before_state_fingerprint: prepared.before_state_fingerprint.clone(),
        request_checksum: prepared.request_checksum.clone(),
        expected_previous_commit_index: prepared.expected_previous_commit_index,
        prepared_commit_index: prepared.prepared_commit_index,
        raft_entry_checksum: prepared.raft_entry_checksum.clone(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_root_capacity_rebalance_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn append_root_capacity_rebalance_item(
    out: &mut Vec<u8>,
    item: &RootCapacityRebalanceItem,
) {
    append_string(out, &item.pressure_node_id);
    append_root_placement_migration_plan(out, &item.plan);
}

pub(crate) fn append_root_capacity_rebalance_report(
    out: &mut Vec<u8>,
    report: &RootCapacityRebalanceReport,
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
        append_root_capacity_rebalance_item(out, item);
    }
    append_u64(out, report.errors.len() as u64);
    for error in &report.errors {
        append_string(out, error);
    }
}

pub(crate) fn root_capacity_rebalance_request_checksum(
    max_objects: usize,
    min_free_bytes: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-ROOT-CAPACITY-REBALANCE-REQUEST1");
    append_u64(&mut material, max_objects as u64);
    append_u64(&mut material, min_free_bytes);
    etag(&material)
}

pub(crate) fn prepared_root_capacity_rebalance_checksum(
    prepared: &PreparedRootCapacityRebalance,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-ROOT-CAPACITY-REBALANCE1");
    append_u64(&mut material, prepared.max_objects as u64);
    append_u64(&mut material, prepared.min_free_bytes);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_root_capacity_rebalance_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn root_leader_campaign_request_checksum(
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

pub(crate) fn root_leader_renew_request_checksum(
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
