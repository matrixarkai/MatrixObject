// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_root_placement_repair_item(out: &mut Vec<u8>, item: &RootPlacementRepairItem) {
    append_root_placement_migration_plan(out, &item.plan);
}

pub(crate) fn append_root_placement_repair_report(
    out: &mut Vec<u8>,
    report: &RootPlacementRepairReport,
) {
    append_u64(out, report.marker.is_some() as u64);
    if let Some(marker) = &report.marker {
        append_object_id(out, marker);
    }
    append_u64(out, report.next_marker.is_some() as u64);
    if let Some(next_marker) = &report.next_marker {
        append_object_id(out, next_marker);
    }
    append_u64(out, report.has_more as u64);
    append_u64(out, report.scanned_objects);
    append_u64(out, report.repair_objects);
    append_u64(out, report.repaired_objects);
    append_u64(out, report.skipped_objects);
    append_u64(out, report.failed_repairs);
    append_u64(out, report.bytes_to_repair);
    append_u64(out, report.bytes_repaired);
    append_u64(out, report.items.len() as u64);
    for item in &report.items {
        append_root_placement_repair_item(out, item);
    }
    append_u64(out, report.errors.len() as u64);
    for error in &report.errors {
        append_string(out, error);
    }
    append_u64(out, report.repair_needed as u64);
    append_u64(out, report.repair_can_apply as u64);
    append_string(out, &report.repair_reason);
}

fn root_placement_repair_report_encoded_len(report: &RootPlacementRepairReport) -> usize {
    let marker_len = report
        .marker
        .as_ref()
        .map(encoded_object_id_len)
        .unwrap_or_default();
    let next_marker_len = report
        .next_marker
        .as_ref()
        .map(encoded_object_id_len)
        .unwrap_or_default();
    let item_len = report
        .items
        .iter()
        .map(|item| placement_migration_plan_encoded_len(&item.plan))
        .sum::<usize>();
    let error_len = report
        .errors
        .iter()
        .map(|error| encoded_string_len(error))
        .sum::<usize>();
    std::mem::size_of::<u64>() * 14
        + marker_len
        + next_marker_len
        + item_len
        + error_len
        + encoded_string_len(&report.repair_reason)
}

pub(crate) fn root_placement_repair_request_checksum(max_objects: usize) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-ROOT-PLACEMENT-REPAIR-REQUEST1")
            + std::mem::size_of::<u64>(),
    );
    append_string(&mut material, "MOS-RUST-ROOT-PLACEMENT-REPAIR-REQUEST1");
    append_u64(&mut material, max_objects as u64);
    etag(&material)
}

pub(crate) fn prepared_root_placement_repair_checksum(
    prepared: &PreparedRootPlacementRepair,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-PREPARED-ROOT-PLACEMENT-REPAIR1")
            + encoded_string_len(&prepared.before_state_fingerprint)
            + encoded_string_len(&prepared.request_checksum)
            + root_placement_repair_report_encoded_len(&prepared.report)
            + std::mem::size_of::<u64>() * 3,
    );
    append_string(&mut material, "MOS-RUST-PREPARED-ROOT-PLACEMENT-REPAIR1");
    append_u64(&mut material, prepared.max_objects as u64);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_root_placement_repair_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn root_unhealthy_placement_repair_request_checksum(
    marker: Option<&ObjectId>,
    max_objects: usize,
    expected_replica_count: usize,
    min_free_bytes: u64,
    max_payload_bytes: u64,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-ROOT-UNHEALTHY-PLACEMENT-REPAIR-REQUEST1")
            + marker.map(encoded_object_id_len).unwrap_or_default()
            + std::mem::size_of::<u64>() * 5,
    );
    append_string(
        &mut material,
        "MOS-RUST-ROOT-UNHEALTHY-PLACEMENT-REPAIR-REQUEST1",
    );
    append_u64(&mut material, marker.is_some() as u64);
    if let Some(marker) = marker {
        append_object_id(&mut material, marker);
    }
    append_u64(&mut material, max_objects as u64);
    append_u64(&mut material, expected_replica_count as u64);
    append_u64(&mut material, min_free_bytes);
    append_u64(&mut material, max_payload_bytes);
    etag(&material)
}

pub(crate) fn prepared_root_unhealthy_placement_repair_checksum(
    prepared: &PreparedRootUnhealthyPlacementRepair,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-PREPARED-ROOT-UNHEALTHY-PLACEMENT-REPAIR1")
            + prepared
                .marker
                .as_ref()
                .map(encoded_object_id_len)
                .unwrap_or_default()
            + encoded_string_len(&prepared.before_state_fingerprint)
            + encoded_string_len(&prepared.request_checksum)
            + root_placement_repair_report_encoded_len(&prepared.report)
            + std::mem::size_of::<u64>() * 9,
    );
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-ROOT-UNHEALTHY-PLACEMENT-REPAIR1",
    );
    append_u64(&mut material, prepared.marker.is_some() as u64);
    if let Some(marker) = &prepared.marker {
        append_object_id(&mut material, marker);
    }
    append_u64(&mut material, prepared.max_objects as u64);
    append_u64(&mut material, prepared.expected_replica_count as u64);
    append_u64(&mut material, prepared.min_free_bytes);
    append_u64(&mut material, prepared.max_payload_bytes);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_root_placement_repair_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn append_root_placement_repair_envelope(
    out: &mut Vec<u8>,
    envelope: &PreparedRootPlacementRepairEnvelope,
) {
    append_string(out, &envelope.repair_kind);
    append_u64(out, envelope.marker.is_some() as u64);
    if let Some(marker) = &envelope.marker {
        append_object_id(out, marker);
    }
    append_u64(out, envelope.max_objects);
    append_u64(out, envelope.expected_replica_count);
    append_u64(out, envelope.min_free_bytes);
    append_u64(out, envelope.max_payload_bytes);
    append_u64(out, envelope.has_more as u64);
    append_u64(out, envelope.next_marker.is_some() as u64);
    if let Some(next_marker) = &envelope.next_marker {
        append_object_id(out, next_marker);
    }
    append_u64(out, envelope.scanned_objects);
    append_u64(out, envelope.repair_objects);
    append_u64(out, envelope.repaired_objects);
    append_u64(out, envelope.skipped_objects);
    append_u64(out, envelope.failed_repairs);
    append_u64(out, envelope.bytes_to_repair);
    append_u64(out, envelope.bytes_repaired);
    append_u64(out, envelope.repair_needed as u64);
    append_u64(out, envelope.repair_can_apply as u64);
    append_string(out, &envelope.repair_reason);
    append_u64(out, envelope.planned_item_count);
    append_u64(out, envelope.first_object.is_some() as u64);
    if let Some(first_object) = &envelope.first_object {
        append_object_id(out, first_object);
    }
    append_string(out, &envelope.first_source_node_id);
    append_string(out, &envelope.first_target_node_id);
    append_string(out, &envelope.before_state_fingerprint);
    append_string(out, &envelope.request_checksum);
    append_u64(out, envelope.expected_previous_commit_index);
    append_u64(out, envelope.prepared_commit_index);
    append_string(out, &envelope.raft_entry_checksum);
}

pub(crate) fn prepared_root_placement_repair_envelope_checksum(
    envelope: &PreparedRootPlacementRepairEnvelope,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-PREPARED-ROOT-PLACEMENT-REPAIR-ENVELOPE1")
            + root_placement_repair_envelope_encoded_len(envelope),
    );
    append_string(
        &mut material,
        "MOS-RUST-PREPARED-ROOT-PLACEMENT-REPAIR-ENVELOPE1",
    );
    append_root_placement_repair_envelope(&mut material, envelope);
    etag(&material)
}

fn root_placement_repair_envelope_encoded_len(
    envelope: &PreparedRootPlacementRepairEnvelope,
) -> usize {
    let marker_len = envelope
        .marker
        .as_ref()
        .map(encoded_object_id_len)
        .unwrap_or_default();
    let next_marker_len = envelope
        .next_marker
        .as_ref()
        .map(encoded_object_id_len)
        .unwrap_or_default();
    let first_object_len = envelope
        .first_object
        .as_ref()
        .map(encoded_object_id_len)
        .unwrap_or_default();
    std::mem::size_of::<u64>() * 20
        + encoded_string_len(&envelope.repair_kind)
        + marker_len
        + next_marker_len
        + encoded_string_len(&envelope.repair_reason)
        + first_object_len
        + encoded_string_len(&envelope.first_source_node_id)
        + encoded_string_len(&envelope.first_target_node_id)
        + encoded_string_len(&envelope.before_state_fingerprint)
        + encoded_string_len(&envelope.request_checksum)
        + encoded_string_len(&envelope.raft_entry_checksum)
}

pub(crate) fn root_placement_repair_envelope_from_report(
    repair_kind: &str,
    marker: Option<ObjectId>,
    max_objects: u64,
    expected_replica_count: u64,
    min_free_bytes: u64,
    max_payload_bytes: u64,
    report: &RootPlacementRepairReport,
    before_state_fingerprint: &str,
    request_checksum: &str,
    expected_previous_commit_index: u64,
    prepared_commit_index: u64,
    raft_entry_checksum: &str,
) -> PreparedRootPlacementRepairEnvelope {
    let first_plan = report.items.first().map(|item| &item.plan);
    let mut envelope = PreparedRootPlacementRepairEnvelope {
        repair_kind: repair_kind.to_string(),
        marker,
        max_objects,
        expected_replica_count,
        min_free_bytes,
        max_payload_bytes,
        has_more: report.has_more,
        next_marker: report.next_marker.clone(),
        scanned_objects: report.scanned_objects,
        repair_objects: report.repair_objects,
        repaired_objects: report.repaired_objects,
        skipped_objects: report.skipped_objects,
        failed_repairs: report.failed_repairs,
        bytes_to_repair: report.bytes_to_repair,
        bytes_repaired: report.bytes_repaired,
        repair_needed: report.repair_needed,
        repair_can_apply: report.repair_can_apply,
        repair_reason: report.repair_reason.clone(),
        planned_item_count: report.items.len() as u64,
        first_object: first_plan.map(|plan| plan.object_id.clone()),
        first_source_node_id: first_plan
            .map(|plan| plan.source_node_id.clone())
            .unwrap_or_default(),
        first_target_node_id: first_plan
            .map(|plan| plan.target_node_id.clone())
            .unwrap_or_default(),
        before_state_fingerprint: before_state_fingerprint.to_string(),
        request_checksum: request_checksum.to_string(),
        expected_previous_commit_index,
        prepared_commit_index,
        raft_entry_checksum: raft_entry_checksum.to_string(),
        envelope_checksum: String::new(),
    };
    envelope.envelope_checksum = prepared_root_placement_repair_envelope_checksum(&envelope);
    envelope
}

pub(crate) fn prepared_draining_root_placement_repair_envelope(
    prepared: &PreparedRootPlacementRepair,
) -> PreparedRootPlacementRepairEnvelope {
    root_placement_repair_envelope_from_report(
        "draining",
        None,
        prepared.max_objects as u64,
        0,
        0,
        0,
        &prepared.report,
        &prepared.before_state_fingerprint,
        &prepared.request_checksum,
        prepared.expected_previous_commit_index,
        prepared.prepared_commit_index,
        &prepared.raft_entry_checksum,
    )
}

pub(crate) fn prepared_unhealthy_root_placement_repair_envelope(
    prepared: &PreparedRootUnhealthyPlacementRepair,
) -> PreparedRootPlacementRepairEnvelope {
    root_placement_repair_envelope_from_report(
        "unhealthy",
        prepared.marker.clone(),
        prepared.max_objects as u64,
        prepared.expected_replica_count as u64,
        prepared.min_free_bytes,
        prepared.max_payload_bytes,
        &prepared.report,
        &prepared.before_state_fingerprint,
        &prepared.request_checksum,
        prepared.expected_previous_commit_index,
        prepared.prepared_commit_index,
        &prepared.raft_entry_checksum,
    )
}

