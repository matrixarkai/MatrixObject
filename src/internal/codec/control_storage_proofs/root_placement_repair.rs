// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn append_placement_repair_item(out: &mut Vec<u8>, item: &PlacementRepairItem) {
    append_placement_migration_plan(out, &item.plan);
}

pub(crate) fn append_placement_repair_report(
    out: &mut Vec<u8>,
    report: &PlacementRepairReport,
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
        append_placement_repair_item(out, item);
    }
    append_u64(out, report.errors.len() as u64);
    for error in &report.errors {
        append_string(out, error);
    }
    append_u64(out, report.repair_needed as u64);
    append_u64(out, report.repair_can_apply as u64);
    append_string(out, &report.repair_reason);
}

fn placement_repair_report_encoded_len(report: &PlacementRepairReport) -> usize {
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

pub(crate) fn placement_repair_request_checksum(max_objects: usize) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-ROOT-PLACEMENT-REPAIR-REQUEST1")
            + std::mem::size_of::<u64>(),
    );
    append_string(&mut material, "MOS-RUST-ROOT-PLACEMENT-REPAIR-REQUEST1");
    append_u64(&mut material, max_objects as u64);
    etag(&material)
}

pub(crate) fn prepared_placement_repair_checksum(
    prepared: &PreparedPlacementRepair,
) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MOS-RUST-PREPARED-ROOT-PLACEMENT-REPAIR1")
            + encoded_string_len(&prepared.before_state_fingerprint)
            + encoded_string_len(&prepared.request_checksum)
            + placement_repair_report_encoded_len(&prepared.report)
            + std::mem::size_of::<u64>() * 3,
    );
    append_string(&mut material, "MOS-RUST-PREPARED-ROOT-PLACEMENT-REPAIR1");
    append_u64(&mut material, prepared.max_objects as u64);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_placement_repair_report(&mut material, &prepared.report);
    etag(&material)
}

pub(crate) fn unhealthy_placement_repair_request_checksum(
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

pub(crate) fn prepared_unhealthy_placement_repair_checksum(
    prepared: &PreparedUnhealthyPlacementRepair,
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
            + placement_repair_report_encoded_len(&prepared.report)
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
    append_placement_repair_report(&mut material, &prepared.report);
    etag(&material)
}

