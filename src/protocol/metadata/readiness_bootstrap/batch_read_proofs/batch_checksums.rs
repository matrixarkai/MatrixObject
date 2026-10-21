// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn object_batch_read_proof_checksum(
    operation: &str,
    report: &ObjectBatchReadReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, OBJECT_BATCH_READ_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.successful_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.payload_bytes);
    append_object_read_proof(&mut material, &report.read_proof);
    append_route_control_proof_material(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_page_read_stats(&mut material, &report.read_stats);
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_object_read_request_material(&mut material, &item.request);
        append_page_read_stats(&mut material, &item.stats);
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(object) = item.object.as_ref() {
            append_u64(&mut material, 1);
            append_metadata(&mut material, &object.metadata);
            append_u64(&mut material, object.data.len() as u64);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn conditional_object_batch_read_proof_checksum(
    operation: &str,
    report: &ConditionalObjectBatchReadReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, OBJECT_BATCH_READ_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.successful_objects);
    append_u64(&mut material, report.not_modified_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.payload_bytes);
    append_object_read_proof(&mut material, &report.read_proof);
    append_route_control_proof_material(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_page_read_stats(&mut material, &report.read_stats);
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_conditional_object_read_request_material(&mut material, &item.request);
        append_page_read_stats(&mut material, &item.stats);
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(object) = item.object.as_ref() {
            append_u64(&mut material, 1);
            append_metadata(&mut material, &object.metadata);
            append_u64(&mut material, object.not_modified as u64);
            append_u64(
                &mut material,
                object
                    .data
                    .as_ref()
                    .map(|data| data.len() as u64)
                    .unwrap_or(0),
            );
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn versioned_conditional_object_batch_read_proof_checksum(
    operation: &str,
    report: &VersionedConditionalObjectBatchReadReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, OBJECT_BATCH_READ_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.successful_objects);
    append_u64(&mut material, report.not_modified_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.payload_bytes);
    append_object_read_proof(&mut material, &report.read_proof);
    append_route_control_proof_material(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_page_read_stats(&mut material, &report.read_stats);
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_versioned_conditional_object_read_request_material(&mut material, &item.request);
        append_page_read_stats(&mut material, &item.stats);
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(object) = item.object.as_ref() {
            append_u64(&mut material, 1);
            append_metadata(&mut material, &object.metadata);
            append_u64(&mut material, object.not_modified as u64);
            append_u64(
                &mut material,
                object
                    .data
                    .as_ref()
                    .map(|data| data.len() as u64)
                    .unwrap_or(0),
            );
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

