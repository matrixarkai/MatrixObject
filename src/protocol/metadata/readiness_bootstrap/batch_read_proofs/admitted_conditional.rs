// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_admitted_batch_common(
    material: &mut Vec<u8>,
    operation: &str,
    requested_objects: u64,
    successful_objects: u64,
    not_modified_objects: u64,
    admission_rejected_objects: u64,
    failed_objects: u64,
    payload_bytes: u64,
    max_payload_bytes: u64,
    reserved_chunk_read_count: u64,
    max_chunk_read_count: u64,
    budget_limited: bool,
    read_proof: &ObjectReadProof,
    route_control_proof: &ChunkRouteControlProof,
    expected_route_control_checksum: &str,
) {
    append_string(material, STORAGE_ADMISSION_PROOF_FORMAT);
    append_string(material, operation);
    append_u64(material, requested_objects);
    append_u64(material, successful_objects);
    append_u64(material, not_modified_objects);
    append_u64(material, admission_rejected_objects);
    append_u64(material, failed_objects);
    append_u64(material, payload_bytes);
    append_u64(material, max_payload_bytes);
    append_u64(material, reserved_chunk_read_count);
    append_u64(material, max_chunk_read_count);
    append_u64(material, budget_limited as u64);
    append_u64(material, read_proof.requested_read_index);
    append_u64(material, read_proof.consensus_read_index);
    append_u64(material, read_proof.metadata_commit_index);
    append_u64(material, read_proof.read_fence_read_index_lag);
    append_u64(material, read_proof.read_fence_applied_lag);
    append_u64(material, route_control_proof.control_receipt as u64);
    append_string(material, &route_control_proof.state_format);
    append_string(material, &route_control_proof.state_checksum);
    append_u64(
        material,
        route_control_proof.chunk_route_control_commit_index,
    );
    append_u64(
        material,
        route_control_proof.chunk_route_control_next_log_index,
    );
    append_u64(material, route_control_proof.write_quorum);
    append_u64(material, route_control_proof.read_quorum);
    append_string(material, expected_route_control_checksum);
}

pub(crate) fn append_admitted_conditional_object(
    material: &mut Vec<u8>,
    object: &AdmittedConditionalObjectData,
) {
    append_u64(material, object.object.is_some() as u64);
    if let Some(value) = object.object.as_ref() {
        append_u64(material, value.not_modified as u64);
        append_u64(
            material,
            value.data.as_ref().map(|data| data.len()).unwrap_or(0) as u64,
        );
        append_u64(material, value.metadata.version);
        append_string(material, &value.metadata.etag);
    }
    append_string(material, &object.admission.storage_admission_proof_format);
    append_string(material, &object.admission.storage_admission_proof_checksum);
    append_u64(material, object.admission.can_accept as u64);
    append_u64(material, object.admission.selected_bytes);
    append_u64(material, object.admission.payload_bytes);
    append_u64(material, object.admission.mapped_chunk_ranges);
    append_u64(material, object.admission.readable_chunk_count);
    append_string(material, &object.admission.storage_admission_reason);
    append_string(material, &object.admission.partial_status);
}

pub(crate) fn admitted_conditional_object_batch_read_proof_checksum(
    operation: &str,
    report: &AdmittedConditionalObjectBatchReadReport,
) -> String {
    let mut material = Vec::new();
    append_admitted_batch_common(
        &mut material,
        operation,
        report.requested_objects,
        report.successful_objects,
        report.not_modified_objects,
        report.admission_rejected_objects,
        report.failed_objects,
        report.payload_bytes,
        report.max_payload_bytes,
        report.reserved_chunk_read_count,
        report.max_chunk_read_count,
        report.budget_limited,
        &report.read_proof,
        &report.route_control_proof,
        &report.expected_route_control_checksum,
    );
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_string(&mut material, &item.request.bucket);
        append_string(&mut material, &item.request.key);
        match item.request.range.as_ref() {
            Some(range) => {
                append_u64(&mut material, 1);
                append_u64(&mut material, range.offset);
                append_u64(&mut material, range.length);
            }
            None => append_u64(&mut material, 0),
        }
        append_string_list(&mut material, &item.request.preconditions.if_match_etags);
        append_string_list(
            &mut material,
            &item.request.preconditions.if_none_match_etags,
        );
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
            append_admitted_conditional_object(&mut material, object);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn admitted_versioned_conditional_object_batch_read_proof_checksum(
    operation: &str,
    report: &AdmittedVersionedConditionalObjectBatchReadReport,
) -> String {
    let mut material = Vec::new();
    append_admitted_batch_common(
        &mut material,
        operation,
        report.requested_objects,
        report.successful_objects,
        report.not_modified_objects,
        report.admission_rejected_objects,
        report.failed_objects,
        report.payload_bytes,
        report.max_payload_bytes,
        report.reserved_chunk_read_count,
        report.max_chunk_read_count,
        report.budget_limited,
        &report.read_proof,
        &report.route_control_proof,
        &report.expected_route_control_checksum,
    );
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_string(&mut material, &item.request.bucket);
        append_string(&mut material, &item.request.key);
        append_u64(&mut material, item.request.version);
        match item.request.range.as_ref() {
            Some(range) => {
                append_u64(&mut material, 1);
                append_u64(&mut material, range.offset);
                append_u64(&mut material, range.length);
            }
            None => append_u64(&mut material, 0),
        }
        append_string_list(&mut material, &item.request.preconditions.if_match_etags);
        append_string_list(
            &mut material,
            &item.request.preconditions.if_none_match_etags,
        );
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
            append_admitted_conditional_object(&mut material, object);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn stamp_admitted_conditional_object_batch_read_proof(
    operation: &str,
    report: &mut AdmittedConditionalObjectBatchReadReport,
) {
    report.storage_admission_proof_format = STORAGE_ADMISSION_PROOF_FORMAT.to_string();
    report.storage_admission_proof_checksum =
        admitted_conditional_object_batch_read_proof_checksum(operation, report);
}

pub(crate) fn stamp_admitted_versioned_conditional_object_batch_read_proof(
    operation: &str,
    report: &mut AdmittedVersionedConditionalObjectBatchReadReport,
) {
    report.storage_admission_proof_format = STORAGE_ADMISSION_PROOF_FORMAT.to_string();
    report.storage_admission_proof_checksum =
        admitted_versioned_conditional_object_batch_read_proof_checksum(operation, report);
}

pub(crate) fn check_expected_admitted_conditional_object_batch_read_proof_checksum(
    operation: &str,
    actual: &str,
    expected_storage_admission_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_storage_admission_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} aggregate storage admission proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

