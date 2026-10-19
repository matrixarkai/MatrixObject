pub(crate) fn admitted_object_batch_read_proof_checksum(
    operation: &str,
    report: &AdmittedObjectBatchReadReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, STORAGE_ADMISSION_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.successful_objects);
    append_u64(&mut material, report.admission_rejected_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.payload_bytes);
    append_u64(&mut material, report.max_payload_bytes);
    append_u64(&mut material, report.reserved_chunk_read_count);
    append_u64(&mut material, report.max_chunk_read_count);
    append_u64(&mut material, report.budget_limited as u64);
    append_u64(&mut material, report.read_proof.requested_read_index);
    append_u64(&mut material, report.read_proof.consensus_read_index);
    append_u64(&mut material, report.read_proof.metadata_commit_index);
    append_u64(&mut material, report.read_proof.read_fence_read_index_lag);
    append_u64(&mut material, report.read_proof.read_fence_applied_lag);
    append_u64(
        &mut material,
        report.route_control_proof.control_receipt as u64,
    );
    append_string(&mut material, &report.route_control_proof.state_format);
    append_string(&mut material, &report.route_control_proof.state_checksum);
    append_u64(
        &mut material,
        report.route_control_proof.chunk_route_control_commit_index,
    );
    append_u64(
        &mut material,
        report
            .route_control_proof
            .chunk_route_control_next_log_index,
    );
    append_u64(&mut material, report.route_control_proof.write_quorum);
    append_u64(&mut material, report.route_control_proof.read_quorum);
    append_string(&mut material, &report.expected_route_control_checksum);
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
        append_u64(&mut material, item.request.version.unwrap_or_default());
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
            append_u64(&mut material, object.object.is_some() as u64);
            append_string(
                &mut material,
                &object.admission.storage_admission_proof_format,
            );
            append_string(
                &mut material,
                &object.admission.storage_admission_proof_checksum,
            );
            append_u64(&mut material, object.admission.can_accept as u64);
            append_u64(&mut material, object.admission.selected_bytes);
            append_u64(&mut material, object.admission.payload_bytes);
            append_u64(&mut material, object.admission.mapped_chunk_ranges);
            append_u64(&mut material, object.admission.readable_chunk_count);
            append_string(&mut material, &object.admission.storage_admission_reason);
            append_string(&mut material, &object.admission.partial_status);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn stamp_admitted_object_batch_read_proof(
    operation: &str,
    report: &mut AdmittedObjectBatchReadReport,
) {
    report.storage_admission_proof_format = STORAGE_ADMISSION_PROOF_FORMAT.to_string();
    report.storage_admission_proof_checksum =
        admitted_object_batch_read_proof_checksum(operation, report);
}

pub(crate) fn check_expected_admitted_object_batch_read_proof_checksum(
    operation: &str,
    report: &AdmittedObjectBatchReadReport,
    expected_storage_admission_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_storage_admission_proof_checksum {
        if !expected.is_empty() && report.storage_admission_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} aggregate storage admission proof checksum mismatch: expected {} got {}",
                operation, expected, report.storage_admission_proof_checksum
            )));
        }
    }
    Ok(())
}

