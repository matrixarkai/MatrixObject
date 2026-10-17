pub(crate) fn object_range_admission_proof_checksum(
    operation: &str,
    admission: &ObjectRangeAdmission,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, STORAGE_ADMISSION_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_string(&mut material, &admission.bucket);
    append_string(&mut material, &admission.key);
    append_u64(&mut material, admission.can_accept as u64);
    append_u64(&mut material, admission.object_length);
    append_u64(&mut material, admission.requested_offset);
    append_u64(&mut material, admission.requested_length);
    append_u64(&mut material, admission.selected_bytes);
    append_u64(&mut material, admission.extent_count);
    append_u64(&mut material, admission.mapped_chunk_ranges);
    append_u64(&mut material, admission.payload_bytes);
    append_u64(&mut material, admission.max_payload_bytes);
    append_u64(&mut material, admission.max_chunk_read_count);
    append_u64(&mut material, admission.max_discard_bytes);
    append_u64(&mut material, admission.budget_limited as u64);
    append_u64(&mut material, admission.min_ready_replica_count);
    append_u64(&mut material, admission.required_quorum);
    append_u64(&mut material, admission.readable_chunk_count);
    append_u64(&mut material, admission.failed_chunk_count);
    append_u64(&mut material, admission.storage_active_replicas);
    append_u64(&mut material, admission.storage_readable_replicas);
    append_u64(&mut material, admission.storage_writable_replicas);
    append_u64(&mut material, admission.storage_quarantined_replicas);
    append_u64(&mut material, admission.storage_frozen_replicas);
    append_u64(&mut material, admission.storage_draining_replicas);
    append_u64(&mut material, admission.storage_capacity_pressure_replicas);
    append_u64(&mut material, admission.storage_trim_flow_limited_replicas);
    append_u64(&mut material, admission.storage_total_capacity_bytes);
    append_u64(&mut material, admission.storage_total_capacity_free_bytes);
    append_u64(&mut material, admission.storage_total_reclaimable_bytes);
    append_u64(
        &mut material,
        admission.storage_total_fragmented_chunk_count,
    );
    append_u64(
        &mut material,
        admission.storage_total_fragmented_discarded_bytes,
    );
    append_u64(&mut material, admission.storage_total_trailing_trim_bytes);
    append_u64(&mut material, admission.storage_reclaim_pressure as u64);
    append_u64(&mut material, admission.storage_read_cache_bytes);
    append_u64(&mut material, admission.storage_read_cache_entries);
    append_u64(&mut material, admission.storage_read_cache_max_bytes);
    append_u64(
        &mut material,
        admission.storage_read_cache_max_cacheable_read_bytes,
    );
    append_u64(&mut material, admission.storage_read_cache_hits);
    append_u64(&mut material, admission.storage_read_cache_misses);
    append_u64(&mut material, admission.storage_read_cache_evictions);
    append_u64(&mut material, admission.storage_read_cache_pressure as u64);
    append_string(&mut material, &admission.storage_admission_reason);
    append_u64(&mut material, admission.storage_read_quorum_limited as u64);
    append_u64(&mut material, admission.storage_write_quorum_limited as u64);
    append_u64(
        &mut material,
        admission.storage_discard_budget_limited as u64,
    );
    append_u64(&mut material, admission.selected_discard_bytes);
    append_u64(&mut material, admission.skipped_discard_bytes);
    append_u64(&mut material, admission.read_stats.chunk_reads);
    append_u64(&mut material, admission.read_stats.cache_hits);
    append_u64(
        &mut material,
        admission.read_stats.coalesced_chunk_read_count,
    );
    append_u64(
        &mut material,
        admission.read_stats.physical_chunk_read_count,
    );
    append_u64(
        &mut material,
        admission.read_stats.metadata_head_batch_requests,
    );
    append_u64(
        &mut material,
        admission.read_stats.metadata_physical_head_count,
    );
    append_u64(
        &mut material,
        admission.read_stats.metadata_coalesced_head_count,
    );
    append_u64(&mut material, admission.read_stats.metadata_cache_hits);
    append_u64(&mut material, admission.read_stats.metadata_cache_misses);
    append_u64(&mut material, admission.read_stats.replica_fallback_reads);
    append_u64(&mut material, admission.read_stats.quorum_verified_chunks);
    append_u64(&mut material, admission.read_stats.replica_read_repairs);
    append_u64(
        &mut material,
        admission.read_stats.replica_read_repair_failures,
    );
    append_u64(
        &mut material,
        admission.read_stats.quorum_read_degraded_chunks,
    );
    append_string(&mut material, &admission.partial_status);
    append_u64(
        &mut material,
        admission.route_control_proof.control_receipt as u64,
    );
    append_string(&mut material, &admission.route_control_proof.state_format);
    append_string(&mut material, &admission.route_control_proof.state_checksum);
    append_u64(
        &mut material,
        admission
            .route_control_proof
            .page_route_control_commit_index,
    );
    append_u64(
        &mut material,
        admission
            .route_control_proof
            .page_route_control_next_log_index,
    );
    append_u64(&mut material, admission.route_control_proof.write_quorum);
    append_u64(&mut material, admission.route_control_proof.read_quorum);
    append_string(&mut material, &admission.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn stamp_object_range_admission_proof(
    operation: &str,
    admission: &mut ObjectRangeAdmission,
) {
    admission.storage_admission_proof_format = STORAGE_ADMISSION_PROOF_FORMAT.to_string();
    admission.storage_admission_proof_checksum =
        object_range_admission_proof_checksum(operation, admission);
}

pub(crate) fn check_expected_object_storage_admission_proof_checksum(
    operation: &str,
    admission: &ObjectRangeAdmission,
    expected_storage_admission_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_storage_admission_proof_checksum {
        if !expected.is_empty() && admission.storage_admission_proof_checksum != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} storage admission proof checksum mismatch: expected {} got {}",
                operation, expected, admission.storage_admission_proof_checksum
            )));
        }
    }
    Ok(())
}

pub(crate) fn verify_object_storage_admission_proof_for_operations(
    label: &str,
    admission: &ObjectRangeAdmission,
    operations: &[&str],
) -> Result<(), ObjectError> {
    if admission.storage_admission_proof_format != STORAGE_ADMISSION_PROOF_FORMAT {
        return Err(ObjectError::PreconditionFailed(format!(
            "{} storage admission proof format mismatch: expected {} got {}",
            label, STORAGE_ADMISSION_PROOF_FORMAT, admission.storage_admission_proof_format
        )));
    }
    if operations.iter().any(|operation| {
        object_range_admission_proof_checksum(operation, admission)
            == admission.storage_admission_proof_checksum
    }) {
        return Ok(());
    }
    Err(ObjectError::PreconditionFailed(format!(
        "{} storage admission proof checksum did not match accepted operations: got {}",
        label, admission.storage_admission_proof_checksum
    )))
}

pub(crate) fn verify_expected_object_storage_admission_proof_for_operations(
    label: &str,
    admission: &ObjectRangeAdmission,
    operations: &[&str],
    expected_storage_admission_proof_checksum: impl AsRef<str>,
) -> Result<(), ObjectError> {
    let expected = expected_storage_admission_proof_checksum.as_ref();
    verify_object_storage_admission_proof_for_operations(label, admission, operations)?;
    if !expected.is_empty() && admission.storage_admission_proof_checksum != expected {
        return Err(ObjectError::PreconditionFailed(format!(
            "{} storage admission proof checksum mismatch: expected {} got {}",
            label, expected, admission.storage_admission_proof_checksum
        )));
    }
    Ok(())
}
