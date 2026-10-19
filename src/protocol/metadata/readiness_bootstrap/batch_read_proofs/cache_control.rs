// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_cache_control_stats(material: &mut Vec<u8>, stats: &CacheControlStats) {
    append_u64(material, stats.requested_ranges);
    append_u64(material, stats.requested_bytes);
    append_u64(material, stats.selected_ranges);
    append_u64(material, stats.selected_bytes);
    append_u64(material, stats.skipped_bytes);
    append_u64(material, stats.contacted_chunk_endpoints);
    append_u64(material, stats.failed_chunk_endpoints);
    append_u64(material, stats.applied_chunk_ranges);
    append_u64(material, stats.failed_chunk_ranges);
    append_u64(material, stats.cache_hits);
    append_u64(material, stats.physical_reads);
    append_u64(material, stats.invalidated_entries);
    append_u64(material, stats.budget_limited as u64);
}

pub(crate) fn object_batch_cache_control_proof_checksum(
    operation: &str,
    report: &ObjectBatchCacheControlReport,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, CACHE_BATCH_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.selected_objects);
    append_u64(&mut material, report.skipped_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.requested_bytes);
    append_u64(&mut material, report.selected_bytes);
    append_u64(&mut material, report.skipped_bytes);
    append_u64(&mut material, report.budget_limited as u64);
    append_cache_control_stats(&mut material, &report.stats);
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
        append_u64(&mut material, item.request.offset);
        append_u64(&mut material, item.request.length);
        append_u64(&mut material, item.request.version.unwrap_or_default());
        append_u64(&mut material, item.request.version.is_some() as u64);
        append_u64(&mut material, item.skipped as u64);
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        append_cache_control_stats(&mut material, &item.stats);
        if let Some(metadata) = item.metadata.as_ref() {
            append_u64(&mut material, 1);
            append_u64(&mut material, metadata.version);
            append_string(&mut material, &metadata.etag);
            append_u64(&mut material, metadata.length);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn stamp_object_batch_cache_control_proof(
    operation: &str,
    report: &mut ObjectBatchCacheControlReport,
) {
    report.cache_batch_proof_format = CACHE_BATCH_PROOF_FORMAT.to_string();
    report.cache_batch_proof_checksum =
        object_batch_cache_control_proof_checksum(operation, report);
}

pub(crate) fn check_expected_object_batch_cache_control_proof_checksum(
    operation: &str,
    actual: &str,
    expected_cache_batch_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_cache_batch_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} cache batch proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

