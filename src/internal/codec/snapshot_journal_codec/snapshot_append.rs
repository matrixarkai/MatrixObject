pub(crate) fn append_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn append_string(out: &mut Vec<u8>, value: &str) {
    append_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

pub(crate) fn encoded_string_len(value: &str) -> usize {
    std::mem::size_of::<u64>() + value.len()
}

pub(crate) fn reserve_snapshot_buffer(
    buffer: &mut Vec<u8>,
    total_bytes: u64,
    context: &str,
) -> Result<(), crate::ObjectError> {
    let total = usize::try_from(total_bytes).map_err(|_| {
        crate::ObjectError::InvalidArgument(format!("{context} snapshot is too large to assemble"))
    })?;
    if buffer.capacity() >= total {
        return Ok(());
    }
    buffer
        .try_reserve_exact(total - buffer.capacity())
        .map_err(|_| {
            crate::ObjectError::InvalidArgument(format!(
                "{context} snapshot buffer reservation failed"
            ))
        })
}

pub(crate) fn snapshot_chunk_vec_capacity(total_bytes: u64, max_payload_bytes: usize) -> usize {
    const SNAPSHOT_CHUNK_VEC_RESERVE_CAP: usize = 1024;
    if total_bytes == 0 || max_payload_bytes == 0 {
        return 0;
    }
    let max_payload_bytes = max_payload_bytes as u64;
    let page_count = total_bytes.saturating_add(max_payload_bytes - 1) / max_payload_bytes;
    usize::try_from(page_count)
        .unwrap_or(SNAPSHOT_CHUNK_VEC_RESERVE_CAP)
        .min(SNAPSHOT_CHUNK_VEC_RESERVE_CAP)
}

pub(crate) fn snapshot_proof_material_capacity(
    total_bytes: u64,
    max_payload_bytes: usize,
    bytes_per_chunk: usize,
) -> usize {
    const SNAPSHOT_PROOF_MATERIAL_RESERVE_CAP: usize = 1024 * 1024;
    snapshot_chunk_vec_capacity(total_bytes, max_payload_bytes)
        .saturating_mul(bytes_per_chunk)
        .min(SNAPSHOT_PROOF_MATERIAL_RESERVE_CAP)
}

pub(crate) fn append_string_set<S: AsRef<str>>(out: &mut Vec<u8>, values: &BTreeSet<S>) {
    append_u64(out, values.len() as u64);
    for value in values {
        append_string(out, value.as_ref());
    }
}

pub(crate) fn append_string_list(out: &mut Vec<u8>, values: &[String]) {
    append_u64(out, values.len() as u64);
    for value in values {
        append_string(out, value);
    }
}

pub(crate) fn append_string_map(out: &mut Vec<u8>, values: &BTreeMap<String, String>) {
    append_u64(out, values.len() as u64);
    for (key, value) in values {
        append_string(out, key);
        append_string(out, value);
    }
}

pub(crate) fn append_object_id(out: &mut Vec<u8>, object_id: &ObjectId) {
    append_string(out, &object_id.bucket);
    append_string(out, &object_id.key);
}

pub(crate) fn append_optional_object_id(out: &mut Vec<u8>, object_id: Option<&ObjectId>) {
    match object_id {
        Some(object_id) => {
            append_string(out, "present");
            append_object_id(out, object_id);
        }
        None => append_string(out, "absent"),
    }
}

pub(crate) fn append_object_metadata_identity(out: &mut Vec<u8>, metadata: &ObjectMetadata) {
    append_object_id(out, &metadata.object_id);
    append_u64(out, metadata.version);
    append_u64(out, metadata.length);
    append_string(out, &metadata.etag);
    append_string(out, &metadata.content_type);
    append_string(out, &metadata.storage_class);
    append_u64(out, metadata.lock.restore_until_ms);
    append_string(out, &metadata.lock.legal_hold.to_string());
    append_u64(out, metadata.lock.legal_hold_update_time_ms);
    append_u64(out, metadata.lock.retention_until_ms);
    append_u64(out, metadata.update_time_ms);
    append_string(out, &metadata.deleted.to_string());
    append_string_map(out, &metadata.user_metadata);
    append_string_map(out, &metadata.tags);
    append_u64(out, metadata.extents.len() as u64);
    for extent in &metadata.extents {
        append_chunk_id(out, &extent.chunk_id);
        append_u64(out, extent.offset);
        append_u64(out, extent.length);
    }
}

pub(crate) fn append_chunk_id(out: &mut Vec<u8>, chunk_id: &ChunkId) {
    append_u64(out, chunk_id.volume_id);
    append_u64(out, chunk_id.id);
}

pub(crate) fn append_range(out: &mut Vec<u8>, range: &DataRange) {
    append_u64(out, range.offset);
    append_u64(out, range.length);
}

pub(crate) fn append_metadata(out: &mut Vec<u8>, metadata: &ObjectMetadata) {
    append_object_id(out, &metadata.object_id);
    append_u64(out, metadata.version);
    append_u64(out, metadata.length);
    append_string(out, &metadata.etag);
    append_string(out, &metadata.content_type);
    append_string_map(out, &metadata.user_metadata);
    append_string(out, &metadata.storage_class);
    append_u64(out, metadata.lock.restore_until_ms);
    append_string_map(out, &metadata.tags);
    append_u64(out, metadata.lock.legal_hold as u64);
    append_u64(out, metadata.lock.legal_hold_update_time_ms);
    append_u64(out, metadata.lock.retention_until_ms);
    append_u64(out, metadata.update_time_ms);
    append_u64(out, metadata.deleted as u64);
    append_u64(out, metadata.extents.len() as u64);
    for extent in &metadata.extents {
        append_chunk_id(out, &extent.chunk_id);
        append_u64(out, extent.offset);
        append_u64(out, extent.length);
    }
}

pub(crate) fn append_raft_apply_receipt(out: &mut Vec<u8>, receipt: &RaftApplyReceipt) {
    append_u64(out, receipt.index);
    append_u64(out, receipt.term);
    append_string(out, &receipt.command_checksum);
    append_metadata(out, &receipt.result);
}

pub(crate) fn append_raft_membership_receipt(out: &mut Vec<u8>, receipt: &RaftMembershipReceipt) {
    append_string(out, &receipt.mutation_id);
    append_u64(out, receipt.target_voter_node_ids.len() as u64);
    for voter in &receipt.target_voter_node_ids {
        append_string(out, voter);
    }
    append_string(out, &receipt.target_voter_checksum);
    append_u64(out, receipt.target_healthy_peer_count);
    append_u64(out, receipt.expected_previous_commit_index);
    append_u64(out, receipt.membership_epoch);
    append_string(out, &receipt.command_checksum);
}

pub(crate) fn raft_membership_receipt_encoded_len(receipt: &RaftMembershipReceipt) -> usize {
    let voter_bytes: usize = receipt
        .target_voter_node_ids
        .iter()
        .map(|voter| encoded_string_len(voter))
        .sum();
    encoded_string_len(&receipt.mutation_id)
        + std::mem::size_of::<u64>() * 4
        + voter_bytes
        + encoded_string_len(&receipt.target_voter_checksum)
        + encoded_string_len(&receipt.command_checksum)
}

pub(crate) fn raft_membership_receipt_proof_checksum(receipt: &RaftMembershipReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-RAFT-MEMBERSHIP-RECEIPT-PROOF1")
            + raft_membership_receipt_encoded_len(receipt),
    );
    append_string(&mut material, "MXOS-RAFT-MEMBERSHIP-RECEIPT-PROOF1");
    append_raft_membership_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn raft_membership_receipt_page_proof_checksum(
    page: &RaftMembershipReceiptPage,
) -> String {
    let receipt_proof_bytes = page
        .receipts
        .len()
        .saturating_mul(encoded_string_len("0000000000000000"));
    let mut material = Vec::with_capacity(
        encoded_string_len("MXOS-RAFT-MEMBERSHIP-RECEIPT-PAGE-PROOF1")
            + std::mem::size_of::<u64>() * 18
            + page
                .next_marker
                .as_ref()
                .map(|value| encoded_string_len(value))
                .unwrap_or(0)
            + page
                .target_voter_node_id
                .as_ref()
                .map(|value| encoded_string_len(value))
                .unwrap_or(0)
            + page
                .target_voter_checksum
                .as_ref()
                .map(|value| encoded_string_len(value))
                .unwrap_or(0)
            + receipt_proof_bytes,
    );
    append_string(&mut material, "MXOS-RAFT-MEMBERSHIP-RECEIPT-PAGE-PROOF1");
    metadata_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
    );
    append_u64(&mut material, page.target_voter_node_id.is_some() as u64);
    if let Some(target_voter_node_id) = &page.target_voter_node_id {
        append_string(&mut material, target_voter_node_id);
    }
    append_u64(&mut material, page.target_voter_checksum.is_some() as u64);
    if let Some(target_voter_checksum) = &page.target_voter_checksum {
        append_string(&mut material, target_voter_checksum);
    }
    append_u64(&mut material, page.quorum_preserved.is_some() as u64);
    if let Some(quorum_preserved) = page.quorum_preserved {
        append_u64(&mut material, quorum_preserved as u64);
    }
    append_u64(&mut material, page.membership_plan_safe.is_some() as u64);
    if let Some(membership_plan_safe) = page.membership_plan_safe {
        append_u64(&mut material, membership_plan_safe as u64);
    }
    append_u64(&mut material, page.skipped_target_voter_count);
    append_u64(&mut material, page.skipped_target_checksum_count);
    append_u64(&mut material, page.skipped_quorum_preserved_count);
    append_u64(&mut material, page.skipped_plan_safe_count);
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn append_chunk_info(out: &mut Vec<u8>, chunk: &ChunkInfo) {
    append_chunk_id(out, &chunk.chunk_id);
    append_u64(out, chunk.logical_bytes);
    append_u64(out, chunk.live_bytes);
    append_u64(out, chunk.stale_bytes);
    append_u64(out, chunk.discarded_bytes);
    append_u64(out, chunk.next_offset);
    append_u64(out, chunk.stale_ranges.len() as u64);
    for range in &chunk.stale_ranges {
        append_range(out, range);
    }
    append_u64(out, chunk.discarded_ranges.len() as u64);
    for range in &chunk.discarded_ranges {
        append_range(out, range);
    }
}

pub(crate) fn append_put_options(out: &mut Vec<u8>, options: &PutOptions) {
    append_u64(out, options.if_none_match as u64);
    append_string(out, options.if_match_etag.as_deref().unwrap_or(""));
    append_string(out, options.expected_checksum.as_deref().unwrap_or(""));
    append_string(out, options.lease_owner.as_deref().unwrap_or(""));
    append_u64(out, options.lease_epoch.unwrap_or(0));
    append_string_map(out, &options.user_metadata);
    append_string_map(out, &options.tags);
    append_string(out, options.storage_class.as_deref().unwrap_or(""));
    append_string_list(out, &options.if_match_etags);
}

pub(crate) fn append_lifecycle_policy(out: &mut Vec<u8>, policy: &BucketLifecyclePolicy) {
    append_u64(out, policy.noncurrent_versions_to_keep as u64);
    append_u64(out, policy.noncurrent_older_than_ms);
    append_u64(out, policy.transition_after_ms);
    append_string(
        out,
        policy.transition_storage_class.as_deref().unwrap_or(""),
    );
    append_u64(out, policy.expire_current_after_ms);
    append_u64(out, policy.expire_delete_markers as u64);
    append_string(out, policy.filter_tag_key.as_deref().unwrap_or(""));
    append_string(out, policy.filter_tag_value.as_deref().unwrap_or(""));
    append_string(out, policy.filter_prefix.as_deref().unwrap_or(""));
    append_u64(out, policy.abort_incomplete_multipart_after_ms);
}

pub(crate) fn append_bucket_policy_document(out: &mut Vec<u8>, policy: &BucketPolicyDocument) {
    append_string(out, &policy.document);
}

pub(crate) fn append_bucket_tag_set(out: &mut Vec<u8>, tag_set: &BucketTagSet) {
    append_string_map(out, &tag_set.tags);
}

pub(crate) fn append_cors_policy(out: &mut Vec<u8>, policy: &BucketCorsPolicy) {
    append_string_list(out, &policy.allowed_origins);
    append_string_list(out, &policy.allowed_methods);
    append_string_list(out, &policy.allowed_headers);
    append_string_list(out, &policy.expose_headers);
    append_u64(out, policy.max_age_seconds);
}

pub(crate) fn append_bucket_policy_receipt(
    out: &mut Vec<u8>,
    receipt: &BucketPolicyMutationReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.method);
    append_string(out, &receipt.bucket);
    append_string(out, &receipt.request_checksum);
    append_metadata(out, &receipt.metadata);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn append_bucket_cors_receipt(out: &mut Vec<u8>, receipt: &BucketCorsMutationReceipt) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.bucket);
    append_string(out, &receipt.request_checksum);
    append_metadata(out, &receipt.report.metadata);
    append_cors_policy(out, &receipt.report.policy);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn append_multipart_upload(out: &mut Vec<u8>, upload: &MultipartUpload) {
    append_string(out, &upload.upload_id);
    append_string(out, &upload.bucket);
    append_string(out, &upload.key);
    append_string(out, &upload.content_type);
    match &upload.create_idempotency_key {
        Some(idempotency_key) => {
            append_u64(out, 1);
            append_string(out, idempotency_key);
        }
        None => append_u64(out, 0),
    }
    append_put_options(out, &upload.options);
    append_u64(out, upload.create_time_ms);
    append_u64(out, upload.update_time_ms);
    append_u64(out, upload.next_part_number as u64);
    append_u64(out, upload.total_bytes);
    append_u64(out, upload.parts.len() as u64);
    for (part_number, part) in &upload.parts {
        append_u64(out, *part_number as u64);
        append_u64(out, part.part_number as u64);
        append_u64(out, part.length);
        append_string(out, &part.etag);
        append_u64(out, part.update_time_ms);
        if let Some(idempotency_key) = &part.idempotency_key {
            append_u64(out, 1);
            append_string(out, idempotency_key);
        } else {
            append_u64(out, 0);
        }
        if let Some(payload) = upload.part_payloads.get(part_number) {
            append_u64(out, payload.len() as u64);
            out.extend_from_slice(payload);
        } else {
            append_u64(out, 0);
        }
    }
}

pub(crate) fn append_multipart_part(out: &mut Vec<u8>, part: &MultipartPart) {
    append_u64(out, part.part_number as u64);
    append_u64(out, part.length);
    append_string(out, &part.etag);
    append_u64(out, part.update_time_ms);
    if let Some(idempotency_key) = &part.idempotency_key {
        append_u64(out, 1);
        append_string(out, idempotency_key);
    } else {
        append_u64(out, 0);
    }
}

pub(crate) fn append_multipart_complete_receipt(
    out: &mut Vec<u8>,
    receipt: &MultipartCompleteReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.upload_id);
    append_string(out, &receipt.request_checksum);
    append_metadata(out, &receipt.metadata);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn append_multipart_abort_receipt(out: &mut Vec<u8>, receipt: &MultipartAbortReceipt) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.upload_id);
    append_string(out, &receipt.request_checksum);
    append_multipart_upload(out, &receipt.upload);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn append_object_change_record(out: &mut Vec<u8>, change: &ObjectChangeRecord) {
    append_u64(out, change.sequence);
    append_u64(out, change.previous_sequence);
    append_string(out, &change.operation);
    append_object_id(out, &change.object_id);
    append_u64(out, change.version);
    append_u64(out, change.length);
    append_string(out, &change.etag());
    append_u64(out, change.deleted as u64);
    append_u64(out, change.event_time_ms);
}

pub(crate) fn object_change_record_payload_bytes(change: &ObjectChangeRecord) -> u64 {
    96_u64
        .saturating_add(change.operation.len() as u64)
        .saturating_add(change.object_id.bucket.len() as u64)
        .saturating_add(change.object_id.key.len() as u64)
        .saturating_add(change.etag().len() as u64)
}

pub(crate) fn append_object_change_cursor(out: &mut Vec<u8>, cursor: &ObjectChangeCursor) {
    append_string(out, &cursor.consumer_id);
    append_u64(out, cursor.sequence);
    append_u64(out, cursor.update_time_ms);
}
