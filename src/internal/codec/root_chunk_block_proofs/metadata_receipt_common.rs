pub(crate) fn append_metadata_batch_receipt(out: &mut Vec<u8>, receipt: &MetadataBatchReceipt) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.request_checksum);
    append_u64(out, receipt.owner_fenced as u64);
    append_string(out, &receipt.expected_owner);
    append_u64(out, receipt.expected_epoch);
    append_string(out, receipt.lease_owner.as_deref().unwrap_or_default());
    append_u64(out, receipt.lease_epoch.unwrap_or_default());
    append_u64(out, receipt.now_ms);
    append_u64(out, receipt.first_commit_index);
    append_u64(out, receipt.last_commit_index);
    append_u64(out, receipt.records.len() as u64);
    for record in &receipt.records {
        append_metadata_record(out, record);
    }
}

pub(crate) fn append_metadata_shard_receipt(out: &mut Vec<u8>, receipt: &MetadataShardReceipt) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.operation);
    append_string(out, &receipt.request_checksum);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
    append_u64(out, receipt.shards.len() as u64);
    for shard in &receipt.shards {
        append_metadata_shard_image(out, shard);
    }
}

pub(crate) fn metadata_shard_receipt_proof_checksum(receipt: &MetadataShardReceipt) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-SHARD-RECEIPT-PROOF1");
    append_metadata_shard_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn metadata_receipt_page_common(
    material: &mut Vec<u8>,
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
) {
    append_u64(material, receipt_count);
    append_u64(material, total_receipt_count);
    append_u64(material, scanned_count);
    append_u64(material, limit);
    append_u64(material, truncated as u64);
    append_u64(material, next_marker.is_some() as u64);
    if let Some(next_marker) = next_marker {
        append_string(material, next_marker);
    }
}

pub(crate) fn metadata_optional_string_filter(material: &mut Vec<u8>, value: &Option<String>) {
    append_u64(material, value.is_some() as u64);
    if let Some(value) = value {
        append_string(material, value);
    }
}

pub(crate) fn metadata_optional_bool_filter(material: &mut Vec<u8>, value: Option<bool>) {
    append_u64(material, value.is_some() as u64);
    if let Some(value) = value {
        append_u64(material, value as u64);
    }
}

pub(crate) fn metadata_optional_u64_filter(material: &mut Vec<u8>, value: Option<u64>) {
    append_u64(material, value.is_some() as u64);
    if let Some(value) = value {
        append_u64(material, value);
    }
}

pub(crate) fn append_metadata_mutation_receipt(
    out: &mut Vec<u8>,
    receipt: &MetadataMutationReceipt,
) {
    append_string(out, &receipt.audit_key);
    append_string(out, &receipt.receipt_family);
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.method);
    append_string(out, &receipt.request_checksum);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
    append_u64(out, receipt.owner_fenced as u64);
    append_string(out, &receipt.expected_owner);
    append_u64(out, receipt.expected_epoch);
    append_string(out, receipt.lease_owner.as_deref().unwrap_or_default());
    append_u64(out, receipt.record_keys.len() as u64);
    for record_key in &receipt.record_keys {
        append_string(out, record_key);
    }
}

pub(crate) fn metadata_batch_receipt_proof_checksum(receipt: &MetadataBatchReceipt) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-BATCH-RECEIPT-PROOF1");
    append_metadata_batch_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn metadata_batch_receipt_page_proof_checksum(
    page: &MetadataBatchReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-BATCH-RECEIPT-PAGE-PROOF1");
    append_u64(&mut material, page.receipt_count);
    append_u64(&mut material, page.total_receipt_count);
    append_u64(&mut material, page.scanned_count);
    append_u64(&mut material, page.matched_count);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.budget_limited as u64);
    append_u64(&mut material, page.truncated as u64);
    metadata_optional_string_filter(&mut material, &page.marker);
    metadata_optional_string_filter(&mut material, &page.next_marker);
    metadata_optional_bool_filter(&mut material, page.owner_fenced);
    metadata_optional_string_filter(&mut material, &page.expected_owner);
    metadata_optional_u64_filter(&mut material, page.expected_epoch);
    metadata_optional_string_filter(&mut material, &page.lease_owner);
    metadata_optional_string_filter(&mut material, &page.record_key_prefix);
    append_u64(&mut material, page.skipped_owner_fence_count);
    append_u64(&mut material, page.skipped_expected_owner_count);
    append_u64(&mut material, page.skipped_expected_epoch_count);
    append_u64(&mut material, page.skipped_lease_owner_count);
    append_u64(&mut material, page.skipped_record_prefix_count);
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn metadata_mutation_receipt_proof_checksum(
    receipt: &MetadataMutationReceipt,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-MUTATION-RECEIPT-PROOF1");
    append_metadata_mutation_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn metadata_mutation_receipt_page_proof_checksum(
    page: &MetadataMutationReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-MUTATION-RECEIPT-PAGE-PROOF1");
    append_u64(&mut material, page.receipt_count);
    append_u64(&mut material, page.total_receipt_count);
    append_u64(&mut material, page.scanned_count);
    append_u64(&mut material, page.matched_count);
    append_u64(&mut material, page.payload_bytes);
    append_u64(&mut material, page.max_payload_bytes);
    append_u64(&mut material, page.budget_limited as u64);
    append_u64(&mut material, page.truncated as u64);
    metadata_optional_string_filter(&mut material, &page.marker);
    metadata_optional_string_filter(&mut material, &page.next_marker);
    metadata_optional_string_filter(&mut material, &page.method);
    metadata_optional_string_filter(&mut material, &page.request_checksum);
    metadata_optional_bool_filter(&mut material, page.owner_fenced);
    metadata_optional_string_filter(&mut material, &page.record_key_prefix);
    append_u64(&mut material, page.skipped_method_count);
    append_u64(&mut material, page.skipped_checksum_count);
    append_u64(&mut material, page.skipped_owner_fence_count);
    append_u64(&mut material, page.skipped_record_prefix_count);
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}
