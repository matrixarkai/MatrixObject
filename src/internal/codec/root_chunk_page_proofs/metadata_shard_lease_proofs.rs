// SPDX-License-Identifier: Apache-2.0
pub(crate) fn metadata_shard_receipt_page_proof_checksum(
    page: &MetadataShardReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-SHARD-RECEIPT-PAGE-PROOF1");
    metadata_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
    );
    append_u64(&mut material, page.operation.is_some() as u64);
    if let Some(operation) = &page.operation {
        append_string(&mut material, operation);
    }
    append_u64(&mut material, page.shard_id.is_some() as u64);
    if let Some(shard_id) = &page.shard_id {
        append_string(&mut material, shard_id);
    }
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn append_metadata_lease(out: &mut Vec<u8>, lease: &MetadataLease) {
    append_string(out, &lease.namespace);
    append_string(out, &lease.owner);
    append_u64(out, lease.epoch);
    append_u64(out, lease.expire_time_ms);
    append_u64(out, lease.ttl_ms);
}

pub(crate) fn metadata_lease_payload_bytes(lease: &MetadataLease) -> Result<u64, ObjectError> {
    (lease.namespace.len() as u64)
        .checked_add(lease.owner.len() as u64)
        .ok_or_else(|| {
            ObjectError::InvalidArgument("metadata lease payload bytes overflow".to_string())
        })
}

pub(crate) fn append_metadata_lease_receipt(out: &mut Vec<u8>, receipt: &MetadataLeaseReceipt) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.operation);
    append_string(out, &receipt.request_checksum);
    append_metadata_lease(out, &receipt.lease);
    append_u64(out, receipt.released as u64);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

pub(crate) fn metadata_lease_receipt_proof_checksum(receipt: &MetadataLeaseReceipt) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-LEASE-RECEIPT-PROOF1");
    append_metadata_lease_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn metadata_lease_receipt_page_proof_checksum(
    page: &MetadataLeaseReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MXOS-META-LEASE-RECEIPT-PAGE-PROOF1");
    metadata_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
    );
    append_u64(&mut material, page.lease_namespace.is_some() as u64);
    if let Some(lease_namespace) = &page.lease_namespace {
        append_string(&mut material, lease_namespace);
    }
    append_u64(&mut material, page.owner.is_some() as u64);
    if let Some(owner) = &page.owner {
        append_string(&mut material, owner);
    }
    append_u64(&mut material, page.released.is_some() as u64);
    if let Some(released) = page.released {
        append_u64(&mut material, released as u64);
    }
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}
