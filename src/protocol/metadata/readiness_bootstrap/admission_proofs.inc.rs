pub(crate) fn object_read_proof_from_readiness(
    readiness: &MetadataRaftReadiness,
) -> ObjectReadProof {
    ObjectReadProof {
        requested_read_index: readiness.min_read_index,
        consensus_read_index: readiness.safe_read_fence,
        metadata_commit_index: readiness.applied_commit_index,
        read_fence_read_index_lag: readiness.read_fence_read_index_lag,
        read_fence_applied_lag: readiness.read_fence_applied_lag,
    }
}

pub(crate) fn proven_metadata_read<T>(
    value: T,
    readiness: &MetadataRaftReadiness,
) -> ProvenMetadataRead<T> {
    ProvenMetadataRead {
        value,
        proof: object_read_proof_from_readiness(readiness),
    }
}

pub(crate) fn receipt_read_proof_from_readiness(
    readiness: &MetadataRaftReadiness,
) -> ReceiptReadProof {
    let mut proof = ReceiptReadProof {
        requested_receipt_read_index: readiness.min_read_index,
        receipt_applied_index: readiness.applied_commit_index,
        receipt_safe_read_fence: readiness.safe_read_fence,
        receipt_read_index_lag: readiness.read_fence_read_index_lag,
        receipt_applied_lag: readiness.read_fence_applied_lag,
        receipt_read_fence_covered: readiness.read_fence_covered,
        receipt_read_proof_checksum: String::new(),
    };
    proof.receipt_read_proof_checksum = receipt_read_proof_checksum(&proof);
    proof
}

pub(crate) fn receipt_read_proof_checksum(proof: &ReceiptReadProof) -> String {
    let mut material = Vec::with_capacity(std::mem::size_of::<u64>() * 6);
    append_u64(&mut material, proof.requested_receipt_read_index);
    append_u64(&mut material, proof.receipt_applied_index);
    append_u64(&mut material, proof.receipt_safe_read_fence);
    append_u64(&mut material, proof.receipt_read_index_lag);
    append_u64(&mut material, proof.receipt_applied_lag);
    append_u64(&mut material, proof.receipt_read_fence_covered as u64);
    etag(&material)
}

pub(crate) fn storage_receipt_page_proof<T>(
    page: &T,
    read_proof: &ReceiptReadProof,
) -> StorageReceiptPageProof
where
    T: StorageReceiptPageProofSource,
{
    let mut material = Vec::with_capacity(
        192 + encoded_string_len("MXOS-STORAGE-RECEIPT-PAGE-PROOF1")
            + encoded_string_len(page.storage_receipt_page_kind())
            + encoded_string_len(page.storage_receipt_page_next_marker().unwrap_or_default())
            + encoded_string_len(&page.storage_receipt_page_typed_proof_checksum())
            + encoded_string_len(&read_proof.receipt_read_proof_checksum),
    );
    append_string(&mut material, "MXOS-STORAGE-RECEIPT-PAGE-PROOF1");
    append_string(&mut material, page.storage_receipt_page_kind());
    append_u64(&mut material, page.storage_receipt_page_receipt_count());
    append_u64(
        &mut material,
        page.storage_receipt_page_total_receipt_count(),
    );
    append_u64(&mut material, page.storage_receipt_page_scanned_count());
    append_u64(&mut material, page.storage_receipt_page_limit());
    append_u64(&mut material, page.storage_receipt_page_truncated() as u64);
    append_string(
        &mut material,
        page.storage_receipt_page_next_marker().unwrap_or_default(),
    );
    append_u64(
        &mut material,
        page.storage_receipt_page_slab_filter().is_some() as u64,
    );
    if let Some(chunk_id) = page.storage_receipt_page_slab_filter() {
        append_chunk_id(&mut material, chunk_id);
    }
    append_u64(
        &mut material,
        page.storage_receipt_page_chunk_filter().is_some() as u64,
    );
    if let Some(page_index) = page.storage_receipt_page_chunk_filter() {
        append_u64(&mut material, page_index);
        append_u64(
            &mut material,
            page.storage_receipt_page_chunk_size().unwrap_or(0),
        );
    }
    append_u64(
        &mut material,
        page.storage_receipt_page_chunk_id_filter().is_some() as u64,
    );
    if let Some(page_id) = page.storage_receipt_page_chunk_id_filter() {
        append_page_id(&mut material, page_id);
    }
    append_u64(
        &mut material,
        page.storage_receipt_page_skipped_chunk_count(),
    );
    append_u64(
        &mut material,
        page.storage_receipt_page_skipped_page_count(),
    );
    append_string(
        &mut material,
        &page.storage_receipt_page_typed_proof_checksum(),
    );
    append_u64(&mut material, read_proof.requested_receipt_read_index);
    append_u64(&mut material, read_proof.receipt_applied_index);
    append_u64(&mut material, read_proof.receipt_safe_read_fence);
    append_u64(&mut material, read_proof.receipt_read_index_lag);
    append_u64(&mut material, read_proof.receipt_applied_lag);
    append_u64(&mut material, read_proof.receipt_read_fence_covered as u64);
    append_string(&mut material, &read_proof.receipt_read_proof_checksum);
    StorageReceiptPageProof {
        storage_receipt_page_proof_format: "MXOS-STORAGE-RECEIPT-PAGE-PROOF1".to_string(),
        storage_receipt_page_proof_checksum: etag(&material),
    }
}
