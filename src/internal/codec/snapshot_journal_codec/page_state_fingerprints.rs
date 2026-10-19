pub(crate) fn append_page_service_fingerprint(
    out: &mut Vec<u8>,
    chunk_service: &MatrixChunkService,
) {
    let readiness = chunk_service.storage_readiness(0, 0, 0);
    append_string(out, &readiness.page_state_fingerprint);
    append_u64(out, readiness.replica_count);
    append_u64(out, readiness.chunk_endpoint_count);
    append_u64(out, readiness.disabled_chunk_endpoint_count);
    append_u64(out, readiness.effective_writable_chunk_endpoints);
    append_u64(out, readiness.write_quorum);
    append_u64(out, readiness.read_quorum);
    append_u64(out, readiness.total_physical_bytes);
    append_u64(out, readiness.total_reclaimable_bytes);
    append_u64(out, readiness.total_capacity_bytes);
    append_u64(out, readiness.total_capacity_free_bytes);
    append_u64(out, readiness.replica_readiness.len() as u64);
    for replica in &readiness.replica_readiness {
        append_string(out, &replica.chunk_state_fingerprint);
        append_u64(out, replica.physical_bytes);
        append_u64(out, replica.reclaimable_bytes);
        append_u64(out, if replica.writable { 1 } else { 0 });
        append_u64(out, if replica.readable { 1 } else { 0 });
    }
}

pub(crate) fn page_audit_repair_image_fingerprint(chunk_service: &MatrixChunkService) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-AUDIT-REPAIR-IMAGE1");
    append_u64(&mut material, chunk_service.write_quorum as u64);
    append_u64(&mut material, chunk_service.read_quorum as u64);
    append_u64(&mut material, chunk_service.chunk_endpoints.len() as u64);
    for (index, endpoint) in chunk_service.chunk_endpoints.iter().enumerate() {
        append_u64(&mut material, index as u64);
        append_string(&mut material, endpoint);
        append_u64(
            &mut material,
            chunk_service.disabled_chunk_endpoints.contains(&index) as u64,
        );
    }
    append_u64(&mut material, chunk_service.replicas.len() as u64);
    for (replica_index, replica) in chunk_service.replicas.iter().enumerate() {
        append_u64(&mut material, replica_index as u64);
        append_u64(
            &mut material,
            chunk_service.quarantined_replicas.contains(&replica_index) as u64,
        );
        append_u64(&mut material, replica.is_frozen() as u64);
        append_u64(&mut material, replica.pages.len() as u64);
        for (page_id, state) in &replica.pages {
            append_chunk_id(&mut material, page_id);
            append_chunk_state(&mut material, state);
        }
    }
    etag(&material)
}

pub(crate) fn page_batch_mutation_image_fingerprint(chunk_service: &MatrixChunkService) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-BATCH-MUTATION-IMAGE1");
    append_u64(&mut material, chunk_service.write_quorum as u64);
    append_u64(&mut material, chunk_service.read_quorum as u64);
    append_u64(&mut material, chunk_service.chunk_endpoints.len() as u64);
    for (index, endpoint) in chunk_service.chunk_endpoints.iter().enumerate() {
        append_u64(&mut material, index as u64);
        append_string(&mut material, endpoint);
        append_u64(
            &mut material,
            chunk_service.disabled_chunk_endpoints.contains(&index) as u64,
        );
    }
    append_u64(
        &mut material,
        chunk_service.quarantined_replicas.len() as u64,
    );
    for replica_index in &chunk_service.quarantined_replicas {
        append_u64(&mut material, *replica_index as u64);
    }
    append_u64(&mut material, chunk_service.replicas.len() as u64);
    for (replica_index, replica) in chunk_service.replicas.iter().enumerate() {
        append_u64(&mut material, replica_index as u64);
        append_u64(&mut material, replica.is_frozen() as u64);
        append_u64(&mut material, replica.pages.len() as u64);
        for (page_id, state) in &replica.pages {
            append_chunk_id(&mut material, page_id);
            append_chunk_state(&mut material, state);
        }
    }
    append_u64(
        &mut material,
        chunk_service.batch_write_request_receipts.len() as u64,
    );
    for receipt in chunk_service.batch_write_request_receipts.values() {
        append_page_batch_write_receipt(&mut material, receipt);
    }
    append_u64(
        &mut material,
        chunk_service.batch_discard_request_receipts.len() as u64,
    );
    for receipt in chunk_service.batch_discard_request_receipts.values() {
        append_page_batch_discard_receipt(&mut material, receipt);
    }
    etag(&material)
}

pub(crate) fn append_page_allocation(out: &mut Vec<u8>, allocation: &ChunkAllocation) {
    append_string(out, &allocation.allocation_id);
    append_slab_id(out, &allocation.chunk_id);
    append_u64(out, allocation.offset);
    append_u64(out, allocation.length);
    append_u64(out, allocation.create_time_ms);
}

pub(crate) fn chunk_allocation_encoded_len(allocation: &ChunkAllocation) -> usize {
    encoded_string_len(&allocation.allocation_id) + std::mem::size_of::<u64>() * 5
}

pub(crate) fn append_page_allocation_receipt(out: &mut Vec<u8>, receipt: &ChunkAllocationReceipt) {
    append_page_allocation(out, &receipt.allocation);
    append_string(out, &receipt.idempotency_key);
    append_u64(out, receipt.released as u64);
    append_u64(out, receipt.deduped as u64);
}

pub(crate) fn chunk_allocation_receipt_encoded_len(receipt: &ChunkAllocationReceipt) -> usize {
    chunk_allocation_encoded_len(&receipt.allocation)
        + encoded_string_len(&receipt.idempotency_key)
        + std::mem::size_of::<u64>() * 2
}

pub(crate) fn chunk_allocation_receipt_proof_checksum(receipt: &ChunkAllocationReceipt) -> String {
    let mut material = Vec::with_capacity(
        encoded_string_len(receipt.proof_format()) + chunk_allocation_receipt_encoded_len(receipt),
    );
    append_string(&mut material, receipt.proof_format());
    append_page_allocation_receipt(&mut material, receipt);
    etag(&material)
}

pub(crate) fn page_allocation_receipt_page_proof_checksum(
    page: &PageAllocationReceiptPage,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, page.proof_format());
    append_page_receipt_page_common(
        &mut material,
        page.receipt_count,
        page.total_receipt_count,
        page.scanned_count,
        page.limit,
        page.truncated,
        &page.next_marker,
        &page.chunk_id,
        page.skipped_slab_count,
    );
    append_page_receipt_page_chunk_filter(
        &mut material,
        page.page_index,
        page.page_size,
        page.skipped_chunk_count,
    );
    append_u64(&mut material, page.receipts.len() as u64);
    for receipt in &page.receipts {
        append_string(&mut material, &receipt.proof_checksum());
    }
    etag(&material)
}

pub(crate) fn chunk_allocation_mutation_image_fingerprint(
    chunk_service: &MatrixChunkService,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-ALLOCATION-MUTATION-IMAGE1");
    append_u64(&mut material, chunk_service.allocator_chunk_size);
    append_u64(&mut material, chunk_service.allocator_next_chunk_id);
    append_u64(&mut material, chunk_service.allocator_next_offset);
    append_u64(
        &mut material,
        chunk_service.pending_allocations.len() as u64,
    );
    for allocation in chunk_service.pending_allocations.values() {
        append_page_allocation(&mut material, allocation);
    }
    append_u64(
        &mut material,
        chunk_service.allocation_request_receipts.len() as u64,
    );
    for (idempotency_key, allocation) in &chunk_service.allocation_request_receipts {
        append_string(&mut material, idempotency_key);
        append_page_allocation(&mut material, allocation);
    }
    append_u64(
        &mut material,
        chunk_service.batch_allocation_request_receipts.len() as u64,
    );
    for (idempotency_key, allocations) in &chunk_service.batch_allocation_request_receipts {
        append_string(&mut material, idempotency_key);
        append_u64(&mut material, allocations.len() as u64);
        for allocation in allocations {
            append_page_allocation(&mut material, allocation);
        }
    }
    append_u64(
        &mut material,
        chunk_service.release_allocation_request_receipts.len() as u64,
    );
    for (idempotency_key, allocation) in &chunk_service.release_allocation_request_receipts {
        append_string(&mut material, idempotency_key);
        append_page_allocation(&mut material, allocation);
    }
    append_u64(&mut material, chunk_service.released_allocation_count);
    etag(&material)
}

pub(crate) fn chunk_route_control_image_fingerprint(chunk_service: &MatrixChunkService) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-BLOCK-ROUTE-CONTROL-IMAGE1");
    append_page_service_fingerprint(&mut material, chunk_service);
    etag(&material)
}

pub(crate) fn chunk_receipt_retention_control_image_fingerprint(
    chunk_service: &MatrixChunkService,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-BLOCK-RECEIPT-RETENTION-CONTROL-IMAGE1",
    );
    append_u64(&mut material, chunk_service.receipt_retention as u64);
    etag(&material)
}

pub(crate) fn replica_receipt_retention_control_image_fingerprint(
    chunk_service: &MatrixChunkService,
) -> String {
    let mut material = Vec::new();
    append_string(
        &mut material,
        "MOS-RUST-BLOCK-CHUNK-RECEIPT-RETENTION-CONTROL-IMAGE1",
    );
    append_u64(&mut material, chunk_service.replicas.len() as u64);
    append_u64(
        &mut material,
        chunk_service.quarantined_replicas.len() as u64,
    );
    for replica_index in &chunk_service.quarantined_replicas {
        append_u64(&mut material, *replica_index as u64);
    }
    append_u64(
        &mut material,
        chunk_service.disabled_chunk_endpoints.len() as u64,
    );
    for replica_index in &chunk_service.disabled_chunk_endpoints {
        append_u64(&mut material, *replica_index as u64);
    }
    for (index, replica) in chunk_service.replicas.iter().enumerate() {
        append_u64(&mut material, index as u64);
        append_u64(&mut material, replica.receipt_retention as u64);
    }
    etag(&material)
}

