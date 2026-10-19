impl ChunkServiceSnapshot {
    fn legacy_receipt_checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(self.encoded_size_hint().min(4 * 1024 * 1024));
        append_u64(&mut material, self.page_size);
        append_u64(&mut material, self.write_quorum as u64);
        append_u64(&mut material, self.read_quorum as u64);
        append_u64(&mut material, self.allocator_chunk_size);
        append_u64(&mut material, self.allocator_next_chunk_id);
        append_u64(&mut material, self.allocator_next_offset);
        append_u64(&mut material, self.released_allocation_count);
        append_u64(&mut material, self.pending_allocations.len() as u64);
        for (allocation_id, allocation) in &self.pending_allocations {
            append_string(&mut material, allocation_id);
            append_string(&mut material, &allocation.allocation_id);
            append_chunk_id(&mut material, &allocation.chunk_id);
            append_u64(&mut material, allocation.offset);
            append_u64(&mut material, allocation.length);
            append_u64(&mut material, allocation.create_time_ms);
        }
        append_u64(&mut material, self.allocation_request_receipts.len() as u64);
        for (idempotency_key, allocation) in &self.allocation_request_receipts {
            append_string(&mut material, idempotency_key);
            append_string(&mut material, &allocation.allocation_id);
            append_chunk_id(&mut material, &allocation.chunk_id);
            append_u64(&mut material, allocation.offset);
            append_u64(&mut material, allocation.length);
            append_u64(&mut material, allocation.create_time_ms);
        }
        append_u64(&mut material, self.write_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.write_request_receipts {
            append_string(&mut material, idempotency_key);
            append_page_write_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.batch_write_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in &self.batch_write_request_receipts {
            append_string(&mut material, idempotency_key);
            append_page_batch_write_receipt_legacy(&mut material, receipt);
        }
        append_u64(&mut material, self.discard_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.discard_request_receipts {
            append_string(&mut material, idempotency_key);
            append_page_discard_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.batch_discard_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in &self.batch_discard_request_receipts {
            append_string(&mut material, idempotency_key);
            append_page_batch_discard_receipt_legacy(&mut material, receipt);
        }
        if !self.quarantined_replicas.is_empty() {
            append_u64(&mut material, self.quarantined_replicas.len() as u64);
            for index in &self.quarantined_replicas {
                append_u64(&mut material, *index as u64);
            }
        }
        append_u64(&mut material, self.replicas.len() as u64);
        for replica in &self.replicas {
            append_string(&mut material, &replica.checksum);
        }
        etag(&material)
    }
}
