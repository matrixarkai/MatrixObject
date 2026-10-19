impl ChunkServiceSnapshot {
    fn encoded_size_hint(&self) -> usize {
        const U64: usize = std::mem::size_of::<u64>();
        let mut bytes = encoded_string_len("MOSRSBLOCK23")
            .saturating_add(U64 * 42)
            .saturating_add(encoded_string_len(&self.checksum));
        for (allocation_id, allocation) in &self.pending_allocations {
            bytes = bytes
                .saturating_add(encoded_string_len(allocation_id))
                .saturating_add(encoded_string_len(&allocation.allocation_id))
                .saturating_add(U64 * 5);
        }
        for (idempotency_key, allocation) in &self.allocation_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(idempotency_key))
                .saturating_add(encoded_string_len(&allocation.allocation_id))
                .saturating_add(U64 * 5);
        }
        for (idempotency_key, allocations) in &self.batch_allocation_request_receipts {
            bytes = bytes.saturating_add(encoded_string_len(idempotency_key)).saturating_add(U64);
            for allocation in allocations {
                bytes = bytes
                    .saturating_add(encoded_string_len(&allocation.allocation_id))
                    .saturating_add(U64 * 5);
            }
        }
        for (idempotency_key, allocation) in &self.release_allocation_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(idempotency_key))
                .saturating_add(encoded_string_len(&allocation.allocation_id))
                .saturating_add(U64 * 5);
        }
        for (key, receipt) in &self.write_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(page_write_receipt_encoded_len(receipt));
        }
        for (key, receipt) in &self.batch_write_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(page_batch_write_receipt_encoded_len(receipt));
        }
        for (key, receipt) in &self.discard_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(page_discard_receipt_encoded_len(receipt));
        }
        for (key, receipt) in &self.batch_discard_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(page_batch_discard_receipt_encoded_len(receipt));
        }
        for endpoint in &self.chunk_endpoints {
            bytes = bytes.saturating_add(encoded_string_len(endpoint));
        }
        bytes = bytes
            .saturating_add(U64 * self.quarantined_replicas.len())
            .saturating_add(U64 * self.disabled_chunk_endpoints.len())
            .saturating_add(U64 * 2 * self.sealed_chunks.len());
        for replica in &self.replicas {
            bytes = bytes
                .saturating_add(U64)
                .saturating_add(replica.encoded_size_hint());
        }
        for (key, receipt) in &self.repair_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_repair_receipt_encoded_len(receipt));
        }
        for (key, receipt) in &self.range_repair_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(page_range_repair_receipt_encoded_len(receipt));
        }
        for (key, receipt) in &self.audit_repair_request_receipts {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(page_audit_repair_receipt_encoded_len(receipt));
        }
        bytes.saturating_add(128)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_size_hint());
        append_string(&mut out, "MOSRSBLOCK23");
        append_u64(&mut out, self.page_size);
        append_u64(&mut out, self.write_quorum as u64);
        append_u64(&mut out, self.read_quorum as u64);
        append_u64(&mut out, self.allocator_chunk_size);
        append_u64(&mut out, self.allocator_next_chunk_id);
        append_u64(&mut out, self.allocator_next_offset);
        append_u64(&mut out, self.receipt_retention as u64);
        append_u64(&mut out, self.released_allocation_count);
        append_u64(&mut out, self.pending_allocations.len() as u64);
        for (allocation_id, allocation) in &self.pending_allocations {
            append_string(&mut out, allocation_id);
            append_string(&mut out, &allocation.allocation_id);
            append_chunk_id(&mut out, &allocation.chunk_id);
            append_u64(&mut out, allocation.offset);
            append_u64(&mut out, allocation.length);
            append_u64(&mut out, allocation.create_time_ms);
        }
        append_u64(&mut out, self.allocation_request_receipts.len() as u64);
        for (idempotency_key, allocation) in &self.allocation_request_receipts {
            append_string(&mut out, idempotency_key);
            append_string(&mut out, &allocation.allocation_id);
            append_chunk_id(&mut out, &allocation.chunk_id);
            append_u64(&mut out, allocation.offset);
            append_u64(&mut out, allocation.length);
            append_u64(&mut out, allocation.create_time_ms);
        }
        append_u64(
            &mut out,
            self.batch_allocation_request_receipts.len() as u64,
        );
        for (idempotency_key, allocations) in &self.batch_allocation_request_receipts {
            append_string(&mut out, idempotency_key);
            append_u64(&mut out, allocations.len() as u64);
            for allocation in allocations {
                append_string(&mut out, &allocation.allocation_id);
                append_chunk_id(&mut out, &allocation.chunk_id);
                append_u64(&mut out, allocation.offset);
                append_u64(&mut out, allocation.length);
                append_u64(&mut out, allocation.create_time_ms);
            }
        }
        append_u64(
            &mut out,
            self.release_allocation_request_receipts.len() as u64,
        );
        for (idempotency_key, allocation) in &self.release_allocation_request_receipts {
            append_string(&mut out, idempotency_key);
            append_string(&mut out, &allocation.allocation_id);
            append_chunk_id(&mut out, &allocation.chunk_id);
            append_u64(&mut out, allocation.offset);
            append_u64(&mut out, allocation.length);
            append_u64(&mut out, allocation.create_time_ms);
        }
        append_u64(&mut out, self.write_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.write_request_receipts {
            append_string(&mut out, idempotency_key);
            append_page_write_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.batch_write_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.batch_write_request_receipts {
            append_string(&mut out, idempotency_key);
            append_page_batch_write_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.discard_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.discard_request_receipts {
            append_string(&mut out, idempotency_key);
            append_page_discard_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.batch_discard_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.batch_discard_request_receipts {
            append_string(&mut out, idempotency_key);
            append_page_batch_discard_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.quarantined_replicas.len() as u64);
        for index in &self.quarantined_replicas {
            append_u64(&mut out, *index as u64);
        }
        append_u64(&mut out, self.chunk_endpoints.len() as u64);
        for endpoint in &self.chunk_endpoints {
            append_string(&mut out, endpoint);
        }
        append_u64(&mut out, self.disabled_chunk_endpoints.len() as u64);
        for index in &self.disabled_chunk_endpoints {
            append_u64(&mut out, *index as u64);
        }
        append_u64(&mut out, self.chunk_route_control_commit_index);
        append_u64(&mut out, self.chunk_route_control_next_log_index);
        append_page_route_qos_config(&mut out, &self.route_qos_config);
        append_page_route_qos_state(&mut out, &self.route_qos_state);
        append_u64(&mut out, self.sealed_chunks.len() as u64);
        for chunk in &self.sealed_chunks {
            append_chunk_id(&mut out, chunk);
        }
        append_u64(&mut out, self.cumulative_quorum_write_degraded_chunks);
        append_u64(&mut out, self.cumulative_quorum_read_degraded_chunks);
        append_u64(&mut out, self.cumulative_quorum_discard_degraded_chunks);
        append_u64(&mut out, self.cumulative_replica_fallback_reads);
        append_u64(&mut out, self.cumulative_physical_chunk_read_count);
        append_u64(&mut out, self.cumulative_coalesced_chunk_read_count);
        append_u64(&mut out, self.cumulative_repair_checked_chunks);
        append_u64(&mut out, self.cumulative_repair_degraded_chunks);
        append_u64(&mut out, self.cumulative_repaired_replicas);
        append_u64(&mut out, self.cumulative_failed_repairs);
        append_u64(&mut out, self.cumulative_replica_read_repairs);
        append_u64(&mut out, self.cumulative_replica_read_repair_failures);
        append_u64(&mut out, self.replicas.len() as u64);
        for replica in &self.replicas {
            let length_offset = out.len();
            append_u64(&mut out, 0);
            let replica_start = out.len();
            replica.append_to_bytes(&mut out);
            let replica_len = (out.len() - replica_start) as u64;
            out[length_offset..length_offset + std::mem::size_of::<u64>()]
                .copy_from_slice(&replica_len.to_le_bytes());
        }
        append_u64(&mut out, self.repair_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.repair_request_receipts {
            append_string(&mut out, idempotency_key);
            append_chunk_repair_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.range_repair_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.range_repair_request_receipts {
            append_string(&mut out, idempotency_key);
            append_page_range_repair_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.audit_repair_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.audit_repair_request_receipts {
            append_string(&mut out, idempotency_key);
            append_page_audit_repair_receipt(&mut out, receipt);
        }
        append_string(&mut out, &self.checksum);
        let envelope_checksum = etag(&out);
        append_string(&mut out, &envelope_checksum);
        Ok(out)
    }
}
