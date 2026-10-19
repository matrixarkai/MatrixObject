impl ChunkServiceSnapshot {
    /// Computes the checksum an unsealed snapshot is missing, so `validate`
    /// accepts it. Paired with the `export_snapshot_unsealed` constructors,
    /// which skip this because it copies every payload byte before hashing.
    pub fn seal_checksum(&mut self) {
        // The replicas first: `install_snapshot` validates each replica
        // snapshot as it installs it, and this snapshot's own material appends
        // every replica checksum, so both depend on them already being sealed.
        for replica in &mut self.replicas {
            replica.seal_checksum();
        }
        self.checksum = self.checksum_material_hash();
    }

    fn checksum_material_hash(&self) -> String {
        let route_qos_state = self.route_qos_state.clone();
        let mut material = Vec::with_capacity(self.encoded_size_hint().min(8 * 1024 * 1024));
        append_u64(&mut material, self.page_size);
        append_u64(&mut material, self.write_quorum as u64);
        append_u64(&mut material, self.read_quorum as u64);
        append_u64(&mut material, self.allocator_chunk_size);
        append_u64(&mut material, self.allocator_next_chunk_id);
        append_u64(&mut material, self.allocator_next_offset);
        append_u64(&mut material, self.receipt_retention as u64);
        append_u64(&mut material, self.released_allocation_count);
        append_u64(&mut material, self.pending_allocations.len() as u64);
        for (allocation_id, allocation) in &self.pending_allocations {
            append_string(&mut material, allocation_id);
            append_string(&mut material, &allocation.allocation_id);
            append_slab_id(&mut material, &allocation.chunk_id);
            append_u64(&mut material, allocation.offset);
            append_u64(&mut material, allocation.length);
            append_u64(&mut material, allocation.create_time_ms);
        }
        append_u64(&mut material, self.allocation_request_receipts.len() as u64);
        for (idempotency_key, allocation) in &self.allocation_request_receipts {
            append_string(&mut material, idempotency_key);
            append_string(&mut material, &allocation.allocation_id);
            append_slab_id(&mut material, &allocation.chunk_id);
            append_u64(&mut material, allocation.offset);
            append_u64(&mut material, allocation.length);
            append_u64(&mut material, allocation.create_time_ms);
        }
        append_u64(
            &mut material,
            self.batch_allocation_request_receipts.len() as u64,
        );
        for (idempotency_key, allocations) in &self.batch_allocation_request_receipts {
            append_string(&mut material, idempotency_key);
            append_u64(&mut material, allocations.len() as u64);
            for allocation in allocations {
                append_page_allocation(&mut material, allocation);
            }
        }
        append_u64(
            &mut material,
            self.release_allocation_request_receipts.len() as u64,
        );
        for (idempotency_key, allocation) in &self.release_allocation_request_receipts {
            append_string(&mut material, idempotency_key);
            append_string(&mut material, &allocation.allocation_id);
            append_slab_id(&mut material, &allocation.chunk_id);
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
            append_page_batch_write_receipt(&mut material, receipt);
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
            append_page_batch_discard_receipt(&mut material, receipt);
        }
        if !self.quarantined_replicas.is_empty() {
            append_u64(&mut material, self.quarantined_replicas.len() as u64);
            for index in &self.quarantined_replicas {
                append_u64(&mut material, *index as u64);
            }
        }
        append_u64(&mut material, self.chunk_endpoints.len() as u64);
        for endpoint in &self.chunk_endpoints {
            append_string(&mut material, endpoint);
        }
        append_u64(&mut material, self.disabled_chunk_endpoints.len() as u64);
        for index in &self.disabled_chunk_endpoints {
            append_u64(&mut material, *index as u64);
        }
        append_u64(&mut material, self.chunk_route_control_commit_index);
        append_u64(&mut material, self.chunk_route_control_next_log_index);
        append_page_route_qos_config(&mut material, &self.route_qos_config);
        append_page_route_qos_state(&mut material, &route_qos_state);
        append_u64(&mut material, self.sealed_chunks.len() as u64);
        for slab in &self.sealed_chunks {
            append_slab_id(&mut material, slab);
        }
        append_u64(&mut material, self.cumulative_quorum_write_degraded_chunks);
        append_u64(&mut material, self.cumulative_quorum_read_degraded_chunks);
        append_u64(
            &mut material,
            self.cumulative_quorum_discard_degraded_chunks,
        );
        append_u64(&mut material, self.cumulative_replica_fallback_reads);
        append_u64(&mut material, self.cumulative_physical_chunk_read_count);
        append_u64(&mut material, self.cumulative_coalesced_chunk_read_count);
        append_u64(&mut material, self.cumulative_repair_checked_chunks);
        append_u64(&mut material, self.cumulative_repair_degraded_chunks);
        append_u64(&mut material, self.cumulative_repaired_replicas);
        append_u64(&mut material, self.cumulative_failed_repairs);
        append_u64(&mut material, self.cumulative_replica_read_repairs);
        append_u64(&mut material, self.cumulative_replica_read_repair_failures);
        append_u64(&mut material, self.replicas.len() as u64);
        for replica in &self.replicas {
            append_string(&mut material, &replica.checksum);
        }
        append_u64(&mut material, self.repair_request_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.repair_request_receipts {
            append_string(&mut material, idempotency_key);
            append_chunk_repair_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.range_repair_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in &self.range_repair_request_receipts {
            append_string(&mut material, idempotency_key);
            append_page_range_repair_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.audit_repair_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in &self.audit_repair_request_receipts {
            append_string(&mut material, idempotency_key);
            append_page_audit_repair_receipt(&mut material, receipt);
        }
        etag(&material)
    }
}
