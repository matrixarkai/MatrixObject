impl ReplicaServiceSnapshot {
    fn checksum_material_hash(&self) -> String {
        let mut material = Vec::with_capacity(self.encoded_size_hint());
        append_u64(&mut material, self.accepting_writes as u64);
        append_u64(&mut material, self.draining as u64);
        append_u64(&mut material, self.frozen as u64);
        append_u64(&mut material, self.capacity_bytes);
        append_u64(&mut material, self.capacity_rejected_writes);
        append_u64(&mut material, self.service_mode_rejected_mutations);
        append_u64(&mut material, self.metadata_dirty as u64);
        append_u64(&mut material, self.metadata_dirty_mutations);
        append_u64(&mut material, self.metadata_checkpoint_interval);
        append_u64(&mut material, self.defer_metadata_checkpoints as u64);
        append_u64(
            &mut material,
            self.deferred_metadata_checkpoint_pending as u64,
        );
        append_u64(&mut material, self.deferred_metadata_checkpoint_requests);
        append_u64(&mut material, self.deferred_metadata_checkpoint_runs);
        append_u64(&mut material, self.trim_flow_rate_bytes_per_sec);
        append_u64(&mut material, self.trim_flow_burst_bytes);
        append_u64(&mut material, self.trim_flow_available_bytes);
        append_u64(&mut material, self.trim_flow_rate_ops_per_sec);
        append_u64(&mut material, self.trim_flow_burst_ops);
        append_u64(&mut material, self.trim_flow_available_ops);
        append_u64(&mut material, self.trim_flow_last_refill_ms);
        append_u64(&mut material, self.trim_flow_throttled_reclaims);
        append_u64(&mut material, self.trim_flow_throttled_ops);
        append_u64(&mut material, self.trim_flow_throttled_bytes);
        append_u64(&mut material, self.partial_reclaim_failures);
        append_string(
            &mut material,
            &self.last_partial_reclaim_failure_status,
        );
        append_u64(&mut material, self.physical_write_count);
        append_u64(&mut material, self.physical_write_bytes);
        append_u64(&mut material, self.physical_discard_ranges);
        append_u64(&mut material, self.physical_discard_bytes);
        append_u64(&mut material, self.compressed_chunk_count);
        append_u64(&mut material, self.compression_saved_bytes);
        append_u64(&mut material, self.ec_reconstructed_reads);
        append_u64(&mut material, self.ec_reconstruction_failures);
        append_u64(&mut material, self.chunk_mutation_control_commit_index);
        append_u64(&mut material, self.chunk_mutation_control_next_log_index);
        append_chunk_mutation_qos_config(&mut material, &self.mutation_qos_config);
        append_chunk_mutation_qos_state(&mut material, &self.mutation_qos_state);
        append_u64(&mut material, self.receipt_retention as u64);
        append_u64(&mut material, self.chunks.len() as u64);
        for (page_id, state) in &self.chunks {
            append_slab_id(&mut material, &page_id.chunk_id);
            append_u64(&mut material, page_id.page_index);
            append_u64(&mut material, state.logical_length);
            append_u64(&mut material, state.write_sequence);
            append_string(&mut material, &state.checksum);
            append_u64(&mut material, state.data.len() as u64);
            material.extend_from_slice(&state.data);
            append_u64(&mut material, state.ec_parity_fragment.len() as u64);
            material.extend_from_slice(&state.ec_parity_fragment);
            append_u64(&mut material, state.discarded_ranges.len() as u64);
            for range in &state.discarded_ranges {
                append_range(&mut material, range);
            }
        }
        append_u64(&mut material, self.write_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.write_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_write_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.discard_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.discard_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_discard_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.batch_write_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in self.batch_write_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_batch_write_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.batch_discard_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in self.batch_discard_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_batch_discard_receipt(&mut material, receipt);
        }
        append_u64(
            &mut material,
            self.state_install_request_receipts.len() as u64,
        );
        for (idempotency_key, receipt) in self.state_install_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_state_install_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.reclaim_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.reclaim_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_reclaim_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.repair_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.repair_request_receipts.iter() {
            append_string(&mut material, idempotency_key);
            append_chunk_repair_receipt(&mut material, receipt);
        }
        etag(&material)
    }

    /// Computes the checksum an unsealed snapshot is missing, so `validate`
    /// accepts it. Paired with the `export_snapshot_unsealed` constructors,
    /// which skip this because it copies every payload byte before hashing.
    pub fn seal_checksum(&mut self) {
        self.checksum = self.checksum_material_hash();
    }

}
