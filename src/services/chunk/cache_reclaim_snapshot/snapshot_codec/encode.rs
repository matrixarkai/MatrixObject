impl ChunkServiceSnapshot {
    pub(crate) fn encoded_size_hint(&self) -> usize {
        const U64: usize = std::mem::size_of::<u64>();
        let mut bytes = encoded_string_len("MOSRSCHUNK22")
            .saturating_add(U64 * 38)
            .saturating_add(encoded_string_len(&self.last_partial_reclaim_failure_status))
            .saturating_add(encoded_string_len(&self.checksum))
            .saturating_add(U64 * 8);
        for state in self.chunks.values() {
            bytes = bytes
                .saturating_add(U64 * 7)
                .saturating_add(encoded_string_len(&state.checksum))
                .saturating_add(state.data.len())
                .saturating_add(U64)
                .saturating_add(state.ec_parity_fragment.len())
                .saturating_add(U64 * 2 * state.discarded_ranges.len());
        }
        for (key, receipt) in self.write_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_id_encoded_len())
                .saturating_add(U64 * 2)
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(encoded_string_len(&receipt.payload_checksum))
                .saturating_add(encoded_string_len(&receipt.idempotency_key));
        }
        for (key, receipt) in self.discard_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_id_encoded_len())
                .saturating_add(U64 * 2)
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(encoded_string_len(&receipt.idempotency_key));
        }
        for (key, receipt) in self.batch_write_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_id_encoded_len())
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(encoded_string_len(&receipt.payload_checksum))
                .saturating_add(encoded_string_len(&receipt.idempotency_key))
                .saturating_add(chunk_write_batch_stats_encoded_len());
        }
        for (key, receipt) in self.batch_discard_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_id_encoded_len())
                .saturating_add(encoded_string_len(&receipt.request_checksum))
                .saturating_add(encoded_string_len(&receipt.idempotency_key))
                .saturating_add(chunk_discard_batch_stats_encoded_len());
        }
        for (key, receipt) in self.state_install_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_state_install_receipt_encoded_len(receipt));
        }
        for (key, receipt) in self.reclaim_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_reclaim_receipt_encoded_len(receipt));
        }
        for (key, receipt) in self.repair_request_receipts.iter() {
            bytes = bytes
                .saturating_add(encoded_string_len(key))
                .saturating_add(chunk_repair_receipt_encoded_len(receipt));
        }
        bytes.saturating_add(128)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.encoded_size_hint());
        self.append_to_bytes(&mut out);
        Ok(out)
    }

    pub(crate) fn append_to_bytes(&self, mut out: &mut Vec<u8>) {
        let snapshot_start = out.len();
        append_string(&mut out, "MOSRSCHUNK22");
        append_u64(&mut out, self.accepting_writes as u64);
        append_u64(&mut out, self.draining as u64);
        append_u64(&mut out, self.frozen as u64);
        append_u64(&mut out, self.capacity_bytes);
        append_u64(&mut out, self.capacity_rejected_writes);
        append_u64(&mut out, self.service_mode_rejected_mutations);
        append_u64(&mut out, self.metadata_dirty as u64);
        append_u64(&mut out, self.metadata_dirty_mutations);
        append_u64(&mut out, self.metadata_checkpoint_interval);
        append_u64(&mut out, self.defer_metadata_checkpoints as u64);
        append_u64(&mut out, self.deferred_metadata_checkpoint_pending as u64);
        append_u64(&mut out, self.deferred_metadata_checkpoint_requests);
        append_u64(&mut out, self.deferred_metadata_checkpoint_runs);
        append_u64(&mut out, self.trim_flow_rate_bytes_per_sec);
        append_u64(&mut out, self.trim_flow_burst_bytes);
        append_u64(&mut out, self.trim_flow_available_bytes);
        append_u64(&mut out, self.trim_flow_rate_ops_per_sec);
        append_u64(&mut out, self.trim_flow_burst_ops);
        append_u64(&mut out, self.trim_flow_available_ops);
        append_u64(&mut out, self.trim_flow_last_refill_ms);
        append_u64(&mut out, self.trim_flow_throttled_reclaims);
        append_u64(&mut out, self.trim_flow_throttled_ops);
        append_u64(&mut out, self.trim_flow_throttled_bytes);
        append_u64(&mut out, self.partial_reclaim_failures);
        append_string(&mut out, &self.last_partial_reclaim_failure_status);
        append_u64(&mut out, self.physical_write_count);
        append_u64(&mut out, self.physical_write_bytes);
        append_u64(&mut out, self.physical_discard_ranges);
        append_u64(&mut out, self.physical_discard_bytes);
        append_u64(&mut out, self.compressed_chunk_count);
        append_u64(&mut out, self.compression_saved_bytes);
        append_u64(&mut out, self.ec_reconstructed_reads);
        append_u64(&mut out, self.ec_reconstruction_failures);
        append_u64(&mut out, self.chunk_mutation_control_commit_index);
        append_u64(&mut out, self.chunk_mutation_control_next_log_index);
        append_chunk_mutation_qos_config(&mut out, &self.mutation_qos_config);
        append_chunk_mutation_qos_state(&mut out, &self.mutation_qos_state);
        append_u64(&mut out, self.receipt_retention as u64);
        append_u64(&mut out, self.chunks.len() as u64);
        for (chunk_id, state) in &self.chunks {
            append_slab_id(&mut out, &chunk_id.slab_id);
            append_u64(&mut out, chunk_id.chunk_index);
            append_u64(&mut out, state.logical_length);
            append_u64(&mut out, state.write_sequence);
            append_string(&mut out, &state.checksum);
            append_u64(&mut out, state.data.len() as u64);
            out.extend_from_slice(&state.data);
            append_u64(&mut out, state.ec_parity_fragment.len() as u64);
            out.extend_from_slice(&state.ec_parity_fragment);
            append_u64(&mut out, state.discarded_ranges.len() as u64);
            for range in &state.discarded_ranges {
                append_range(&mut out, range);
            }
        }
        append_u64(&mut out, self.write_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.write_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_write_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.discard_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.discard_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_discard_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.batch_write_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.batch_write_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_batch_write_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.batch_discard_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.batch_discard_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_batch_discard_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.state_install_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.state_install_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_state_install_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.reclaim_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.reclaim_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_reclaim_receipt(&mut out, receipt);
        }
        append_u64(&mut out, self.repair_request_receipts.len() as u64);
        for (idempotency_key, receipt) in self.repair_request_receipts.iter() {
            append_string(&mut out, idempotency_key);
            append_chunk_repair_receipt(&mut out, receipt);
        }
        append_string(&mut out, &self.checksum);
        let envelope_checksum = etag(&out[snapshot_start..]);
        append_string(&mut out, &envelope_checksum);
    }
}
