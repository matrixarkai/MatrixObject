impl MatrixReplicaService {
    pub fn service_mode_rejected_mutations(&self) -> u64 {
        self.service_mode_rejected_mutations
    }

    pub fn physical_bytes(&self) -> u64 {
        self.pages.values().fold(0u64, |acc, state| {
            acc.saturating_add(state.data.len() as u64)
        })
    }

    pub fn discarded_bytes(&self) -> u64 {
        self.pages.values().fold(0u64, |acc, state| {
            acc.saturating_add(discarded_range_bytes(&merge_range_slice(
                &state.discarded_ranges,
            )))
        })
    }

    pub fn capacity_free_bytes(&self) -> u64 {
        if self.capacity_bytes == 0 {
            u64::MAX
        } else {
            self.capacity_bytes.saturating_sub(self.physical_bytes())
        }
    }

    pub fn chunk_state_fingerprint(&self) -> String {
        let chunk_bytes = self.pages.values().fold(0usize, |sum, state| {
            sum.saturating_add(encoded_string_len(&state.checksum))
                .saturating_add(
                    state
                        .discarded_ranges
                        .len()
                        .saturating_mul(std::mem::size_of::<u64>() * 2),
                )
                .saturating_add(std::mem::size_of::<u64>() * 6)
        });
        let mut material = Vec::with_capacity(
            encoded_string_len("MOSRSCHUNK-FINGERPRINT4")
                + std::mem::size_of::<u64>() * 38
                + chunk_bytes,
        );
        append_string(&mut material, "MOSRSCHUNK-FINGERPRINT4");
        append_u64(&mut material, self.pages.len() as u64);
        append_u64(&mut material, self.capacity_bytes);
        append_u64(&mut material, self.capacity_rejected_writes);
        append_u64(&mut material, self.service_mode_rejected_mutations);
        append_u64(&mut material, self.accepting_writes as u64);
        append_u64(&mut material, self.draining as u64);
        append_u64(&mut material, self.frozen as u64);
        append_u64(&mut material, self.trim_flow_rate_bytes_per_sec);
        append_u64(&mut material, self.trim_flow_burst_bytes);
        append_u64(&mut material, self.trim_flow_throttled_reclaims);
        append_u64(&mut material, self.trim_flow_throttled_ops);
        append_u64(&mut material, self.trim_flow_throttled_bytes);
        append_u64(&mut material, self.partial_reclaim_failures);
        append_string(&mut material, &self.last_partial_reclaim_failure_status);
        append_u64(&mut material, self.mutation_qos_config.window_ms);
        append_u64(
            &mut material,
            self.mutation_qos_config.max_write_ops_per_window,
        );
        append_u64(
            &mut material,
            self.mutation_qos_config.max_write_bytes_per_window,
        );
        append_u64(
            &mut material,
            self.mutation_qos_config.max_discard_ops_per_window,
        );
        append_u64(
            &mut material,
            self.mutation_qos_config.max_discard_bytes_per_window,
        );
        append_u64(&mut material, self.mutation_qos_state.write_ops_in_window);
        append_u64(&mut material, self.mutation_qos_state.write_bytes_in_window);
        append_u64(&mut material, self.mutation_qos_state.discard_ops_in_window);
        append_u64(
            &mut material,
            self.mutation_qos_state.discard_bytes_in_window,
        );
        append_u64(&mut material, self.mutation_qos_state.throttled_ops);
        append_u64(&mut material, self.mutation_qos_state.throttled_bytes);
        append_u64(&mut material, self.physical_write_count);
        append_u64(&mut material, self.physical_write_bytes);
        append_u64(&mut material, self.physical_discard_ranges);
        append_u64(&mut material, self.physical_discard_bytes);
        let (compressed_chunk_count, compression_saved_bytes) = self.compression_stats();
        append_u64(&mut material, compressed_chunk_count);
        append_u64(&mut material, compression_saved_bytes);
        append_u64(&mut material, self.ec_reconstructed_reads());
        append_u64(&mut material, self.ec_reconstruction_failures());
        append_u64(&mut material, self.receipt_retention as u64);
        append_u64(&mut material, self.write_request_receipts.len() as u64);
        append_u64(&mut material, self.discard_request_receipts.len() as u64);
        append_u64(
            &mut material,
            self.batch_write_request_receipts.len() as u64,
        );
        append_u64(
            &mut material,
            self.batch_discard_request_receipts.len() as u64,
        );
        for (page_id, state) in &self.pages {
            append_page_id(&mut material, page_id);
            append_u64(&mut material, state.logical_length);
            append_u64(&mut material, state.write_sequence);
            append_string(&mut material, &state.checksum);
            append_u64(&mut material, state.discarded_ranges.len() as u64);
            for range in &state.discarded_ranges {
                append_range(&mut material, range);
            }
        }
        etag(&material)
    }

    pub fn storage_readiness(
        &self,
        min_capacity_free_bytes: u64,
        min_reclaimable_bytes: u64,
        now_ms: u64,
    ) -> ReplicaStorageReadiness {
        let mut discarded_range_count = 0u64;
        let mut reclaimable_bytes = 0u64;
        let mut trailing_trim_bytes = 0u64;
        let mut fragmented_chunk_count = 0u64;
        let mut fragmented_discarded_bytes = 0u64;
        for state in self.pages.values() {
            let ranges = merge_range_slice(&state.discarded_ranges);
            discarded_range_count = discarded_range_count.saturating_add(ranges.len() as u64);
            let discarded = discarded_range_bytes(&ranges);
            reclaimable_bytes = reclaimable_bytes.saturating_add(discarded);
            trailing_trim_bytes = trailing_trim_bytes
                .saturating_add(trailing_discarded_bytes(state.logical_length, &ranges));
            if ranges.len() > 1 {
                fragmented_chunk_count = fragmented_chunk_count.saturating_add(1);
                fragmented_discarded_bytes = fragmented_discarded_bytes.saturating_add(discarded);
            }
        }
        let physical_bytes = self.physical_bytes();
        let capacity_free_bytes = if self.capacity_bytes == 0 {
            u64::MAX
        } else {
            self.capacity_bytes.saturating_sub(physical_bytes)
        };
        let capacity_pressure =
            self.capacity_bytes > 0 && capacity_free_bytes < min_capacity_free_bytes;
        let reclaimable = reclaimable_bytes > 0
            && (min_reclaimable_bytes == 0 || reclaimable_bytes >= min_reclaimable_bytes);
        let trim_flow_available_bytes = self.estimated_trim_flow_available_bytes(now_ms);
        let trim_flow_available_ops = self.estimated_trim_flow_available_ops(now_ms);
        let trim_flow_limited = self.trim_flow_enabled()
            && reclaimable
            && (trim_flow_available_bytes < reclaimable_bytes.min(min_reclaimable_bytes.max(1))
                || trim_flow_available_ops == 0);
        let writable =
            self.accepting_writes && !self.draining && !self.frozen && !capacity_pressure;
        let readable = true;
        let ready = readable && writable;
        let reason = if ready {
            "ready"
        } else if !self.accepting_writes {
            "not_accepting_writes"
        } else if self.draining {
            "draining"
        } else if self.frozen {
            "frozen"
        } else {
            "capacity_pressure"
        }
        .to_string();
        let cache = self.read_cache_stats();
        let control_proof = self.control_proof();
        ReplicaStorageReadiness {
            readable,
            writable,
            reclaimable,
            ready,
            reason,
            chunk_state_fingerprint: self.chunk_state_fingerprint(),
            accepting_writes: self.accepting_writes,
            draining: self.draining,
            frozen: self.frozen,
            page_count: self.pages.len() as u64,
            physical_bytes,
            capacity_bytes: self.capacity_bytes,
            capacity_free_bytes,
            min_capacity_free_bytes,
            capacity_pressure,
            metadata_dirty: self.metadata_dirty,
            metadata_dirty_mutations: self.metadata_dirty_mutations,
            metadata_checkpoint_interval: self.metadata_checkpoint_interval,
            defer_metadata_checkpoints: self.defer_metadata_checkpoints,
            deferred_metadata_checkpoint_pending: self.deferred_metadata_checkpoint_pending,
            deferred_metadata_checkpoint_requests: self.deferred_metadata_checkpoint_requests,
            deferred_metadata_checkpoint_runs: self.deferred_metadata_checkpoint_runs,
            discarded_range_count,
            reclaimable_bytes,
            trailing_trim_bytes,
            fragmented_chunk_count,
            fragmented_discarded_bytes,
            min_reclaimable_bytes,
            trim_flow_enabled: self.trim_flow_enabled(),
            trim_flow_available_bytes,
            trim_flow_available_ops,
            trim_flow_throttled_ops: self.trim_flow_throttled_ops,
            trim_flow_limited,
            partial_reclaim_failures: self.partial_reclaim_failures,
            last_partial_reclaim_failure_status: self.last_partial_reclaim_failure_status.clone(),
            mutation_qos_window_ms: self.mutation_qos_config.window_ms,
            mutation_qos_write_ops_in_window: self.mutation_qos_state.write_ops_in_window,
            mutation_qos_write_bytes_in_window: self.mutation_qos_state.write_bytes_in_window,
            mutation_qos_discard_ops_in_window: self.mutation_qos_state.discard_ops_in_window,
            mutation_qos_discard_bytes_in_window: self.mutation_qos_state.discard_bytes_in_window,
            mutation_qos_throttled_ops: self.mutation_qos_state.throttled_ops,
            mutation_qos_throttled_bytes: self.mutation_qos_state.throttled_bytes,
            mutation_qos_pressure: self.mutation_qos_pressure(),
            read_cache_entries: cache.entries,
            read_cache_bytes: cache.bytes,
            read_cache_max_bytes: cache.max_bytes,
            read_cache_max_cacheable_read_bytes: cache.max_cacheable_read_bytes,
            read_cache_hits: cache.hits,
            read_cache_misses: cache.misses,
            read_cache_evictions: cache.evictions,
            read_cache_corrupt_pages: cache.corrupt_pages,
            read_cache_corrupt_bytes: cache.corrupt_bytes,
            read_cache_invalidations: cache.invalidations,
            read_cache_invalidated_entries: cache.invalidated_entries,
            read_cache_invalidated_bytes: cache.invalidated_bytes,
            read_cache_clears: cache.clears,
            read_cache_cleared_entries: cache.cleared_entries,
            read_cache_cleared_bytes: cache.cleared_bytes,
            read_cache_direct_fill_pages: cache.direct_fill_pages,
            read_cache_direct_fill_bytes: cache.direct_fill_bytes,
            read_cache_compressed_fill_pages: cache.compressed_fill_pages,
            read_cache_compressed_fill_bytes: cache.compressed_fill_bytes,
            read_cache_ec_fill_pages: cache.ec_fill_pages,
            read_cache_ec_fill_bytes: cache.ec_fill_bytes,
            read_cache_uncacheable_read_pages: cache.uncacheable_read_pages,
            read_cache_uncacheable_read_bytes: cache.uncacheable_read_bytes,
            capacity_rejected_writes: self.capacity_rejected_writes,
            service_mode_rejected_mutations: self.service_mode_rejected_mutations,
            physical_write_count: self.physical_write_count,
            physical_write_bytes: self.physical_write_bytes,
            physical_discard_ranges: self.physical_discard_ranges,
            physical_discard_bytes: self.physical_discard_bytes,
            compressed_chunk_count: self.compressed_chunk_count(),
            compression_saved_bytes: self.compression_saved_bytes(),
            ec_reconstructed_reads: self.ec_reconstructed_reads(),
            ec_reconstruction_failures: self.ec_reconstruction_failures(),
            receipt_retention: self.receipt_retention as u64,
            write_receipt_count: self.write_request_receipts.len() as u64,
            discard_receipt_count: self.discard_request_receipts.len() as u64,
            batch_write_receipt_count: self.batch_write_request_receipts.len() as u64,
            batch_discard_receipt_count: self.batch_discard_request_receipts.len() as u64,
            replicaserver_control_proof_format: control_proof.state_format.clone(),
            replicaserver_control_proof_checksum: control_proof.state_checksum.clone(),
            replicaserver_control_proof_bytes: control_proof.state_bytes,
            control_proof,
        }
    }

}
