impl MatrixChunkService {
    pub fn control_proof(&self) -> ChunkServiceControlProof {
        let snapshot_checksum = self.export_snapshot().checksum;
        let physical_bytes = self.physical_bytes();
        let capacity_free_bytes = if self.capacity_bytes == 0 {
            u64::MAX
        } else {
            self.capacity_bytes.saturating_sub(physical_bytes)
        };
        let capacity_pressure = self.capacity_bytes > 0 && capacity_free_bytes == 0;
        let mut material = Vec::with_capacity(
            encoded_string_len("MOSRSCHUNKCONTROL12")
                + encoded_string_len(&snapshot_checksum)
                + std::mem::size_of::<u64>() * 74,
        );
        append_string(&mut material, "MOSRSCHUNKCONTROL12");
        append_u64(&mut material, self.chunk_mutation_control_commit_index);
        append_u64(&mut material, self.chunk_mutation_control_next_log_index);
        append_u64(&mut material, self.accepting_writes as u64);
        append_u64(&mut material, self.draining as u64);
        append_u64(&mut material, self.frozen as u64);
        append_u64(&mut material, self.capacity_bytes);
        append_u64(&mut material, physical_bytes);
        append_u64(&mut material, capacity_free_bytes);
        append_u64(&mut material, capacity_pressure as u64);
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
        append_u64(&mut material, self.physical_write_count);
        append_u64(&mut material, self.physical_write_bytes);
        append_u64(&mut material, self.physical_discard_ranges);
        append_u64(&mut material, self.physical_discard_bytes);
        let (compressed_chunk_count, compression_saved_bytes) = self.compression_stats();
        append_u64(&mut material, compressed_chunk_count);
        append_u64(&mut material, compression_saved_bytes);
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
        append_u64(&mut material, self.max_read_cache_bytes.get());
        append_u64(&mut material, self.max_cacheable_read_bytes.get());
        append_u64(&mut material, 1);
        append_u64(&mut material, 1);
        append_u64(&mut material, 1);
        append_u64(&mut material, 1);
        append_u64(&mut material, 1);
        append_u64(&mut material, self.trim_flow_rate_bytes_per_sec);
        append_u64(&mut material, self.trim_flow_burst_bytes);
        append_u64(&mut material, self.trim_flow_available_bytes);
        append_u64(&mut material, self.trim_flow_rate_ops_per_sec);
        append_u64(&mut material, self.trim_flow_burst_ops);
        append_u64(&mut material, self.trim_flow_available_ops);
        append_u64(&mut material, self.trim_flow_throttled_ops);
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
        append_u64(&mut material, self.mutation_qos_state.throttled_ops);
        append_u64(&mut material, self.mutation_qos_state.throttled_bytes);
        append_u64(&mut material, self.read_cache_hits.get());
        append_u64(&mut material, self.read_cache_misses.get());
        append_u64(&mut material, self.read_cache_evictions.get());
        append_u64(&mut material, self.read_cache_corrupt_pages.get());
        append_u64(&mut material, self.read_cache_corrupt_bytes.get());
        append_u64(&mut material, self.read_cache_invalidations.get());
        append_u64(&mut material, self.read_cache_invalidated_entries.get());
        append_u64(&mut material, self.read_cache_invalidated_bytes.get());
        append_u64(&mut material, self.read_cache_clears.get());
        append_u64(&mut material, self.read_cache_cleared_entries.get());
        append_u64(&mut material, self.read_cache_cleared_bytes.get());
        append_u64(&mut material, self.read_cache_direct_fill_pages.get());
        append_u64(&mut material, self.read_cache_direct_fill_bytes.get());
        append_u64(&mut material, self.read_cache_compressed_fill_pages.get());
        append_u64(&mut material, self.read_cache_compressed_fill_bytes.get());
        append_u64(&mut material, self.read_cache_ec_fill_pages.get());
        append_u64(&mut material, self.read_cache_ec_fill_bytes.get());
        append_u64(&mut material, self.read_cache_uncacheable_read_pages.get());
        append_u64(&mut material, self.read_cache_uncacheable_read_bytes.get());
        append_u64(&mut material, self.ec_reconstructed_reads());
        append_u64(&mut material, self.ec_reconstruction_failures());
        append_string(&mut material, &snapshot_checksum);
        ChunkServiceControlProof {
            control_receipt: true,
            state_format: "MOSRSCHUNKCONTROL12".to_string(),
            state_bytes: material.len() as u64,
            state_checksum: etag(&material),
            chunk_mutation_control_commit_index: self.chunk_mutation_control_commit_index,
            chunk_mutation_control_next_log_index: self.chunk_mutation_control_next_log_index,
            accepting_writes: self.accepting_writes,
            draining: self.draining,
            frozen: self.frozen,
            capacity_bytes: self.capacity_bytes,
            physical_bytes,
            capacity_free_bytes,
            capacity_pressure,
            capacity_rejected_writes: self.capacity_rejected_writes,
            service_mode_rejected_mutations: self.service_mode_rejected_mutations,
            metadata_dirty: self.metadata_dirty,
            metadata_dirty_mutations: self.metadata_dirty_mutations,
            metadata_checkpoint_interval: self.metadata_checkpoint_interval,
            defer_metadata_checkpoints: self.defer_metadata_checkpoints,
            deferred_metadata_checkpoint_pending: self.deferred_metadata_checkpoint_pending,
            deferred_metadata_checkpoint_requests: self.deferred_metadata_checkpoint_requests,
            deferred_metadata_checkpoint_runs: self.deferred_metadata_checkpoint_runs,
            physical_write_count: self.physical_write_count,
            physical_write_bytes: self.physical_write_bytes,
            physical_discard_ranges: self.physical_discard_ranges,
            physical_discard_bytes: self.physical_discard_bytes,
            compressed_chunk_count: self.compressed_chunk_count(),
            compression_saved_bytes: self.compression_saved_bytes(),
            receipt_retention: self.receipt_retention as u64,
            write_receipt_count: self.write_request_receipts.len() as u64,
            discard_receipt_count: self.discard_request_receipts.len() as u64,
            batch_write_receipt_count: self.batch_write_request_receipts.len() as u64,
            batch_discard_receipt_count: self.batch_discard_request_receipts.len() as u64,
            read_cache_max_bytes: self.max_read_cache_bytes.get(),
            read_cache_max_cacheable_read_bytes: self.max_cacheable_read_bytes.get(),
            read_cache_config_raft_backed: true,
            capacity_config_raft_backed: true,
            service_mode_config_raft_backed: true,
            trim_flow_config_raft_backed: true,
            mutation_qos_config_raft_backed: true,
            trim_flow_rate_bytes_per_sec: self.trim_flow_rate_bytes_per_sec,
            trim_flow_burst_bytes: self.trim_flow_burst_bytes,
            trim_flow_available_bytes: self.trim_flow_available_bytes,
            trim_flow_rate_ops_per_sec: self.trim_flow_rate_ops_per_sec,
            trim_flow_burst_ops: self.trim_flow_burst_ops,
            trim_flow_available_ops: self.trim_flow_available_ops,
            trim_flow_throttled_ops: self.trim_flow_throttled_ops,
            mutation_qos_window_ms: self.mutation_qos_config.window_ms,
            mutation_qos_max_write_ops_per_window: self
                .mutation_qos_config
                .max_write_ops_per_window,
            mutation_qos_max_write_bytes_per_window: self
                .mutation_qos_config
                .max_write_bytes_per_window,
            mutation_qos_max_discard_ops_per_window: self
                .mutation_qos_config
                .max_discard_ops_per_window,
            mutation_qos_max_discard_bytes_per_window: self
                .mutation_qos_config
                .max_discard_bytes_per_window,
            mutation_qos_throttled_ops: self.mutation_qos_state.throttled_ops,
            mutation_qos_throttled_bytes: self.mutation_qos_state.throttled_bytes,
            read_cache_hits: self.read_cache_hits.get(),
            read_cache_misses: self.read_cache_misses.get(),
            read_cache_evictions: self.read_cache_evictions.get(),
            read_cache_corrupt_pages: self.read_cache_corrupt_pages.get(),
            read_cache_corrupt_bytes: self.read_cache_corrupt_bytes.get(),
            read_cache_invalidations: self.read_cache_invalidations.get(),
            read_cache_invalidated_entries: self.read_cache_invalidated_entries.get(),
            read_cache_invalidated_bytes: self.read_cache_invalidated_bytes.get(),
            read_cache_clears: self.read_cache_clears.get(),
            read_cache_cleared_entries: self.read_cache_cleared_entries.get(),
            read_cache_cleared_bytes: self.read_cache_cleared_bytes.get(),
            read_cache_direct_fill_pages: self.read_cache_direct_fill_pages.get(),
            read_cache_direct_fill_bytes: self.read_cache_direct_fill_bytes.get(),
            read_cache_compressed_fill_pages: self.read_cache_compressed_fill_pages.get(),
            read_cache_compressed_fill_bytes: self.read_cache_compressed_fill_bytes.get(),
            read_cache_ec_fill_pages: self.read_cache_ec_fill_pages.get(),
            read_cache_ec_fill_bytes: self.read_cache_ec_fill_bytes.get(),
            read_cache_uncacheable_read_pages: self.read_cache_uncacheable_read_pages.get(),
            read_cache_uncacheable_read_bytes: self.read_cache_uncacheable_read_bytes.get(),
            ec_reconstructed_reads: self.ec_reconstructed_reads(),
            ec_reconstruction_failures: self.ec_reconstruction_failures(),
            snapshot_checksum,
        }
    }

    pub(crate) fn check_expected_control_checksum(
        &self,
        expected_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        let Some(expected_checksum) = expected_checksum else {
            return Ok(());
        };
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk control checksum expectation is empty".to_string(),
            ));
        }
        let actual_checksum = self.control_proof().state_checksum;
        if expected_checksum != actual_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk control checksum mismatch: expected {} got {}",
                expected_checksum, actual_checksum
            )));
        }
        Ok(())
    }

}
