impl MatrixChunkService {
    pub(crate) fn snapshot_cache_stamp_material(&self) -> String {
        use std::fmt::Write as _;

        let mut stamp = String::with_capacity(
            512usize.saturating_add(self.snapshot_cache_stamp_growth_hint()),
        );
        let _ = write!(
            stamp,
            "chunk:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.receipt_retention,
            self.accepting_writes,
            self.draining,
            self.frozen,
            self.capacity_bytes,
            self.capacity_rejected_writes,
            self.service_mode_rejected_mutations,
            self.metadata_dirty,
            self.metadata_dirty_mutations,
            self.metadata_checkpoint_interval,
            self.defer_metadata_checkpoints,
            self.deferred_metadata_checkpoint_pending,
            self.deferred_metadata_checkpoint_requests,
            self.deferred_metadata_checkpoint_runs,
            self.trim_flow_rate_bytes_per_sec,
            self.trim_flow_burst_bytes,
            self.trim_flow_available_bytes,
            self.trim_flow_rate_ops_per_sec,
            self.trim_flow_burst_ops,
            self.trim_flow_available_ops,
            self.trim_flow_last_refill_ms,
            self.trim_flow_throttled_reclaims,
            self.trim_flow_throttled_ops,
            self.trim_flow_throttled_bytes,
            self.partial_reclaim_failures,
            self.last_partial_reclaim_failure_status,
            self.chunk_mutation_control_commit_index,
        );
        let _ = write!(
            stamp,
            ":{}:{}:{}:{}:{}:{}:{}:{}",
            self.physical_write_count,
            self.physical_write_bytes,
            self.physical_discard_ranges,
            self.physical_discard_bytes,
            self.ec_reconstructed_reads.get(),
            self.ec_reconstruction_failures.get(),
            self.chunk_mutation_control_next_log_index,
            self.mutation_qos_state.window_start_ms,
        );
        let _ = write!(
            stamp,
            ":{}:{}:{}:{}:{}:{}",
            self.mutation_qos_state.write_ops_in_window,
            self.mutation_qos_state.write_bytes_in_window,
            self.mutation_qos_state.discard_ops_in_window,
            self.mutation_qos_state.discard_bytes_in_window,
            self.mutation_qos_state.throttled_ops,
            self.mutation_qos_state.throttled_bytes,
        );
        let _ = write!(stamp, ":qoscfg:{:?}", self.mutation_qos_config);
        for key in self.write_request_receipts.keys() {
            stamp.push_str("|wr:");
            stamp.push_str(key);
        }
        for key in self.discard_request_receipts.keys() {
            stamp.push_str("|dr:");
            stamp.push_str(key);
        }
        for key in self.batch_write_request_receipts.keys() {
            stamp.push_str("|bwr:");
            stamp.push_str(key);
        }
        for key in self.batch_discard_request_receipts.keys() {
            stamp.push_str("|bdr:");
            stamp.push_str(key);
        }
        for key in self.state_install_request_receipts.keys() {
            stamp.push_str("|si:");
            stamp.push_str(key);
        }
        for key in self.reclaim_request_receipts.keys() {
            stamp.push_str("|rc:");
            stamp.push_str(key);
        }
        for key in self.repair_request_receipts.keys() {
            stamp.push_str("|rp:");
            stamp.push_str(key);
        }
        for (chunk_id, state) in &self.chunks {
            let _ = write!(
                stamp,
                "|c:{}/{}:{}:{}:{}:{}:{}:{}",
                chunk_id.slab_id.volume_id,
                chunk_id.slab_id.id,
                chunk_id.chunk_index,
                state.logical_length,
                state.write_sequence,
                state.checksum,
                state.data.len(),
                state.discarded_ranges.len()
            );
            if !state.ec_parity_fragment.is_empty() {
                let _ = write!(stamp, ":ps{}:", state.ec_parity_fragment.len());
                append_etag(&mut stamp, &state.ec_parity_fragment);
            }
            for range in &state.discarded_ranges {
                let _ = write!(stamp, ":r{}+{}", range.offset, range.length);
            }
        }
        stamp
    }

    pub(crate) fn snapshot_cache_stamp(&self) -> String {
        format!("chunk:snapshot-cache:{}", etag(self.snapshot_cache_stamp_material().as_bytes()))
    }

    fn snapshot_cache_stamp_growth_hint(&self) -> usize {
        let receipt_key_bytes = self
            .write_request_receipts
            .keys()
            .chain(self.discard_request_receipts.keys())
            .chain(self.batch_write_request_receipts.keys())
            .chain(self.batch_discard_request_receipts.keys())
            .chain(self.state_install_request_receipts.keys())
            .chain(self.reclaim_request_receipts.keys())
            .chain(self.repair_request_receipts.keys())
            .fold(0usize, |total, key| total.saturating_add(key.len() + 8));
        let chunk_bytes = self.chunks.values().fold(0usize, |total, state| {
            total
                .saturating_add(96)
                .saturating_add(state.checksum.len())
                .saturating_add(state.discarded_ranges.len().saturating_mul(32))
                .saturating_add(if state.ec_parity_fragment.is_empty() { 0 } else { 48 })
        });
        receipt_key_bytes.saturating_add(chunk_bytes)
    }

    fn export_snapshot_image(&self) -> Result<ChunkSnapshotExportCache, ObjectError> {
        let stamp = self.snapshot_cache_stamp();
        if let Some(cache) = self.snapshot_export_cache.borrow().as_ref() {
            if cache.stamp == stamp {
                return Ok(cache.clone());
            }
        }
        let bytes = self.export_snapshot().to_bytes()?;
        let cache = ChunkSnapshotExportCache {
            stamp,
            checksum: etag(&bytes),
            bytes: std::sync::Arc::from(bytes.into_boxed_slice()),
        };
        *self.snapshot_export_cache.borrow_mut() = Some(cache.clone());
        Ok(cache)
    }

    pub fn export_snapshot(&self) -> ChunkServiceSnapshot {
        let mut snapshot = self.export_snapshot_unsealed();
        let (compressed_chunk_count, compression_saved_bytes) = self.compression_stats();
        snapshot.compressed_chunk_count = compressed_chunk_count;
        snapshot.compression_saved_bytes = compression_saved_bytes;
        snapshot.seal_checksum();
        snapshot
    }

    /// Builds the snapshot without computing its checksum.
    ///
    /// `checksum_material_hash` copies every chunk payload into one buffer
    /// before hashing it, so it costs O(resident bytes) per call. Callers that
    /// only keep the snapshot in memory -- the mutation-batch rollback copy --
    /// take this and call `seal_checksum` on the rare path that installs it.
    pub fn export_snapshot_unsealed(&self) -> ChunkServiceSnapshot {
        ChunkServiceSnapshot {
            chunks: self.chunks.clone(),
            receipt_retention: self.receipt_retention,
            write_request_receipts: self.write_request_receipts.clone(),
            discard_request_receipts: self.discard_request_receipts.clone(),
            batch_write_request_receipts: self.batch_write_request_receipts.clone(),
            batch_discard_request_receipts: self.batch_discard_request_receipts.clone(),
            state_install_request_receipts: self.state_install_request_receipts.clone(),
            reclaim_request_receipts: self.reclaim_request_receipts.clone(),
            repair_request_receipts: self.repair_request_receipts.clone(),
            accepting_writes: self.accepting_writes,
            draining: self.draining,
            frozen: self.frozen,
            capacity_bytes: self.capacity_bytes,
            capacity_rejected_writes: self.capacity_rejected_writes,
            service_mode_rejected_mutations: self.service_mode_rejected_mutations,
            metadata_dirty: self.metadata_dirty,
            metadata_dirty_mutations: self.metadata_dirty_mutations,
            metadata_checkpoint_interval: self.metadata_checkpoint_interval,
            defer_metadata_checkpoints: self.defer_metadata_checkpoints,
            deferred_metadata_checkpoint_pending: self.deferred_metadata_checkpoint_pending,
            deferred_metadata_checkpoint_requests: self.deferred_metadata_checkpoint_requests,
            deferred_metadata_checkpoint_runs: self.deferred_metadata_checkpoint_runs,
            trim_flow_rate_bytes_per_sec: self.trim_flow_rate_bytes_per_sec,
            trim_flow_burst_bytes: self.trim_flow_burst_bytes,
            trim_flow_available_bytes: self.trim_flow_available_bytes,
            trim_flow_rate_ops_per_sec: self.trim_flow_rate_ops_per_sec,
            trim_flow_burst_ops: self.trim_flow_burst_ops,
            trim_flow_available_ops: self.trim_flow_available_ops,
            trim_flow_last_refill_ms: self.trim_flow_last_refill_ms,
            trim_flow_throttled_reclaims: self.trim_flow_throttled_reclaims,
            trim_flow_throttled_ops: self.trim_flow_throttled_ops,
            trim_flow_throttled_bytes: self.trim_flow_throttled_bytes,
            partial_reclaim_failures: self.partial_reclaim_failures,
            last_partial_reclaim_failure_status: self.last_partial_reclaim_failure_status.clone(),
            physical_write_count: self.physical_write_count,
            physical_write_bytes: self.physical_write_bytes,
            physical_discard_ranges: self.physical_discard_ranges,
            physical_discard_bytes: self.physical_discard_bytes,
            // Left unaccounted: deriving these walks every payload byte, and
            // this snapshot is captured once per mutation and never reported.
            // `validate` skips its own comparison while both are zero, and
            // `export_snapshot` fills them in for the snapshots that are read.
            compressed_chunk_count: 0,
            compression_saved_bytes: 0,
            ec_reconstructed_reads: self.ec_reconstructed_reads(),
            ec_reconstruction_failures: self.ec_reconstruction_failures(),
            chunk_mutation_control_commit_index: self.chunk_mutation_control_commit_index,
            chunk_mutation_control_next_log_index: self.chunk_mutation_control_next_log_index,
            mutation_qos_config: self.mutation_qos_config.clone(),
            mutation_qos_state: self.mutation_qos_state.clone(),
            checksum: String::new(),
        }
    }

    pub fn export_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<ChunkServiceSnapshotChunk, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let chunk = export_chunk_service_snapshot_chunk_slice_with_checksum(
            image.bytes.as_ref(),
            &image.checksum,
            offset,
            max_payload_bytes,
        )?;
        if chunk.eof {
            let mut cache = self.snapshot_export_cache.borrow_mut();
            if cache
                .as_ref()
                .map(|cached| cached.stamp == image.stamp)
                .unwrap_or(false)
            {
                *cache = None;
            }
        }
        Ok(chunk)
    }

    pub fn export_snapshot_chunk_stream<F>(
        &self,
        max_payload_bytes: usize,
        mut emit: F,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        F: FnMut(ChunkServiceSnapshotChunk) -> Result<(), ObjectError>,
    {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut offset = 0;
        loop {
            let chunk = export_chunk_service_snapshot_chunk_slice_with_checksum(
                image.bytes.as_ref(),
                &image.checksum,
                offset,
                max_payload_bytes,
            )?;
            offset = offset.saturating_add(chunk.payload.len() as u64);
            let eof = chunk.eof;
            let progress = chunk.install_progress();
            if !eof && chunk.payload.is_empty() {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot export made no progress".to_string(),
                ));
            }
            emit(chunk)?;
            if eof {
                let mut cache = self.snapshot_export_cache.borrow_mut();
                if cache
                    .as_ref()
                    .map(|cached| cached.stamp == image.stamp)
                    .unwrap_or(false)
                {
                    *cache = None;
                }
                return Ok(progress);
            }
        }
    }

    pub fn export_snapshot_chunks(
        &self,
        max_payload_bytes: usize,
    ) -> Result<Vec<ChunkServiceSnapshotChunk>, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk service snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut chunks = Vec::with_capacity(snapshot_chunk_vec_capacity(
            image.bytes.len() as u64,
            max_payload_bytes,
        ));
        let mut offset = 0;
        loop {
            let chunk = export_chunk_service_snapshot_chunk_slice_with_checksum(
                image.bytes.as_ref(),
                &image.checksum,
                offset,
                max_payload_bytes,
            )?;
            offset = offset.saturating_add(chunk.payload.len() as u64);
            let eof = chunk.eof;
            if !eof && chunk.payload.is_empty() {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot export made no progress".to_string(),
                ));
            }
            chunks.push(chunk);
            if eof {
                let mut cache = self.snapshot_export_cache.borrow_mut();
                if cache
                    .as_ref()
                    .map(|cached| cached.stamp == image.stamp)
                    .unwrap_or(false)
                {
                    *cache = None;
                }
                break;
            }
        }
        Ok(chunks)
    }

    pub fn install_snapshot(&mut self, snapshot: ChunkServiceSnapshot) -> Result<(), ObjectError> {
        snapshot.validate()?;
        self.snapshot_assembler.reset();
        if snapshot.capacity_bytes > 0 {
            let snapshot_bytes = snapshot.chunks.values().fold(0u64, |acc, state| {
                acc.saturating_add(state.data.len() as u64)
            });
            if snapshot_bytes > snapshot.capacity_bytes {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot exceeds configured capacity".to_string(),
                ));
            }
        }
        self.chunks = snapshot.chunks;
        self.receipt_retention = snapshot.receipt_retention;
        self.write_request_receipts = snapshot.write_request_receipts;
        self.discard_request_receipts = snapshot.discard_request_receipts;
        self.batch_write_request_receipts = snapshot.batch_write_request_receipts;
        self.batch_discard_request_receipts = snapshot.batch_discard_request_receipts;
        self.state_install_request_receipts = snapshot.state_install_request_receipts;
        self.reclaim_request_receipts = snapshot.reclaim_request_receipts;
        self.repair_request_receipts = snapshot.repair_request_receipts;
        self.accepting_writes = snapshot.accepting_writes;
        self.draining = snapshot.draining;
        self.frozen = snapshot.frozen;
        self.capacity_bytes = snapshot.capacity_bytes;
        self.capacity_rejected_writes = snapshot.capacity_rejected_writes;
        self.service_mode_rejected_mutations = snapshot.service_mode_rejected_mutations;
        self.metadata_dirty = snapshot.metadata_dirty;
        self.metadata_dirty_mutations = snapshot.metadata_dirty_mutations;
        self.metadata_checkpoint_interval = snapshot.metadata_checkpoint_interval;
        self.defer_metadata_checkpoints = snapshot.defer_metadata_checkpoints;
        self.deferred_metadata_checkpoint_pending = snapshot.deferred_metadata_checkpoint_pending;
        self.deferred_metadata_checkpoint_requests = snapshot.deferred_metadata_checkpoint_requests;
        self.deferred_metadata_checkpoint_runs = snapshot.deferred_metadata_checkpoint_runs;
        self.trim_flow_rate_bytes_per_sec = snapshot.trim_flow_rate_bytes_per_sec;
        self.trim_flow_burst_bytes = snapshot.trim_flow_burst_bytes;
        self.trim_flow_available_bytes = snapshot.trim_flow_available_bytes;
        self.trim_flow_rate_ops_per_sec = snapshot.trim_flow_rate_ops_per_sec;
        self.trim_flow_burst_ops = snapshot.trim_flow_burst_ops;
        self.trim_flow_available_ops = snapshot.trim_flow_available_ops;
        self.trim_flow_last_refill_ms = snapshot.trim_flow_last_refill_ms;
        self.trim_flow_throttled_reclaims = snapshot.trim_flow_throttled_reclaims;
        self.trim_flow_throttled_ops = snapshot.trim_flow_throttled_ops;
        self.trim_flow_throttled_bytes = snapshot.trim_flow_throttled_bytes;
        self.partial_reclaim_failures = snapshot.partial_reclaim_failures;
        self.last_partial_reclaim_failure_status =
            snapshot.last_partial_reclaim_failure_status.clone();
        self.physical_write_count = snapshot.physical_write_count;
        self.physical_write_bytes = snapshot.physical_write_bytes;
        self.physical_discard_ranges = snapshot.physical_discard_ranges;
        self.physical_discard_bytes = snapshot.physical_discard_bytes;
        self.ec_reconstructed_reads
            .set(snapshot.ec_reconstructed_reads);
        self.ec_reconstruction_failures
            .set(snapshot.ec_reconstruction_failures);
        self.chunk_mutation_control_commit_index = snapshot.chunk_mutation_control_commit_index;
        self.chunk_mutation_control_next_log_index = snapshot.chunk_mutation_control_next_log_index;
        self.mutation_qos_config = snapshot.mutation_qos_config;
        self.mutation_qos_state = snapshot.mutation_qos_state;
        self.active_mutation_batch = None;
        self.clear_read_cache();
        *self.snapshot_export_cache.borrow_mut() = None;
        Ok(())
    }

    pub fn install_snapshot_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<ChunkServiceSnapshot, ObjectError> {
        let snapshot = ChunkServiceSnapshot::from_bytes(bytes)?;
        self.install_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub fn install_snapshot_bytes_in_place(&mut self, bytes: &[u8]) -> Result<(), ObjectError> {
        self.install_snapshot(ChunkServiceSnapshot::from_bytes(bytes)?)
    }

    pub fn install_snapshot_chunks<I>(
        &mut self,
        chunks: I,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ChunkServiceSnapshotChunk>,
    {
        let mut progress = self.snapshot_assembler.install_progress();
        for chunk in chunks {
            if let Some(assembled) = self.snapshot_assembler.push_chunk_with_checksum(chunk)? {
                self.install_snapshot_bytes_in_place(&assembled.bytes)?;
                return Ok(snapshot_install_progress(
                    true,
                    false,
                    assembled.bytes.len() as u64,
                    assembled.bytes.len() as u64,
                    assembled.full_checksum,
                    0,
                    0,
                ));
            }
            progress = self.snapshot_assembler.install_progress();
        }
        if progress.stream_active {
            return Ok(progress);
        }
        Err(ObjectError::InvalidArgument(
            "chunk service snapshot chunk stream is empty".to_string(),
        ))
    }

    pub fn snapshot_install_progress(&self) -> SnapshotInstallProgress {
        self.snapshot_assembler.install_progress()
    }

    pub fn abort_snapshot_install(
        &mut self,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.snapshot_assembler.abort_install(expected_stream_id)
    }

}
