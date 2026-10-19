impl MatrixPageService {
    fn snapshot_cache_stamp_size_hint(&self) -> usize {
        let mut bytes = 1024usize;
        bytes = bytes.saturating_add(self.pending_allocations.len().saturating_mul(96));
        bytes = bytes.saturating_add(self.allocation_request_receipts.len().saturating_mul(96));
        bytes = bytes.saturating_add(
            self.batch_allocation_request_receipts
                .len()
                .saturating_mul(96),
        );
        bytes = bytes.saturating_add(
            self.release_allocation_request_receipts
                .len()
                .saturating_mul(96),
        );
        bytes = bytes.saturating_add(self.write_request_receipts.len().saturating_mul(96));
        bytes = bytes.saturating_add(
            self.batch_write_request_receipts
                .len()
                .saturating_mul(96),
        );
        bytes = bytes.saturating_add(self.discard_request_receipts.len().saturating_mul(96));
        bytes = bytes.saturating_add(
            self.batch_discard_request_receipts
                .len()
                .saturating_mul(96),
        );
        bytes = bytes.saturating_add(self.repair_request_receipts.len().saturating_mul(96));
        bytes = bytes.saturating_add(
            self.range_repair_request_receipts
                .len()
                .saturating_mul(96),
        );
        bytes = bytes.saturating_add(
            self.audit_repair_request_receipts
                .len()
                .saturating_mul(96),
        );
        bytes = bytes.saturating_add(self.chunk_endpoints.len().saturating_mul(96));
        bytes = bytes.saturating_add(self.disabled_chunk_endpoints.len().saturating_mul(24));
        bytes = bytes.saturating_add(self.quarantined_replicas.len().saturating_mul(24));
        bytes = bytes.saturating_add(self.sealed_slabs.len().saturating_mul(48));
        bytes = bytes.saturating_add(self.replicas.len().saturating_mul(128));
        bytes.min(8 * 1024 * 1024)
    }

    fn snapshot_cache_stamp(&self) -> String {
        use std::fmt::Write as _;

        let mut stamp = String::with_capacity(self.snapshot_cache_stamp_size_hint());
        let _ = write!(
            stamp,
            "block:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.page_size,
            self.write_quorum,
            self.read_quorum,
            self.allocator_slab_size,
            self.allocator_next_slab_id,
            self.allocator_next_offset,
            self.receipt_retention,
            self.released_allocation_count,
            self.page_route_control_commit_index,
            self.page_route_control_next_log_index,
            self.cumulative_quorum_write_degraded_chunks,
            self.cumulative_quorum_read_degraded_chunks,
            self.cumulative_quorum_discard_degraded_chunks,
            self.cumulative_replica_fallback_reads,
            self.cumulative_physical_chunk_read_count.get(),
            self.cumulative_coalesced_chunk_read_count.get(),
            self.cumulative_repair_checked_chunks,
            self.cumulative_repair_degraded_chunks,
            self.cumulative_repaired_replicas,
            self.cumulative_failed_repairs,
            self.cumulative_replica_read_repairs,
            self.cumulative_replica_read_repair_failures,
            self.page_read_parallelism,
            self.page_write_parallelism,
            self.page_batch_read_parallelism,
            self.page_batch_write_parallelism,
            self.page_batch_discard_parallelism,
            self.page_repair_parallelism,
            self.page_chunk_fanout_parallelism
        );
        let _ = write!(
            stamp,
            ":routeqoscfg:{:?}:routeqosstate:{:?}",
            self.route_qos_config,
            self.route_qos_state.borrow()
        );
        for key in self.pending_allocations.keys() {
            stamp.push_str("|pa:");
            stamp.push_str(key);
        }
        for key in self.allocation_request_receipts.keys() {
            stamp.push_str("|ar:");
            stamp.push_str(key);
        }
        for key in self.batch_allocation_request_receipts.keys() {
            stamp.push_str("|bar:");
            stamp.push_str(key);
        }
        for key in self.release_allocation_request_receipts.keys() {
            stamp.push_str("|rar:");
            stamp.push_str(key);
        }
        for key in self.write_request_receipts.keys() {
            stamp.push_str("|wr:");
            stamp.push_str(key);
        }
        for key in self.batch_write_request_receipts.keys() {
            stamp.push_str("|bwr:");
            stamp.push_str(key);
        }
        for key in self.discard_request_receipts.keys() {
            stamp.push_str("|dr:");
            stamp.push_str(key);
        }
        for key in self.batch_discard_request_receipts.keys() {
            stamp.push_str("|bdr:");
            stamp.push_str(key);
        }
        for key in self.repair_request_receipts.keys() {
            stamp.push_str("|rr:");
            stamp.push_str(key);
        }
        for key in self.range_repair_request_receipts.keys() {
            stamp.push_str("|rgr:");
            stamp.push_str(key);
        }
        for key in self.audit_repair_request_receipts.keys() {
            stamp.push_str("|aur:");
            stamp.push_str(key);
        }
        for endpoint in &self.chunk_endpoints {
            stamp.push_str("|ep:");
            stamp.push_str(endpoint);
        }
        for index in &self.disabled_chunk_endpoints {
            let _ = write!(stamp, "|de:{index}");
        }
        for index in &self.quarantined_replicas {
            let _ = write!(stamp, "|qr:{index}");
        }
        for slab in &self.sealed_slabs {
            let _ = write!(stamp, "|ss:{}/{}", slab.volume_id, slab.id);
        }
        for replica in &self.replicas {
            stamp.push_str("|rep:");
            stamp.push_str(&replica.snapshot_cache_stamp());
        }
        stamp
    }

    fn export_snapshot_image(&self) -> Result<PageSnapshotExportCache, ObjectError> {
        let stamp = self.snapshot_cache_stamp();
        if let Some(cache) = self.snapshot_export_cache.borrow().as_ref() {
            if cache.stamp == stamp {
                return Ok(cache.clone());
            }
        }
        let bytes = self.export_snapshot().to_bytes()?;
        let cache = PageSnapshotExportCache {
            stamp,
            checksum: etag(&bytes),
            bytes: std::sync::Arc::from(bytes.into_boxed_slice()),
        };
        *self.snapshot_export_cache.borrow_mut() = Some(cache.clone());
        Ok(cache)
    }

    pub fn export_snapshot(&self) -> PageServiceSnapshot {
        let mut snapshot = self.export_snapshot_unsealed();
        snapshot.seal_checksum();
        snapshot
    }

    /// Builds the snapshot without computing any checksum, its replicas'
    /// included. See `MatrixReplicaService::export_snapshot_unsealed`.
    pub fn export_snapshot_unsealed(&self) -> PageServiceSnapshot {
        PageServiceSnapshot {
            page_size: self.page_size,
            write_quorum: self.write_quorum,
            read_quorum: self.read_quorum,
            allocator_slab_size: self.allocator_slab_size,
            allocator_next_slab_id: self.allocator_next_slab_id,
            allocator_next_offset: self.allocator_next_offset,
            receipt_retention: self.receipt_retention,
            pending_allocations: self.pending_allocations.clone(),
            allocation_request_receipts: self.allocation_request_receipts.clone(),
            batch_allocation_request_receipts: self.batch_allocation_request_receipts.clone(),
            release_allocation_request_receipts: self.release_allocation_request_receipts.clone(),
            write_request_receipts: self.write_request_receipts.clone(),
            batch_write_request_receipts: self.batch_write_request_receipts.clone(),
            discard_request_receipts: self.discard_request_receipts.clone(),
            batch_discard_request_receipts: self.batch_discard_request_receipts.clone(),
            repair_request_receipts: self.repair_request_receipts.clone(),
            range_repair_request_receipts: self.range_repair_request_receipts.clone(),
            audit_repair_request_receipts: self.audit_repair_request_receipts.clone(),
            released_allocation_count: self.released_allocation_count,
            quarantined_replicas: self.quarantined_replicas.clone(),
            chunk_endpoints: self.chunk_endpoints.clone(),
            disabled_chunk_endpoints: self.disabled_chunk_endpoints.clone(),
            page_route_control_commit_index: self.page_route_control_commit_index,
            page_route_control_next_log_index: self.page_route_control_next_log_index,
            route_qos_config: self.route_qos_config.clone(),
            route_qos_state: self.route_qos_state(),
            sealed_slabs: self.sealed_slabs.clone(),
            cumulative_quorum_write_degraded_chunks: self.cumulative_quorum_write_degraded_chunks,
            cumulative_quorum_read_degraded_chunks: self.cumulative_quorum_read_degraded_chunks,
            cumulative_quorum_discard_degraded_chunks: self
                .cumulative_quorum_discard_degraded_chunks,
            cumulative_replica_fallback_reads: self.cumulative_replica_fallback_reads,
            cumulative_physical_chunk_read_count: self.cumulative_physical_chunk_read_count.get(),
            cumulative_coalesced_chunk_read_count: self.cumulative_coalesced_chunk_read_count.get(),
            cumulative_repair_checked_chunks: self.cumulative_repair_checked_chunks,
            cumulative_repair_degraded_chunks: self.cumulative_repair_degraded_chunks,
            cumulative_repaired_replicas: self.cumulative_repaired_replicas,
            cumulative_failed_repairs: self.cumulative_failed_repairs,
            cumulative_replica_read_repairs: self.cumulative_replica_read_repairs,
            cumulative_replica_read_repair_failures: self.cumulative_replica_read_repair_failures,
            replicas: self
                .replicas
                .iter()
                .map(MatrixReplicaService::export_snapshot_unsealed)
                .collect(),
            checksum: String::new(),
        }
    }

    pub fn export_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<PageServiceSnapshotChunk, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "block service snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let chunk = export_page_service_snapshot_chunk_slice_with_checksum(
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
        F: FnMut(PageServiceSnapshotChunk) -> Result<(), ObjectError>,
    {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "block service snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut offset = 0;
        loop {
            let chunk = export_page_service_snapshot_chunk_slice_with_checksum(
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
                    "block service snapshot export made no progress".to_string(),
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
    ) -> Result<Vec<PageServiceSnapshotChunk>, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "block service snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut chunks = Vec::with_capacity(snapshot_chunk_vec_capacity(
            image.bytes.len() as u64,
            max_payload_bytes,
        ));
        let mut offset = 0;
        loop {
            let chunk = export_page_service_snapshot_chunk_slice_with_checksum(
                image.bytes.as_ref(),
                &image.checksum,
                offset,
                max_payload_bytes,
            )?;
            offset = offset.saturating_add(chunk.payload.len() as u64);
            let eof = chunk.eof;
            if !eof && chunk.payload.is_empty() {
                return Err(ObjectError::Corruption(
                    "block service snapshot export made no progress".to_string(),
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

    pub fn install_snapshot(&mut self, snapshot: PageServiceSnapshot) -> Result<(), ObjectError> {
        snapshot.validate()?;
        self.snapshot_assembler.reset();
        let restored_replica_count = snapshot.replicas.len();
        let mut replicas = Vec::with_capacity(restored_replica_count);
        for replica_snapshot in snapshot.replicas {
            let mut replica = MatrixReplicaService::new();
            replica.install_snapshot(replica_snapshot)?;
            // The sidecar policy has to match what construction decides, or a restored
            // replica keeps sidecars its peers do not and never agrees with them again --
            // quorum agreement compares whole chunk states. Set after the install so it
            // cannot be undone by what that assigns.
            replica.keep_ec_sidecar = restored_replica_count <= 1;
            replicas.push(replica);
        }
        self.page_size = snapshot.page_size;
        self.write_quorum = snapshot.write_quorum;
        self.read_quorum = snapshot.read_quorum;
        self.allocator_slab_size = snapshot.allocator_slab_size;
        self.allocator_next_slab_id = snapshot.allocator_next_slab_id;
        self.allocator_next_offset = snapshot.allocator_next_offset;
        self.receipt_retention = snapshot.receipt_retention;
        self.pending_allocations = snapshot.pending_allocations;
        self.allocation_request_receipts = snapshot.allocation_request_receipts;
        self.batch_allocation_request_receipts = snapshot.batch_allocation_request_receipts;
        self.release_allocation_request_receipts = snapshot.release_allocation_request_receipts;
        self.write_request_receipts = snapshot.write_request_receipts;
        self.batch_write_request_receipts = snapshot.batch_write_request_receipts;
        self.discard_request_receipts = snapshot.discard_request_receipts;
        self.batch_discard_request_receipts = snapshot.batch_discard_request_receipts;
        self.repair_request_receipts = snapshot.repair_request_receipts;
        self.range_repair_request_receipts = snapshot.range_repair_request_receipts;
        self.audit_repair_request_receipts = snapshot.audit_repair_request_receipts;
        self.released_allocation_count = snapshot.released_allocation_count;
        self.quarantined_replicas = snapshot.quarantined_replicas;
        self.chunk_endpoints = snapshot.chunk_endpoints;
        self.disabled_chunk_endpoints = snapshot.disabled_chunk_endpoints;
        self.page_route_control_commit_index = snapshot.page_route_control_commit_index;
        self.page_route_control_next_log_index = snapshot.page_route_control_next_log_index;
        self.route_qos_config = snapshot.route_qos_config;
        *self.route_qos_state.borrow_mut() = snapshot.route_qos_state;
        self.sealed_slabs = snapshot.sealed_slabs;
        self.cumulative_quorum_write_degraded_chunks =
            snapshot.cumulative_quorum_write_degraded_chunks;
        self.cumulative_quorum_read_degraded_chunks =
            snapshot.cumulative_quorum_read_degraded_chunks;
        self.cumulative_quorum_discard_degraded_chunks =
            snapshot.cumulative_quorum_discard_degraded_chunks;
        self.cumulative_replica_fallback_reads = snapshot.cumulative_replica_fallback_reads;
        self.cumulative_physical_chunk_read_count
            .set(snapshot.cumulative_physical_chunk_read_count);
        self.cumulative_coalesced_chunk_read_count
            .set(snapshot.cumulative_coalesced_chunk_read_count);
        self.cumulative_repair_checked_chunks = snapshot.cumulative_repair_checked_chunks;
        self.cumulative_repair_degraded_chunks = snapshot.cumulative_repair_degraded_chunks;
        self.cumulative_repaired_replicas = snapshot.cumulative_repaired_replicas;
        self.cumulative_failed_repairs = snapshot.cumulative_failed_repairs;
        self.cumulative_replica_read_repairs = snapshot.cumulative_replica_read_repairs;
        self.cumulative_replica_read_repair_failures =
            snapshot.cumulative_replica_read_repair_failures;
        self.replicas = replicas;
        self.active_mutation_batch = None;
        *self.snapshot_export_cache.borrow_mut() = None;
        Ok(())
    }

    pub fn install_snapshot_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<PageServiceSnapshot, ObjectError> {
        let snapshot = PageServiceSnapshot::from_bytes(bytes)?;
        self.install_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub fn install_snapshot_bytes_in_place(&mut self, bytes: &[u8]) -> Result<(), ObjectError> {
        self.install_snapshot(PageServiceSnapshot::from_bytes(bytes)?)
    }

    pub fn install_snapshot_chunks<I>(
        &mut self,
        chunks: I,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = PageServiceSnapshotChunk>,
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
            "block service snapshot chunk stream is empty".to_string(),
        ))
    }

    pub fn export_chunk_replica_snapshot_chunk(
        &self,
        replica_index: usize,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<ReplicaServiceSnapshotChunk, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .export_snapshot_chunk(offset, max_payload_bytes)
    }

    pub fn export_chunk_replica_snapshot_chunk_stream<F>(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
        emit: F,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        F: FnMut(ReplicaServiceSnapshotChunk) -> Result<(), ObjectError>,
    {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .export_snapshot_chunk_stream(max_payload_bytes, emit)
    }

    pub fn export_chunk_replica_snapshot_chunks(
        &self,
        replica_index: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<ReplicaServiceSnapshotChunk>, ObjectError> {
        self.replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .export_snapshot_chunks(max_payload_bytes)
    }

    pub fn install_chunk_replica_snapshot_chunks<I>(
        &mut self,
        replica_index: usize,
        chunks: I,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ReplicaServiceSnapshotChunk>,
    {
        self.replicas
            .get_mut(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .install_snapshot_chunks(chunks)
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

    pub fn chunk_replica_snapshot_install_progress(
        &self,
        replica_index: usize,
    ) -> Result<SnapshotInstallProgress, ObjectError> {
        Ok(self
            .replicas
            .get(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .snapshot_install_progress())
    }

    pub fn abort_chunk_replica_snapshot_install(
        &mut self,
        replica_index: usize,
        expected_stream_id: Option<&str>,
    ) -> Result<SnapshotAbortReport, ObjectError> {
        self.replicas
            .get_mut(replica_index)
            .ok_or_else(|| ObjectError::InvalidArgument("replica index out of range".to_string()))?
            .abort_snapshot_install(expected_stream_id)
    }
}
