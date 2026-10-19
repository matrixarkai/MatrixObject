impl MatrixObjectStore {
    /// Fallible because the payload bytes are no longer held here: they are read
    /// back from the page service, which can fail on a damaged chunk. The
    /// metadata-only export below passes an empty map and stays infallible.
    pub fn export_snapshot(&self) -> Result<StoreSnapshot, ObjectError> {
        let payloads = self.materialize_snapshot_payloads()?;
        Ok(self.export_snapshot_with_materialized_payloads(payloads))
    }

    pub fn export_compact_metadata_snapshot(&self) -> StoreSnapshot {
        self.export_snapshot_with_materialized_payloads(std::collections::BTreeMap::new())
    }

    /// Read every indexed payload back out of the page service.
    fn materialize_snapshot_payloads(
        &self,
    ) -> Result<std::collections::BTreeMap<(SlabId, u64), std::sync::Arc<Vec<u8>>>, ObjectError>
    {
        let mut out = std::collections::BTreeMap::new();
        for ((slab_id, offset), stored) in &self.payloads {
            let (bytes, _) =
                self.chunk_service
                    .read_slab_range(slab_id, *offset, stored.len)?;
            out.insert((slab_id.clone(), *offset), std::sync::Arc::new(bytes));
        }
        Ok(out)
    }

    pub fn snapshot_payload_footprint_bytes(&self) -> u64 {
        self.snapshot_payload_footprint_and_multipart_count().0
    }

    pub fn compact_metadata_snapshot_footprint_bytes(&self) -> u64 {
        self.compact_metadata_checksum_size_hint() as u64
    }

    fn snapshot_payload_footprint_and_multipart_count(&self) -> (u64, u64) {
        let committed_payload_bytes = self
            .payloads
            .values()
            .fold(0u64, |total, payload| total.saturating_add(payload.len));
        self.multipart_uploads
            .values()
            .fold((committed_payload_bytes, 0u64), |(bytes, count), upload| {
                let upload_bytes = upload
                    .part_payloads
                    .values()
                    .fold(0u64, |total, payload| {
                        total.saturating_add(payload.len() as u64)
                    });
                (
                    bytes.saturating_add(upload_bytes),
                    count.saturating_add(upload.part_payloads.len() as u64),
                )
            })
    }

    fn compact_multipart_upload(upload: &MultipartUpload) -> MultipartUpload {
        MultipartUpload {
            upload_id: upload.upload_id.clone(),
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            content_type: upload.content_type.clone(),
            create_idempotency_key: upload.create_idempotency_key.clone(),
            options: upload.options.clone(),
            create_time_ms: upload.create_time_ms,
            update_time_ms: upload.update_time_ms,
            next_part_number: upload.next_part_number,
            total_bytes: upload.total_bytes,
            parts: upload.parts.clone(),
            part_payloads: std::collections::BTreeMap::new(),
        }
    }

    fn export_snapshot_with_materialized_payloads(
        &self,
        payloads: std::collections::BTreeMap<(SlabId, u64), std::sync::Arc<Vec<u8>>>,
    ) -> StoreSnapshot {
        let include_payloads = !payloads.is_empty() || self.payloads.is_empty();
        let multipart_uploads = if include_payloads {
            self.multipart_uploads.clone()
        } else {
            self.multipart_uploads
                .iter()
                .map(|(upload_id, upload)| (upload_id.clone(), Self::compact_multipart_upload(upload)))
                .collect()
        };
        let multipart_abort_receipts = if include_payloads {
            self.multipart_abort_receipts.clone()
        } else {
            self.multipart_abort_receipts
                .iter()
                .map(|(idempotency_key, receipt)| {
                    (
                        idempotency_key.clone(),
                        MultipartAbortReceipt {
                            idempotency_key: receipt.idempotency_key.clone(),
                            upload_id: receipt.upload_id.clone(),
                            request_checksum: receipt.request_checksum.clone(),
                            upload: Self::compact_multipart_upload(&receipt.upload),
                            commit_index: receipt.commit_index,
                            update_time_ms: receipt.update_time_ms,
                        },
                    )
                })
                .collect()
        };
        let mut snapshot = StoreSnapshot {
            options: self.options.clone(),
            buckets: self.buckets.clone(),
            bucket_versioning: self.bucket_versioning.clone(),
            bucket_retention: self.bucket_retention.clone(),
            bucket_quotas: self.bucket_quotas.clone(),
            bucket_lifecycle: self.bucket_lifecycle.clone(),
            bucket_policy_documents: self.bucket_policy_documents.clone(),
            bucket_tags: self.bucket_tags.clone(),
            bucket_cors: self.bucket_cors.clone(),
            bucket_policy_receipts: self.bucket_policy_receipts.clone(),
            bucket_cors_receipts: self.bucket_cors_receipts.clone(),
            objects: self.objects.clone(),
            versions: self.versions.clone(),
            multipart_uploads,
            multipart_complete_receipts: self.multipart_complete_receipts.clone(),
            multipart_abort_receipts,
            object_changes: self.object_changes.clone(),
            object_change_cursors: self.object_change_cursors.clone(),
            next_object_change_sequence: self.next_object_change_sequence,
            slabs: self.slabs.clone(),
            payloads,
            meta_snapshot: self.meta_service.export_snapshot(),
            root_snapshot: self.meta_service.export_placement_snapshot(),
            next_slab_id: self.next_slab_id,
            next_upload_sequence: self.next_upload_sequence,
            logical_written_bytes: self.logical_written_bytes,
            physical_written_bytes: self.physical_written_bytes,
            last_applied_index: self.last_applied_index,
            last_applied_term: self.last_applied_term,
            applied_raft_receipts: self.applied_raft_receipts.clone(),
            raft_voter_node_ids: self.raft_voter_node_ids.clone(),
            raft_healthy_peer_count: self.raft_healthy_peer_count,
            raft_membership_epoch: self.raft_membership_epoch,
            raft_membership_receipts: self.raft_membership_receipts.clone(),
            chunk_read_cache_max_bytes_per_replica: self.chunk_read_cache_max_bytes_per_replica,
            chunk_read_cache_max_cacheable_read_bytes_per_replica: self
                .chunk_read_cache_max_cacheable_read_bytes_per_replica,
            page_write_quorum: self.chunk_service.write_quorum(),
            page_read_quorum: self.chunk_service.read_quorum(),
            replica_endpoints: self.chunk_service.chunk_endpoints().to_vec(),
            page_disabled_chunk_endpoints: self.chunk_service.disabled_chunk_endpoints.clone(),
            snapshot_checksum: String::new(),
        };
        snapshot.snapshot_checksum = snapshot.checksum_material_hash();
        snapshot
    }

    fn snapshot_cache_stamp(&self) -> String {
        format!(
            "store:fingerprint:{}",
            self.state_fingerprint_without_payload_clones()
        )
    }

    fn state_fingerprint_without_payload_clones(&self) -> String {
        use std::fmt::Write as _;

        let committed_payload_bytes = self
            .payloads
            .values()
            .fold(0u64, |total, payload| total.saturating_add(payload.len));
        let (multipart_payload_bytes, multipart_payload_count) = self
            .multipart_uploads
            .values()
            .fold((0u64, 0u64), |(bytes, count), upload| {
                upload.part_payloads.values().fold((bytes, count), |(bytes, count), payload| {
                    (
                        bytes.saturating_add(payload.len() as u64),
                        count.saturating_add(1),
                    )
                })
            });
        let snapshot_payload_bytes = committed_payload_bytes.saturating_add(multipart_payload_bytes);
        let mut material = String::with_capacity(
            64usize
                .saturating_add(self.payloads.len().saturating_mul(96))
                .saturating_add(self.multipart_uploads.len().saturating_mul(64))
                .saturating_add(128),
        );
        material.push_str("store-state-v1|compact:");
        material.push_str(&self.compact_metadata_checksum_without_payload_clones());
        let _ = write!(
            material,
            "|payloads:{}:{}",
            self.payloads.len(),
            snapshot_payload_bytes
        );
        for ((slab_id, offset), payload) in &self.payloads {
            let _ = write!(
                material,
                "|p:{}/{}@{}:{}:",
                slab_id.volume_id,
                slab_id.id,
                offset,
                payload.len
            );
            // The entry holds the digest, not its spelling. This writes the same
            // "fnv1a64:" prefix and the same 16 hex digits the stored string used
            // to carry, so the material is unchanged and the fingerprint keeps
            // its value.
            append_etag_of_digest(&mut material, payload.etag_fnv1a64);
        }
        let _ = write!(material, "|multipart-payloads:{multipart_payload_count}");
        for (upload_id, upload) in &self.multipart_uploads {
            for (part_number, payload) in &upload.part_payloads {
                let _ = write!(
                    material,
                    "|mp:{}#{}:{}:",
                    upload_id,
                    part_number,
                    payload.len()
                );
                append_etag(&mut material, payload);
            }
        }
        etag(material.as_bytes())
    }

    fn compact_metadata_checksum_without_payload_clones(&self) -> String {
        let meta_snapshot = self.meta_service.export_snapshot();
        let root_snapshot = self.meta_service.export_placement_snapshot();
        let mut material = Vec::with_capacity(self.compact_metadata_checksum_size_hint());
        append_u64(&mut material, self.options.slab_size);
        append_u64(&mut material, self.options.max_extent_bytes);
        append_u64(&mut material, self.options.page_size);
        append_u64(&mut material, self.options.volume_count);
        append_u64(&mut material, self.options.replica_count as u64);
        append_u64(&mut material, self.options.write_quorum as u64);
        append_u64(
            &mut material,
            self.options.compaction_fragment_threshold as u64,
        );
        append_u64(
            &mut material,
            self.options.metadata_route_cache_entries as u64,
        );
        append_u64(&mut material, self.options.metadata_route_cache_ttl_ms);
        append_u64(&mut material, self.options.page_read_parallelism as u64);
        append_u64(&mut material, self.options.page_write_parallelism as u64);
        append_u64(
            &mut material,
            self.options.page_batch_read_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.page_batch_write_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.page_batch_discard_parallelism as u64,
        );
        append_u64(&mut material, self.options.page_repair_parallelism as u64);
        append_u64(
            &mut material,
            self.options.replica_fanout_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.local_replica_store_parallelism as u64,
        );
        append_u64(
            &mut material,
            self.options.control_plane_mode.snapshot_code(),
        );
        append_u64(
            &mut material,
            self.options.object_manifest_checkpoint_interval,
        );
        append_u64(
            &mut material,
            self.options.object_metadata_checkpoint_interval,
        );
        append_string(&mut material, &self.options.metaserver_catalog_endpoints);
        append_string(&mut material, &self.options.rootserver_catalog_endpoint);
        append_u64(&mut material, self.next_slab_id);
        append_u64(&mut material, self.logical_written_bytes);
        append_u64(&mut material, self.physical_written_bytes);
        append_u64(&mut material, self.last_applied_index);
        append_u64(&mut material, self.last_applied_term);
        append_u64(&mut material, self.applied_raft_receipts.len() as u64);
        for (index, receipt) in &self.applied_raft_receipts {
            append_u64(&mut material, *index);
            append_raft_apply_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.raft_voter_node_ids.len() as u64);
        for voter in &self.raft_voter_node_ids {
            append_string(&mut material, voter);
        }
        append_u64(&mut material, self.raft_healthy_peer_count);
        append_u64(&mut material, self.raft_membership_epoch);
        append_u64(&mut material, self.raft_membership_receipts.len() as u64);
        for (mutation_id, receipt) in &self.raft_membership_receipts {
            append_string(&mut material, mutation_id);
            append_raft_membership_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.chunk_read_cache_max_bytes_per_replica);
        append_u64(
            &mut material,
            self.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        );
        append_u64(&mut material, self.chunk_service.write_quorum() as u64);
        append_u64(&mut material, self.chunk_service.read_quorum() as u64);
        let replica_endpoints = self.chunk_service.chunk_endpoints();
        append_u64(&mut material, replica_endpoints.len() as u64);
        for endpoint in replica_endpoints {
            append_string(&mut material, endpoint);
        }
        append_u64(
            &mut material,
            self.chunk_service.disabled_chunk_endpoints.len() as u64,
        );
        for index in &self.chunk_service.disabled_chunk_endpoints {
            append_u64(&mut material, *index as u64);
        }
        append_string_set(&mut material, &self.buckets);
        append_string_set(&mut material, &self.bucket_versioning);
        append_u64(&mut material, self.bucket_retention.len() as u64);
        for (bucket, retain_for_ms) in &self.bucket_retention {
            append_string(&mut material, bucket);
            append_u64(&mut material, *retain_for_ms);
        }
        append_u64(&mut material, self.bucket_quotas.len() as u64);
        for (bucket, quota) in &self.bucket_quotas {
            append_string(&mut material, bucket);
            append_u64(&mut material, quota.max_live_bytes);
            append_u64(&mut material, quota.max_live_objects);
        }
        append_u64(&mut material, self.bucket_lifecycle.len() as u64);
        for (bucket, policy) in &self.bucket_lifecycle {
            append_string(&mut material, bucket);
            append_lifecycle_policy(&mut material, policy);
        }
        append_u64(&mut material, self.bucket_policy_documents.len() as u64);
        for (bucket, policy) in &self.bucket_policy_documents {
            append_string(&mut material, bucket);
            append_bucket_policy_document(&mut material, policy);
        }
        append_u64(&mut material, self.bucket_tags.len() as u64);
        for (bucket, tag_set) in &self.bucket_tags {
            append_string(&mut material, bucket);
            append_bucket_tag_set(&mut material, tag_set);
        }
        append_u64(&mut material, self.bucket_cors.len() as u64);
        for (bucket, policy) in &self.bucket_cors {
            append_string(&mut material, bucket);
            append_cors_policy(&mut material, policy);
        }
        append_u64(&mut material, self.bucket_policy_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.bucket_policy_receipts {
            append_string(&mut material, idempotency_key);
            append_bucket_policy_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.bucket_cors_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.bucket_cors_receipts {
            append_string(&mut material, idempotency_key);
            append_bucket_cors_receipt(&mut material, receipt);
        }
        append_string(&mut material, &meta_snapshot.checksum);
        append_string(&mut material, &root_snapshot.checksum);
        append_u64(&mut material, self.next_upload_sequence);
        append_u64(&mut material, self.next_object_change_sequence);
        for (object_id, metadata) in &self.objects {
            append_object_id(&mut material, object_id);
            append_metadata(&mut material, metadata);
        }
        for (object_id, versions) in &self.versions {
            append_object_id(&mut material, object_id);
            append_u64(&mut material, versions.len() as u64);
            for metadata in versions {
                append_metadata(&mut material, metadata);
            }
        }
        for (slab_id, slab) in &self.slabs {
            append_slab_id(&mut material, slab_id);
            append_slab_info(&mut material, slab);
        }
        append_u64(&mut material, 0);
        for (upload_id, upload) in &self.multipart_uploads {
            append_string(&mut material, upload_id);
            append_compact_multipart_upload(&mut material, upload);
        }
        append_u64(&mut material, self.multipart_complete_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.multipart_complete_receipts {
            append_string(&mut material, idempotency_key);
            append_multipart_complete_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.multipart_abort_receipts.len() as u64);
        for (idempotency_key, receipt) in &self.multipart_abort_receipts {
            append_string(&mut material, idempotency_key);
            append_compact_multipart_abort_receipt(&mut material, receipt);
        }
        append_u64(&mut material, self.object_changes.len() as u64);
        for change in &self.object_changes {
            append_object_change_record(&mut material, change);
        }
        append_u64(&mut material, self.object_change_cursors.len() as u64);
        for (consumer_id, cursor) in &self.object_change_cursors {
            append_string(&mut material, consumer_id);
            append_object_change_cursor(&mut material, cursor);
        }
        etag(&material)
    }

    fn compact_metadata_checksum_size_hint(&self) -> usize {
        4096usize
            .saturating_add(self.buckets.len().saturating_mul(32))
            .saturating_add(self.objects.len().saturating_mul(160))
            .saturating_add(self.versions.len().saturating_mul(160))
            .saturating_add(self.slabs.len().saturating_mul(96))
            .saturating_add(self.multipart_uploads.len().saturating_mul(192))
            .saturating_add(self.object_changes.len().saturating_mul(96))
    }

    fn export_snapshot_image(&self) -> Result<StoreSnapshotExportCache, ObjectError> {
        let stamp = self.snapshot_cache_stamp();
        self.export_snapshot_image_for_stamp(stamp)
    }

    fn export_snapshot_image_for_stamp(
        &self,
        stamp: String,
    ) -> Result<StoreSnapshotExportCache, ObjectError> {
        if let Some(cache) = self.snapshot_export_cache.borrow().as_ref() {
            if cache.stamp == stamp {
                return Ok(cache.clone());
            }
        }
        let bytes = self.export_snapshot()?.to_bytes()?;
        let cache = StoreSnapshotExportCache {
            stamp,
            checksum: etag(&bytes),
            bytes: std::sync::Arc::from(bytes.into_boxed_slice()),
        };
        *self.snapshot_export_cache.borrow_mut() = Some(cache.clone());
        Ok(cache)
    }

    pub fn export_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<StoreSnapshotChunk, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "store snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let chunk = export_store_snapshot_chunk_bytes_with_checksum(
            image.bytes.as_ref(),
            &image.checksum,
            self.last_applied_index,
            self.last_applied_term,
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
    ) -> Result<(), ObjectError>
    where
        F: FnMut(StoreSnapshotChunk) -> Result<(), ObjectError>,
    {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "store snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut offset = 0;
        loop {
            let chunk = export_store_snapshot_chunk_bytes_with_checksum(
                image.bytes.as_ref(),
                &image.checksum,
                self.last_applied_index,
                self.last_applied_term,
                offset,
                max_payload_bytes,
            )?;
            let eof = chunk.eof;
            let payload_len = chunk.payload.len() as u64;
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
                return Ok(());
            }
            if payload_len == 0 {
                return Err(ObjectError::Corruption(
                    "store snapshot export made no progress".to_string(),
                ));
            }
            offset = offset.saturating_add(payload_len);
        }
    }

    pub fn export_snapshot_chunks(
        &self,
        max_payload_bytes: usize,
    ) -> Result<Vec<StoreSnapshotChunk>, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "store snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_snapshot_image()?;
        let mut chunks = Vec::with_capacity(snapshot_chunk_vec_capacity(
            image.bytes.len() as u64,
            max_payload_bytes,
        ));
        let mut offset = 0;
        loop {
            let chunk = export_store_snapshot_chunk_bytes_with_checksum(
                image.bytes.as_ref(),
                &image.checksum,
                self.last_applied_index,
                self.last_applied_term,
                offset,
                max_payload_bytes,
            )?;
            let eof = chunk.eof;
            let payload_len = chunk.payload.len() as u64;
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
            if payload_len == 0 {
                return Err(ObjectError::Corruption(
                    "store snapshot export made no progress".to_string(),
                ));
            }
            offset = offset.saturating_add(payload_len);
        }
        Ok(chunks)
    }

    pub fn install_snapshot(&mut self, snapshot: StoreSnapshot) -> Result<(), ObjectError> {
        snapshot.validate()?;
        let persistent_snapshot_path = self.persistent_snapshot_path.clone();
        *self.snapshot_export_cache.borrow_mut() = None;
        *self.persistent_snapshot_durable_stamp.borrow_mut() = None;
        self.snapshot_assembler.reset();
        let mut chunk_service = MatrixChunkService::new_with_parallelism(
            snapshot.options.page_size,
            snapshot.options.replica_count,
            snapshot.page_write_quorum,
            snapshot.options.page_read_parallelism,
            snapshot.options.page_write_parallelism,
            snapshot.options.page_batch_read_parallelism,
            snapshot.options.page_batch_write_parallelism,
            snapshot.options.page_batch_discard_parallelism,
            snapshot.options.page_repair_parallelism,
            snapshot.options.replica_fanout_parallelism,
            snapshot.options.local_replica_store_parallelism,
        )?;
        let mut payload_index = std::collections::BTreeMap::new();
        for ((slab_id, offset), payload) in &snapshot.payloads {
            chunk_service.write_slab_range(slab_id, *offset, payload)?;
            payload_index.insert(
                (slab_id.clone(), *offset),
                StoredPayload {
                    len: payload.len() as u64,
                    etag_fnv1a64: fnv1a64(payload),
                },
            );
        }
        chunk_service.configure_chunk_endpoints(
            snapshot.replica_endpoints.clone(),
            snapshot.page_disabled_chunk_endpoints.clone(),
            snapshot.page_write_quorum,
            snapshot.page_read_quorum,
        )?;
        self.options = snapshot.options;
        self.buckets = snapshot.buckets;
        self.bucket_versioning = snapshot.bucket_versioning;
        self.bucket_retention = snapshot.bucket_retention;
        self.bucket_quotas = snapshot.bucket_quotas;
        self.bucket_lifecycle = snapshot.bucket_lifecycle;
        self.bucket_policy_documents = snapshot.bucket_policy_documents;
        self.bucket_tags = snapshot.bucket_tags;
        self.bucket_cors = snapshot.bucket_cors;
        self.bucket_policy_receipts = snapshot.bucket_policy_receipts;
        self.bucket_cors_receipts = snapshot.bucket_cors_receipts;
        self.objects = snapshot.objects;
        // The pool is derived, so a snapshot does not carry it. Rebuilt in map
        // order, which may intern a different subset past the bound than a
        // replica that replayed the log -- a footprint difference and not a
        // state one, since nothing reads the pool but this sharing.
        self.content_types.clear();
        let mut shared: Vec<(ObjectId, SharedText)> = Vec::new();
        for (object_id, metadata) in self.objects.iter() {
            if let Some(existing) = self.content_types.get(metadata.content_type.as_str()) {
                if !existing.shares_with(&metadata.content_type) {
                    shared.push((object_id.clone(), existing.clone()));
                }
                continue;
            }
            if self.content_types.len() < MAX_SHARED_CONTENT_TYPES {
                self.content_types.insert(metadata.content_type.clone());
            }
        }
        for (object_id, content_type) in shared {
            if let Some(metadata) = self.objects.get_mut(&object_id) {
                metadata.content_type = content_type;
            }
        }
        self.versions = snapshot.versions;
        self.multipart_uploads = snapshot.multipart_uploads;
        self.multipart_complete_receipts = snapshot.multipart_complete_receipts;
        self.multipart_abort_receipts = snapshot.multipart_abort_receipts;
        self.object_changes = snapshot.object_changes;
        self.object_change_cursors = snapshot.object_change_cursors;
        self.next_object_change_sequence = snapshot.next_object_change_sequence;
        self.meta_service.install_snapshot(snapshot.meta_snapshot)?;
        self.meta_service.install_placement_snapshot(snapshot.root_snapshot)?;
        self.slabs = snapshot.slabs;
        self.payloads = payload_index;
        self.chunk_service = chunk_service;
        self.next_slab_id = snapshot.next_slab_id;
        self.next_upload_sequence = snapshot.next_upload_sequence;
        self.logical_written_bytes = snapshot.logical_written_bytes;
        self.physical_written_bytes = snapshot.physical_written_bytes;
        self.last_applied_index = snapshot.last_applied_index;
        self.last_applied_term = snapshot.last_applied_term;
        self.applied_raft_receipts = snapshot.applied_raft_receipts;
        self.raft_voter_node_ids = snapshot.raft_voter_node_ids;
        self.raft_healthy_peer_count = snapshot.raft_healthy_peer_count;
        self.raft_membership_epoch = snapshot.raft_membership_epoch;
        self.raft_membership_receipts = snapshot.raft_membership_receipts;
        self.flush_object_checkpoint_indexes();
        self.configure_chunk_read_cache_with_stream_bypass(
            snapshot.chunk_read_cache_max_bytes_per_replica,
            snapshot.chunk_read_cache_max_cacheable_read_bytes_per_replica,
        )?;
        self.persistent_snapshot_path = persistent_snapshot_path;
        self.options.persistent_snapshot_path = self.persistent_snapshot_path.clone();
        self.persistent_journal_path.clear();
        self.options.persistent_journal_path.clear();
        self.invalidate_metadata_route_cache();
        Ok(())
    }

    pub fn install_snapshot_bytes(&mut self, bytes: &[u8]) -> Result<StoreSnapshot, ObjectError> {
        let snapshot = StoreSnapshot::from_bytes(bytes)?;
        self.install_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub(crate) fn install_snapshot_bytes_in_place(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), ObjectError> {
        self.install_snapshot(StoreSnapshot::from_bytes(bytes)?)
    }

    pub fn flush_persistent_snapshot(&self) -> Result<(), ObjectError> {
        if self.persistent_snapshot_path.is_empty() {
            return Ok(());
        }
        let stamp = self.snapshot_cache_stamp();
        let path = std::path::Path::new(&self.persistent_snapshot_path);
        if persistent_snapshot_durable_stamp_matches(
            path,
            stamp.as_str(),
            self.persistent_snapshot_durable_stamp.borrow().as_ref(),
        ) {
            return Ok(());
        }
        let image = self.export_snapshot_image_for_stamp(stamp.clone())?;
        if let Some(file_stamp) =
            persistent_snapshot_matching_file_stamp(path, image.bytes.as_ref(), &image.checksum)
        {
            *self.persistent_snapshot_durable_stamp.borrow_mut() = Some(
                PersistentSnapshotDurableStamp {
                    state_stamp: stamp,
                    file_stamp,
                },
            );
            *self.snapshot_export_cache.borrow_mut() = None;
            return Ok(());
        }
        sync_persistent_journal(&self.persistent_journal_path)?;
        if let Some(parent) = path.parent() {
            ensure_persistent_snapshot_parent_dir(parent)?;
        }
        let tmp_path = persistent_snapshot_temp_path(path);
        if let Err(err) =
            write_persistent_snapshot_temp(path, &tmp_path, image.bytes.as_ref())
        {
            return Err(ObjectError::Corruption(format!(
                "failed to write persistent snapshot {}: {}",
                tmp_path.display(),
                err
            )));
        }
        if let Err(err) = std::fs::rename(&tmp_path, path) {
            let _ = std::fs::remove_file(&tmp_path);
            forget_persistent_snapshot_file_stamp(path);
            return Err(ObjectError::Corruption(format!(
                "failed to install persistent snapshot {}: {}",
                path.display(),
                err
            )));
        }
        if persistent_snapshot_durable_file_sync_enabled() {
            sync_persistent_snapshot_parent_dir(path)?;
        }
        *self.persistent_snapshot_durable_stamp.borrow_mut() =
            remember_persistent_snapshot_file_stamp(path, image.bytes.as_ref(), &image.checksum)
                .map(|file_stamp| PersistentSnapshotDurableStamp {
                    state_stamp: stamp,
                    file_stamp,
                });
        *self.snapshot_export_cache.borrow_mut() = None;
        Ok(())
    }
}

fn persistent_snapshot_strict_fsync_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MATRIX_OBJECTSTORE_RUST_SNAPSHOT_STRICT_FSYNC")
            .or_else(|_| std::env::var("MATRIX_OBJECTSTORE_STRICT_FSYNC"))
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    })
}

fn append_compact_multipart_upload(out: &mut Vec<u8>, upload: &MultipartUpload) {
    append_string(out, &upload.upload_id);
    append_string(out, &upload.bucket);
    append_string(out, &upload.key);
    append_string(out, &upload.content_type);
    if let Some(idempotency_key) = &upload.create_idempotency_key {
        append_u64(out, 1);
        append_string(out, idempotency_key);
    } else {
        append_u64(out, 0);
    }
    append_put_options(out, &upload.options);
    append_u64(out, upload.create_time_ms);
    append_u64(out, upload.update_time_ms);
    append_u64(out, upload.next_part_number as u64);
    append_u64(out, upload.total_bytes);
    append_u64(out, upload.parts.len() as u64);
    for (part_number, part) in &upload.parts {
        append_u64(out, *part_number as u64);
        append_u64(out, part.part_number as u64);
        append_u64(out, part.length);
        append_string(out, &part.etag);
        append_u64(out, part.update_time_ms);
        if let Some(idempotency_key) = &part.idempotency_key {
            append_u64(out, 1);
            append_string(out, idempotency_key);
        } else {
            append_u64(out, 0);
        }
        append_u64(out, 0);
    }
}

fn append_compact_multipart_abort_receipt(
    out: &mut Vec<u8>,
    receipt: &MultipartAbortReceipt,
) {
    append_string(out, &receipt.idempotency_key);
    append_string(out, &receipt.upload_id);
    append_string(out, &receipt.request_checksum);
    append_compact_multipart_upload(out, &receipt.upload);
    append_u64(out, receipt.commit_index);
    append_u64(out, receipt.update_time_ms);
}

fn persistent_snapshot_full_file_fsync_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("MATRIX_OBJECTSTORE_RUST_SNAPSHOT_FULL_FSYNC")
            .or_else(|_| std::env::var("MATRIX_OBJECTSTORE_FULL_FSYNC"))
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    })
}

fn persistent_snapshot_durable_file_sync_enabled() -> bool {
    persistent_snapshot_strict_fsync_enabled() || persistent_snapshot_full_file_fsync_enabled()
}

fn sync_persistent_snapshot_parent_dir(path: &std::path::Path) -> Result<(), ObjectError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = std::fs::File::open(parent).map_err(|err| {
        ObjectError::Corruption(format!(
            "failed to open persistent snapshot directory {} for sync: {}",
            parent.display(),
            err
        ))
    })?;
    dir.sync_all().map_err(|err| {
        ObjectError::Corruption(format!(
            "failed to sync persistent snapshot directory {}: {}",
            parent.display(),
            err
        ))
    })
}

fn persistent_snapshot_temp_path(path: &std::path::Path) -> std::path::PathBuf {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let pid = std::process::id();
    let mut thread_hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&std::thread::current().id(), &mut thread_hasher);
    let thread_id = std::hash::Hasher::finish(&thread_hasher);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let mut tmp = path.to_string_lossy().into_owned();
    tmp.reserve(80);
    tmp.push_str(".tmp.");
    tmp.push_str(&now_ms.to_string());
    tmp.push('.');
    tmp.push_str(&pid.to_string());
    tmp.push('.');
    tmp.push_str(&thread_id.to_string());
    tmp.push('.');
    tmp.push_str(&id.to_string());
    std::path::PathBuf::from(tmp)
}

fn persistent_snapshot_known_parent_dirs(
) -> &'static std::sync::Mutex<std::collections::BTreeSet<String>> {
    static DIRS: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeSet<String>>> =
        std::sync::OnceLock::new();
    DIRS.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeSet::new()))
}

const MAX_PERSISTENT_SNAPSHOT_PARENT_DIR_CACHE_ENTRIES: usize = 4096;

fn ensure_persistent_snapshot_parent_dir(parent: &std::path::Path) -> Result<(), ObjectError> {
    let key = parent.to_string_lossy().into_owned();
    if persistent_snapshot_known_parent_dirs()
        .lock()
        .map(|dirs| dirs.contains(&key))
        .unwrap_or(false)
    {
        return Ok(());
    }
    std::fs::create_dir_all(parent).map_err(|err| {
        ObjectError::Corruption(format!(
            "failed to create persistent snapshot directory {}: {}",
            parent.display(),
            err
        ))
    })?;
    if let Ok(mut dirs) = persistent_snapshot_known_parent_dirs().lock() {
        dirs.insert(key);
        let excess = dirs
            .len()
            .saturating_sub(MAX_PERSISTENT_SNAPSHOT_PARENT_DIR_CACHE_ENTRIES);
        if excess > 0 {
            let evicted: Vec<_> = dirs.iter().take(excess).cloned().collect();
            for dir in evicted {
                dirs.remove(&dir);
            }
        }
    }
    Ok(())
}

fn forget_persistent_snapshot_parent_dir(parent: &std::path::Path) {
    let key = parent.to_string_lossy().into_owned();
    if let Ok(mut dirs) = persistent_snapshot_known_parent_dirs().lock() {
        dirs.remove(&key);
    }
}

fn write_persistent_snapshot_temp(
    target_path: &std::path::Path,
    tmp_path: &std::path::Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    match write_persistent_snapshot_temp_once(tmp_path, bytes) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = target_path.parent() {
                forget_persistent_snapshot_parent_dir(parent);
                ensure_persistent_snapshot_parent_dir(parent)
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
            }
            write_persistent_snapshot_temp_once(tmp_path, bytes)
        }
        Err(err) => Err(err),
    }
}

fn write_persistent_snapshot_temp_once(
    tmp_path: &std::path::Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(tmp_path)?;
    if let Err(err) = file.write_all(bytes) {
        let _ = std::fs::remove_file(tmp_path);
        return Err(err);
    }
    if let Err(err) = file.flush() {
        let _ = std::fs::remove_file(tmp_path);
        return Err(err);
    }
    if persistent_snapshot_durable_file_sync_enabled() {
        let sync_result = if persistent_snapshot_full_file_fsync_enabled() {
            file.sync_all()
        } else {
            file.sync_data()
        };
        if let Err(err) = sync_result {
            let _ = std::fs::remove_file(tmp_path);
            return Err(err);
        }
    }
    drop(file);
    Ok(())
}

#[derive(Clone, Debug)]
struct PersistentSnapshotFileStamp {
    len: u64,
    modified: std::time::SystemTime,
    snapshot_checksum: String,
}

const MAX_PERSISTENT_SNAPSHOT_FILE_STAMP_CACHE_ENTRIES: usize = 4096;

fn persistent_snapshot_file_stamp_cache(
) -> &'static std::sync::Mutex<std::collections::BTreeMap<String, PersistentSnapshotFileStamp>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<String, PersistentSnapshotFileStamp>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}

fn persistent_snapshot_file_stamp_key(path: &std::path::Path) -> String {
    path.to_string_lossy().into_owned()
}

fn read_persistent_snapshot_file_stamp(
    path: &std::path::Path,
    expected_len: u64,
    expected_checksum: &str,
) -> Option<PersistentSnapshotFileStamp> {
    let (len, modified) = read_persistent_snapshot_file_metadata_stamp(path, expected_len)?;
    Some(PersistentSnapshotFileStamp {
        len,
        modified,
        snapshot_checksum: expected_checksum.to_string(),
    })
}

fn read_persistent_snapshot_durable_stamp(
    path: &std::path::Path,
    state_stamp: String,
    expected_len: u64,
    expected_checksum: &str,
) -> Option<PersistentSnapshotDurableStamp> {
    read_persistent_snapshot_file_stamp(path, expected_len, expected_checksum).map(|file_stamp| {
        PersistentSnapshotDurableStamp {
            state_stamp,
            file_stamp,
        }
    })
}

fn read_persistent_snapshot_file_metadata_stamp(
    path: &std::path::Path,
    expected_len: u64,
) -> Option<(u64, std::time::SystemTime)> {
    let Ok(metadata) = std::fs::metadata(path) else {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    };
    if metadata.len() != expected_len {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    }
    let Ok(modified) = metadata.modified() else {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    };
    Some((expected_len, modified))
}

fn remember_persistent_snapshot_file_stamp(
    path: &std::path::Path,
    expected: &[u8],
    expected_checksum: &str,
) -> Option<PersistentSnapshotFileStamp> {
    let Some(stamp) =
        read_persistent_snapshot_file_stamp(path, expected.len() as u64, expected_checksum)
    else {
        return None;
    };
    if let Ok(mut cache) = persistent_snapshot_file_stamp_cache().lock() {
        remember_persistent_snapshot_file_stamp_locked(&mut cache, path, stamp.clone());
    }
    Some(stamp)
}

fn remember_persistent_snapshot_file_stamp_locked(
    cache: &mut std::collections::BTreeMap<String, PersistentSnapshotFileStamp>,
    path: &std::path::Path,
    stamp: PersistentSnapshotFileStamp,
) {
    cache.insert(persistent_snapshot_file_stamp_key(path), stamp);
    let excess = cache
        .len()
        .saturating_sub(MAX_PERSISTENT_SNAPSHOT_FILE_STAMP_CACHE_ENTRIES);
    if excess > 0 {
        let evicted: Vec<_> = cache.keys().take(excess).cloned().collect();
        for key in evicted {
            cache.remove(&key);
        }
    }
}

fn remember_persistent_snapshot_file_stamp_from_metadata(
    path: &std::path::Path,
    metadata: &std::fs::Metadata,
    expected_checksum: &str,
) -> Option<PersistentSnapshotFileStamp> {
    let Ok(modified) = metadata.modified() else {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    };
    let stamp = PersistentSnapshotFileStamp {
        len: metadata.len(),
        modified,
        snapshot_checksum: expected_checksum.to_string(),
    };
    if let Ok(mut cache) = persistent_snapshot_file_stamp_cache().lock() {
        remember_persistent_snapshot_file_stamp_locked(&mut cache, path, stamp.clone());
    }
    Some(stamp)
}

fn forget_persistent_snapshot_file_stamp(path: &std::path::Path) {
    if let Ok(mut cache) = persistent_snapshot_file_stamp_cache().lock() {
        cache.remove(&persistent_snapshot_file_stamp_key(path));
    }
}

fn matching_persistent_snapshot_file_stamp(
    path: &std::path::Path,
    expected_len: u64,
    expected_checksum: &str,
) -> Option<PersistentSnapshotFileStamp> {
    let key = persistent_snapshot_file_stamp_key(path);
    let cached = persistent_snapshot_file_stamp_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(&key).cloned());
    let Some(cached) = cached else {
        return None;
    };
    let Some((current_len, current_modified)) =
        read_persistent_snapshot_file_metadata_stamp(path, expected_len)
    else {
        return None;
    };
    if cached.len != current_len
        || cached.modified != current_modified
        || cached.snapshot_checksum != expected_checksum
    {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    }
    Some(PersistentSnapshotFileStamp {
        len: current_len,
        modified: current_modified,
        snapshot_checksum: cached.snapshot_checksum,
    })
}

fn persistent_snapshot_durable_stamp_matches(
    path: &std::path::Path,
    state_stamp: &str,
    cached: Option<&PersistentSnapshotDurableStamp>,
) -> bool {
    let Some(cached) = cached else {
        return false;
    };
    if cached.state_stamp != state_stamp {
        return false;
    }
    let Some((current_len, current_modified)) =
        read_persistent_snapshot_file_metadata_stamp(path, cached.file_stamp.len)
    else {
        return false;
    };
    if cached.file_stamp.len != current_len || cached.file_stamp.modified != current_modified {
        forget_persistent_snapshot_file_stamp(path);
        return false;
    }
    true
}

fn persistent_snapshot_matching_file_stamp(
    path: &std::path::Path,
    expected: &[u8],
    expected_checksum: &str,
) -> Option<PersistentSnapshotFileStamp> {
    if let Some(stamp) =
        matching_persistent_snapshot_file_stamp(path, expected.len() as u64, expected_checksum)
    {
        return Some(stamp);
    }
    let Ok(metadata) = std::fs::metadata(path) else {
        return None;
    };
    if metadata.len() != expected.len() as u64 {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    };
    let matches = PERSISTENT_SNAPSHOT_COMPARE_BUFFER.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        let mut consumed = 0usize;
        while consumed < expected.len() {
            let want = buffer.len().min(expected.len() - consumed);
            use std::io::Read;
            if file.read_exact(&mut buffer[..want]).is_err() {
                return false;
            }
            if buffer[..want] != expected[consumed..consumed + want] {
                return false;
            }
            consumed += want;
        }
        true
    });
    if !matches {
        forget_persistent_snapshot_file_stamp(path);
        return None;
    }
    remember_persistent_snapshot_file_stamp_from_metadata(path, &metadata, expected_checksum)
}

thread_local! {
    static PERSISTENT_SNAPSHOT_COMPARE_BUFFER: std::cell::RefCell<[u8; 256 * 1024]> =
        const { std::cell::RefCell::new([0u8; 256 * 1024]) };
}
