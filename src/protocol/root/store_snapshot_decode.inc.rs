/// Hands back one allocation per distinct piece of key text seen in a decode.
///
/// A snapshot states an object's identity once as the map key and again inside
/// the value, and the change feed states it a third time. Decoded separately
/// those are three allocations of equal characters, so a store restored from a
/// snapshot would hold copies that the store which wrote it did not. Interning
/// is purely about which allocation the characters live in; the characters
/// themselves are untouched.
#[derive(Default)]
struct ObjectKeyPool {
    texts: std::collections::HashMap<String, SharedText>,
}

impl ObjectKeyPool {
    fn intern(&mut self, text: SharedText) -> SharedText {
        match self.texts.get(text.as_str()) {
            Some(shared) => shared.clone(),
            None => {
                self.texts.insert(text.as_str().to_string(), text.clone());
                text
            }
        }
    }

    fn intern_object_id(&mut self, object_id: &mut ObjectId) {
        let bucket = std::mem::take(&mut object_id.bucket);
        let key = std::mem::take(&mut object_id.key);
        object_id.bucket = self.intern(bucket);
        object_id.key = self.intern(key);
    }
}

impl StoreSnapshot {
    pub fn from_bytes(data: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = SnapshotByteCursor::new(data);
        let magic = cursor.read_string()?;
        let snapshot_version = magic
            .strip_prefix("MOSRSSTORE")
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|version| (1..=27).contains(version))
            .ok_or_else(|| {
                ObjectError::Corruption("store snapshot byte magic mismatch".to_string())
            })?;
        if magic != format!("MOSRSSTORE{snapshot_version}") {
            return Err(ObjectError::Corruption(
                "store snapshot byte magic mismatch".to_string(),
            ));
        }
        let mut options = StoreOptions {
            slab_size: cursor.read_u64()?,
            max_extent_bytes: cursor.read_u64()?,
            page_size: cursor.read_u64()?,
            volume_count: cursor.read_u64()?,
            persistent_snapshot_path: String::new(),
            persistent_snapshot_flush_on_commit: false,
            persistent_journal_path: String::new(),
            replica_count: cursor.read_count("store snapshot replica count")?,
            write_quorum: cursor.read_count("store snapshot write quorum")?,
            reuse_stale_slab_space: StoreOptions::default().reuse_stale_slab_space,
        compaction_fragment_threshold: cursor
                .read_count("store snapshot compaction fragment threshold")?,
            object_manifest_checkpoint_interval: 1,
            object_metadata_checkpoint_interval: 1,
            metadata_route_cache_entries: DEFAULT_METADATA_ROUTE_CACHE_ENTRIES,
            metadata_route_cache_ttl_ms: DEFAULT_METADATA_ROUTE_CACHE_TTL_MS,
            page_read_parallelism: DEFAULT_PAGE_READ_PARALLELISM,
            page_write_parallelism: DEFAULT_PAGE_WRITE_PARALLELISM,
            page_batch_read_parallelism: DEFAULT_PAGE_BATCH_READ_PARALLELISM,
            page_batch_write_parallelism: DEFAULT_PAGE_BATCH_WRITE_PARALLELISM,
            page_batch_discard_parallelism: DEFAULT_PAGE_BATCH_DISCARD_PARALLELISM,
            page_repair_parallelism: DEFAULT_PAGE_REPAIR_PARALLELISM,
            replica_fanout_parallelism: DEFAULT_PAGE_CHUNK_FANOUT_PARALLELISM,
            local_replica_store_parallelism: DEFAULT_LOCAL_REPLICA_STORE_PARALLELISM,
            control_plane_mode: ControlPlaneMode::Auto,
            metaserver_catalog_endpoints: String::new(),
            rootserver_catalog_endpoint: String::new(),
        };
        if snapshot_version >= 4 {
            options.metadata_route_cache_entries =
                cursor.read_count("store snapshot metadata route cache entries")?;
            options.metadata_route_cache_ttl_ms = cursor.read_u64()?;
        }
        if snapshot_version >= 5 {
            options.page_read_parallelism =
                cursor.read_count("store snapshot block read parallelism")?;
            options.page_write_parallelism =
                cursor.read_count("store snapshot block write parallelism")?;
            options.page_batch_read_parallelism =
                cursor.read_count("store snapshot block batch read parallelism")?;
            options.page_batch_write_parallelism =
                cursor.read_count("store snapshot block batch write parallelism")?;
            options.page_batch_discard_parallelism =
                cursor.read_count("store snapshot block batch discard parallelism")?;
            options.page_repair_parallelism =
                cursor.read_count("store snapshot block repair parallelism")?;
            options.replica_fanout_parallelism =
                cursor.read_count("store snapshot block chunk fanout parallelism")?;
        }
        if snapshot_version >= 6 {
            options.local_replica_store_parallelism =
                cursor.read_count("store snapshot local replica store parallelism")?;
        }
        if snapshot_version >= 9 {
            options.control_plane_mode = ControlPlaneMode::from_snapshot_code(cursor.read_u64()?)?;
        }
        if snapshot_version >= 16 {
            options.object_manifest_checkpoint_interval = cursor.read_u64()?;
            options.object_metadata_checkpoint_interval = cursor.read_u64()?;
        }
        if snapshot_version >= 18 {
            options.metaserver_catalog_endpoints = cursor.read_string()?;
        }
        if snapshot_version >= 17 {
            options.rootserver_catalog_endpoint = cursor.read_string()?;
        }
        let next_slab_id = cursor.read_u64()?;
        let next_upload_sequence = cursor.read_u64()?;
        let next_object_change_sequence = cursor.read_u64()?;
        let logical_written_bytes = cursor.read_u64()?;
        let physical_written_bytes = cursor.read_u64()?;
        let last_applied_index = cursor.read_u64()?;
        let last_applied_term = cursor.read_u64()?;
        let buckets: BTreeSet<SharedText> =
            read_string_set(&mut cursor, "store snapshot bucket count")?
                .into_iter()
                .map(SharedText::from)
                .collect();
        let bucket_versioning =
            read_string_set(&mut cursor, "store snapshot versioning bucket count")?;
        let mut bucket_retention = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot bucket retention count")? {
            bucket_retention.insert(cursor.read_string()?, cursor.read_u64()?);
        }
        let mut bucket_quotas = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot bucket quota count")? {
            bucket_quotas.insert(
                cursor.read_string()?,
                BucketQuota {
                    max_live_bytes: cursor.read_u64()?,
                    max_live_objects: cursor.read_u64()?,
                },
            );
        }
        let mut bucket_lifecycle = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot lifecycle count")? {
            bucket_lifecycle.insert(
                cursor.read_string()?,
                read_lifecycle_policy(&mut cursor, snapshot_version)?,
            );
        }
        let mut bucket_policy_documents = BTreeMap::new();
        if snapshot_version >= 19 {
            for _ in 0..cursor.read_count("store snapshot bucket policy document count")? {
                bucket_policy_documents.insert(
                    cursor.read_string()?,
                    read_bucket_policy_document(&mut cursor)?,
                );
            }
        }
        let mut bucket_tags = BTreeMap::new();
        if snapshot_version >= 20 {
            for _ in 0..cursor.read_count("store snapshot bucket tag set count")? {
                bucket_tags.insert(cursor.read_string()?, read_bucket_tag_set(&mut cursor)?);
            }
        }
        let mut bucket_cors = BTreeMap::new();
        if snapshot_version >= 12 {
            for _ in 0..cursor.read_count("store snapshot cors count")? {
                bucket_cors.insert(cursor.read_string()?, read_cors_policy(&mut cursor)?);
            }
        }
        let mut bucket_policy_receipts = BTreeMap::new();
        if snapshot_version >= 14 {
            for _ in 0..cursor.read_count("store snapshot bucket policy receipt count")? {
                bucket_policy_receipts.insert(
                    cursor.read_string()?,
                    read_bucket_policy_receipt(&mut cursor, snapshot_version >= 21)?,
                );
            }
        }
        let mut bucket_cors_receipts = BTreeMap::new();
        if snapshot_version >= 13 {
            for _ in 0..cursor.read_count("store snapshot cors receipt count")? {
                bucket_cors_receipts.insert(
                    cursor.read_string()?,
                    read_bucket_cors_receipt(&mut cursor, snapshot_version >= 21)?,
                );
            }
        }
        let mut key_pool = ObjectKeyPool::default();
        let mut objects = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot object count")? {
            let mut object_id = read_object_id(&mut cursor)?;
            key_pool.intern_object_id(&mut object_id);
            let mut metadata = read_metadata(&mut cursor, snapshot_version >= 21)?;
            key_pool.intern_object_id(&mut metadata.object_id);
            objects.insert(object_id, metadata);
        }
        let mut versions = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot version key count")? {
            let mut object_id = read_object_id(&mut cursor)?;
            key_pool.intern_object_id(&mut object_id);
            let mut object_versions = Vec::new();
            for _ in 0..cursor.read_count("store snapshot object version count")? {
                let mut row = read_metadata(&mut cursor, snapshot_version >= 21)?;
                key_pool.intern_object_id(&mut row.object_id);
                object_versions.push(row);
            }
            versions.insert(object_id, object_versions);
        }
        let mut multipart_uploads = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot multipart upload count")? {
            multipart_uploads.insert(
                cursor.read_string()?,
                read_multipart_upload(&mut cursor, snapshot_version)?,
            );
        }
        let mut multipart_complete_receipts = BTreeMap::new();
        if snapshot_version >= 24 {
            for _ in 0..cursor.read_count("store snapshot multipart complete receipt count")? {
                multipart_complete_receipts.insert(
                    cursor.read_string()?,
                    read_multipart_complete_receipt(&mut cursor, snapshot_version >= 21)?,
                );
            }
        }
        let mut multipart_abort_receipts = BTreeMap::new();
        if snapshot_version >= 25 {
            for _ in 0..cursor.read_count("store snapshot multipart abort receipt count")? {
                multipart_abort_receipts.insert(
                    cursor.read_string()?,
                    read_multipart_abort_receipt(&mut cursor, snapshot_version)?,
                );
            }
        }
        let mut object_changes = Vec::new();
        for _ in 0..cursor.read_count("store snapshot object change count")? {
            let mut change = read_object_change_record(&mut cursor)?;
            key_pool.intern_object_id(&mut change.object_id);
            object_changes.push(change);
        }
        let mut object_change_cursors = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot object change cursor count")? {
            object_change_cursors.insert(
                cursor.read_string()?,
                read_object_change_cursor(&mut cursor)?,
            );
        }
        let mut slabs = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot segment count")? {
            slabs.insert(
                read_slab_id(&mut cursor)?,
                read_slab_info(&mut cursor)?,
            );
        }
        let mut payloads = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot payload count")? {
            let slab_id = read_slab_id(&mut cursor)?;
            let offset = cursor.read_u64()?;
            let len = bounded_snapshot_len(cursor.read_u64()?, "store snapshot payload bytes")?;
            let payload = cursor.read_bytes(len)?.to_vec();
            payloads.insert((slab_id, offset), std::sync::Arc::new(payload));
        }
        let meta_len = bounded_snapshot_len(cursor.read_u64()?, "store metadata snapshot bytes")?;
        let meta_snapshot = MetaSnapshot::from_bytes(cursor.read_bytes(meta_len)?)?;
        let root_len = bounded_snapshot_len(cursor.read_u64()?, "store root snapshot bytes")?;
        let root_snapshot = RootSnapshot::from_bytes(cursor.read_bytes(root_len)?)?;
        let mut applied_raft_receipts = BTreeMap::new();
        for _ in 0..cursor.read_count("store snapshot raft receipt count")? {
            applied_raft_receipts.insert(
                cursor.read_u64()?,
                read_raft_apply_receipt(&mut cursor, snapshot_version >= 21)?,
            );
        }
        let (
            raft_voter_node_ids,
            raft_healthy_peer_count,
            raft_membership_epoch,
            raft_membership_receipts,
        ) = if snapshot_version >= 2 {
            let mut voters = Vec::new();
            for _ in 0..cursor.read_count("store snapshot raft voter count")? {
                voters.push(cursor.read_string()?);
            }
            let healthy = cursor.read_u64()?;
            let epoch = cursor.read_u64()?;
            let mut receipts = BTreeMap::new();
            for _ in 0..cursor.read_count("store snapshot raft membership receipt count")? {
                receipts.insert(
                    cursor.read_string()?,
                    read_raft_membership_receipt(&mut cursor)?,
                );
            }
            (voters, healthy, epoch, receipts)
        } else {
            (vec!["local".to_string()], 1, 1, BTreeMap::new())
        };
        let chunk_read_cache_max_bytes_per_replica = if snapshot_version >= 3 {
            cursor.read_u64()?
        } else {
            DEFAULT_CHUNK_READ_CACHE_MAX_BYTES
        };
        let chunk_read_cache_max_cacheable_read_bytes_per_replica = if snapshot_version >= 10 {
            cursor.read_u64()?
        } else {
            DEFAULT_CHUNK_READ_CACHE_MAX_CACHEABLE_READ_BYTES
        };
        let (
            page_write_quorum,
            page_read_quorum,
            replica_endpoints,
            page_disabled_chunk_endpoints,
        ) = if snapshot_version >= 7 {
            let page_write_quorum = cursor.read_count("store snapshot block write quorum")?;
            let page_read_quorum = cursor.read_count("store snapshot block read quorum")?;
            let mut endpoints = Vec::new();
            for _ in 0..cursor.read_count("store snapshot block chunk endpoint count")? {
                endpoints.push(cursor.read_string()?);
            }
            let mut disabled = BTreeSet::new();
            for _ in 0..cursor.read_count("store snapshot block disabled endpoint count")? {
                disabled.insert(cursor.read_count("store snapshot block disabled endpoint index")?);
            }
            (page_write_quorum, page_read_quorum, endpoints, disabled)
        } else {
            (
                options.write_quorum,
                options.write_quorum,
                default_chunk_endpoints(options.replica_count),
                BTreeSet::new(),
            )
        };
        let snapshot_checksum = cursor.read_string()?;
        let checksum_end = cursor.position();
        let envelope_checksum = cursor.read_string()?;
        if cursor.position() != data.len() {
            return Err(ObjectError::Corruption(
                "store snapshot byte payload has trailing data".to_string(),
            ));
        }
        if !etag_matches(&data[..checksum_end], &envelope_checksum) {
            return Err(ObjectError::Corruption(
                "store snapshot byte envelope checksum mismatch".to_string(),
            ));
        }
        let mut snapshot = Self {
            options,
            buckets,
            bucket_versioning,
            bucket_retention,
            bucket_quotas,
            bucket_lifecycle,
            bucket_policy_documents,
            bucket_tags,
            bucket_cors,
            bucket_policy_receipts,
            bucket_cors_receipts,
            objects,
            versions,
            multipart_uploads,
            multipart_complete_receipts,
            multipart_abort_receipts,
            object_changes,
            object_change_cursors,
            next_object_change_sequence,
            slabs,
            payloads,
            meta_snapshot,
            root_snapshot,
            next_slab_id,
            next_upload_sequence,
            logical_written_bytes,
            physical_written_bytes,
            last_applied_index,
            last_applied_term,
            applied_raft_receipts,
            raft_voter_node_ids,
            raft_healthy_peer_count,
            raft_membership_epoch,
            raft_membership_receipts,
            chunk_read_cache_max_bytes_per_replica,
            chunk_read_cache_max_cacheable_read_bytes_per_replica,
            page_write_quorum,
            page_read_quorum,
            replica_endpoints,
            page_disabled_chunk_endpoints,
            snapshot_checksum,
        };
        if snapshot_version < 7 {
            snapshot.snapshot_checksum = snapshot.checksum_material_hash();
        }
        snapshot.validate()?;
        Ok(snapshot)
    }
}
