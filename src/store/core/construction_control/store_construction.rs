// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn new(options: StoreOptions) -> Result<Self, ObjectError> {
        if options.chunk_size == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk_size must be positive".to_string(),
            ));
        }
        // AN EXTENT'S OFFSET AND LENGTH ARE 32-BIT, and both are bounded by this, so a
        // chunk at or above 4 GiB could produce one that does not fit. Refused here rather
        // than at the extent, because a store that accepted the setting and then failed
        // every write would have taken the configuration and not honoured it.
        if options.chunk_size > u32::MAX as u64 {
            return Err(ObjectError::InvalidArgument(
                "chunk_size must be under 4 GiB, which is what an extent's offset holds"
                    .to_string(),
            ));
        }
        if options.max_extent_bytes == 0 || options.max_extent_bytes > options.chunk_size {
            return Err(ObjectError::InvalidArgument(
                "max_extent_bytes must be 1..chunk_size".to_string(),
            ));
        }
        if options.volume_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "volume_count must be positive".to_string(),
            ));
        }
        // A CHUNK'S VOLUME IS 32-BIT, and a chunk's volume is `(id - 1) % volume_count`, so
        // a count above that bound could mint one that does not fit. Refused here for the
        // same reason the chunk_size ceiling above is refused here: a store that took the
        // setting and then failed the first write to reach a high volume would have accepted
        // the configuration without honouring it.
        if options.volume_count > u64::from(u32::MAX) {
            return Err(ObjectError::InvalidArgument(
                "volume_count must be under 2^32, which is what a chunk's volume holds"
                    .to_string(),
            ));
        }
        // NO MODE READS A ROOT CATALOG. Refused here rather than ignored, for the reason the
        // chunk_size ceiling above is refused here: a store that accepted the setting and
        // then answered from the metaserver anyway would have taken the configuration and
        // not honoured it.
        if !options.rootserver_catalog_endpoint.trim().is_empty() {
            return Err(ObjectError::InvalidArgument(
                "rootserver_catalog_endpoint cannot be set: this store's control plane is the \
                 metaserver, so nothing would read it"
                    .to_string(),
            ));
        }
        if options.page_size == 0 || options.page_size > options.chunk_size {
            return Err(ObjectError::InvalidArgument(
                "page_size must be 1..chunk_size".to_string(),
            ));
        }
        if options.replica_count == 0
            || options.write_quorum == 0
            || options.write_quorum > options.replica_count
        {
            return Err(ObjectError::InvalidArgument(
                "replica_count/write_quorum are invalid".to_string(),
            ));
        }
        if options.object_manifest_checkpoint_interval == 0 {
            return Err(ObjectError::InvalidArgument(
                "object_manifest_checkpoint_interval must be positive".to_string(),
            ));
        }
        if options.object_metadata_checkpoint_interval == 0 {
            return Err(ObjectError::InvalidArgument(
                "object_metadata_checkpoint_interval must be positive".to_string(),
            ));
        }
        if !options.metaserver_catalog_endpoints.is_empty()
            && options.metaserver_catalog_endpoints.trim().is_empty()
        {
            return Err(ObjectError::InvalidArgument(
                "metaserver_catalog_endpoints must not be empty".to_string(),
            ));
        }
        parse_catalog_endpoint_list(non_empty_endpoint(Some(
            options.metaserver_catalog_endpoints.as_str(),
        )))?;
        if !options.rootserver_catalog_endpoint.is_empty()
            && options.rootserver_catalog_endpoint.trim().is_empty()
        {
            return Err(ObjectError::InvalidArgument(
                "rootserver_catalog_endpoint must not be empty".to_string(),
            ));
        }
        if options.metadata_route_cache_entries == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata_route_cache_entries must be positive".to_string(),
            ));
        }
        if options.metadata_route_cache_ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata_route_cache_ttl_ms must be positive".to_string(),
            ));
        }
        validate_storage_parallelism(options.page_read_parallelism, "block_read_parallelism")?;
        validate_storage_parallelism(options.page_write_parallelism, "block_write_parallelism")?;
        validate_storage_parallelism(
            options.page_batch_read_parallelism,
            "block_batch_read_parallelism",
        )?;
        validate_storage_parallelism(
            options.page_batch_write_parallelism,
            "block_batch_write_parallelism",
        )?;
        validate_storage_parallelism(
            options.page_batch_discard_parallelism,
            "block_batch_discard_parallelism",
        )?;
        validate_storage_parallelism(options.page_repair_parallelism, "block_repair_parallelism")?;
        validate_storage_parallelism(
            options.replica_fanout_parallelism,
            "block_chunk_fanout_parallelism",
        )?;
        validate_storage_parallelism(
            options.local_replica_store_parallelism,
            "local_replica_store_parallelism",
        )?;
        let persistent_snapshot_path = options.persistent_snapshot_path.clone();
        let persistent_journal_path = options.persistent_journal_path.clone();
        if !persistent_snapshot_path.is_empty() && !persistent_journal_path.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "persistent_snapshot_path and persistent_journal_path are mutually exclusive"
                    .to_string(),
            ));
        }
        if !persistent_snapshot_path.is_empty() {
            match std::fs::read(&persistent_snapshot_path) {
                Ok(bytes) => {
                    let mut store = Self::new(StoreOptions {
                        persistent_snapshot_path: String::new(),
                        persistent_journal_path: String::new(),
                        ..options.clone()
                    })?;
                    store.install_snapshot_bytes_in_place(&bytes)?;
                    store.options.persistent_snapshot_path = persistent_snapshot_path.clone();
                    store.persistent_snapshot_path = persistent_snapshot_path.clone();
                    let state_stamp = store.snapshot_cache_stamp();
                    let snapshot_checksum = etag(&bytes);
                    *store.persistent_snapshot_durable_stamp.borrow_mut() =
                        read_persistent_snapshot_durable_stamp(
                            std::path::Path::new(&persistent_snapshot_path),
                            state_stamp,
                            bytes.len() as u64,
                            &snapshot_checksum,
                        );
                    return Ok(store);
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(ObjectError::Corruption(format!(
                        "failed to read persistent snapshot {}: {}",
                        persistent_snapshot_path, err
                    )));
                }
            }
        }
        let mut chunk_service = MatrixChunkService::new_with_parallelism(
            options.page_size,
            options.replica_count,
            options.write_quorum,
            options.page_read_parallelism,
            options.page_write_parallelism,
            options.page_batch_read_parallelism,
            options.page_batch_write_parallelism,
            options.page_batch_discard_parallelism,
            options.page_repair_parallelism,
            options.replica_fanout_parallelism,
            options.local_replica_store_parallelism,
        )?;
        // BEFORE ANYTHING IS WRITTEN, which is the only time the layout can be set: it
        // decides where a byte lives, so a service that already holds pages refuses it.
        chunk_service.configure_stripe_layout(options.stripe_layout()?)?;
        let mut store = Self {
            persistent_snapshot_path,
            persistent_journal_path: persistent_journal_path.clone(),
            options,
            buckets: BTreeSet::new(),
            content_types: BTreeSet::new(),
            bucket_versioning: BTreeSet::new(),
            objects: BTreeMap::new(),
            versions: BTreeMap::new(),
            bucket_retention: BTreeMap::new(),
            bucket_quotas: BTreeMap::new(),
            bucket_lifecycle: BTreeMap::new(),
            bucket_policy_documents: BTreeMap::new(),
            bucket_tags: BTreeMap::new(),
            bucket_cors: BTreeMap::new(),
            bucket_policy_receipts: BTreeMap::new(),
            bucket_cors_receipts: BTreeMap::new(),
            multipart_uploads: BTreeMap::new(),
            multipart_complete_receipts: BTreeMap::new(),
            multipart_abort_receipts: BTreeMap::new(),
            object_changes: Vec::new(),
            object_change_cursors: BTreeMap::new(),
            next_object_change_sequence: 1,
            meta_service: MatrixMetaService::new(),
            metadata_route_cache: BTreeMap::new(),
            metadata_route_shared: BTreeMap::new(),
            metadata_route_checksums: BTreeMap::new(),
            metadata_route_cache_hits: 0,
            metadata_route_cache_misses: 0,
            metadata_route_cache_invalidations: 0,
            metadata_route_cache_expired_invalidations: 0,
            metadata_route_cache_capacity_evictions: 0,
            metadata_route_cache_sequence: 0,
            chunks: BTreeMap::new(),
            payloads: PayloadIndex::default(),
            chunk_service,
            next_chunk_id: 1,
            compacting_chunk: None,
            next_upload_sequence: 1,
            logical_written_bytes: 0,
            physical_written_bytes: 0,
            last_object_manifest_checkpoint_index: 0,
            last_object_metadata_checkpoint_index: 0,
            last_applied_index: 0,
            last_applied_term: 0,
            applied_raft_receipts: BTreeMap::new(),
            raft_voter_node_ids: vec!["local".to_string()],
            raft_healthy_peer_count: 1,
            raft_membership_epoch: 1,
            raft_membership_receipts: BTreeMap::new(),
            consensus_read_refreshes: Cell::new(0),
            consensus_read_cache_hits: Cell::new(0),
            consensus_read_last_status: RefCell::new("init".to_string()),
            chunk_read_cache_max_bytes_per_replica: DEFAULT_CHUNK_READ_CACHE_MAX_BYTES,
            chunk_read_cache_max_cacheable_read_bytes_per_replica:
                DEFAULT_CHUNK_READ_CACHE_MAX_CACHEABLE_READ_BYTES,
            snapshot_export_cache: RefCell::new(None),
            persistent_snapshot_durable_stamp: RefCell::new(None),
            snapshot_assembler: StoreSnapshotChunkAssembler::default(),
        };
        if !persistent_journal_path.is_empty() {
            store.persistent_journal_path.clear();
            store.options.persistent_journal_path.clear();
            store.replay_persistent_journal(&persistent_journal_path)?;
            store.persistent_journal_path = persistent_journal_path.clone();
            store.options.persistent_journal_path = persistent_journal_path;
        }
        Ok(store)
    }

}
