impl MatrixChunkService {
    pub fn new(
        page_size: u64,
        replica_count: usize,
        write_quorum: usize,
    ) -> Result<Self, ObjectError> {
        if page_size == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk_size must be positive".to_string(),
            ));
        }
        if replica_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "replica_count must be positive".to_string(),
            ));
        }
        if write_quorum == 0 || write_quorum > replica_count {
            return Err(ObjectError::InvalidArgument(
                "write_quorum must be 1..replica_count".to_string(),
            ));
        }
        Self::new_with_parallelism(
            page_size,
            replica_count,
            write_quorum,
            DEFAULT_PAGE_READ_PARALLELISM,
            DEFAULT_PAGE_WRITE_PARALLELISM,
            DEFAULT_PAGE_BATCH_READ_PARALLELISM,
            DEFAULT_PAGE_BATCH_WRITE_PARALLELISM,
            DEFAULT_PAGE_BATCH_DISCARD_PARALLELISM,
            DEFAULT_PAGE_REPAIR_PARALLELISM,
            DEFAULT_PAGE_CHUNK_FANOUT_PARALLELISM,
            DEFAULT_LOCAL_REPLICA_STORE_PARALLELISM,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_parallelism(
        page_size: u64,
        replica_count: usize,
        write_quorum: usize,
        page_read_parallelism: usize,
        page_write_parallelism: usize,
        page_batch_read_parallelism: usize,
        page_batch_write_parallelism: usize,
        page_batch_discard_parallelism: usize,
        page_repair_parallelism: usize,
        replica_fanout_parallelism: usize,
        local_replica_store_parallelism: usize,
    ) -> Result<Self, ObjectError> {
        validate_storage_parallelism(page_read_parallelism, "block_read_parallelism")?;
        validate_storage_parallelism(page_write_parallelism, "block_write_parallelism")?;
        validate_storage_parallelism(page_batch_read_parallelism, "block_batch_read_parallelism")?;
        validate_storage_parallelism(
            page_batch_write_parallelism,
            "block_batch_write_parallelism",
        )?;
        validate_storage_parallelism(
            page_batch_discard_parallelism,
            "block_batch_discard_parallelism",
        )?;
        validate_storage_parallelism(page_repair_parallelism, "block_repair_parallelism")?;
        validate_storage_parallelism(
            replica_fanout_parallelism,
            "block_chunk_fanout_parallelism",
        )?;
        validate_storage_parallelism(
            local_replica_store_parallelism,
            "local_replica_store_parallelism",
        )?;
        if page_size == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk_size must be positive".to_string(),
            ));
        }
        if replica_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "replica_count must be positive".to_string(),
            ));
        }
        if write_quorum == 0 || write_quorum > replica_count {
            return Err(ObjectError::InvalidArgument(
                "write_quorum must be 1..replica_count".to_string(),
            ));
        }
        Ok(Self {
            page_size,
            write_quorum,
            read_quorum: write_quorum,
            allocator_slab_size: page_size.saturating_mul(1024).max(page_size),
            allocator_next_slab_id: 1,
            allocator_next_offset: 0,
            receipt_retention: DEFAULT_STORAGE_RECEIPT_RETENTION,
            pending_allocations: BTreeMap::new(),
            allocation_request_receipts: BTreeMap::new(),
            batch_allocation_request_receipts: BTreeMap::new(),
            release_allocation_request_receipts: BTreeMap::new(),
            write_request_receipts: BTreeMap::new(),
            batch_write_request_receipts: BTreeMap::new(),
            discard_request_receipts: BTreeMap::new(),
            batch_discard_request_receipts: BTreeMap::new(),
            repair_request_receipts: BTreeMap::new(),
            range_repair_request_receipts: BTreeMap::new(),
            audit_repair_request_receipts: BTreeMap::new(),
            released_allocation_count: 0,
            quarantined_replicas: BTreeSet::new(),
            chunk_endpoints: default_chunk_endpoints(replica_count),
            disabled_chunk_endpoints: BTreeSet::new(),
            chunk_route_control_commit_index: 0,
            chunk_route_control_next_log_index: 1,
            route_qos_config: ChunkRouteQosConfig::default(),
            route_qos_state: RefCell::new(ChunkRouteQosState::default()),
            sealed_slabs: BTreeSet::new(),
            cumulative_quorum_write_degraded_chunks: 0,
            cumulative_quorum_read_degraded_chunks: 0,
            cumulative_quorum_discard_degraded_chunks: 0,
            cumulative_replica_fallback_reads: 0,
            cumulative_physical_chunk_read_count: Cell::new(0),
            cumulative_coalesced_chunk_read_count: Cell::new(0),
            cumulative_cache_prewarm_applied_ranges: 0,
            cumulative_cache_prewarm_failed_ranges: 0,
            cumulative_cache_invalidate_applied_ranges: 0,
            cumulative_cache_invalidate_failed_ranges: 0,
            cumulative_repair_checked_chunks: 0,
            cumulative_repair_degraded_chunks: 0,
            cumulative_repaired_replicas: 0,
            cumulative_failed_repairs: 0,
            cumulative_replica_read_repairs: 0,
            cumulative_replica_read_repair_failures: 0,
            page_read_parallelism,
            page_write_parallelism,
            page_batch_read_parallelism,
            page_batch_write_parallelism,
            page_batch_discard_parallelism,
            page_repair_parallelism,
            replica_fanout_parallelism,
            primary_batch_read_repair_probe_limit: DEFAULT_PRIMARY_BATCH_READ_REPAIR_PROBE_LIMIT,
            local_replica_store_parallelism,
            snapshot_export_cache: RefCell::new(None),
            snapshot_assembler: ChunkServiceSnapshotAssembler::default(),
            active_mutation_batch: None,
            replicas: (0..replica_count)
                .map(|_| {
                    let mut replica = MatrixReplicaService::new();
                    // Uniform across the set, and only kept when this replica is the
                    // only copy of the data.
                    replica.keep_ec_sidecar = replica_count <= 1;
                    replica
                })
                .collect(),
        })
    }
}
