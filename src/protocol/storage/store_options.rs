#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreOptions {
    pub chunk_size: u64,
    pub max_extent_bytes: u64,
    pub page_size: u64,
    pub volume_count: u64,
    pub persistent_snapshot_path: String,
    pub persistent_snapshot_flush_on_commit: bool,
    pub persistent_journal_path: String,
    pub replica_count: usize,
    pub write_quorum: usize,
    /// Take the space a superseded object leaves instead of growing the chunk.
    ///
    /// Off by default: reclaiming immediately means an overwrite stops
    /// accumulating stale bytes for the collector, which changes what
    /// `gc_report` reports.
    pub reuse_stale_chunk_space: bool,
    pub compaction_fragment_threshold: usize,
    pub object_manifest_checkpoint_interval: u64,
    pub object_metadata_checkpoint_interval: u64,
    pub metadata_route_cache_entries: usize,
    pub metadata_route_cache_ttl_ms: u64,
    pub page_read_parallelism: usize,
    pub page_write_parallelism: usize,
    pub page_batch_read_parallelism: usize,
    pub page_batch_write_parallelism: usize,
    pub page_batch_discard_parallelism: usize,
    pub page_repair_parallelism: usize,
    pub replica_fanout_parallelism: usize,
    pub local_replica_store_parallelism: usize,
    pub control_plane_mode: ControlPlaneMode,
    pub metaserver_catalog_endpoints: String,
    pub rootserver_catalog_endpoint: String,
}

