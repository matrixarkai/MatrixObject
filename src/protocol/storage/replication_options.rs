// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataReplicationProofReport {
    pub format: String,
    pub ready: bool,
    pub reason: String,
    pub root_ready: bool,
    pub root_status: String,
    pub root_raft_status_checksum_format: String,
    pub root_raft_status_checksum: String,
    pub metadata_commit_index: u64,
    pub metadata_snapshot_floor_commit_index: u64,
    pub metadata_change_checkpoint_lag: u64,
    pub object_manifest_checkpoint_lag: u64,
    pub object_metadata_checkpoint_lag: u64,
    pub object_deferred_manifest_checkpoints: bool,
    pub object_checkpoint_pressure: bool,
    pub metadata_replication_enabled: bool,
    pub metadata_replica_count: u64,
    pub metadata_replication_quorum: u64,
    pub metadata_replication_acks: u64,
    pub metadata_replication_failures: u64,
    pub metadata_replication_last_status: String,
    pub metadata_read_ready: bool,
    pub metadata_write_ready: bool,
    pub metadata_caught_up: bool,
    pub metadata_quorum_ready: bool,
    pub metadata_state_fingerprint: String,
    pub metadata_route_cache_pressure: bool,
    pub metadata_route_cache_fresh: bool,
    pub metadata_route_topology_checksum: String,
    pub metadata_route_session_checksum: String,
    pub metaserver_route_topology_checksum: String,
    pub metaserver_route_session_checksum: String,
    pub distributed_readiness_checksum: String,
    pub envelope_checksum: String,
}

impl MetadataReplicationProofReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("format", &self.format),
            ("reason", &self.reason),
            ("root_status", &self.root_status),
            (
                "root_raft_status_checksum_format",
                &self.root_raft_status_checksum_format,
            ),
            ("root_raft_status_checksum", &self.root_raft_status_checksum),
            (
                "metadata_replication_last_status",
                &self.metadata_replication_last_status,
            ),
            (
                "metadata_state_fingerprint",
                &self.metadata_state_fingerprint,
            ),
            (
                "metadata_route_topology_checksum",
                &self.metadata_route_topology_checksum,
            ),
            (
                "metadata_route_session_checksum",
                &self.metadata_route_session_checksum,
            ),
            (
                "metaserver_route_topology_checksum",
                &self.metaserver_route_topology_checksum,
            ),
            (
                "metaserver_route_session_checksum",
                &self.metaserver_route_session_checksum,
            ),
            (
                "distributed_readiness_checksum",
                &self.distributed_readiness_checksum,
            ),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("ready", self.ready),
            ("root_ready", self.root_ready),
            (
                "object_deferred_manifest_checkpoints",
                self.object_deferred_manifest_checkpoints,
            ),
            (
                "object_checkpoint_pressure",
                self.object_checkpoint_pressure,
            ),
            (
                "metadata_replication_enabled",
                self.metadata_replication_enabled,
            ),
            ("metadata_read_ready", self.metadata_read_ready),
            ("metadata_write_ready", self.metadata_write_ready),
            ("metadata_caught_up", self.metadata_caught_up),
            ("metadata_quorum_ready", self.metadata_quorum_ready),
            (
                "metadata_route_cache_pressure",
                self.metadata_route_cache_pressure,
            ),
            (
                "metadata_route_cache_fresh",
                self.metadata_route_cache_fresh,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("metadata_commit_index", self.metadata_commit_index),
            (
                "metadata_snapshot_floor_commit_index",
                self.metadata_snapshot_floor_commit_index,
            ),
            (
                "metadata_change_checkpoint_lag",
                self.metadata_change_checkpoint_lag,
            ),
            (
                "object_manifest_checkpoint_lag",
                self.object_manifest_checkpoint_lag,
            ),
            (
                "object_metadata_checkpoint_lag",
                self.object_metadata_checkpoint_lag,
            ),
            ("metadata_replica_count", self.metadata_replica_count),
            (
                "metadata_replication_quorum",
                self.metadata_replication_quorum,
            ),
            ("metadata_replication_acks", self.metadata_replication_acks),
            (
                "metadata_replication_failures",
                self.metadata_replication_failures,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            chunk_size: 64 * 1024 * 1024,
            max_extent_bytes: 4 * 1024 * 1024,
            page_size: 1024 * 1024,
            volume_count: 1,
            persistent_snapshot_path: String::new(),
            persistent_snapshot_flush_on_commit: false,
            persistent_journal_path: String::new(),
            replica_count: 3,
            write_quorum: 2,
            reuse_stale_chunk_space: false,
            compaction_fragment_threshold: 128,
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
        }
    }
}
