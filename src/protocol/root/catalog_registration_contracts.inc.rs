#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCatalogChunkRegistration {
    pub node: RootNode,
    pub readiness: ReplicaStorageReadiness,
    pub replica_index: usize,
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub catalog_endpoint_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCatalogPageRegistration {
    pub node: RootNode,
    pub readiness: ChunkStorageReadiness,
    pub allocator_shard_index: u64,
    pub allocator_shard_count: u64,
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub catalog_endpoint_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCatalogObjectStoreRegistration {
    pub node: RootNode,
    pub catalog_source: String,
    pub catalog_endpoint: String,
    pub catalog_endpoint_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenRootCatalogRegistration<T> {
    pub registration: T,
    pub raft_receipts: Vec<ObjectMetadata>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCatalogLivenessReceipt {
    pub format: String,
    pub checksum: String,
    pub method: String,
    pub node_id: String,
    pub service_kind: String,
    pub heartbeat_ms: u64,
    pub term: u64,
    pub commit_index: u64,
    pub read_index: u64,
    pub node_count: u64,
}

