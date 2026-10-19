#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReplicaAuditItem {
    pub chunk_id: PageId,
    pub replica_index: usize,
    pub status: String,
    pub logical_length: u64,
    pub write_sequence: u64,
    pub checksum: String,
    pub discarded_range_count: u64,
    pub frozen: bool,
    pub quarantined: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkReplicaAuditChunk {
    pub items: Vec<ChunkReplicaAuditItem>,
    pub marker: Option<PageId>,
    pub next_marker: Option<PageId>,
    pub scanned_chunks: u64,
    pub healthy_chunks: u64,
    pub degraded_chunks: u64,
    pub missing_replicas: u64,
    pub corrupt_replicas: u64,
    pub divergent_replicas: u64,
    pub quarantined_replicas: u64,
    pub frozen_replicas: u64,
    pub payload_bytes: u64,
    pub budget_limited: bool,
    pub has_more: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkReplicaAuditRepairReport {
    pub audit: ChunkReplicaAuditChunk,
    pub candidate_chunks: u64,
    pub repaired_chunks: u64,
    pub unrepaired_chunks: u64,
    pub stats: RepairStats,
    pub repair_needed: bool,
    pub repair_can_apply: bool,
    pub repair_reason: String,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}
