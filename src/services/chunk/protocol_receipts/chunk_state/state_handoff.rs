#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaStateHandoff {
    pub page_id: PageId,
    pub state: PageState,
    pub payload_checksum: String,
    pub state_checksum: String,
    pub handoff_token: String,
    pub install_id: String,
    pub proof_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkLocalStateHandoffEnvelope {
    pub envelope_format: String,
    pub page_id: PageId,
    pub max_payload_bytes: u64,
    pub request_checksum: String,
    pub payload_bytes: u64,
    pub payload_checksum: String,
    pub state_checksum: String,
    pub write_sequence: u64,
    pub discard_range_count: u64,
    pub handoff_token: String,
    pub handoff_state_checksum: String,
    pub install_id: String,
    pub handoff_proof_version: u64,
    pub budget_limited: bool,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkLocalStateHandoff {
    pub handoff: ReplicaStateHandoff,
    pub envelope: PreparedChunkLocalStateHandoffEnvelope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkStateHandoffEnvelope {
    pub envelope_format: String,
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub length: u64,
    pub source_replica_index: usize,
    pub max_payload_bytes: u64,
    pub request_checksum: String,
    pub payload_bytes: u64,
    pub payload_checksum: String,
    pub handoff_count: u64,
    pub budget_limited: bool,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub first_handoff_chunk_id: Option<PageId>,
    pub first_handoff_state_checksum: String,
    pub first_handoff_payload_bytes: u64,
    pub handoff_state_checksums: Vec<String>,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedChunkStateHandoffEnvelope {
    pub envelope: PreparedChunkStateHandoffEnvelope,
    pub proof: StorageReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaStateHandoffApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub target_replica_index: usize,
    pub handoff_count: u64,
    pub payload_bytes: u64,
    pub payload_checksum: String,
    pub stats: RepairStats,
    pub metadata: ObjectMetadata,
}
