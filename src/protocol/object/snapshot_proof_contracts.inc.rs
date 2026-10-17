#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageSnapshotProofStreamReport {
    pub format: String,
    pub proof_count: u64,
    pub aggregate_checksum: String,
    pub source_control_kind: String,
    pub source_control_checksum: String,
    pub source_read_index: u64,
    pub source_commit_index: u64,
    pub total_payload_bytes: u64,
    pub first_offset: u64,
    pub last_end_offset: u64,
    pub full_snapshot_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageSnapshotInstallSourceProofReport {
    pub format: String,
    pub source_control_kind: String,
    pub expected_snapshot_read_proof_checksum: String,
    pub expected_source_control_checksum: String,
    pub source_read_index: u64,
    pub source_commit_index: u64,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataSnapshotProofStreamReport {
    pub format: String,
    pub proof_count: u64,
    pub aggregate_checksum: String,
    pub requested_read_index: u64,
    pub consensus_read_index: u64,
    pub metadata_commit_index: u64,
    pub total_payload_bytes: u64,
    pub first_offset: u64,
    pub last_end_offset: u64,
    pub snapshot_last_applied_index: u64,
    pub snapshot_term: u64,
    pub full_snapshot_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataSnapshotInstallSourceProofReport {
    pub format: String,
    pub expected_snapshot_read_proof_checksum: String,
    pub source_read_index: u64,
    pub source_commit_index: u64,
    pub source_applied_index: u64,
    pub source_term: u64,
    pub full_snapshot_checksum: String,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetaServerSnapshotProofStreamReport {
    pub format: String,
    pub proof_count: u64,
    pub aggregate_checksum: String,
    pub requested_read_index: u64,
    pub consensus_read_index: u64,
    pub metadata_commit_index: u64,
    pub total_payload_bytes: u64,
    pub first_offset: u64,
    pub last_end_offset: u64,
    pub snapshot_last_applied_index: u64,
    pub full_snapshot_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootSnapshotProofStreamReport {
    pub format: String,
    pub proof_count: u64,
    pub aggregate_checksum: String,
    pub operation: String,
    pub requested_root_read_index: u64,
    pub requested_root_commit_index: u64,
    pub root_applied_index: u64,
    pub root_raft_term: u64,
    pub root_state_fingerprint: String,
    pub total_payload_bytes: u64,
    pub first_offset: u64,
    pub last_end_offset: u64,
    pub full_snapshot_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServiceSnapshotInstallSourceProofReport {
    pub format: String,
    pub service: String,
    pub expected_snapshot_read_proof_checksum: String,
    pub source_state_fingerprint: String,
    pub source_read_index: u64,
    pub source_commit_index: u64,
    pub source_applied_index: u64,
    pub envelope_checksum: String,
}

