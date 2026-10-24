// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MetaSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    full_checksum: String,
    last_applied_index: u64,
}

// The placement snapshot cache, moved here with the state it caches:
// its fields are private, and privacy is by module.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootSnapshotExportCache {
    stamp: String,
    bytes: std::sync::Arc<[u8]>,
    checksum: String,
}

#[derive(Clone, Debug)]
pub struct MatrixMetaService {
    // Placement state, folded in from the service that used to own it. Each
    // name differs from its metadata counterpart on purpose: they count and
    // cache different things, and sharing a name would fuse them.
    pub(crate) nodes: BTreeMap<String, RootNode>,
    pub(crate) placements: BTreeMap<ObjectId, ObjectPlacement>,
    leader_lease: Option<RootLeaderLease>,
    leader_lease_receipts: BTreeMap<String, RootLeaderLeaseReceipt>,
    placement_coordinator_config: MetadataCoordinatorConfig,
    pub(crate) placement_commit_index: u64,
    placement_snapshot_export_cache: RefCell<Option<RootSnapshotExportCache>>,
    root_state_fingerprint_cache: RefCell<Option<String>>,
    /// Ordered by each record's own key, which the record already holds -- see
    /// `KeyedRecord`. A map would keep that key twice.
    pub(crate) records: BTreeSet<KeyedRecord>,
    pub(crate) leases: BTreeMap<String, MetadataLease>,
    lease_receipts: BTreeMap<String, MetadataLeaseReceipt>,
    pub(crate) coordinator_config: MetadataCoordinatorConfig,
    coordinator_config_receipts: BTreeMap<String, MetadataCoordinatorConfigReceipt>,
    pub(crate) coordinator_sessions: BTreeMap<String, CoordinatorSession>,
    coordinator_session_receipts: BTreeMap<String, CoordinatorSessionReceipt>,
    pub(crate) shards: BTreeMap<String, MetadataShard>,
    shard_receipts: BTreeMap<String, MetadataShardReceipt>,
    pub(crate) batch_request_receipts: BTreeMap<String, MetadataBatchReceipt>,
    pub(crate) transactions: BTreeMap<String, MetadataTransaction>,
    transaction_receipts: BTreeMap<String, MetadataTransactionReceipt>,
    change_checkpoints: BTreeMap<String, MetadataChangeCheckpoint>,
    changes: Vec<MetadataChange>,
    pub(crate) next_commit_index: u64,
    next_change_sequence: u64,
    compacted_tombstone_count: u64,
    last_tombstone_compaction_commit_index: u64,
    raft_snapshot_floor_commit_index: u64,
    raft_snapshot_compacted_change_count: u64,
    raft_snapshot_compacted_batch_receipt_count: u64,
    change_log_retention_limit: usize,
    mutation_receipt_retention_limit: usize,
    admission_policy: MetadataAdmissionPolicy,
    qos_config: MetadataQosConfig,
    qos_state: MetadataQosState,
    snapshot_export_cache: RefCell<Option<MetaSnapshotExportCache>>,
    metadata_snapshot_assembler: MetaSnapshotChunkAssembler,
}
