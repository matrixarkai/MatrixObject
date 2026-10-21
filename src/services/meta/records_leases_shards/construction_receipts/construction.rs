// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            placements: BTreeMap::new(),
            leader_lease: None,
            leader_lease_receipts: BTreeMap::new(),
            placement_coordinator_config: MetadataCoordinatorConfig {
                namespace_path: "/matrixobjectstore/rootserver".to_string(),
                ..MetadataCoordinatorConfig::default()
            },
            placement_commit_index: 1,
            placement_snapshot_export_cache: RefCell::new(None),
            root_state_fingerprint_cache: RefCell::new(None),
            records: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_receipts: BTreeMap::new(),
            coordinator_config: MetadataCoordinatorConfig::default(),
            coordinator_config_receipts: BTreeMap::new(),
            coordinator_sessions: BTreeMap::new(),
            coordinator_session_receipts: BTreeMap::new(),
            shards: BTreeMap::new(),
            shard_receipts: BTreeMap::new(),
            batch_request_receipts: BTreeMap::new(),
            transactions: BTreeMap::new(),
            transaction_receipts: BTreeMap::new(),
            change_checkpoints: BTreeMap::new(),
            changes: Vec::new(),
            next_commit_index: 1,
            next_change_sequence: 1,
            compacted_tombstone_count: 0,
            last_tombstone_compaction_commit_index: 0,
            raft_snapshot_floor_commit_index: 0,
            raft_snapshot_compacted_change_count: 0,
            raft_snapshot_compacted_batch_receipt_count: 0,
            change_log_retention_limit: DEFAULT_META_RETENTION_LIMIT,
            mutation_receipt_retention_limit: DEFAULT_META_RETENTION_LIMIT,
            admission_policy: MetadataAdmissionPolicy::default(),
            qos_config: MetadataQosConfig::default(),
            qos_state: MetadataQosState::default(),
            snapshot_export_cache: RefCell::new(None),
            metadata_snapshot_assembler: MetaSnapshotChunkAssembler::default(),
        }
    }

}
