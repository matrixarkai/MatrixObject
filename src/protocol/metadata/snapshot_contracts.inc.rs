#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaSnapshot {
    pub records: BTreeMap<String, MetadataRecord>,
    pub leases: BTreeMap<String, MetadataLease>,
    pub lease_receipts: BTreeMap<String, MetadataLeaseReceipt>,
    pub coordinator_config: MetadataCoordinatorConfig,
    pub coordinator_config_receipts: BTreeMap<String, MetadataCoordinatorConfigReceipt>,
    pub coordinator_sessions: BTreeMap<String, CoordinatorSession>,
    pub coordinator_session_receipts: BTreeMap<String, CoordinatorSessionReceipt>,
    pub shards: BTreeMap<String, MetadataShard>,
    pub shard_receipts: BTreeMap<String, MetadataShardReceipt>,
    pub batch_request_receipts: BTreeMap<String, MetadataBatchReceipt>,
    pub transactions: BTreeMap<String, MetadataTransaction>,
    pub transaction_receipts: BTreeMap<String, MetadataTransactionReceipt>,
    pub change_checkpoints: BTreeMap<String, MetadataChangeCheckpoint>,
    pub changes: Vec<MetadataChange>,
    pub next_commit_index: u64,
    pub next_change_sequence: u64,
    pub compacted_tombstone_count: u64,
    pub last_tombstone_compaction_commit_index: u64,
    pub raft_snapshot_floor_commit_index: u64,
    pub raft_snapshot_compacted_change_count: u64,
    pub raft_snapshot_compacted_batch_receipt_count: u64,
    pub change_log_retention_limit: usize,
    pub mutation_receipt_retention_limit: usize,
    pub admission_policy: MetadataAdmissionPolicy,
    pub qos_config: MetadataQosConfig,
    pub qos_state: MetadataQosState,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaSnapshotChunk {
    pub offset: u64,
    pub total_bytes: u64,
    pub chunk_checksum: String,
    pub full_checksum: String,
    pub last_applied_index: u64,
    pub eof: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct MetaSnapshotChunkAssembler {
    expected_total_bytes: Option<u64>,
    expected_full_checksum: Option<String>,
    expected_last_applied_index: Option<u64>,
    buffer: Vec<u8>,
}

pub(crate) struct MetaSnapshotAssembledImage {
    pub bytes: Vec<u8>,
    pub full_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotInstallProgress {
    pub resume_supported: bool,
    pub stream_active: bool,
    pub next_offset: u64,
    pub remaining_bytes: u64,
    pub stream_id: String,
    pub total_bytes: u64,
    pub last_applied_index: u64,
    pub term: u64,
    pub full_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotAbortReport {
    pub aborted: bool,
    pub previous_stream_active: bool,
    pub previous_next_offset: u64,
    pub previous_remaining_bytes: u64,
    pub previous_stream_id: String,
    pub cleared_progress: SnapshotInstallProgress,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataSnapshotInstallReport {
    pub installed: bool,
    pub validated: bool,
    pub raft_snapshot_install: bool,
    pub snapshot_bytes: u64,
    pub snapshot_checksum: String,
    pub last_applied_index: u64,
    pub next_log_index: u64,
    pub record_count: u64,
    pub live_record_count: u64,
    pub deleted_record_count: u64,
    pub metadata_lease_count: u64,
    pub applied_mutation_count: u64,
    pub change_log_count: u64,
    pub metadata_shard_count: u64,
    pub install_progress: SnapshotInstallProgress,
}

impl SnapshotInstallProgress {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}resume_supported"),
            self.resume_supported.to_string(),
        );
        fields.insert(
            format!("{field_prefix}stream_active"),
            self.stream_active.to_string(),
        );
        fields.insert(format!("{field_prefix}stream_id"), self.stream_id.clone());
        fields.insert(
            format!("{field_prefix}full_checksum"),
            self.full_checksum.clone(),
        );
        for (name, value) in [
            ("next_offset", self.next_offset),
            ("remaining_bytes", self.remaining_bytes),
            ("total_bytes", self.total_bytes),
            ("last_applied_index", self.last_applied_index),
            ("term", self.term),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl SnapshotAbortReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}aborted"), self.aborted.to_string());
        fields.insert(
            format!("{field_prefix}previous_stream_active"),
            self.previous_stream_active.to_string(),
        );
        fields.insert(
            format!("{field_prefix}previous_stream_id"),
            self.previous_stream_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}previous_next_offset"),
            self.previous_next_offset.to_string(),
        );
        fields.insert(
            format!("{field_prefix}previous_remaining_bytes"),
            self.previous_remaining_bytes.to_string(),
        );
        fields.extend(
            self.cleared_progress
                .to_control_fields(&format!("{field_prefix}cleared_progress.")),
        );
        fields
    }
}

impl MetadataSnapshotInstallReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("installed", self.installed),
            ("validated", self.validated),
            ("raft_snapshot_install", self.raft_snapshot_install),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}snapshot_checksum"),
            self.snapshot_checksum.clone(),
        );
        for (name, value) in [
            ("snapshot_bytes", self.snapshot_bytes),
            ("last_applied_index", self.last_applied_index),
            ("next_log_index", self.next_log_index),
            ("record_count", self.record_count),
            ("live_record_count", self.live_record_count),
            ("deleted_record_count", self.deleted_record_count),
            ("metadata_lease_count", self.metadata_lease_count),
            ("applied_mutation_count", self.applied_mutation_count),
            ("change_log_count", self.change_log_count),
            ("metadata_shard_count", self.metadata_shard_count),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.extend(
            self.install_progress
                .to_control_fields(&format!("{field_prefix}install_progress.")),
        );
        fields
    }
}

impl MetaSnapshotChunk {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            !self.eof,
            self.offset.saturating_add(self.payload.len() as u64),
            self.total_bytes,
            self.full_checksum.clone(),
            self.last_applied_index,
            0,
        )
    }
}

