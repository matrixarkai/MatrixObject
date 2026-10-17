#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataCoordinatorSession {
    pub operation: String,
    pub session: CoordinatorSession,
    pub released: bool,
    pub idempotency_key: String,
    pub request_checksum: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_now_ms: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataShardMigrationPlan {
    pub operation: String,
    pub source_owner_node_id: String,
    pub target_owner_node_id: String,
    pub max_shards: usize,
    pub now_ms: u64,
    pub report: MetadataShardMigrationReport,
    pub expected_plan_checksum: String,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataShardSplit {
    pub source: MetadataShard,
    pub left: MetadataShard,
    pub right: MetadataShard,
    pub split_key: String,
    pub now_ms: u64,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataShardMerge {
    pub left: MetadataShard,
    pub right: MetadataShard,
    pub merged: MetadataShard,
    pub now_ms: u64,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataShardMigration {
    pub before: MetadataShard,
    pub after: MetadataShard,
    pub now_ms: u64,
    pub before_state_fingerprint: String,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

macro_rules! impl_prepared_metadata_raft_entry_checksum_guard {
    ($ty:ty, $label:literal) => {
        impl $ty {
            pub fn verify_expected_raft_entry_checksum(
                &self,
                expected_raft_entry_checksum: impl AsRef<str>,
            ) -> Result<(), ObjectError> {
                let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
                if expected_raft_entry_checksum.is_empty() {
                    return Ok(());
                }
                if self.raft_entry_checksum != expected_raft_entry_checksum {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "prepared metadata {} raft entry checksum mismatch: expected {} got {}",
                        $label, expected_raft_entry_checksum, self.raft_entry_checksum
                    )));
                }
                Ok(())
            }
        }
    };
}

impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataShardSplit, "shard split");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataShardMerge, "shard merge");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataBatch, "batch");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataTransaction, "transaction");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataLease, "lease");
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataCoordinatorConfig,
    "coordinator config"
);
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataCoordinatorSession,
    "coordinator session"
);
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataCoordinatorSessionExpiry,
    "coordinator session expiry"
);
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataChangeCheckpoint,
    "change checkpoint"
);
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataChangefeedCompaction,
    "changefeed compaction"
);
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataTombstoneCompaction,
    "tombstone compaction"
);
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataTransactionGc, "transaction gc");
impl_prepared_metadata_raft_entry_checksum_guard!(
    PreparedMetadataSnapshotCompaction,
    "snapshot compaction"
);
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataRetentionConfig, "retention");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataAdmissionPolicy, "admission");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataQosConfig, "qos");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataRouteCacheConfig, "route cache");
impl_prepared_metadata_raft_entry_checksum_guard!(PreparedMetadataPrefixDelete, "prefix delete");

