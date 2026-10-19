#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSnapshot {
    pub options: StoreOptions,
    pub buckets: BTreeSet<SharedText>,
    pub bucket_versioning: BTreeSet<String>,
    pub bucket_retention: BTreeMap<String, u64>,
    pub bucket_quotas: BTreeMap<String, BucketQuota>,
    pub bucket_lifecycle: BTreeMap<String, BucketLifecyclePolicy>,
    pub bucket_policy_documents: BTreeMap<String, BucketPolicyDocument>,
    pub bucket_tags: BTreeMap<String, BucketTagSet>,
    pub bucket_cors: BTreeMap<String, BucketCorsPolicy>,
    pub bucket_policy_receipts: BTreeMap<String, BucketPolicyMutationReceipt>,
    pub bucket_cors_receipts: BTreeMap<String, BucketCorsMutationReceipt>,
    pub objects: BTreeMap<ObjectId, ObjectMetadata>,
    pub versions: BTreeMap<ObjectId, Vec<ObjectMetadata>>,
    pub multipart_uploads: BTreeMap<String, MultipartUpload>,
    pub multipart_complete_receipts: BTreeMap<String, MultipartCompleteReceipt>,
    pub multipart_abort_receipts: BTreeMap<String, MultipartAbortReceipt>,
    pub object_changes: Vec<ObjectChangeRecord>,
    pub object_change_cursors: BTreeMap<String, ObjectChangeCursor>,
    pub next_object_change_sequence: u64,
    pub slabs: BTreeMap<SlabId, SlabInfo>,
    pub payloads: BTreeMap<(SlabId, u64), std::sync::Arc<Vec<u8>>>,
    pub meta_snapshot: MetaSnapshot,
    pub root_snapshot: RootSnapshot,
    pub next_slab_id: u64,
    pub next_upload_sequence: u64,
    pub logical_written_bytes: u64,
    pub physical_written_bytes: u64,
    pub last_applied_index: u64,
    pub last_applied_term: u64,
    pub applied_raft_receipts: BTreeMap<u64, RaftApplyReceipt>,
    pub raft_voter_node_ids: Vec<String>,
    pub raft_healthy_peer_count: u64,
    pub raft_membership_epoch: u64,
    pub raft_membership_receipts: BTreeMap<String, RaftMembershipReceipt>,
    pub chunk_read_cache_max_bytes_per_replica: u64,
    pub chunk_read_cache_max_cacheable_read_bytes_per_replica: u64,
    pub page_write_quorum: usize,
    pub page_read_quorum: usize,
    pub replica_endpoints: Vec<String>,
    pub page_disabled_chunk_endpoints: BTreeSet<usize>,
    pub snapshot_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreSnapshotChunk {
    pub offset: u64,
    pub total_bytes: u64,
    pub chunk_checksum: String,
    pub full_checksum: String,
    pub last_applied_index: u64,
    pub term: u64,
    pub eof: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default)]
pub struct StoreSnapshotChunkAssembler {
    expected_total_bytes: Option<u64>,
    expected_full_checksum: Option<String>,
    expected_last_applied_index: Option<u64>,
    expected_term: Option<u64>,
    buffer: Vec<u8>,
}

impl StoreSnapshotChunkAssembler {
    /// What a partly received store snapshot holds. Its fields are private to
    /// this module, which is why the accounting lives here.
    pub(crate) fn footprint_bytes(&self) -> u64 {
        (self.buffer.capacity() as u64).saturating_add(
            self.expected_full_checksum
                .as_ref()
                .map(|checksum| checksum.capacity() as u64)
                .unwrap_or(0),
        )
    }
}

pub(crate) struct StoreSnapshotAssembledImage {
    pub bytes: Vec<u8>,
    pub full_checksum: String,
}

impl StoreSnapshotChunk {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            !self.eof,
            self.offset.saturating_add(self.payload.len() as u64),
            self.total_bytes,
            self.full_checksum.clone(),
            self.last_applied_index,
            self.term,
        )
    }
}

