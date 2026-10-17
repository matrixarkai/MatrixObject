#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkServiceSnapshot {
    pub chunks: BTreeMap<ChunkId, ChunkState>,
    pub receipt_retention: usize,
    pub write_request_receipts: BTreeMap<String, ChunkWriteReceipt>,
    pub discard_request_receipts: BTreeMap<String, ChunkDiscardReceipt>,
    pub batch_write_request_receipts: BTreeMap<String, ChunkBatchWriteReceipt>,
    pub batch_discard_request_receipts: BTreeMap<String, ChunkBatchDiscardReceipt>,
    pub state_install_request_receipts: BTreeMap<String, ChunkStateInstallReceipt>,
    pub reclaim_request_receipts: BTreeMap<String, ChunkReclaimReceipt>,
    pub repair_request_receipts: BTreeMap<String, ChunkRepairReceipt>,
    pub accepting_writes: bool,
    pub draining: bool,
    pub frozen: bool,
    pub capacity_bytes: u64,
    pub capacity_rejected_writes: u64,
    pub service_mode_rejected_mutations: u64,
    pub metadata_dirty: bool,
    pub metadata_dirty_mutations: u64,
    pub metadata_checkpoint_interval: u64,
    pub defer_metadata_checkpoints: bool,
    pub deferred_metadata_checkpoint_pending: bool,
    pub deferred_metadata_checkpoint_requests: u64,
    pub deferred_metadata_checkpoint_runs: u64,
    pub trim_flow_rate_bytes_per_sec: u64,
    pub trim_flow_burst_bytes: u64,
    pub trim_flow_available_bytes: u64,
    pub trim_flow_rate_ops_per_sec: u64,
    pub trim_flow_burst_ops: u64,
    pub trim_flow_available_ops: u64,
    pub trim_flow_last_refill_ms: u64,
    pub trim_flow_throttled_reclaims: u64,
    pub trim_flow_throttled_ops: u64,
    pub trim_flow_throttled_bytes: u64,
    pub partial_reclaim_failures: u64,
    pub last_partial_reclaim_failure_status: String,
    pub physical_write_count: u64,
    pub physical_write_bytes: u64,
    pub physical_discard_ranges: u64,
    pub physical_discard_bytes: u64,
    pub compressed_chunk_count: u64,
    pub compression_saved_bytes: u64,
    pub ec_reconstructed_reads: u64,
    pub ec_reconstruction_failures: u64,
    pub chunk_mutation_control_commit_index: u64,
    pub chunk_mutation_control_next_log_index: u64,
    pub mutation_qos_config: ChunkMutationQosConfig,
    pub mutation_qos_state: ChunkMutationQosState,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkServiceSnapshotChunk {
    pub offset: u64,
    pub total_bytes: u64,
    pub chunk_checksum: String,
    pub full_checksum: String,
    pub eof: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkServiceSnapshotAssembler {
    expected_total_bytes: Option<u64>,
    expected_full_checksum: Option<String>,
    buffer: Vec<u8>,
}

pub(crate) struct ChunkServiceSnapshotAssembledImage {
    pub bytes: Vec<u8>,
    pub full_checksum: String,
}

impl ChunkServiceSnapshotChunk {
    pub fn install_progress(&self) -> SnapshotInstallProgress {
        snapshot_install_progress(
            true,
            !self.eof,
            self.offset.saturating_add(self.payload.len() as u64),
            self.total_bytes,
            self.full_checksum.clone(),
            0,
            0,
        )
    }
}

