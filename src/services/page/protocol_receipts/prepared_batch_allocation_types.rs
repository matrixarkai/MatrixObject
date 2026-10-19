#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchAllocationReservation {
    pub lengths: Vec<u64>,
    pub now_ms: u64,
    pub idempotency_key: String,
    pub request_checksum: String,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub receipt: ChunkBatchAllocationReceipt,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub pending_allocation_count_after: u64,
    pub allocation_receipt_count_after: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchAllocationReservationApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub receipt: ChunkBatchAllocationReceipt,
    pub metadata: ObjectMetadata,
}

impl PreparedPageBatchAllocationReservationApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .receipt
            .to_control_fields(&format!("{field_prefix}receipt."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_commit_index"),
            self.mutation_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_log_index"),
            self.next_log_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}committed"),
            self.committed.to_string(),
        );
        fields.insert(format!("{field_prefix}prepared"), self.prepared.to_string());
        fields.insert(
            format!("{field_prefix}external_raft_required"),
            self.external_raft_required.to_string(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchAllocationReservationEnvelope {
    pub envelope_format: String,
    pub lengths: Vec<u64>,
    pub now_ms: u64,
    pub idempotency_key: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
    pub allocation_count: u64,
    pub allocation_ids: Vec<String>,
    pub allocation_slabs: Vec<SlabId>,
    pub allocation_offsets: Vec<u64>,
    pub allocation_lengths: Vec<u64>,
    pub allocation_create_time_ms: Vec<u64>,
    pub pending_allocation_count_after: u64,
    pub allocation_receipt_count_after: u64,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedPageBatchAllocationReservationEnvelope {
    pub envelope: PreparedPageBatchAllocationReservationEnvelope,
    pub proof: StorageReadProof,
}

