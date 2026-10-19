#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageRepairEnvelope {
    pub operation: String,
    pub chunk_id: Option<ChunkId>,
    pub page_id: Option<PageId>,
    pub offset: u64,
    pub length: u64,
    pub max_repair_chunks: u64,
    pub start_chunk_index: u64,
    pub total_chunk_count: u64,
    pub selected_chunk_count: u64,
    pub skipped_page_count: u64,
    pub selected_repair_bytes: u64,
    pub has_more: bool,
    pub next_chunk_index: u64,
    pub checked_chunks: u64,
    pub degraded_chunks: u64,
    pub repaired_chunks: u64,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub unrepaired_chunks: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedPageRepairEnvelope {
    pub envelope: PreparedPageRepairEnvelope,
    pub proof: StorageReadProof,
}

impl PreparedPageRepairEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("offset", self.offset),
            ("length", self.length),
            ("max_repair_chunks", self.max_repair_chunks),
            ("start_chunk_index", self.start_chunk_index),
            ("total_chunk_count", self.total_chunk_count),
            ("selected_chunk_count", self.selected_chunk_count),
            ("skipped_chunk_count", self.skipped_page_count),
            ("selected_repair_bytes", self.selected_repair_bytes),
            ("next_chunk_index", self.next_chunk_index),
            ("checked_chunks", self.checked_chunks),
            ("degraded_chunks", self.degraded_chunks),
            ("repaired_chunks", self.repaired_chunks),
            ("repaired_replicas", self.repaired_replicas),
            ("failed_repairs", self.failed_repairs),
            ("unrepaired_chunks", self.unrepaired_chunks),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("route_control_proof_bytes", self.route_control_proof_bytes),
            (
                "route_control_commit_index",
                self.route_control_commit_index,
            ),
            (
                "route_control_next_log_index",
                self.route_control_next_log_index,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("operation", &self.operation),
            (
                "before_block_service_fingerprint",
                &self.before_page_service_fingerprint,
            ),
            (
                "after_block_service_fingerprint",
                &self.after_page_service_fingerprint,
            ),
            (
                "route_control_proof_format",
                &self.route_control_proof_format,
            ),
            (
                "route_control_proof_checksum",
                &self.route_control_proof_checksum,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        if let Some(chunk_id) = &self.chunk_id {
            fields.extend(chunk_id.to_control_fields(&format!("{field_prefix}segment.")));
        }
        if let Some(page_id) = &self.page_id {
            fields.extend(page_id.to_control_fields(&format!("{field_prefix}chunk.")));
        }
        fields
    }
}

impl ProvenPreparedPageRepairEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.envelope.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}

