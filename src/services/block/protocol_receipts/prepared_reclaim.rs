#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageReplicaReclaim {
    pub max_chunks: usize,
    pub max_rewrite_bytes: u64,
    pub min_discarded_bytes: u64,
    pub min_discard_ratio_ppm: u64,
    pub max_discard_ranges: usize,
    pub max_trim_bytes: u64,
    pub now_ms: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub plan: PageReplicaReclaimPlan,
    pub report: ChunkPartialReclaimReport,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageReclaimApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub report: ChunkPartialReclaimReport,
    pub metadata: ObjectMetadata,
}

impl PreparedPageReclaimApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .report
            .to_control_fields(&format!("{field_prefix}report."));
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
