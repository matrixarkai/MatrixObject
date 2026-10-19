// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageAuditRepair {
    pub after_chunk_id: Option<PageId>,
    pub max_chunks: usize,
    pub max_payload_bytes: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub report: ChunkReplicaAuditRepairReport,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub chunk_control_proof: ReplicaControlProof,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageAuditRepairApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub report: ChunkReplicaAuditRepairReport,
    pub receipt: ChunkAuditRepairReceipt,
    pub metadata: ObjectMetadata,
}

impl PreparedPageAuditRepairApplyReport {
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
            self.receipt
                .to_control_fields(&format!("{field_prefix}receipt.")),
        );
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageAuditRepairEnvelope {
    pub envelope_format: String,
    pub after_chunk_id: Option<PageId>,
    pub max_chunks: u64,
    pub max_payload_bytes: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
    pub scanned_chunks: u64,
    pub candidate_chunks: u64,
    pub repaired_chunks: u64,
    pub unrepaired_chunks: u64,
    pub missing_replicas: u64,
    pub corrupt_replicas: u64,
    pub divergent_replicas: u64,
    pub quarantined_replicas: u64,
    pub frozen_replicas: u64,
    pub payload_bytes: u64,
    pub checked_chunks: u64,
    pub degraded_chunks: u64,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub has_more: bool,
    pub repair_needed: bool,
    pub repair_can_apply: bool,
    pub repair_reason: String,
    pub first_candidate_chunk_id: Option<PageId>,
    pub first_candidate_replica_index: Option<usize>,
    pub first_candidate_status: String,
    pub first_candidate_logical_length: u64,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPreparedPageAuditRepairEnvelope {
    pub envelope: PreparedPageAuditRepairEnvelope,
    pub proof: StorageReadProof,
}
