// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataLeaseExpiry {
    pub prefix: String,
    pub now_ms: u64,
    pub limit: usize,
    pub expired: Vec<MetadataLease>,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataLease {
    pub operation: String,
    pub namespace: String,
    pub owner: String,
    pub epoch: u64,
    pub ttl_ms: u64,
    pub now_ms: u64,
    pub lease: MetadataLease,
    pub released: bool,
    pub request_checksum: String,
    pub before_state_fingerprint: String,
    pub after_state_fingerprint: String,
    pub metadata_read_index: u64,
    pub metadata_raft_term: u64,
    pub metadata_control_proof_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataCoordinatorSessionExpiry {
    pub now_ms: u64,
    pub limit: usize,
    pub include_fenced: bool,
    pub expired: Vec<CoordinatorSession>,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataCoordinatorSessionExpiryEnvelope {
    pub now_ms: u64,
    pub limit: u64,
    pub include_fenced: bool,
    pub expired_session_count: u64,
    pub endpoint_fenced_expired_session_count: u64,
    pub expired_payload_bytes: u64,
    pub first_session_id: String,
    pub first_owner: String,
    pub first_epoch: u64,
    pub first_expire_time_ms: u64,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataMaintenance {
    pub now_ms: u64,
    pub lease_prefix: String,
    pub lease_limit: usize,
    pub session_limit: usize,
    pub transaction_limit: usize,
    pub max_payload_bytes: u64,
    pub maintenance_payload_bytes: u64,
    pub payload_budget_limited: bool,
    pub payload_budget_phase: String,
    pub payload_budget_marker: String,
    pub lease_report: MetadataLeaseExpiryReport,
    pub expired_sessions: Vec<CoordinatorSession>,
    pub expired_transactions: Vec<MetadataTransaction>,
    pub tombstone_report: MetadataTombstoneCompactionReport,
    pub compacted_tombstones: Vec<MetadataRecord>,
    pub route_cache_status: MetadataRouteCacheStatus,
    pub metadata_read_index: u64,
    pub metadata_raft_term: u64,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMetadataMaintenanceEnvelope {
    pub now_ms: u64,
    pub lease_prefix: String,
    pub lease_limit: u64,
    pub session_limit: u64,
    pub transaction_limit: u64,
    pub max_payload_bytes: u64,
    pub maintenance_payload_bytes: u64,
    pub payload_budget_limited: bool,
    pub payload_budget_phase: String,
    pub payload_budget_marker: String,
    pub planned_lease_expirations: u64,
    pub planned_session_expirations: u64,
    pub planned_transaction_expirations: u64,
    pub planned_tombstone_compactions: u64,
    pub planned_tombstone_compaction_bytes: u64,
    pub route_cache_entries: u64,
    pub route_cache_stale_entries: u64,
    pub route_cache_expired_entries: u64,
    pub first_lease_namespace: String,
    pub first_session_id: String,
    pub first_transaction_id: String,
    pub first_tombstone_key: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub metadata_read_index: u64,
    pub metadata_raft_term: u64,
    pub metadata_control_proof_format: String,
    pub metadata_control_proof_checksum: String,
    pub metadata_control_proof_bytes: u64,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

impl PreparedMetadataCoordinatorSessionExpiryEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("now_ms", self.now_ms),
            ("limit", self.limit),
            ("expired_session_count", self.expired_session_count),
            (
                "endpoint_fenced_expired_session_count",
                self.endpoint_fenced_expired_session_count,
            ),
            ("expired_payload_bytes", self.expired_payload_bytes),
            (
                "metadata_control_proof_bytes",
                self.metadata_control_proof_bytes,
            ),
            ("first_epoch", self.first_epoch),
            ("first_expire_time_ms", self.first_expire_time_ms),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}include_fenced"),
            self.include_fenced.to_string(),
        );
        for (name, value) in [
            ("first_session_id", &self.first_session_id),
            ("first_owner", &self.first_owner),
            (
                "metadata_control_proof_format",
                &self.metadata_control_proof_format,
            ),
            (
                "metadata_control_proof_checksum",
                &self.metadata_control_proof_checksum,
            ),
            ("request_checksum", &self.request_checksum),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields
    }
}

impl ProvenMetadataRead<PreparedMetadataCoordinatorSessionExpiryEnvelope> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .value
            .to_control_fields(&format!("{field_prefix}envelope."));
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}metadata_read_proof.")),
        );
        fields
    }
}

impl PreparedMetadataMaintenanceEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("now_ms", self.now_ms),
            ("lease_limit", self.lease_limit),
            ("session_limit", self.session_limit),
            ("transaction_limit", self.transaction_limit),
            ("max_payload_bytes", self.max_payload_bytes),
            ("maintenance_payload_bytes", self.maintenance_payload_bytes),
            ("planned_lease_expirations", self.planned_lease_expirations),
            (
                "planned_session_expirations",
                self.planned_session_expirations,
            ),
            (
                "planned_transaction_expirations",
                self.planned_transaction_expirations,
            ),
            (
                "planned_tombstone_compactions",
                self.planned_tombstone_compactions,
            ),
            (
                "planned_tombstone_compaction_bytes",
                self.planned_tombstone_compaction_bytes,
            ),
            ("route_cache_entries", self.route_cache_entries),
            ("route_cache_stale_entries", self.route_cache_stale_entries),
            (
                "route_cache_expired_entries",
                self.route_cache_expired_entries,
            ),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("metadata_read_index", self.metadata_read_index),
            ("metadata_raft_term", self.metadata_raft_term),
            (
                "metadata_control_proof_bytes",
                self.metadata_control_proof_bytes,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}payload_budget_limited"),
            self.payload_budget_limited.to_string(),
        );
        for (name, value) in [
            ("lease_prefix", &self.lease_prefix),
            ("payload_budget_phase", &self.payload_budget_phase),
            ("payload_budget_marker", &self.payload_budget_marker),
            ("first_lease_namespace", &self.first_lease_namespace),
            ("first_session_id", &self.first_session_id),
            ("first_transaction_id", &self.first_transaction_id),
            ("first_tombstone_key", &self.first_tombstone_key),
            ("request_checksum", &self.request_checksum),
            (
                "metadata_control_proof_format",
                &self.metadata_control_proof_format,
            ),
            (
                "metadata_control_proof_checksum",
                &self.metadata_control_proof_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields
    }
}

impl ProvenMetadataRead<PreparedMetadataMaintenanceEnvelope> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .value
            .to_control_fields(&format!("{field_prefix}envelope."));
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}metadata_read_proof.")),
        );
        fields
    }
}

