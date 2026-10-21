// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataControlProof {
    pub state_format: String,
    pub state_bytes: u64,
    pub state_checksum: String,
    pub read_ready: bool,
    pub write_ready: bool,
    pub commit_bounds_valid: bool,
    pub admission_pressure: bool,
    pub qos_pressure: bool,
    pub topology_checksum: String,
    pub session_checksum: String,
    pub coordinator_config_checksum: String,
    pub coordinator_backend: String,
    pub coordinator_normalized_backend: String,
    pub coordinator_policy_format: String,
    pub coordinator_policy_checksum: String,
    pub coordinator_preferred_backend: String,
    pub coordinator_preferred_backend_active: bool,
    pub coordinator_external_compatibility_mode: bool,
    pub coordinator_production_ready: bool,
    pub coordinator_external_dependency: String,
    pub coordinator_config_revision: u64,
    pub coordinator_config_endpoint_count: u64,
    pub coordinator_config_receipt_count: u64,
    pub external_coordinator_required: bool,
    pub external_coordinator_endpoints_ready: bool,
    pub external_coordinator_owner_sessions_ready: bool,
    pub external_coordinator_ready: bool,
    pub state_fingerprint: String,
    pub active_owner_sessions: u64,
    pub expired_owner_sessions: u64,
    pub writable_shards: u64,
    pub draining_shards: u64,
    pub stale_owner_shards: u64,
    pub raft_membership_epoch: u64,
    pub raft_membership_receipt_count: u64,
    pub metadata_replication_quorum: u64,
    pub metadata_replication_acks: u64,
    pub retained_mutation_receipts: u64,
    pub change_log_pressure: bool,
    pub mutation_receipt_pressure: bool,
    pub max_change_checkpoint_lag: u64,
    pub remaining_open_transactions: u64,
    pub remaining_namespace_leases: u64,
    pub remaining_coordinator_sessions: u64,
    pub remaining_shards: u64,
    pub open_transaction_admission_pressure: bool,
    pub namespace_lease_admission_pressure: bool,
    pub coordinator_session_admission_pressure: bool,
    pub shard_admission_pressure: bool,
    pub last_applied_mutation_commit_index: u64,
    pub last_applied_mutation_time_ms: u64,
    pub last_applied_mutation_age_ms: u64,
    pub apply_commit_lag: u64,
    pub apply_caught_up: bool,
    pub requested_read_fence: u64,
    pub safe_read_fence: u64,
    pub read_fence_covered: bool,
    pub read_fence_commit_lag: u64,
    pub read_fence_read_index_lag: u64,
    pub read_fence_applied_lag: u64,
}

impl MetadataControlProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("state_bytes", self.state_bytes),
            (
                "coordinator_config_revision",
                self.coordinator_config_revision,
            ),
            (
                "coordinator_config_endpoint_count",
                self.coordinator_config_endpoint_count,
            ),
            (
                "coordinator_config_receipt_count",
                self.coordinator_config_receipt_count,
            ),
            ("active_owner_sessions", self.active_owner_sessions),
            ("expired_owner_sessions", self.expired_owner_sessions),
            ("writable_shards", self.writable_shards),
            ("draining_shards", self.draining_shards),
            ("stale_owner_shards", self.stale_owner_shards),
            ("raft_membership_epoch", self.raft_membership_epoch),
            (
                "raft_membership_receipt_count",
                self.raft_membership_receipt_count,
            ),
            (
                "metadata_replication_quorum",
                self.metadata_replication_quorum,
            ),
            ("metadata_replication_acks", self.metadata_replication_acks),
            (
                "retained_mutation_receipts",
                self.retained_mutation_receipts,
            ),
            ("max_change_checkpoint_lag", self.max_change_checkpoint_lag),
            (
                "remaining_open_transactions",
                self.remaining_open_transactions,
            ),
            (
                "remaining_namespace_leases",
                self.remaining_namespace_leases,
            ),
            (
                "remaining_coordinator_sessions",
                self.remaining_coordinator_sessions,
            ),
            ("remaining_shards", self.remaining_shards),
            (
                "last_applied_mutation_commit_index",
                self.last_applied_mutation_commit_index,
            ),
            (
                "last_applied_mutation_time_ms",
                self.last_applied_mutation_time_ms,
            ),
            (
                "last_applied_mutation_age_ms",
                self.last_applied_mutation_age_ms,
            ),
            ("apply_commit_lag", self.apply_commit_lag),
            ("requested_read_fence", self.requested_read_fence),
            ("safe_read_fence", self.safe_read_fence),
            ("read_fence_commit_lag", self.read_fence_commit_lag),
            ("read_fence_read_index_lag", self.read_fence_read_index_lag),
            ("read_fence_applied_lag", self.read_fence_applied_lag),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("read_ready", self.read_ready),
            ("write_ready", self.write_ready),
            ("metadata_control_read_ready", self.read_ready),
            ("metadata_control_write_ready", self.write_ready),
            ("commit_bounds_valid", self.commit_bounds_valid),
            ("admission_pressure", self.admission_pressure),
            ("qos_pressure", self.qos_pressure),
            (
                "coordinator_preferred_backend_active",
                self.coordinator_preferred_backend_active,
            ),
            (
                "coordinator_external_compatibility_mode",
                self.coordinator_external_compatibility_mode,
            ),
            (
                "coordinator_production_ready",
                self.coordinator_production_ready,
            ),
            (
                "external_coordinator_required",
                self.external_coordinator_required,
            ),
            (
                "external_coordinator_endpoints_ready",
                self.external_coordinator_endpoints_ready,
            ),
            (
                "external_coordinator_owner_sessions_ready",
                self.external_coordinator_owner_sessions_ready,
            ),
            (
                "external_coordinator_ready",
                self.external_coordinator_ready,
            ),
            ("change_log_pressure", self.change_log_pressure),
            ("mutation_receipt_pressure", self.mutation_receipt_pressure),
            (
                "open_transaction_admission_pressure",
                self.open_transaction_admission_pressure,
            ),
            (
                "namespace_lease_admission_pressure",
                self.namespace_lease_admission_pressure,
            ),
            (
                "coordinator_session_admission_pressure",
                self.coordinator_session_admission_pressure,
            ),
            ("shard_admission_pressure", self.shard_admission_pressure),
            ("apply_caught_up", self.apply_caught_up),
            ("read_fence_covered", self.read_fence_covered),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("state_format", self.state_format.as_str()),
            ("state_checksum", self.state_checksum.as_str()),
            ("topology_checksum", self.topology_checksum.as_str()),
            ("session_checksum", self.session_checksum.as_str()),
            (
                "coordinator_config_checksum",
                self.coordinator_config_checksum.as_str(),
            ),
            ("coordinator_backend", self.coordinator_backend.as_str()),
            (
                "coordinator_normalized_backend",
                self.coordinator_normalized_backend.as_str(),
            ),
            (
                "coordinator_policy_format",
                self.coordinator_policy_format.as_str(),
            ),
            (
                "coordinator_policy_checksum",
                self.coordinator_policy_checksum.as_str(),
            ),
            (
                "coordinator_preferred_backend",
                self.coordinator_preferred_backend.as_str(),
            ),
            (
                "coordinator_external_dependency",
                self.coordinator_external_dependency.as_str(),
            ),
            ("state_fingerprint", self.state_fingerprint.as_str()),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}
