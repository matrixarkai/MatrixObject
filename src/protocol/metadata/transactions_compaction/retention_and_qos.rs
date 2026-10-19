// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataRetentionConfigReport {
    pub change_log_retention_limit: u64,
    pub mutation_receipt_retention_limit: u64,
    pub trimmed_change_records: u64,
    pub trimmed_batch_receipts: u64,
    pub trimmed_lease_receipts: u64,
    pub trimmed_coordinator_config_receipts: u64,
    pub trimmed_coordinator_session_receipts: u64,
    pub trimmed_shard_receipts: u64,
    pub trimmed_transaction_receipts: u64,
    pub retained_change_records: u64,
    pub retained_mutation_receipts: u64,
}

impl MetadataRetentionConfigReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            (
                "change_log_retention_limit",
                self.change_log_retention_limit,
            ),
            (
                "mutation_receipt_retention_limit",
                self.mutation_receipt_retention_limit,
            ),
            ("trimmed_change_records", self.trimmed_change_records),
            ("trimmed_batch_receipts", self.trimmed_batch_receipts),
            ("trimmed_lease_receipts", self.trimmed_lease_receipts),
            (
                "trimmed_coordinator_config_receipts",
                self.trimmed_coordinator_config_receipts,
            ),
            (
                "trimmed_coordinator_session_receipts",
                self.trimmed_coordinator_session_receipts,
            ),
            ("trimmed_shard_receipts", self.trimmed_shard_receipts),
            (
                "trimmed_transaction_receipts",
                self.trimmed_transaction_receipts,
            ),
            ("retained_change_records", self.retained_change_records),
            (
                "retained_mutation_receipts",
                self.retained_mutation_receipts,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataAdmissionPolicy {
    pub max_batch_mutations: usize,
    pub max_batch_value_bytes: u64,
    pub max_open_transactions: usize,
    pub max_namespace_leases: usize,
    pub max_coordinator_sessions: usize,
    pub max_shards: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataQosConfig {
    pub window_ms: u64,
    pub max_write_ops_per_window: u64,
    pub max_write_bytes_per_window: u64,
}

impl Default for MetadataQosConfig {
    fn default() -> Self {
        Self {
            window_ms: DEFAULT_META_QOS_WINDOW_MS,
            max_write_ops_per_window: 0,
            max_write_bytes_per_window: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataQosState {
    pub window_start_ms: u64,
    pub write_ops_in_window: u64,
    pub write_bytes_in_window: u64,
    pub throttled_write_ops: u64,
    pub throttled_write_bytes: u64,
}

impl Default for MetadataAdmissionPolicy {
    fn default() -> Self {
        Self {
            max_batch_mutations: DEFAULT_META_MAX_BATCH_MUTATIONS,
            max_batch_value_bytes: DEFAULT_META_MAX_BATCH_VALUE_BYTES,
            max_open_transactions: DEFAULT_META_MAX_OPEN_TRANSACTIONS,
            max_namespace_leases: DEFAULT_META_MAX_NAMESPACE_LEASES,
            max_coordinator_sessions: DEFAULT_META_MAX_COORDINATOR_SESSIONS,
            max_shards: DEFAULT_META_MAX_SHARDS,
        }
    }
}

impl MetadataAdmissionPolicy {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}max_batch_mutations"),
            self.max_batch_mutations.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_batch_value_bytes"),
            self.max_batch_value_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_open_transactions"),
            self.max_open_transactions.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_namespace_leases"),
            self.max_namespace_leases.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_coordinator_sessions"),
            self.max_coordinator_sessions.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_shards"),
            self.max_shards.to_string(),
        );
        fields
    }
}

