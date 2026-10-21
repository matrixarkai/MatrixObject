// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataLeaseExpiryReport {
    pub scanned_leases: u64,
    pub expired_leases: u64,
    pub skipped_leases: u64,
    pub prefix: String,
    pub limit: usize,
    pub expired: Vec<MetadataLease>,
}

impl MetadataLeaseExpiryReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}scanned_leases"),
            self.scanned_leases.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expired_leases"),
            self.expired_leases.to_string(),
        );
        fields.insert(
            format!("{field_prefix}skipped_leases"),
            self.skipped_leases.to_string(),
        );
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        fields.insert(format!("{field_prefix}limit"), self.limit.to_string());
        fields.insert(
            format!("{field_prefix}expired_count"),
            self.expired.len().to_string(),
        );
        for (index, lease) in self.expired.iter().enumerate() {
            fields.extend(lease.to_control_fields(&format!("{field_prefix}expired.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataMaintenanceContinuation {
    pub has_more: bool,
    pub continuation_safe: bool,
    pub phase: String,
    pub marker: String,
    pub reason: String,
    pub payload_budget_limited: bool,
    pub tombstone_marker_advanced: bool,
    pub lease_marker_advanced: bool,
    pub coordinator_session_marker_advanced: bool,
    pub transaction_marker_advanced: bool,
}

impl MetadataMaintenanceContinuation {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}continuation_safe"),
            self.continuation_safe.to_string(),
        );
        fields.insert(format!("{field_prefix}phase"), self.phase.clone());
        fields.insert(format!("{field_prefix}marker"), self.marker.clone());
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields.insert(
            format!("{field_prefix}payload_budget_limited"),
            self.payload_budget_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}tombstone_marker_advanced"),
            self.tombstone_marker_advanced.to_string(),
        );
        fields.insert(
            format!("{field_prefix}lease_marker_advanced"),
            self.lease_marker_advanced.to_string(),
        );
        fields.insert(
            format!("{field_prefix}coordinator_session_marker_advanced"),
            self.coordinator_session_marker_advanced.to_string(),
        );
        fields.insert(
            format!("{field_prefix}transaction_marker_advanced"),
            self.transaction_marker_advanced.to_string(),
        );
        fields
    }
}

