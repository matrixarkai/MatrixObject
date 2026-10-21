// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataMaintenanceReport {
    pub now_ms: u64,
    pub lease_report: MetadataLeaseExpiryReport,
    pub expired_sessions: Vec<CoordinatorSession>,
    pub expired_transactions: Vec<MetadataTransaction>,
    pub tombstone_report: MetadataTombstoneCompactionReport,
    pub compacted_tombstones: Vec<MetadataRecord>,
    pub route_cache_audit: MetadataRouteCacheAudit,
    pub continuation: MetadataMaintenanceContinuation,
}

impl MetadataMaintenanceReport {
    pub fn total_actions(&self) -> u64 {
        self.lease_report
            .expired_leases
            .saturating_add(self.expired_sessions.len() as u64)
            .saturating_add(self.expired_transactions.len() as u64)
            .saturating_add(self.tombstone_report.compacted_records)
            .saturating_add(self.route_cache_audit.evicted_entries)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}now_ms"), self.now_ms.to_string());
        fields.insert(
            format!("{field_prefix}total_action_count"),
            self.total_actions().to_string(),
        );
        fields.extend(
            self.lease_report
                .to_control_fields(&format!("{field_prefix}lease.")),
        );
        fields.insert(
            format!("{field_prefix}expired_session_count"),
            self.expired_sessions.len().to_string(),
        );
        for (index, session) in self.expired_sessions.iter().enumerate() {
            fields.extend(
                session.to_control_fields(&format!("{field_prefix}expired_session.{index}.")),
            );
        }
        fields.insert(
            format!("{field_prefix}expired_transaction_count"),
            self.expired_transactions.len().to_string(),
        );
        for (index, transaction) in self.expired_transactions.iter().enumerate() {
            fields.extend(
                transaction
                    .to_control_fields(&format!("{field_prefix}expired_transaction.{index}.")),
            );
        }
        fields.extend(
            self.tombstone_report
                .to_control_fields(&format!("{field_prefix}tombstone.")),
        );
        fields.insert(
            format!("{field_prefix}compacted_tombstone_count"),
            self.compacted_tombstones.len().to_string(),
        );
        for (index, record) in self.compacted_tombstones.iter().enumerate() {
            fields.extend(
                record.to_control_fields(&format!("{field_prefix}compacted_tombstone.{index}.")),
            );
        }
        fields.extend(
            self.route_cache_audit
                .to_control_fields(&format!("{field_prefix}route_cache.")),
        );
        fields.extend(
            self.continuation
                .to_control_fields(&format!("{field_prefix}continuation.")),
        );
        fields
    }
}

