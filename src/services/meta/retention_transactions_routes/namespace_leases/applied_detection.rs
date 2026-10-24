// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    fn prepared_lease_already_applied(&self, prepared: &PreparedMetadataLease) -> bool {
        if !self.metadata_log_has_applied(prepared.prepared_commit_index) {
            return false;
        }
        match prepared.operation.as_str() {
            "acquire" | "renew" => self.leases.get(&prepared.namespace) == Some(&prepared.lease),
            "release" => !self.leases.contains_key(&prepared.namespace),
            _ => false,
        }
    }

    fn prepared_lease_expiry_already_applied(
        &self,
        prepared: &PreparedMetadataLeaseExpiry,
    ) -> bool {
        self.metadata_log_has_applied(prepared.prepared_commit_index)
            && prepared
                .expired
                .iter()
                .all(|lease| !self.leases.contains_key(&lease.namespace))
    }

    fn prepared_coordinator_session_expiry_already_applied(
        &self,
        prepared: &PreparedMetadataCoordinatorSessionExpiry,
    ) -> bool {
        self.metadata_log_has_applied(prepared.prepared_commit_index)
            && prepared
                .expired
                .iter()
                .all(|session| !self.coordinator_sessions.contains_key(&session.session_id))
    }

    fn prepared_maintenance_already_applied(&self, prepared: &PreparedMetadataMaintenance) -> bool {
        self.metadata_log_has_applied(prepared.prepared_commit_index)
            && prepared
                .lease_report
                .expired
                .iter()
                .all(|lease| !self.leases.contains_key(&lease.namespace))
            && prepared
                .expired_sessions
                .iter()
                .all(|session| !self.coordinator_sessions.contains_key(&session.session_id))
            && prepared.expired_transactions.iter().all(|transaction| {
                self.transactions
                    .get(&transaction.transaction_id)
                    .map_or(true, |current| {
                        current.state == "expired"
                            && current.update_time_ms == prepared.now_ms
                            && current.owner == transaction.owner
                            && current.mutations == transaction.mutations
                    })
            })
            && prepared
                .compacted_tombstones
                .iter()
                .all(|record| !self.records.contains(record.key.as_str()))
            && (prepared.tombstone_report.compacted_records == 0
                || (self.compacted_tombstone_count
                    >= prepared.tombstone_report.compacted_tombstone_count
                    && self.last_tombstone_compaction_commit_index
                        >= prepared
                            .tombstone_report
                            .last_tombstone_compaction_commit_index))
    }
}
