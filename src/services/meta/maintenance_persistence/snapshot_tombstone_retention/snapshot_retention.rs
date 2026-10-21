// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn compact_raft_snapshot_retention(
        &mut self,
        snapshot_commit_index: u64,
        max_change_records: usize,
        max_batch_receipts: usize,
    ) -> Result<MetadataSnapshotCompactionReport, ObjectError> {
        let committed = self.next_commit_index.saturating_sub(1);
        if snapshot_commit_index == 0 || snapshot_commit_index > committed {
            return Err(ObjectError::InvalidArgument(
                "metadata raft snapshot compaction floor must be within committed metadata log"
                    .to_string(),
            ));
        }
        if snapshot_commit_index < self.raft_snapshot_floor_commit_index {
            return Err(ObjectError::InvalidArgument(
                "metadata raft snapshot compaction floor must not move backward".to_string(),
            ));
        }

        let safe_sequence = self
            .change_checkpoints
            .values()
            .map(|checkpoint| checkpoint.after_change_sequence)
            .min()
            .unwrap_or_else(|| self.next_change_sequence.saturating_sub(1));
        let mut compacted_change_records = 0u64;
        let mut retained_changes = Vec::with_capacity(self.changes.len());
        for change in self.changes.drain(..) {
            let eligible = change.commit_index <= snapshot_commit_index
                && change.sequence <= safe_sequence
                && (max_change_records == 0
                    || compacted_change_records < max_change_records as u64);
            if eligible {
                compacted_change_records = compacted_change_records.saturating_add(1);
            } else {
                retained_changes.push(change);
            }
        }
        self.changes = retained_changes;

        let mut compacted_batch_receipts = 0u64;
        if !self.batch_request_receipts.is_empty() {
            let mut receipt_keys: Vec<_> = self
                .batch_request_receipts
                .iter()
                .filter_map(|(key, receipt)| {
                    (receipt.last_commit_index <= snapshot_commit_index).then(|| key.clone())
                })
                .collect();
            receipt_keys.sort_by(|left, right| {
                let left_receipt = &self.batch_request_receipts[left];
                let right_receipt = &self.batch_request_receipts[right];
                left_receipt
                    .last_commit_index
                    .cmp(&right_receipt.last_commit_index)
                    .then_with(|| {
                        left_receipt
                            .first_commit_index
                            .cmp(&right_receipt.first_commit_index)
                    })
                    .then_with(|| left.cmp(right))
            });
            for key in receipt_keys {
                if max_batch_receipts != 0 && compacted_batch_receipts >= max_batch_receipts as u64
                {
                    break;
                }
                if self.batch_request_receipts.remove(&key).is_some() {
                    compacted_batch_receipts = compacted_batch_receipts.saturating_add(1);
                }
            }
        }

        let mut remaining_receipt_budget = max_batch_receipts
            .checked_sub(compacted_batch_receipts as usize)
            .unwrap_or(0);
        let mut compacted_lease_receipts = 0u64;
        if !self.lease_receipts.is_empty()
            && (max_batch_receipts == 0 || remaining_receipt_budget > 0)
        {
            let mut receipt_keys: Vec<_> = self
                .lease_receipts
                .iter()
                .filter_map(|(key, receipt)| {
                    (receipt.commit_index <= snapshot_commit_index).then(|| key.clone())
                })
                .collect();
            receipt_keys.sort_by(|left, right| {
                let left_receipt = &self.lease_receipts[left];
                let right_receipt = &self.lease_receipts[right];
                left_receipt
                    .commit_index
                    .cmp(&right_receipt.commit_index)
                    .then_with(|| left.cmp(right))
            });
            for key in receipt_keys {
                if max_batch_receipts != 0 && remaining_receipt_budget == 0 {
                    break;
                }
                if self.lease_receipts.remove(&key).is_some() {
                    compacted_lease_receipts = compacted_lease_receipts.saturating_add(1);
                    remaining_receipt_budget = remaining_receipt_budget.saturating_sub(1);
                }
            }
        }

        let mut compacted_coordinator_config_receipts = 0u64;
        if !self.coordinator_config_receipts.is_empty()
            && (max_batch_receipts == 0 || remaining_receipt_budget > 0)
        {
            let mut receipt_keys: Vec<_> = self
                .coordinator_config_receipts
                .iter()
                .filter_map(|(key, receipt)| {
                    (receipt.commit_index <= snapshot_commit_index).then(|| key.clone())
                })
                .collect();
            receipt_keys.sort_by(|left, right| {
                let left_receipt = &self.coordinator_config_receipts[left];
                let right_receipt = &self.coordinator_config_receipts[right];
                left_receipt
                    .commit_index
                    .cmp(&right_receipt.commit_index)
                    .then_with(|| left.cmp(right))
            });
            for key in receipt_keys {
                if max_batch_receipts != 0 && remaining_receipt_budget == 0 {
                    break;
                }
                if self.coordinator_config_receipts.remove(&key).is_some() {
                    compacted_coordinator_config_receipts =
                        compacted_coordinator_config_receipts.saturating_add(1);
                    remaining_receipt_budget = remaining_receipt_budget.saturating_sub(1);
                }
            }
        }

        let mut compacted_coordinator_session_receipts = 0u64;
        if !self.coordinator_session_receipts.is_empty()
            && (max_batch_receipts == 0 || remaining_receipt_budget > 0)
        {
            let mut receipt_keys: Vec<_> = self
                .coordinator_session_receipts
                .iter()
                .filter_map(|(key, receipt)| {
                    (receipt.commit_index <= snapshot_commit_index).then(|| key.clone())
                })
                .collect();
            receipt_keys.sort_by(|left, right| {
                let left_receipt = &self.coordinator_session_receipts[left];
                let right_receipt = &self.coordinator_session_receipts[right];
                left_receipt
                    .commit_index
                    .cmp(&right_receipt.commit_index)
                    .then_with(|| left.cmp(right))
            });
            for key in receipt_keys {
                if max_batch_receipts != 0 && remaining_receipt_budget == 0 {
                    break;
                }
                if self.coordinator_session_receipts.remove(&key).is_some() {
                    compacted_coordinator_session_receipts =
                        compacted_coordinator_session_receipts.saturating_add(1);
                    remaining_receipt_budget = remaining_receipt_budget.saturating_sub(1);
                }
            }
        }

        let mut compacted_shard_receipts = 0u64;
        if !self.shard_receipts.is_empty()
            && (max_batch_receipts == 0 || remaining_receipt_budget > 0)
        {
            let mut receipt_keys: Vec<_> = self
                .shard_receipts
                .iter()
                .filter_map(|(key, receipt)| {
                    (receipt.commit_index <= snapshot_commit_index).then(|| key.clone())
                })
                .collect();
            receipt_keys.sort_by(|left, right| {
                let left_receipt = &self.shard_receipts[left];
                let right_receipt = &self.shard_receipts[right];
                left_receipt
                    .commit_index
                    .cmp(&right_receipt.commit_index)
                    .then_with(|| left.cmp(right))
            });
            for key in receipt_keys {
                if max_batch_receipts != 0 && remaining_receipt_budget == 0 {
                    break;
                }
                if self.shard_receipts.remove(&key).is_some() {
                    compacted_shard_receipts = compacted_shard_receipts.saturating_add(1);
                    remaining_receipt_budget = remaining_receipt_budget.saturating_sub(1);
                }
            }
        }

        let mut compacted_transaction_receipts = 0u64;
        if !self.transaction_receipts.is_empty()
            && (max_batch_receipts == 0 || remaining_receipt_budget > 0)
        {
            let mut receipt_keys: Vec<_> = self
                .transaction_receipts
                .iter()
                .filter_map(|(key, receipt)| {
                    (receipt.commit_index <= snapshot_commit_index).then(|| key.clone())
                })
                .collect();
            receipt_keys.sort_by(|left, right| {
                let left_receipt = &self.transaction_receipts[left];
                let right_receipt = &self.transaction_receipts[right];
                left_receipt
                    .commit_index
                    .cmp(&right_receipt.commit_index)
                    .then_with(|| left.cmp(right))
            });
            for key in receipt_keys {
                if max_batch_receipts != 0 && remaining_receipt_budget == 0 {
                    break;
                }
                if self.transaction_receipts.remove(&key).is_some() {
                    compacted_transaction_receipts =
                        compacted_transaction_receipts.saturating_add(1);
                    remaining_receipt_budget = remaining_receipt_budget.saturating_sub(1);
                }
            }
        }

        self.raft_snapshot_floor_commit_index = snapshot_commit_index;
        self.raft_snapshot_compacted_change_count = self
            .raft_snapshot_compacted_change_count
            .saturating_add(compacted_change_records);
        self.raft_snapshot_compacted_batch_receipt_count = self
            .raft_snapshot_compacted_batch_receipt_count
            .saturating_add(compacted_batch_receipts)
            .saturating_add(compacted_lease_receipts)
            .saturating_add(compacted_coordinator_config_receipts)
            .saturating_add(compacted_coordinator_session_receipts)
            .saturating_add(compacted_shard_receipts)
            .saturating_add(compacted_transaction_receipts);
        self.next_commit_index = self.next_commit_index.saturating_add(1);

        Ok(MetadataSnapshotCompactionReport {
            raft_snapshot_floor_commit_index: self.raft_snapshot_floor_commit_index,
            compacted_change_records,
            compacted_batch_receipts,
            compacted_lease_receipts,
            compacted_coordinator_config_receipts,
            compacted_coordinator_session_receipts,
            compacted_shard_receipts,
            compacted_transaction_receipts,
            raft_snapshot_compacted_change_count: self.raft_snapshot_compacted_change_count,
            raft_snapshot_compacted_batch_receipt_count: self
                .raft_snapshot_compacted_batch_receipt_count,
            remaining_changes: self.changes.len() as u64,
            remaining_batch_receipts: self.batch_request_receipts.len() as u64,
            remaining_lease_receipts: self.lease_receipts.len() as u64,
            remaining_coordinator_config_receipts: self.coordinator_config_receipts.len() as u64,
            remaining_coordinator_session_receipts: self.coordinator_session_receipts.len() as u64,
            remaining_shard_receipts: self.shard_receipts.len() as u64,
            remaining_transaction_receipts: self.transaction_receipts.len() as u64,
        })
    }
}
