// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn list_changes_after(&self, sequence: u64, limit: usize) -> Vec<MetadataChange> {
        self.changes
            .iter()
            .filter(|change| change.sequence > sequence)
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn page_changes(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangePage, ObjectError> {
        validate_meta_key(prefix)?;
        if max_changes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata change page max_changes must be positive".to_string(),
            ));
        }
        let mut page = MetadataChangePage {
            prefix: prefix.to_string(),
            after_sequence,
            ..MetadataChangePage::default()
        };
        for change in self
            .changes
            .iter()
            .filter(|change| change.sequence > after_sequence)
        {
            page.scanned_changes = page.scanned_changes.saturating_add(1);
            if !change.key.starts_with(prefix) {
                page.skipped_changes = page.skipped_changes.saturating_add(1);
                continue;
            }
            if page.changes.len() >= max_changes {
                page.has_more = true;
                break;
            }
            let change_payload = metadata_change_payload_bytes(change);
            if max_payload_bytes > 0
                && !page.changes.is_empty()
                && page.payload_bytes.saturating_add(change_payload) > max_payload_bytes
            {
                page.has_more = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(change_payload);
            page.max_commit_index = page.max_commit_index.max(change.commit_index);
            page.next_sequence_marker = Some(change.sequence);
            page.changes.push(change.clone());
        }
        Ok(page)
    }

    pub fn watch_changes(
        &self,
        prefix: &str,
        after_sequence: u64,
        max_changes: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataWatchPage, ObjectError> {
        validate_meta_key(prefix)?;
        if max_changes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata watch max_changes must be positive".to_string(),
            ));
        }
        let first_retained_change_sequence =
            self.changes.first().map_or(0, |change| change.sequence);
        let last_retained_change_sequence = self.changes.last().map_or(0, |change| change.sequence);
        let first_retained_commit_index =
            self.changes.first().map_or(0, |change| change.commit_index);
        let last_retained_commit_index =
            self.changes.last().map_or(0, |change| change.commit_index);
        let retained_changes_compacted = if first_retained_change_sequence > 0 {
            after_sequence.saturating_add(1) < first_retained_change_sequence
        } else {
            after_sequence.saturating_add(1) < self.next_change_sequence
        };
        let effective_after_sequence = if retained_changes_compacted {
            first_retained_change_sequence
                .saturating_sub(1)
                .max(after_sequence.min(self.next_change_sequence.saturating_sub(1)))
        } else {
            after_sequence
        };
        let page = self.page_changes(
            prefix,
            effective_after_sequence,
            max_changes,
            max_payload_bytes,
        )?;
        let resync_from_sequence = if retained_changes_compacted {
            if first_retained_change_sequence > 0 {
                first_retained_change_sequence
            } else {
                self.next_change_sequence
            }
        } else {
            page.next_sequence_marker.unwrap_or(after_sequence)
        };
        Ok(MetadataWatchPage {
            changes: page.changes,
            prefix: page.prefix,
            requested_after_sequence: after_sequence,
            effective_after_sequence,
            next_sequence_marker: page.next_sequence_marker,
            cursor_compacted: retained_changes_compacted,
            resync_required: retained_changes_compacted,
            resync_from_sequence,
            first_retained_change_sequence,
            last_retained_change_sequence,
            first_retained_commit_index,
            last_retained_commit_index,
            scanned_changes: page.scanned_changes,
            skipped_changes: page.skipped_changes,
            payload_bytes: page.payload_bytes,
            max_commit_index: page.max_commit_index,
            has_more: page.has_more,
        })
    }

    pub fn compact_changefeed(
        &mut self,
        prefix: &str,
        max_changes: usize,
    ) -> Result<MetadataChangefeedCompactionReport, ObjectError> {
        validate_meta_key(prefix)?;
        if max_changes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata changefeed compaction max_changes must be positive".to_string(),
            ));
        }
        let safe_sequence = self
            .change_checkpoints
            .values()
            .filter(|checkpoint| checkpoint.prefix == prefix)
            .map(|checkpoint| checkpoint.after_change_sequence)
            .min()
            .unwrap_or(0);
        let mut report = MetadataChangefeedCompactionReport {
            safe_sequence,
            ..MetadataChangefeedCompactionReport::default()
        };
        if safe_sequence == 0 {
            report.remaining_changes = self.changes.len() as u64;
            return Ok(report);
        }
        let mut compacted = 0usize;
        let mut retained = Vec::with_capacity(self.changes.len());
        for change in self.changes.drain(..) {
            report.scanned_changes = report.scanned_changes.saturating_add(1);
            let eligible = change.sequence <= safe_sequence && change.key.starts_with(prefix);
            if eligible && compacted < max_changes {
                compacted = compacted.saturating_add(1);
                report.compacted_changes = report.compacted_changes.saturating_add(1);
            } else {
                report.retained_changes = report.retained_changes.saturating_add(1);
                retained.push(change);
            }
        }
        report.remaining_changes = retained.len() as u64;
        self.changes = retained;
        if report.compacted_changes > 0 {
            self.next_commit_index = self.next_commit_index.saturating_add(1);
        }
        Ok(report)
    }

    pub fn prepare_changefeed_compaction_raft_entry(
        &self,
        prefix: &str,
        max_changes: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataChangefeedCompaction, ObjectError> {
        validate_meta_key(prefix)?;
        if max_changes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata changefeed compaction max_changes must be positive".to_string(),
            ));
        }
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata changefeed compaction expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let safe_sequence = self
            .change_checkpoints
            .values()
            .filter(|checkpoint| checkpoint.prefix == prefix)
            .map(|checkpoint| checkpoint.after_change_sequence)
            .min()
            .unwrap_or(0);
        let mut report = MetadataChangefeedCompactionReport {
            safe_sequence,
            ..MetadataChangefeedCompactionReport::default()
        };
        let mut compacted = Vec::new();
        if safe_sequence == 0 {
            report.remaining_changes = self.changes.len() as u64;
        } else {
            for change in &self.changes {
                report.scanned_changes = report.scanned_changes.saturating_add(1);
                let eligible = change.sequence <= safe_sequence && change.key.starts_with(prefix);
                if eligible && compacted.len() < max_changes {
                    report.compacted_changes = report.compacted_changes.saturating_add(1);
                    compacted.push(change.clone());
                } else {
                    report.retained_changes = report.retained_changes.saturating_add(1);
                }
            }
            report.remaining_changes = report.retained_changes;
        }
        let prepared_commit_index = if compacted.is_empty() {
            expected_previous_commit_index
        } else {
            expected_previous_commit_index.saturating_add(1)
        };
        let request_checksum = metadata_changefeed_compaction_request_checksum(prefix, max_changes);
        let mut prepared = PreparedMetadataChangefeedCompaction {
            prefix: prefix.to_string(),
            max_changes,
            report,
            compacted,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_changefeed_compaction_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_changefeed_compaction(
        &mut self,
        prepared: PreparedMetadataChangefeedCompaction,
    ) -> Result<MetadataChangefeedCompactionReport, ObjectError> {
        let expected_checksum = prepared_metadata_changefeed_compaction_checksum(
            &PreparedMetadataChangefeedCompaction {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            },
        );
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata changefeed compaction checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_changefeed_compaction_request_checksum(
                &prepared.prefix,
                prepared.max_changes,
            )
        {
            return Err(ObjectError::Corruption(
                "prepared metadata changefeed compaction request checksum mismatch".to_string(),
            ));
        }
        if !prepared.compacted.is_empty()
            && prepared.prepared_commit_index <= self.next_commit_index.saturating_sub(1)
            && prepared
                .compacted
                .iter()
                .all(|compacted| !self.changes.iter().any(|change| change == compacted))
        {
            return Ok(prepared.report);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata changefeed compaction expected previous commit {} does not match current {}",
                prepared.expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let expected_commit_index = if prepared.compacted.is_empty() {
            prepared.expected_previous_commit_index
        } else {
            prepared.expected_previous_commit_index.saturating_add(1)
        };
        if prepared.prepared_commit_index != expected_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata changefeed compaction commit index mismatch".to_string(),
            ));
        }
        let current = self.prepare_changefeed_compaction_raft_entry(
            &prepared.prefix,
            prepared.max_changes,
            prepared.expected_previous_commit_index,
        )?;
        if current.report != prepared.report || current.compacted != prepared.compacted {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata changefeed compaction no longer matches current state"
                    .to_string(),
            ));
        }
        if prepared.compacted.is_empty() {
            return Ok(prepared.report);
        }
        let mut compacted_index = 0usize;
        let mut retained = Vec::with_capacity(self.changes.len());
        for change in self.changes.drain(..) {
            if compacted_index < prepared.compacted.len()
                && change == prepared.compacted[compacted_index]
            {
                compacted_index = compacted_index.saturating_add(1);
            } else {
                retained.push(change);
            }
        }
        if compacted_index != prepared.compacted.len() {
            return Err(ObjectError::Corruption(
                "prepared metadata changefeed compaction compacted set was incomplete".to_string(),
            ));
        }
        self.changes = retained;
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(prepared.report)
    }
}
