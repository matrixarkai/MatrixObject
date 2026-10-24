// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn plan_tombstone_compaction(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        self.collect_tombstone_compaction(prefix, after_key, max_records, max_compact_bytes, false)
            .map(|(report, _)| report)
    }

    pub fn compact_tombstones(
        &mut self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        let (mut report, remove_keys) = self.collect_tombstone_compaction(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
            true,
        )?;
        if remove_keys.is_empty() {
            return Ok(report);
        }
        for key in remove_keys {
            self.records.remove(key.as_str());
        }
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        report.compacted_tombstone_count = self
            .compacted_tombstone_count
            .saturating_add(report.compacted_records);
        report.last_tombstone_compaction_commit_index = self.next_commit_index.saturating_sub(1);
        self.compacted_tombstone_count = report.compacted_tombstone_count;
        self.last_tombstone_compaction_commit_index = report.last_tombstone_compaction_commit_index;
        Ok(report)
    }

    pub fn prepare_tombstone_compaction_raft_entry(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataTombstoneCompaction, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata tombstone compaction expected previous commit {} does not match current {}",
                expected_previous_commit_index,
                self.next_commit_index.saturating_sub(1)
            )));
        }
        let (mut report, remove_keys) = self.collect_tombstone_compaction(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
            true,
        )?;
        let compacted: Vec<MetadataRecord> = remove_keys
            .iter()
            .filter_map(|key| self.record(key.as_str()).cloned())
            .collect();
        if compacted.len() != remove_keys.len() {
            return Err(ObjectError::Corruption(
                "metadata tombstone compaction selected a missing record".to_string(),
            ));
        }
        let prepared_commit_index = if compacted.is_empty() {
            expected_previous_commit_index
        } else {
            expected_previous_commit_index.saturating_add(1)
        };
        if !compacted.is_empty() {
            report.compacted_tombstone_count = self
                .compacted_tombstone_count
                .saturating_add(report.compacted_records);
            report.last_tombstone_compaction_commit_index = prepared_commit_index;
        }
        let request_checksum = metadata_tombstone_compaction_request_checksum(
            prefix,
            after_key,
            max_records,
            max_compact_bytes,
        );
        let mut prepared = PreparedMetadataTombstoneCompaction {
            prefix: prefix.to_string(),
            after_key: after_key.map(ToString::to_string),
            max_records,
            max_compact_bytes,
            report,
            compacted,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_tombstone_compaction_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_tombstone_compaction(
        &mut self,
        prepared: PreparedMetadataTombstoneCompaction,
    ) -> Result<MetadataTombstoneCompactionReport, ObjectError> {
        let expected_checksum =
            prepared_metadata_tombstone_compaction_checksum(&PreparedMetadataTombstoneCompaction {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared metadata tombstone compaction checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_tombstone_compaction_request_checksum(
                &prepared.prefix,
                prepared.after_key.as_deref(),
                prepared.max_records,
                prepared.max_compact_bytes,
            )
        {
            return Err(ObjectError::Corruption(
                "prepared metadata tombstone compaction request checksum mismatch".to_string(),
            ));
        }
        if !prepared.compacted.is_empty()
            && prepared.prepared_commit_index <= self.next_commit_index.saturating_sub(1)
            && prepared
                .compacted
                .iter()
                .all(|record| !self.records.contains(record.key.as_str()))
        {
            return Ok(prepared.report);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared metadata tombstone compaction expected previous commit {} does not match current {}",
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
                "prepared metadata tombstone compaction commit index mismatch".to_string(),
            ));
        }
        let current = self.prepare_tombstone_compaction_raft_entry(
            &prepared.prefix,
            prepared.after_key.as_deref(),
            prepared.max_records,
            prepared.max_compact_bytes,
            prepared.expected_previous_commit_index,
        )?;
        if current.report != prepared.report || current.compacted != prepared.compacted {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata tombstone compaction no longer matches current state"
                    .to_string(),
            ));
        }
        if prepared.compacted.is_empty() {
            return Ok(prepared.report);
        }
        for record in &prepared.compacted {
            match self.record(record.key.as_str()) {
                Some(current) if current == record => {}
                _ => {
                    return Err(ObjectError::PreconditionFailed(
                        "prepared metadata tombstone compaction record image changed".to_string(),
                    ));
                }
            }
        }
        for record in &prepared.compacted {
            self.records.remove(record.key.as_str());
        }
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        self.compacted_tombstone_count = prepared.report.compacted_tombstone_count;
        self.last_tombstone_compaction_commit_index =
            prepared.report.last_tombstone_compaction_commit_index;
        Ok(prepared.report)
    }
}
