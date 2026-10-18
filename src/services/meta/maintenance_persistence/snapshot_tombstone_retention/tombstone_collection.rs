impl MatrixMetaService {
    fn collect_tombstone_compaction(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_compact_bytes: u64,
        collect_keys: bool,
    ) -> Result<(MetadataTombstoneCompactionReport, Vec<String>), ObjectError> {
        validate_meta_key(prefix)?;
        if let Some(after_key) = after_key {
            validate_meta_key(after_key)?;
            if !after_key.starts_with(prefix) {
                return Err(ObjectError::InvalidArgument(
                    "metadata tombstone marker must be within prefix".to_string(),
                ));
            }
        }
        if max_records == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata tombstone compaction max_records must be positive".to_string(),
            ));
        }
        let safe_sequence = self
            .change_checkpoints
            .values()
            .filter(|checkpoint| checkpoint.prefix == prefix)
            .map(|checkpoint| checkpoint.after_change_sequence)
            .min()
            .unwrap_or(0);
        let mut report = MetadataTombstoneCompactionReport {
            safe_sequence,
            compacted_tombstone_count: self.compacted_tombstone_count,
            last_tombstone_compaction_commit_index: self.last_tombstone_compaction_commit_index,
            ..MetadataTombstoneCompactionReport::default()
        };
        let mut remove_keys = Vec::new();
        let start = after_key.unwrap_or(prefix).to_string();
        let mut last_selected_key: Option<String> = None;
        for (key, record) in self.records.range::<str, _>((
                std::ops::Bound::Included(start.as_str()),
                std::ops::Bound::Unbounded,
            )) {
            if !key.starts_with(prefix) {
                break;
            }
            if after_key.map_or(false, |marker| key.as_str() <= marker) {
                continue;
            }
            report.scanned_records = report.scanned_records.saturating_add(1);
            if report.compacted_records as usize >= max_records {
                report.truncated = true;
                report.next_key_marker = last_selected_key;
                break;
            }
            if !record.deleted {
                report.skipped_records = report.skipped_records.saturating_add(1);
                continue;
            }
            if !self.tombstone_delete_change_is_checkpointed(record, safe_sequence) {
                report.skipped_records = report.skipped_records.saturating_add(1);
                continue;
            }
            let tombstone_bytes = metadata_record_payload_bytes(record);
            if max_compact_bytes > 0
                && report.compacted_bytes.saturating_add(tombstone_bytes) > max_compact_bytes
            {
                report.budget_limited = true;
                report.next_key_marker = last_selected_key;
                break;
            }
            report.compactable_records = report.compactable_records.saturating_add(1);
            report.compacted_records = report.compacted_records.saturating_add(1);
            report.compacted_bytes = report.compacted_bytes.saturating_add(tombstone_bytes);
            last_selected_key = Some(key.to_string());
            if collect_keys {
                remove_keys.push(key.to_string());
            }
        }
        Ok((report, remove_keys))
    }

    fn tombstone_delete_change_is_checkpointed(
        &self,
        record: &MetadataRecord,
        safe_sequence: u64,
    ) -> bool {
        if safe_sequence == 0 {
            return false;
        }
        self.changes.iter().any(|change| {
            change.key == record.key
                && change.version == record.version
                && change.deleted
                && change.sequence <= safe_sequence
        })
    }
}
