// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn compact_object_changefeed(
        &mut self,
        max_records: u64,
    ) -> Result<ObjectChangefeedCompactionReport, ObjectError> {
        let mut report = ObjectChangefeedCompactionReport::default();
        if self.object_changes.is_empty() || self.object_change_cursors.is_empty() {
            return Ok(report);
        }
        let safe_floor = self
            .object_change_cursors
            .values()
            .map(|cursor| cursor.sequence)
            .min()
            .unwrap_or(0);
        if safe_floor == 0 {
            return Ok(report);
        }
        report.safe_floor_sequence = safe_floor;
        report.cursor_count = self.object_change_cursors.len() as u64;
        let mut compact_count = 0usize;
        for change in &self.object_changes {
            if change.sequence > safe_floor {
                break;
            }
            if max_records > 0 && compact_count as u64 >= max_records {
                report.limited = true;
                break;
            }
            report.compacted_records = report.compacted_records.saturating_add(1);
            report.compacted_bytes = report
                .compacted_bytes
                .saturating_add(object_change_record_payload_bytes(change));
            compact_count += 1;
        }
        if compact_count > 0 {
            self.object_changes.drain(0..compact_count);
            // `drain` moves the tail down and keeps the allocation, so without
            // this the feed holds its high-water mark for the life of the
            // process. Shrinking here costs one copy per compaction, not one
            // per append, which is why it is done on this path rather than by
            // growing the vector exactly.
            self.object_changes.shrink_to_fit();
        }
        for change in &self.object_changes {
            report.retained_records = report.retained_records.saturating_add(1);
            report.retained_bytes = report
                .retained_bytes
                .saturating_add(object_change_record_payload_bytes(change));
            if change.sequence <= safe_floor {
                report.limited = true;
            }
        }
        Ok(report)
    }

    pub fn scan_metadata_records(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<MetadataRecordPage, ObjectError> {
        self.meta_service
            .scan_records(prefix, after_key, max_records, max_payload_bytes)
    }

    pub fn scan_metadata_records_with_metadata_read(
        &self,
        prefix: &str,
        after_key: Option<&str>,
        max_records: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(MetadataRecordPage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.prepare_metadata_read(prefix, &options)?;
        let page = self.scan_metadata_records(prefix, after_key, max_records, max_payload_bytes)?;
        Ok((page, readiness))
    }
}
