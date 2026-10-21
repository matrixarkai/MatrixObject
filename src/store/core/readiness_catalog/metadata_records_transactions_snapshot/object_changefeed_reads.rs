// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn list_object_changes_after(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ObjectChangeRecord>, ObjectError> {
        Ok(self
            .list_object_changes_page_after(after_sequence, limit, 0)?
            .records)
    }

    pub fn list_object_changes_page_after(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectChangePage, ObjectError> {
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "object changefeed limit must be positive".to_string(),
            ));
        }
        let mut page = ObjectChangePage {
            next_sequence: after_sequence,
            max_payload_bytes,
            ..ObjectChangePage::default()
        };
        if let Some(first_change) = self.object_changes.first() {
            page.retained_floor_sequence = first_change.sequence.saturating_sub(1);
            page.resync_from_sequence = page.retained_floor_sequence;
            if after_sequence < page.retained_floor_sequence {
                page.cursor_compacted = true;
                page.resync_required = true;
            }
        } else if self.next_object_change_sequence > 1 {
            page.retained_floor_sequence = self.next_object_change_sequence.saturating_sub(1);
            page.resync_from_sequence = page.retained_floor_sequence;
            if after_sequence < page.retained_floor_sequence {
                page.cursor_compacted = true;
                page.resync_required = true;
            }
        }
        let start = self
            .object_changes
            .partition_point(|change| change.sequence <= after_sequence);
        for change in self.object_changes[start..].iter() {
            if page.records.len() >= limit {
                page.truncated = true;
                break;
            }
            let record_payload_bytes = object_change_record_payload_bytes(change);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(record_payload_bytes) > max_payload_bytes
            {
                if page.records.is_empty() {
                    return Err(ObjectError::InvalidArgument(
                        "object changefeed first record exceeds max_payload_bytes".to_string(),
                    ));
                }
                page.truncated = true;
                page.budget_limited = true;
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(record_payload_bytes);
            page.next_sequence = change.sequence;
            page.records.push(change.clone());
        }
        stamp_object_change_page_proof(
            after_sequence,
            limit,
            "ListObjectChanges",
            &mut page,
            None,
        )?;
        Ok(page)
    }

    pub fn list_object_changes_after_with_metadata_read(
        &self,
        after_sequence: u64,
        limit: usize,
        options: MetadataReadOptions,
    ) -> Result<(Vec<ObjectChangeRecord>, MetadataRaftReadiness), ObjectError> {
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object changefeed read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        let records = self.list_object_changes_after(after_sequence, limit)?;
        Ok((records, readiness))
    }

    pub fn list_object_changes_page_after_with_metadata_read(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectChangePage, MetadataRaftReadiness), ObjectError> {
        self.list_object_changes_page_after_with_metadata_read_change_page_proof_fenced(
            after_sequence,
            limit,
            max_payload_bytes,
            options,
            None,
        )
    }

    pub fn list_object_changes_page_after_with_metadata_read_change_page_proof_fenced(
        &self,
        after_sequence: u64,
        limit: usize,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
        expected_object_change_page_proof_checksum: Option<&str>,
    ) -> Result<(ObjectChangePage, MetadataRaftReadiness), ObjectError> {
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object changefeed read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        let mut page =
            self.list_object_changes_page_after(after_sequence, limit, max_payload_bytes)?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_change_page_proof(
            after_sequence,
            limit,
            "ListObjectChanges",
            &mut page,
            expected_object_change_page_proof_checksum,
        )?;
        Ok((page, readiness))
    }
}
