// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn object_change_cursor_with_metadata_read(
        &self,
        consumer_id: &str,
        options: MetadataReadOptions,
    ) -> Result<(ObjectChangeCursor, MetadataRaftReadiness), ObjectError> {
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object change cursor read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        let cursor = self.object_change_cursor(consumer_id)?;
        Ok((cursor, readiness))
    }

    pub fn object_change_cursor(
        &self,
        consumer_id: &str,
    ) -> Result<ObjectChangeCursor, ObjectError> {
        validate_changefeed_consumer_id(consumer_id)?;
        let compacted_floor = self
            .object_changes
            .first()
            .and_then(|change| change.sequence.checked_sub(1))
            .unwrap_or_else(|| self.next_object_change_sequence.saturating_sub(1));
        Ok(self
            .object_change_cursors
            .get(consumer_id)
            .cloned()
            .unwrap_or_else(|| ObjectChangeCursor {
                consumer_id: consumer_id.to_string(),
                sequence: compacted_floor,
                update_time_ms: 0,
            }))
    }

    pub fn commit_object_change_cursor(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
    ) -> Result<ObjectChangeCursor, ObjectError> {
        validate_changefeed_consumer_id(consumer_id)?;
        let current = self.object_change_cursor(consumer_id)?;
        if sequence < current.sequence {
            return Err(ObjectError::PreconditionFailed(
                "object change cursor cannot move backwards".to_string(),
            ));
        }
        if sequence == current.sequence {
            return Ok(current);
        }
        if sequence >= self.next_object_change_sequence {
            return Err(ObjectError::PreconditionFailed(
                "object change cursor is ahead of changefeed".to_string(),
            ));
        }
        if sequence > 0
            && !self
                .object_changes
                .iter()
                .any(|change| change.sequence == sequence)
        {
            return Err(ObjectError::PreconditionFailed(
                "object change cursor target has been compacted or is missing".to_string(),
            ));
        }
        let cursor = ObjectChangeCursor {
            consumer_id: consumer_id.to_string(),
            sequence,
            update_time_ms,
        };
        self.object_change_cursors
            .insert(consumer_id.to_string(), cursor.clone());
        Ok(cursor)
    }

    pub fn commit_object_change_cursor_with_metadata_read(
        &mut self,
        consumer_id: &str,
        sequence: u64,
        update_time_ms: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectChangeCursor, MetadataRaftReadiness), ObjectError> {
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object change cursor commit read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        let cursor = self.commit_object_change_cursor(consumer_id, sequence, update_time_ms)?;
        Ok((cursor, readiness))
    }
}
