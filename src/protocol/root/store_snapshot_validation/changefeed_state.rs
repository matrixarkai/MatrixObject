// SPDX-License-Identifier: Apache-2.0
impl StoreSnapshot {
    fn validate_changefeed_state(&self) -> Result<(), ObjectError> {
        let mut previous_sequence = 0u64;
        for change in &self.object_changes {
            validate_bucket(&change.object_id.bucket)?;
            validate_key(&change.object_id.key)?;
            if change.sequence == 0 || change.sequence <= previous_sequence {
                return Err(ObjectError::Corruption(
                    "snapshot object changes are not strictly ordered".to_string(),
                ));
            }
            if change.sequence >= self.next_object_change_sequence {
                return Err(ObjectError::Corruption(
                    "snapshot object change sequence exceeds next sequence".to_string(),
                ));
            }
            previous_sequence = change.sequence;
        }
        for (consumer_id, cursor) in &self.object_change_cursors {
            validate_changefeed_consumer_id(consumer_id)?;
            if &cursor.consumer_id != consumer_id {
                return Err(ObjectError::Corruption(
                    "snapshot object change cursor key mismatch".to_string(),
                ));
            }
            if cursor.sequence >= self.next_object_change_sequence {
                return Err(ObjectError::Corruption(
                    "snapshot object change cursor is ahead of changefeed".to_string(),
                ));
            }
        }
        Ok(())
    }
}
