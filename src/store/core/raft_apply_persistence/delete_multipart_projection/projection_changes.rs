impl MatrixObjectStore {
    fn commit_meta_projection(
        &mut self,
        object_id: &ObjectId,
        value: &str,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<MetadataRecord, ObjectError> {
        let key = meta_object_key(object_id);
        let expected_version = self
            .meta_service
            .records
            .get(key.as_str())
            .map_or(0, |record| record.version);
        self.meta_service.commit_record(
            &key,
            value.to_string(),
            expected_version,
            lease_owner,
            lease_epoch,
            now_ms,
        )
    }

    fn append_object_change(&mut self, operation: &str, metadata: &ObjectMetadata) {
        let sequence = self.next_object_change_sequence;
        self.next_object_change_sequence = self.next_object_change_sequence.saturating_add(1);
        let previous_sequence = self
            .object_changes
            .last()
            .map_or(0, |change| change.sequence);
        self.bound_unconsumed_object_changes();
        self.object_changes.push(ObjectChangeRecord {
            sequence,
            previous_sequence,
            operation: operation.to_string(),
            object_id: metadata.object_id.clone(),
            version: metadata.version,
            length: metadata.length,
            etag: metadata.etag.clone(),
            deleted: metadata.deleted,
            event_time_ms: metadata.update_time_ms,
        });
    }

    /// Trim the feed when nothing is subscribed to it.
    ///
    /// Called before the push rather than after, so the vector never doubles
    /// past the bound and its capacity settles at the bound instead of twice
    /// it. `last()` is what `previous_sequence` reads and trimming the front
    /// does not move it.
    fn bound_unconsumed_object_changes(&mut self) {
        if !self.object_change_cursors.is_empty() {
            return;
        }
        let len = self.object_changes.len();
        if len < UNCONSUMED_OBJECT_CHANGE_RETENTION {
            return;
        }
        // Room for the record about to be pushed, so the length afterwards is
        // the bound exactly.
        let overflow = len + 1 - UNCONSUMED_OBJECT_CHANGE_RETENTION;
        self.object_changes.drain(0..overflow);
        if overflow > 1 {
            // Steady state drops exactly one record per append, so this is
            // reached only once: by a feed loaded from a snapshot taken before
            // the bound existed, which converges in a single step. Shrinking
            // on every append would instead make each push reallocate.
            self.object_changes
                .shrink_to(UNCONSUMED_OBJECT_CHANGE_RETENTION);
        }
    }

    fn replace_last_object_change_metadata(&mut self, operation: &str, metadata: &ObjectMetadata) {
        let Some(change) = self.object_changes.last_mut() else {
            return;
        };
        if change.operation != operation || change.object_id != metadata.object_id {
            return;
        }
        change.version = metadata.version;
        change.length = metadata.length;
        change.etag = metadata.etag.clone();
        change.deleted = metadata.deleted;
        change.event_time_ms = metadata.update_time_ms;
    }

}
