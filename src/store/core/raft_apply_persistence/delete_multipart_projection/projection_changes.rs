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
            .get(&key)
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
