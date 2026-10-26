// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn put_object_with_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
    ) -> Result<(ObjectMetadata, ObjectPlacement), ObjectError> {
        let content_type = content_type.into();
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let journal_payload = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(Self::build_object_journal_payload(
                bucket,
                key,
                &data,
                &content_type,
                update_time_ms,
                &options,
                false,
                Some(replica_count),
            ))
        };
        let metadata = self.apply_put_with_placement(
            bucket.to_string(),
            key.to_string(),
            data,
            content_type,
            update_time_ms,
            options,
            replica_count,
        )?;
        if let Some(journal_payload) = journal_payload {
            if let Err(err) = self.append_object_journal_payload(&journal_payload) {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        let placement = self.meta_service.object_placement(&metadata.object_id)?;
        Ok((metadata, placement))
    }

}
