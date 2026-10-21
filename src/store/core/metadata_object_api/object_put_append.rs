// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn put_object(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.put_object_with_options(bucket, key, data, content_type, PutOptions::default())
    }

    pub fn put_object_with_options(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_put_options_read_fence("put object", &options)?;
        let content_type = content_type.into();
        let update_time_ms = now_millis();
        if self.persistent_journal_path.is_empty() {
            return self.apply_command(ObjectCommand::PutObject {
                bucket: bucket.to_string(),
                key: key.to_string(),
                data,
                content_type,
                update_time_ms,
                options,
            });
        }
        let rollback = Some(self.capture_object_mutation_rollback());
        let journal_payload = Self::build_object_journal_payload(
            bucket,
            key,
            &data,
            &content_type,
            update_time_ms,
            &options,
            false,
            None,
        );
        let command = ObjectCommand::PutObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            data,
            content_type,
            update_time_ms,
            options,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_object_journal_payload(&journal_payload) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn append_object(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.append_object_with_options(bucket, key, data, content_type, PutOptions::default())
    }

    pub fn append_object_with_options(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_put_options_read_fence("append object", &options)?;
        let content_type = content_type.into();
        let update_time_ms = now_millis();
        if self.persistent_journal_path.is_empty() {
            return self.apply_append(
                bucket.to_string(),
                key.to_string(),
                data,
                content_type,
                update_time_ms,
                options,
            );
        }
        let rollback = Some(self.capture_object_mutation_rollback());
        let journal_payload = Self::build_object_journal_payload(
            bucket,
            key,
            &data,
            &content_type,
            update_time_ms,
            &options,
            true,
            None,
        );
        let metadata = self.apply_append(
            bucket.to_string(),
            key.to_string(),
            data,
            content_type,
            update_time_ms,
            options,
        )?;
        if let Err(err) = self.append_object_journal_payload(&journal_payload) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn append_object_with_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        let content_type = content_type.into();
        let update_time_ms = now_millis();
        if self.persistent_journal_path.is_empty() {
            return self.apply_append_with_root_placement(
                bucket.to_string(),
                key.to_string(),
                data,
                content_type,
                update_time_ms,
                options,
                replica_count,
            );
        }
        let rollback = Some(self.capture_object_mutation_rollback());
        let journal_payload = Self::build_object_journal_payload(
            bucket,
            key,
            &data,
            &content_type,
            update_time_ms,
            &options,
            true,
            Some(replica_count),
        );
        let metadata = self.apply_append_with_root_placement(
            bucket.to_string(),
            key.to_string(),
            data,
            content_type,
            update_time_ms,
            options,
            replica_count,
        )?;
        if let Err(err) = self.append_object_journal_payload(&journal_payload) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

}
