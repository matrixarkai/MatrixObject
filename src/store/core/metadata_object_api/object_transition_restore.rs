// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn transition_object(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let storage_class = storage_class.into();
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::TransitionObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            storage_class: storage_class.clone(),
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_object_transition_journal_record(
            bucket,
            key,
            &storage_class,
            update_time_ms,
            None,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn transition_object_with_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let storage_class = storage_class.into();
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::TransitionObjectWithRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            storage_class: storage_class.clone(),
            update_time_ms,
            replica_count,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_object_transition_journal_record(
            bucket,
            key,
            &storage_class,
            update_time_ms,
            Some(replica_count),
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn restore_object(
        &mut self,
        bucket: &str,
        key: &str,
        restore_for_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let now_ms = now_millis();
        let restore_until_ms = now_ms.checked_add(restore_for_ms).ok_or_else(|| {
            ObjectError::InvalidArgument("restore window overflows u64".to_string())
        })?;
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::RestoreObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            restore_until_ms,
            update_time_ms: now_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) =
            self.append_object_restore_journal_record(bucket, key, restore_until_ms, now_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn restore_object_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::RestoreObjectVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_object_version_restore_journal_record(
            bucket,
            key,
            version,
            update_time_ms,
            None,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn restore_object_version_with_placement(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::RestoreObjectVersionWithRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
            replica_count,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_object_version_restore_journal_record(
            bucket,
            key,
            version,
            update_time_ms,
            Some(replica_count),
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

}
