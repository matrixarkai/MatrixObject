// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn copy_object(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.copy_object_with_options(
            source_bucket,
            source_key,
            destination_bucket,
            destination_key,
            None,
            PutOptions::default(),
        )
    }

    pub fn copy_object_with_options(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.copy_object_version_with_options(
            source_bucket,
            source_key,
            None,
            destination_bucket,
            destination_key,
            content_type,
            options,
        )
    }

    pub fn copy_object_version_with_options(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::CopyObject {
            source_bucket: source_bucket.to_string(),
            source_key: source_key.to_string(),
            source_version,
            destination_bucket: destination_bucket.to_string(),
            destination_key: destination_key.to_string(),
            content_type: content_type.clone(),
            update_time_ms,
            options: options.clone(),
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_copy_journal_record(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type.as_deref(),
            update_time_ms,
            &options,
            None,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn copy_object_with_root_placement(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
    ) -> Result<ObjectMetadata, ObjectError> {
        let update_time_ms = now_millis();
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let command = ObjectCommand::CopyObjectWithRootPlacement {
            source_bucket: source_bucket.to_string(),
            source_key: source_key.to_string(),
            source_version,
            destination_bucket: destination_bucket.to_string(),
            destination_key: destination_key.to_string(),
            content_type: content_type.clone(),
            update_time_ms,
            options: options.clone(),
            replica_count,
        };
        let metadata = self.apply_command(command)?;
        if let Err(err) = self.append_copy_journal_record(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type.as_deref(),
            update_time_ms,
            &options,
            Some(replica_count),
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(metadata)
    }

    pub fn copy_object_with_metadata_read(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        read_options: MetadataReadOptions,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.copy_object_version_with_metadata_read(
            source_bucket,
            source_key,
            None,
            destination_bucket,
            destination_key,
            content_type,
            options,
            read_options,
        )
    }

    pub fn copy_object_version_with_metadata_read(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        read_options: MetadataReadOptions,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        let readiness = match source_version {
            Some(version) => {
                self.head_object_version_with_metadata_read(
                    source_bucket,
                    source_key,
                    version,
                    read_options,
                )?
                .1
            }
            None => {
                self.head_object_with_metadata_read(source_bucket, source_key, read_options)?
                    .1
            }
        };
        let metadata = self.copy_object_version_with_options(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            options,
        )?;
        Ok((metadata, readiness))
    }

}
