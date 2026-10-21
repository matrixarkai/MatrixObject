// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        if version == 0 {
            return Err(ObjectError::InvalidArgument(
                "version must be positive".to_string(),
            ));
        }
        let object_id = ObjectId::new(bucket, key);
        let metadata = self
            .objects
            .get(&object_id)
            .filter(|metadata| metadata.version == version)
            .or_else(|| {
                self.versions.get(&object_id).and_then(|versions| {
                    versions.iter().find(|metadata| metadata.version == version)
                })
            })
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(format!("{}/{}#{}", bucket, key, version)))?;
        if metadata.deleted {
            return Err(ObjectError::NotFound(format!(
                "{}/{}#{}",
                bucket, key, version
            )));
        }
        Ok(metadata)
    }

    pub fn head_object_version_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let object_id = ObjectId::new(bucket, key);
        let readiness = self.prepare_metadata_read(&meta_object_key(&object_id), &options)?;
        let metadata = self.head_object_version(bucket, key, version)?;
        Ok((metadata, readiness))
    }

    pub fn head_object_conditional(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<ConditionalObjectMetadata, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        Ok(ConditionalObjectMetadata {
            metadata,
            not_modified,
        })
    }

    pub fn head_object_conditional_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(ConditionalObjectMetadata, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        Ok((
            ConditionalObjectMetadata {
                metadata,
                not_modified,
            },
            readiness,
        ))
    }

    pub fn head_object_version_conditional(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<ConditionalObjectMetadata, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        Ok(ConditionalObjectMetadata {
            metadata,
            not_modified,
        })
    }

    pub fn head_object_version_conditional_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(ConditionalObjectMetadata, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        Ok((
            ConditionalObjectMetadata {
                metadata,
                not_modified,
            },
            readiness,
        ))
    }
}
