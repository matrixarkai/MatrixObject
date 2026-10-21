// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectData, ObjectError> {
        self.get_object_range_bounded(bucket, key, offset, length, 0)
    }

    pub fn get_object_range_bounded(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok(ObjectData { metadata, data })
    }

    pub fn get_object_version_range_bounded(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ObjectData, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok(ObjectData { metadata, data })
    }

    pub fn get_object_range_conditional(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<ConditionalObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok(ConditionalObjectData {
                metadata,
                data: None,
                not_modified: true,
            });
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok(ConditionalObjectData {
            metadata,
            data: Some(data),
            not_modified: false,
        })
    }

    pub fn get_object_version_range_conditional(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<ConditionalObjectData, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok(ConditionalObjectData {
                metadata,
                data: None,
                not_modified: true,
            });
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok(ConditionalObjectData {
            metadata,
            data: Some(data),
            not_modified: false,
        })
    }

    pub fn get_object_range_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok((ObjectData { metadata, data }, readiness))
    }

    pub fn get_object_version_range_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok((ObjectData { metadata, data }, readiness))
    }

    pub fn get_object_range_conditional_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok((
                ConditionalObjectData {
                    metadata,
                    data: None,
                    not_modified: true,
                },
                readiness,
            ));
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok((
            ConditionalObjectData {
                metadata,
                data: Some(data),
                not_modified: false,
            },
            readiness,
        ))
    }

    pub fn get_object_version_range_conditional_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok((
                ConditionalObjectData {
                    metadata,
                    data: None,
                    not_modified: true,
                },
                readiness,
            ));
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let available = metadata.length - offset;
        let bounded = length.min(available);
        if max_payload_bytes > 0 && bounded > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(
                "object range read payload budget exceeded".to_string(),
            ));
        }
        let data =
            self.read_extents_bounded(&metadata.extents, offset, bounded, max_payload_bytes)?;
        Ok((
            ConditionalObjectData {
                metadata,
                data: Some(data),
                not_modified: false,
            },
            readiness,
        ))
    }

}
