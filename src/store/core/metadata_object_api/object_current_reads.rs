// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let data = self.read_extents(&metadata.extents, 0, metadata.length)?;
        Ok(ObjectData { metadata, data })
    }

    pub fn get_object_conditional(
        &self,
        bucket: &str,
        key: &str,
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
        let data = self.read_extents(&metadata.extents, 0, metadata.length)?;
        Ok(ConditionalObjectData {
            metadata,
            data: Some(data),
            not_modified: false,
        })
    }

    pub fn get_object_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        options: MetadataReadOptions,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let data = self.read_extents(&metadata.extents, 0, metadata.length)?;
        Ok((ObjectData { metadata, data }, readiness))
    }

    pub fn get_object_conditional_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
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
        let data = self.read_extents(&metadata.extents, 0, metadata.length)?;
        Ok((
            ConditionalObjectData {
                metadata,
                data: Some(data),
                not_modified: false,
            },
            readiness,
        ))
    }

    pub fn get_object_with_read_stats(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectData, ChunkReadStats), ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let (data, stats) = self.read_extents_with_stats(&metadata.extents, 0, metadata.length)?;
        Ok((ObjectData { metadata, data }, stats))
    }

    pub fn get_object_with_read_admission(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        self.get_object_with_bounded_read_admission(bucket, key, max_payload_bytes, 0)
    }

    pub fn get_object_with_bounded_read_admission(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let admission = self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        if !admission.can_accept {
            return Ok(AdmittedObjectData {
                object: None,
                admission,
            });
        }
        let data = self.read_extents_bounded_by_chunks(
            &metadata.extents,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok(AdmittedObjectData {
            object: Some(ObjectData { metadata, data }),
            admission,
        })
    }

    pub fn get_object_with_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.get_object_with_bounded_read_admission_and_metadata_read(
            bucket,
            key,
            max_payload_bytes,
            0,
            options,
        )
    }

    pub fn get_object_with_bounded_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        let admission = self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        if !admission.can_accept {
            return Ok((
                AdmittedObjectData {
                    object: None,
                    admission,
                },
                readiness,
            ));
        }
        let data = self.read_extents_bounded_by_chunks(
            &metadata.extents,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok((
            AdmittedObjectData {
                object: Some(ObjectData { metadata, data }),
                admission,
            },
            readiness,
        ))
    }

    pub fn get_object_conditional_with_read_admission(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        self.get_object_conditional_with_bounded_read_admission(
            bucket,
            key,
            max_payload_bytes,
            0,
            preconditions,
        )
    }

    pub fn get_object_conditional_with_bounded_read_admission(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok(AdmittedConditionalObjectData {
                admission: self.not_modified_object_read_admission(&metadata, max_payload_bytes),
                object: Some(ConditionalObjectData {
                    metadata,
                    data: None,
                    not_modified: true,
                }),
            });
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        let admission = self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        if !admission.can_accept {
            return Ok(AdmittedConditionalObjectData {
                object: None,
                admission,
            });
        }
        let data = self.read_extents_bounded_by_chunks(
            &metadata.extents,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok(AdmittedConditionalObjectData {
            object: Some(ConditionalObjectData {
                metadata,
                data: Some(data),
                not_modified: false,
            }),
            admission,
        })
    }

    pub fn get_object_conditional_with_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.get_object_conditional_with_bounded_read_admission_and_metadata_read(
            bucket,
            key,
            max_payload_bytes,
            0,
            preconditions,
            options,
        )
    }

    pub fn get_object_conditional_with_bounded_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let not_modified = evaluate_object_read_preconditions(&metadata, preconditions)?;
        if not_modified {
            return Ok((
                AdmittedConditionalObjectData {
                    admission: self
                        .not_modified_object_read_admission(&metadata, max_payload_bytes),
                    object: Some(ConditionalObjectData {
                        metadata,
                        data: None,
                        not_modified: true,
                    }),
                },
                readiness,
            ));
        }
        check_payload_read_allowed(&metadata, now_millis())?;
        let admission = self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        if !admission.can_accept {
            return Ok((
                AdmittedConditionalObjectData {
                    object: None,
                    admission,
                },
                readiness,
            ));
        }
        let data = self.read_extents_bounded_by_chunks(
            &metadata.extents,
            0,
            metadata.length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok((
            AdmittedConditionalObjectData {
                object: Some(ConditionalObjectData {
                    metadata,
                    data: Some(data),
                    not_modified: false,
                }),
                admission,
            },
            readiness,
        ))
    }

}
