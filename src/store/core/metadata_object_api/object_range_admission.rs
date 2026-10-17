impl MatrixObjectStore {
    pub fn get_object_range_with_read_admission(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        self.get_object_range_with_bounded_read_admission(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            0,
        )
    }

    pub fn get_object_range_with_bounded_read_admission(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        self.get_object_metadata_range_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn get_object_range_with_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.get_object_range_with_bounded_read_admission_and_metadata_read(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            0,
            options,
        )
    }

    pub fn get_object_range_with_bounded_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let admitted = self.get_object_metadata_range_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok((admitted, readiness))
    }

    pub fn get_object_version_range_with_read_admission(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        self.get_object_version_range_with_bounded_read_admission(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            0,
        )
    }

    pub fn get_object_version_range_with_bounded_read_admission(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<AdmittedObjectData, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        self.get_object_metadata_range_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn get_object_version_range_with_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.get_object_version_range_with_bounded_read_admission_and_metadata_read(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            0,
            options,
        )
    }

    pub fn get_object_version_range_with_bounded_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        let admitted = self.get_object_metadata_range_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        Ok((admitted, readiness))
    }

    pub fn get_object_range_conditional_with_read_admission(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        self.get_object_range_conditional_with_bounded_read_admission(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            0,
            preconditions,
        )
    }

    pub fn get_object_range_conditional_with_bounded_read_admission(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        self.get_object_metadata_range_conditional_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            preconditions,
        )
    }

    pub fn get_object_range_conditional_with_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.get_object_range_conditional_with_bounded_read_admission_and_metadata_read(
            bucket,
            key,
            offset,
            length,
            max_payload_bytes,
            0,
            preconditions,
            options,
        )
    }

    pub fn get_object_range_conditional_with_bounded_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let admitted = self.get_object_metadata_range_conditional_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            preconditions,
        )?;
        Ok((admitted, readiness))
    }

    pub fn get_object_version_range_conditional_with_read_admission(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        self.get_object_version_range_conditional_with_bounded_read_admission(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            0,
            preconditions,
        )
    }

    pub fn get_object_version_range_conditional_with_bounded_read_admission(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
    ) -> Result<AdmittedConditionalObjectData, ObjectError> {
        let metadata = self.head_object_version(bucket, key, version)?;
        self.get_object_metadata_range_conditional_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            preconditions,
        )
    }

    pub fn get_object_version_range_conditional_with_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.get_object_version_range_conditional_with_bounded_read_admission_and_metadata_read(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            0,
            preconditions,
            options,
        )
    }

    pub fn get_object_version_range_conditional_with_bounded_read_admission_and_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        preconditions: &ObjectReadPreconditions,
        options: MetadataReadOptions,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_with_metadata_read(bucket, key, version, options)?;
        let admitted = self.get_object_metadata_range_conditional_with_bounded_read_admission(
            metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
            preconditions,
        )?;
        Ok((admitted, readiness))
    }
}
