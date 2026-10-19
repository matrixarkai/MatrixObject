// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_version_range_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_version_range_with_metadata_read(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn get_object_version_range_with_read_admission_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(AdmittedObjectData, MetadataRaftReadiness), ObjectError> {
        self.store
            .get_object_version_range_with_read_admission_and_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn get_object_version_range_conditional_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.store
            .get_object_version_range_conditional_with_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_payload_bytes,
                preconditions,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn get_object_version_range_conditional_with_read_admission_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(AdmittedConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.store
            .get_object_version_range_conditional_with_read_admission_and_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_payload_bytes,
                preconditions,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn get_object_version_range_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectData, ObjectError> {
        let (object, readiness) = self.get_object_version_range_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_version_range_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectData>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "get object version range",
            expected_page_route_control_checksum,
        )?;
        let (object, readiness) = self.get_object_version_range_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            object,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn get_object_version_range_with_read_admission_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        let (object, readiness) = self.get_object_version_range_with_read_admission_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_version_range_with_read_admission_proof_linearizable_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        self.get_object_version_range_with_read_admission_proof_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            now_ms,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }

    pub fn get_object_version_range_conditional_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectData, ObjectError> {
        let (object, readiness) = self.get_object_version_range_conditional_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            preconditions,
            now_ms,
        )?;
        Ok(ProvenConditionalObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_version_range_conditional_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectData>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "conditional get object version range",
            expected_page_route_control_checksum,
        )?;
        let (object, readiness) = self.get_object_version_range_conditional_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            preconditions,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            object,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn get_object_version_range_conditional_with_read_admission_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        let (object, readiness) = self
            .get_object_version_range_conditional_with_read_admission_linearizable(
                bucket,
                key,
                version,
                offset,
                length,
                max_payload_bytes,
                preconditions,
                now_ms,
            )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_version_range_conditional_with_read_admission_proof_linearizable_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        self.get_object_version_range_conditional_with_read_admission_proof_linearizable(
            bucket,
            key,
            version,
            offset,
            length,
            max_payload_bytes,
            preconditions,
            now_ms,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }

}
