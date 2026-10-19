// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn head_object_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store.head_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn head_object_conditional_linearizable(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(ConditionalObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store.head_object_conditional_with_metadata_read(
            bucket,
            key,
            preconditions,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn head_object_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<ProvenObjectMetadata, ObjectError> {
        let (metadata, readiness) = self.head_object_linearizable(bucket, key, now_ms)?;
        Ok(ProvenObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "head object",
            expected_page_route_control_checksum,
        )?;
        let (metadata, readiness) = self.head_object_linearizable(bucket, key, now_ms)?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn head_object_conditional_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectMetadata, ObjectError> {
        let (metadata, readiness) =
            self.head_object_conditional_linearizable(bucket, key, preconditions, now_ms)?;
        Ok(ProvenConditionalObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_conditional_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "conditional head object",
            expected_page_route_control_checksum,
        )?;
        let (metadata, readiness) =
            self.head_object_conditional_linearizable(bucket, key, preconditions, now_ms)?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn head_object_version_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        now_ms: u64,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store.head_object_version_with_metadata_read(
            bucket,
            key,
            version,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn head_object_version_conditional_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<(ConditionalObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store
            .head_object_version_conditional_with_metadata_read(
                bucket,
                key,
                version,
                preconditions,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn head_object_version_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectMetadata, ObjectError> {
        let (metadata, readiness) =
            self.head_object_version_linearizable(bucket, key, version, now_ms)?;
        Ok(ProvenObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_version_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "head object version",
            expected_page_route_control_checksum,
        )?;
        let (metadata, readiness) =
            self.head_object_version_linearizable(bucket, key, version, now_ms)?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn head_object_version_conditional_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectMetadata, ObjectError> {
        let (metadata, readiness) = self.head_object_version_conditional_linearizable(
            bucket,
            key,
            version,
            preconditions,
            now_ms,
        )?;
        Ok(ProvenConditionalObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_version_conditional_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "conditional head object version",
            expected_page_route_control_checksum,
        )?;
        let (metadata, readiness) = self.head_object_version_conditional_linearizable(
            bucket,
            key,
            version,
            preconditions,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

}
