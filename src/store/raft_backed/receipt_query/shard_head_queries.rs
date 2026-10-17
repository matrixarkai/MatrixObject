impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn head_object_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store.head_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn head_object_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectMetadata, ObjectError> {
        let (metadata, readiness) = self.head_object_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_with_route_proof_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "head object from metaserver shard",
            expected_page_route_control_checksum,
            min_read_index,
            0,
        )?;
        let (metadata, readiness) = self.head_object_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn head_object_conditional_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ConditionalObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store.head_object_conditional_with_metadata_read(
            bucket,
            key,
            preconditions,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn head_object_conditional_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectMetadata, ObjectError> {
        let (metadata, readiness) = self.head_object_conditional_from_meta_shard(
            bucket,
            key,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenConditionalObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_conditional_with_route_proof_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "conditional head object from metaserver shard",
            expected_page_route_control_checksum,
            min_read_index,
            0,
        )?;
        let (metadata, readiness) = self.head_object_conditional_from_meta_shard(
            bucket,
            key,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn head_object_version_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store.head_object_version_with_metadata_read(
            bucket,
            key,
            version,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn head_object_version_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectMetadata, ObjectError> {
        let (metadata, readiness) = self.head_object_version_from_meta_shard(
            bucket,
            key,
            version,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_version_with_route_proof_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "head object version from metaserver shard",
            expected_page_route_control_checksum,
            min_read_index,
            0,
        )?;
        let (metadata, readiness) = self.head_object_version_from_meta_shard(
            bucket,
            key,
            version,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn head_object_version_conditional_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ConditionalObjectMetadata, MetadataRaftReadiness), ObjectError> {
        self.store
            .head_object_version_conditional_with_metadata_read(
                bucket,
                key,
                version,
                preconditions,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
    }

    pub fn head_object_version_conditional_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectMetadata, ObjectError> {
        let (metadata, readiness) = self.head_object_version_conditional_from_meta_shard(
            bucket,
            key,
            version,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenConditionalObjectMetadata {
            metadata,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn head_object_version_conditional_with_route_proof_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        version: u64,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectMetadata>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "conditional head object version from metaserver shard",
            expected_page_route_control_checksum,
            min_read_index,
            0,
        )?;
        let (metadata, readiness) = self.head_object_version_conditional_from_meta_shard(
            bucket,
            key,
            version,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            metadata,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

}
