// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectData, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_with_metadata_read(
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

    pub fn get_object_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectData, ObjectError> {
        let (object, readiness) = self.get_object_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_with_route_proof_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectData>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "get object from metaserver shard",
            expected_page_route_control_checksum,
            min_read_index,
            0,
        )?;
        let (object, readiness) = self.get_object_from_meta_shard(
            bucket,
            key,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            object,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn get_object_with_read_admission_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        let (object, readiness) = self.get_object_with_read_admission_from_meta_shard(
            bucket,
            key,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_with_read_admission_proof_from_meta_shard_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        self.get_object_with_read_admission_proof_from_meta_shard(
            bucket,
            key,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }

    pub fn get_object_conditional_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ConditionalObjectData, MetadataRaftReadiness), ObjectError> {
        self.store.get_object_conditional_with_metadata_read(
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

    pub fn get_object_conditional_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectData, ObjectError> {
        let (object, readiness) = self.get_object_conditional_from_meta_shard(
            bucket,
            key,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenConditionalObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_conditional_with_route_proof_from_meta_shard_fenced(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectData>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready_at(
            "conditional get object from metaserver shard",
            expected_page_route_control_checksum,
            min_read_index,
            0,
        )?;
        let (object, readiness) = self.get_object_conditional_from_meta_shard(
            bucket,
            key,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(self.route_fenced_metadata_read(
            object,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn get_object_conditional_with_read_admission_proof_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        let (object, readiness) = self.get_object_conditional_with_read_admission_from_meta_shard(
            bucket,
            key,
            max_payload_bytes,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_conditional_with_read_admission_proof_from_meta_shard_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        self.get_object_conditional_with_read_admission_proof_from_meta_shard(
            bucket,
            key,
            max_payload_bytes,
            preconditions,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }

}
