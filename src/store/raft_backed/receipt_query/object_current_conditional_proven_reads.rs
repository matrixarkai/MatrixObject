// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_conditional_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectData, ObjectError> {
        let (object, readiness) =
            self.get_object_conditional_linearizable(bucket, key, preconditions, now_ms)?;
        Ok(ProvenConditionalObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_conditional_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ConditionalObjectData>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "conditional get object",
            expected_page_route_control_checksum,
        )?;
        let (object, readiness) =
            self.get_object_conditional_linearizable(bucket, key, preconditions, now_ms)?;
        Ok(self.route_fenced_metadata_read(
            object,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn get_object_conditional_with_read_proof_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenConditionalObjectData, ObjectError> {
        let (object, readiness) = self.get_object_conditional_linearizable_committed_at(
            bucket,
            key,
            preconditions,
            now_ms,
            min_commit_index,
        )?;
        Ok(ProvenConditionalObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_conditional_with_read_admission_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        let (object, readiness) = self.get_object_conditional_with_read_admission_linearizable(
            bucket,
            key,
            max_payload_bytes,
            preconditions,
            now_ms,
        )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_conditional_with_read_admission_proof_linearizable_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        self.get_object_conditional_with_read_admission_proof_linearizable(
            bucket,
            key,
            max_payload_bytes,
            preconditions,
            now_ms,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }

    pub fn get_object_conditional_with_read_admission_proof_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        let (object, readiness) = self
            .get_object_conditional_with_read_admission_linearizable_committed_at(
                bucket,
                key,
                max_payload_bytes,
                preconditions,
                now_ms,
                min_commit_index,
            )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_conditional_with_read_admission_proof_linearizable_committed_at_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        preconditions: &ObjectReadPreconditions,
        now_ms: u64,
        min_commit_index: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedConditionalObjectData>, ObjectError> {
        self.get_object_conditional_with_read_admission_proof_linearizable_committed_at(
            bucket,
            key,
            max_payload_bytes,
            preconditions,
            now_ms,
            min_commit_index,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }
}
