// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn get_object_with_read_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<ProvenObjectData, ObjectError> {
        let (object, readiness) = self.get_object_linearizable(bucket, key, now_ms)?;
        Ok(ProvenObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_with_route_proof_linearizable_fenced(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectData>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready("get object", expected_page_route_control_checksum)?;
        let (object, readiness) = self.get_object_linearizable(bucket, key, now_ms)?;
        Ok(self.route_fenced_metadata_read(
            object,
            &readiness,
            expected_page_route_control_checksum,
        ))
    }

    pub fn get_object_with_read_proof_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenObjectData, ObjectError> {
        let (object, readiness) =
            self.get_object_linearizable_committed_at(bucket, key, now_ms, min_commit_index)?;
        Ok(ProvenObjectData {
            object,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn get_object_with_read_admission_proof_linearizable(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        let (object, readiness) = self.get_object_with_read_admission_linearizable(
            bucket,
            key,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_with_read_admission_proof_linearizable_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        self.get_object_with_read_admission_proof_linearizable(
            bucket,
            key,
            max_payload_bytes,
            now_ms,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }

    pub fn get_object_with_read_admission_proof_linearizable_committed_at(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        let (object, readiness) = self.get_object_with_read_admission_linearizable_committed_at(
            bucket,
            key,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        Ok(proven_metadata_read(object, &readiness))
    }

    pub fn get_object_with_read_admission_proof_linearizable_committed_at_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<AdmittedObjectData>, ObjectError> {
        self.get_object_with_read_admission_proof_linearizable_committed_at(
            bucket,
            key,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?
        .require_object_read_storage_admission_proof_checksum(
            expected_storage_admission_proof_checksum,
        )
    }
}
