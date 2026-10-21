// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn batch_get_object_versions_conditional_from_meta_shard(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<VersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_from_meta_shard_fenced(
            requests,
            max_items,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn batch_get_object_versions_conditional_from_meta_shard_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<VersionedConditionalObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "versioned conditional batch object read from meta shard",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_object_versions_conditional_with_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        Ok(report)
    }

    pub fn batch_get_object_versions_conditional_with_read_proof_from_meta_shard(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenVersionedConditionalObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_get_object_versions_conditional_from_meta_shard_fenced(
            requests,
            max_items,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenVersionedConditionalObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_object_versions_conditional_with_read_proof_linearizable_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_batch_read_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenVersionedConditionalObjectBatchReadReport, ObjectError> {
        let expected = expected_object_batch_read_proof_checksum.as_ref();
        let mut proven = self.batch_get_object_versions_conditional_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        check_expected_object_batch_read_proof_checksum(
            "VersionedConditionalBatchReadObjects",
            &proven.object_batch_read_proof_checksum,
            Some(expected),
        )?;
        proven.expected_object_batch_read_proof_checksum = expected.to_string();
        let proof = proven.read_proof.clone();
        Ok(ProvenVersionedConditionalObjectBatchReadReport {
            report: proven,
            proof,
        })
    }

    pub fn batch_get_object_versions_conditional_with_read_admission_from_meta_shard(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard(
            requests,
            max_items,
            max_payload_bytes,
            0,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn batch_get_object_versions_conditional_with_read_admission_from_meta_shard_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_fenced(
            requests,
            max_items,
            max_payload_bytes,
            0,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "versioned admitted conditional batch object read from meta shard",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_object_versions_conditional_with_bounded_read_admission_and_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        stamp_admitted_versioned_conditional_object_batch_read_proof(
            "VersionedConditionalBatchReadObjectsWithAdmission",
            &mut report,
        );
        Ok(report)
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_storage_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<AdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let mut report = self
            .batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                min_read_index,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
                expected_page_route_control_checksum,
            )?;
        report.expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.to_string();
        check_expected_admitted_conditional_object_batch_read_proof_checksum(
            "versioned admitted conditional batch object read from meta shard",
            &report.storage_admission_proof_checksum,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok(report)
    }

    pub fn batch_get_object_versions_conditional_with_read_admission_proof_from_meta_shard(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_object_versions_conditional_with_bounded_read_admission_proof_from_meta_shard(
            requests,
            max_items,
            max_payload_bytes,
            0,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_proof_from_meta_shard(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        self.batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )
        .map(|report| {
            let proof = report.read_proof.clone();
            ProvenAdmittedVersionedConditionalObjectBatchReadReport { report, proof }
        })
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_proof_from_meta_shard_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                min_read_index,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
                expected_page_route_control_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedVersionedConditionalObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_object_versions_conditional_with_bounded_read_admission_proof_from_meta_shard_storage_fenced(
        &self,
        requests: Vec<VersionedConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedVersionedConditionalObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_object_versions_conditional_with_bounded_read_admission_from_meta_shard_storage_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                min_read_index,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
                expected_page_route_control_checksum,
                expected_storage_admission_proof_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedVersionedConditionalObjectBatchReadReport { report, proof })
    }
}
