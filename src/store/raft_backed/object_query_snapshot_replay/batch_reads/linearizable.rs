// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn batch_get_objects_linearizable(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        let (options, readiness) = self.metadata_read_options_at_raft_position(
            "batch get objects linearizable",
            now_ms,
            0,
        )?;
        let mut report = self.store.batch_get_objects_with_metadata_read(
            requests,
            max_items,
            max_payload_bytes,
            options,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_batch_read_proof("BatchGetObjects", &mut report);
        Ok(report)
    }

    pub fn batch_get_objects_linearizable_committed_at(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        let (options, readiness) = self.metadata_read_options_at_raft_position(
            "batch object read",
            now_ms,
            min_commit_index,
        )?;
        let mut report = self.store.batch_get_objects_with_metadata_read(
            requests,
            max_items,
            max_payload_bytes,
            options,
        )?;
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_batch_read_proof("BatchGetObjects", &mut report);
        Ok(report)
    }

    pub fn batch_get_objects_with_read_proof_linearizable(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectBatchReadReport, ObjectError> {
        let report =
            self.batch_get_objects_linearizable(requests, max_items, max_payload_bytes, now_ms)?;
        let proof = report.read_proof.clone();
        Ok(ProvenObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_read_proof_linearizable_fenced(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_batch_read_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenObjectBatchReadReport, ObjectError> {
        let expected = expected_object_batch_read_proof_checksum.as_ref();
        let mut proven = self.batch_get_objects_with_read_proof_linearizable(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
        )?;
        check_expected_object_batch_read_proof_checksum(
            "BatchGetObjects",
            &proven.report.object_batch_read_proof_checksum,
            Some(expected),
        )?;
        proven.report.expected_object_batch_read_proof_checksum = expected.to_string();
        Ok(proven)
    }

    pub fn batch_get_objects_with_read_proof_linearizable_committed_at(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenObjectBatchReadReport, ObjectError> {
        let report = self.batch_get_objects_linearizable_committed_at(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_read_proof_linearizable_committed_at_fenced(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_object_batch_read_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenObjectBatchReadReport, ObjectError> {
        let expected = expected_object_batch_read_proof_checksum.as_ref();
        let mut proven = self.batch_get_objects_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        check_expected_object_batch_read_proof_checksum(
            "BatchGetObjects",
            &proven.report.object_batch_read_proof_checksum,
            Some(expected),
        )?;
        proven.report.expected_object_batch_read_proof_checksum = expected.to_string();
        Ok(proven)
    }

    pub fn batch_get_objects_with_read_admission_linearizable(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_linearizable(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_linearizable(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            "",
        )
    }

    pub fn batch_get_objects_with_read_admission_linearizable_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_linearizable_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        let (options, readiness) = self.metadata_read_options_at_raft_position(
            "batch get objects with bounded read admission linearizable fenced",
            now_ms,
            0,
        )?;
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "admitted batch object read",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_objects_with_bounded_read_admission_and_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                options,
            )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_admitted_object_batch_read_proof("BatchReadObjectsWithAdmission", &mut report);
        Ok(report)
    }

    pub fn batch_get_objects_with_bounded_read_admission_linearizable_storage_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let mut report = self.batch_get_objects_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        report.expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.to_string();
        check_expected_admitted_object_batch_read_proof_checksum(
            "admitted batch object read",
            &report,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok(report)
    }

    pub fn batch_get_objects_with_read_admission_linearizable_committed_at(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_linearizable_committed_at(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
            min_commit_index,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_linearizable_committed_at(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_linearizable_committed_at_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            min_commit_index,
            "",
        )
    }

    pub fn batch_get_objects_with_read_admission_linearizable_committed_at_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_linearizable_committed_at_fenced(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
            min_commit_index,
            expected_page_route_control_checksum,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_linearizable_committed_at_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        let (options, readiness) = self.metadata_read_options_at_raft_position(
            "admitted batch object read",
            now_ms,
            min_commit_index,
        )?;
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "admitted batch object read",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_objects_with_bounded_read_admission_and_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                options,
            )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        report.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_admitted_object_batch_read_proof("BatchReadObjectsWithAdmission", &mut report);
        Ok(report)
    }

    pub fn batch_get_objects_with_bounded_read_admission_linearizable_committed_at_storage_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let mut report = self
            .batch_get_objects_with_bounded_read_admission_linearizable_committed_at_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                min_commit_index,
                expected_page_route_control_checksum,
            )?;
        report.expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.to_string();
        check_expected_admitted_object_batch_read_proof_checksum(
            "admitted batch object read",
            &report,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok(report)
    }
}
