// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn batch_get_objects_with_read_admission_proof_linearizable(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_proof_linearizable(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_linearizable(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_get_objects_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            &route_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_read_admission_proof_linearizable_committed_at(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_proof_linearizable_committed_at(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
            min_commit_index,
        )
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_linearizable_committed_at(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self
            .batch_get_objects_with_bounded_read_admission_linearizable_committed_at_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                min_commit_index,
                &route_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_linearizable_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let report = self.batch_get_objects_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_linearizable_storage_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_objects_with_bounded_read_admission_linearizable_storage_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                expected_page_route_control_checksum,
                expected_storage_admission_proof_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_linearizable_committed_at_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_objects_with_bounded_read_admission_linearizable_committed_at_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                min_commit_index,
                expected_page_route_control_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_linearizable_committed_at_storage_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_objects_with_bounded_read_admission_linearizable_committed_at_storage_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                min_commit_index,
                expected_page_route_control_checksum,
                expected_storage_admission_proof_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }
}
