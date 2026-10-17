impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn batch_get_objects_conditional_linearizable(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
            "",
        )
    }

    pub fn batch_get_objects_conditional_linearizable_fenced(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ConditionalObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "conditional batch object read",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_objects_conditional_with_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        stamp_conditional_object_batch_read_proof("ConditionalBatchReadObjects", &mut report);
        Ok(report)
    }

    pub fn batch_get_objects_conditional_with_read_proof_linearizable(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenConditionalObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .page_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_get_objects_conditional_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
            &route_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenConditionalObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_conditional_with_read_proof_linearizable_fenced(
        &mut self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_batch_read_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenConditionalObjectBatchReadReport, ObjectError> {
        let expected = expected_object_batch_read_proof_checksum.as_ref();
        let mut proven = self.batch_get_objects_conditional_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        check_expected_object_batch_read_proof_checksum(
            "ConditionalBatchReadObjects",
            &proven.object_batch_read_proof_checksum,
            Some(expected),
        )?;
        proven.expected_object_batch_read_proof_checksum = expected.to_string();
        let proof = proven.read_proof.clone();
        Ok(ProvenConditionalObjectBatchReadReport {
            report: proven,
            proof,
        })
    }

    pub fn batch_get_objects_conditional_with_read_admission_linearizable(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_bounded_read_admission_linearizable(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
        )
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_linearizable(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            "",
        )
    }

    pub fn batch_get_objects_conditional_with_read_admission_linearizable_fenced(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_linearizable_fenced(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "conditional admitted batch object read",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_objects_conditional_with_bounded_read_admission_and_metadata_read(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )?;
        report.expected_route_control_checksum = expected_page_route_control_checksum.to_string();
        stamp_admitted_conditional_object_batch_read_proof(
            "ConditionalBatchReadObjectsWithAdmission",
            &mut report,
        );
        Ok(report)
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_linearizable_storage_fenced(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<AdmittedConditionalObjectBatchReadReport, ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let mut report = self
            .batch_get_objects_conditional_with_bounded_read_admission_linearizable_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                expected_page_route_control_checksum,
            )?;
        report.expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.to_string();
        check_expected_admitted_conditional_object_batch_read_proof_checksum(
            "conditional admitted batch object read",
            &report.storage_admission_proof_checksum,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok(report)
    }

    pub fn batch_get_objects_conditional_with_read_admission_proof_linearizable(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedConditionalObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_conditional_with_bounded_read_admission_proof_linearizable(
            requests,
            max_items,
            max_payload_bytes,
            0,
            now_ms,
        )
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_proof_linearizable(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedConditionalObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .page_service
            .route_control_proof()
            .state_checksum;
        self.batch_get_objects_conditional_with_bounded_read_admission_linearizable_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            now_ms,
            &route_checksum,
        )
        .map(|report| {
            let proof = report.read_proof.clone();
            ProvenAdmittedConditionalObjectBatchReadReport { report, proof }
        })
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_proof_linearizable_fenced(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedConditionalObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_objects_conditional_with_bounded_read_admission_linearizable_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                expected_page_route_control_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedConditionalObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_conditional_with_bounded_read_admission_proof_linearizable_storage_fenced(
        &self,
        requests: Vec<ConditionalObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedConditionalObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_objects_conditional_with_bounded_read_admission_linearizable_storage_fenced(
                requests,
                max_items,
                max_payload_bytes,
                max_chunk_read_count,
                now_ms,
                expected_page_route_control_checksum,
                expected_storage_admission_proof_checksum,
            )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedConditionalObjectBatchReadReport { report, proof })
    }
}
