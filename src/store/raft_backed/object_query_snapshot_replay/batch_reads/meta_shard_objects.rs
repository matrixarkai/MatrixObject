impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn batch_get_objects_from_meta_shard(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_from_meta_shard_fenced(
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

    pub fn batch_get_objects_from_meta_shard_fenced(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "batch object read from meta shard",
            expected_page_route_control_checksum,
        )?;
        let mut report = self.store.batch_get_objects_with_metadata_read(
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

    pub fn batch_get_objects_with_read_proof_from_meta_shard(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_get_objects_from_meta_shard_fenced(
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
        Ok(ProvenObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_read_admission_from_meta_shard(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_from_meta_shard(
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

    pub fn batch_get_objects_with_bounded_read_admission_from_meta_shard(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_from_meta_shard_fenced(
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

    pub fn batch_get_objects_with_read_admission_from_meta_shard_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_from_meta_shard_fenced(
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

    pub fn batch_get_objects_with_bounded_read_admission_from_meta_shard_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "admitted batch object read from meta shard",
            expected_page_route_control_checksum,
        )?;
        let mut report = self
            .store
            .batch_get_objects_with_bounded_read_admission_and_metadata_read(
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
        stamp_admitted_object_batch_read_proof("BatchReadObjectsWithAdmission", &mut report);
        Ok(report)
    }

    pub fn batch_get_objects_with_bounded_read_admission_from_meta_shard_storage_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<AdmittedObjectBatchReadReport, ObjectError> {
        let expected_storage_admission_proof_checksum =
            expected_storage_admission_proof_checksum.as_ref();
        let mut report = self
            .batch_get_objects_with_bounded_read_admission_from_meta_shard_fenced(
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
        check_expected_admitted_object_batch_read_proof_checksum(
            "admitted batch object read from meta shard",
            &report,
            Some(expected_storage_admission_proof_checksum),
        )?;
        Ok(report)
    }

    pub fn batch_get_objects_with_read_admission_proof_from_meta_shard(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        self.batch_get_objects_with_bounded_read_admission_proof_from_meta_shard(
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

    pub fn batch_get_objects_with_bounded_read_admission_proof_from_meta_shard(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_get_objects_with_bounded_read_admission_from_meta_shard_fenced(
            requests,
            max_items,
            max_payload_bytes,
            max_chunk_read_count,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_from_meta_shard_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let report = self.batch_get_objects_with_bounded_read_admission_from_meta_shard_fenced(
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
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }

    pub fn batch_get_objects_with_bounded_read_admission_proof_from_meta_shard_storage_fenced(
        &self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_storage_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenAdmittedObjectBatchReadReport, ObjectError> {
        let report = self
            .batch_get_objects_with_bounded_read_admission_from_meta_shard_storage_fenced(
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
        Ok(ProvenAdmittedObjectBatchReadReport { report, proof })
    }
}
