impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_get_object_repair_report(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        let metadata = self.store.head_object(bucket, key)?;
        self.propose_get_object_range_repair_report(bucket, key, 0, metadata.length)
    }

    pub fn propose_get_object_range_repair_report(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        self.propose_get_object_range_repair_report_fenced(bucket, key, offset, length, "")
    }

    pub fn propose_get_object_range_repair_report_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "object range read repair",
            expected_page_route_control_checksum,
        )?;
        let mut projected = self.store.clone();
        let report = projected.get_object_range_repair_report_fenced(
            bucket,
            key,
            offset,
            length,
            (!expected_page_route_control_checksum.is_empty())
                .then_some(expected_page_route_control_checksum),
        )?;
        let metadata = report.object.metadata.clone();
        self.propose_repair_object_metadata_range_bounded(
            &metadata,
            offset,
            length,
            0,
            0,
            Some(expected_page_route_control_checksum),
        )?;
        Ok(report)
    }

    pub fn propose_get_object_range_repair_report_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectReadRepairReport, ObjectError> {
        let readiness = self.ensure_metadata_read_index("object range read repair", now_ms)?;
        let route_checksum = self
            .store
            .page_service
            .route_control_proof()
            .state_checksum;
        let report = self.propose_get_object_range_repair_report_fenced(
            bucket,
            key,
            offset,
            length,
            &route_checksum,
        )?;
        Ok(ProvenObjectReadRepairReport {
            report,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
