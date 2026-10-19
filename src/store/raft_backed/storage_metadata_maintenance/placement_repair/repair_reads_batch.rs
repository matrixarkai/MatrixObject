impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_batch_get_objects_repairing(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        self.propose_batch_get_objects_repairing_fenced(requests, max_items, max_payload_bytes, "")
    }

    pub fn propose_batch_get_objects_repairing_fenced(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectBatchReadReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "batch object read repair",
            expected_page_route_control_checksum,
        )?;
        let mut projected = self.store.clone();
        let report = projected.batch_get_objects_repairing_fenced(
            requests.clone(),
            max_items,
            max_payload_bytes,
            (!expected_page_route_control_checksum.is_empty())
                .then_some(expected_page_route_control_checksum),
        )?;
        for item in &report.items {
            if item.error_code.is_some() {
                continue;
            }
            let Some(object) = item.object.as_ref() else {
                continue;
            };
            if item.stats.replica_read_repairs == 0
                && item.stats.replica_read_repair_failures == 0
                && item.stats.quorum_read_degraded_chunks == 0
            {
                continue;
            }
            let (offset, length) = match item.request.range.as_ref() {
                Some(range) => (range.offset, range.length),
                None => (0, object.metadata.length),
            };
            self.propose_repair_object_metadata_range_bounded(
                &object.metadata,
                offset,
                length,
                0,
                0,
                Some(expected_page_route_control_checksum),
            )?;
        }
        Ok(report)
    }

    pub fn propose_batch_get_objects_repairing_with_read_proof_linearizable(
        &mut self,
        requests: Vec<ObjectReadRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectBatchReadReport, ObjectError> {
        let readiness = self.ensure_metadata_read_index("batch object read repair", now_ms)?;
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.propose_batch_get_objects_repairing_fenced(
            requests,
            max_items,
            max_payload_bytes,
            &route_checksum,
        )?;
        Ok(ProvenObjectBatchReadReport {
            report,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
