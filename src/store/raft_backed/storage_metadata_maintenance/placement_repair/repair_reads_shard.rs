impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_get_object_repair_report_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        let (metadata, _) = self.store.head_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )?;
        self.propose_get_object_range_repair_report_from_meta_shard(
            bucket,
            key,
            0,
            metadata.length,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn propose_get_object_range_repair_report_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        self.propose_get_object_range_repair_report_from_meta_shard_fenced(
            bucket,
            key,
            offset,
            length,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn propose_get_object_range_repair_report_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectReadRepairReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.store.head_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )?;
        self.propose_get_object_range_repair_report_fenced(
            bucket,
            key,
            offset,
            length,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_get_object_range_repair_report_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectReadRepairReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (_, readiness) = self.store.head_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )?;
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

    pub fn propose_get_object_repair_report_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectReadRepairReport, ObjectError> {
        let (metadata, _) = self.store.head_object_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )?;
        self.propose_get_object_range_repair_report_with_read_proof_from_meta_shard(
            bucket,
            key,
            0,
            metadata.length,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }
}
