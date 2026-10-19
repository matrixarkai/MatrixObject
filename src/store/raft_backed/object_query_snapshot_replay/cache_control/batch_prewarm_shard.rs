// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn batch_prewarm_object_range_cache_from_meta_shard(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        self.batch_prewarm_object_range_cache_from_meta_shard_fenced(
            requests,
            max_items,
            max_prewarm_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn batch_prewarm_object_range_cache_from_meta_shard_fenced(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "batch object cache prewarm from meta shard",
            expected_page_route_control_checksum,
        )?;
        self.store
            .batch_prewarm_object_range_cache_with_metadata_read(
                requests,
                max_items,
                max_prewarm_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
            .map(|mut report| {
                report.expected_route_control_checksum =
                    expected_page_route_control_checksum.to_string();
                stamp_object_batch_cache_control_proof("BatchPrewarmObjectRange", &mut report);
                report
            })
    }

    pub fn batch_prewarm_object_range_cache_routed_from_meta_shard_fenced(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "routed batch object cache prewarm from meta shard",
            expected_page_route_control_checksum,
        )?;
        self.store
            .batch_prewarm_object_range_cache_routed_with_metadata_read(
                requests,
                max_items,
                max_prewarm_bytes,
                target_replica_index,
                include_disabled_replicas,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
            .map(|mut report| {
                report.expected_route_control_checksum =
                    expected_page_route_control_checksum.to_string();
                stamp_object_batch_cache_control_proof("BatchPrewarmObjectRange", &mut report);
                report
            })
    }

    pub fn batch_prewarm_object_range_cache_from_meta_shard_cache_fenced(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_cache_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        let expected_cache_batch_proof_checksum = expected_cache_batch_proof_checksum.as_ref();
        let mut report = self.batch_prewarm_object_range_cache_from_meta_shard_fenced(
            requests,
            max_items,
            max_prewarm_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        report.expected_cache_batch_proof_checksum =
            expected_cache_batch_proof_checksum.to_string();
        check_expected_object_batch_cache_control_proof_checksum(
            "batch object cache prewarm from meta shard",
            &report.cache_batch_proof_checksum,
            Some(expected_cache_batch_proof_checksum),
        )?;
        Ok(report)
    }

    pub fn batch_prewarm_object_range_cache_routed_from_meta_shard_cache_fenced(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_cache_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectBatchCacheControlReport, ObjectError> {
        let expected_cache_batch_proof_checksum = expected_cache_batch_proof_checksum.as_ref();
        let mut report = self.batch_prewarm_object_range_cache_routed_from_meta_shard_fenced(
            requests,
            max_items,
            max_prewarm_bytes,
            target_replica_index,
            include_disabled_replicas,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            expected_page_route_control_checksum,
        )?;
        report.expected_cache_batch_proof_checksum =
            expected_cache_batch_proof_checksum.to_string();
        check_expected_object_batch_cache_control_proof_checksum(
            "routed batch object cache prewarm from meta shard",
            &report.cache_batch_proof_checksum,
            Some(expected_cache_batch_proof_checksum),
        )?;
        Ok(report)
    }

    pub fn batch_prewarm_object_range_cache_with_read_proof_from_meta_shard(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectBatchCacheControlReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_prewarm_object_range_cache_from_meta_shard_fenced(
            requests,
            max_items,
            max_prewarm_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenObjectBatchCacheControlReport { report, proof })
    }

    pub fn batch_prewarm_object_range_cache_routed_with_read_proof_from_meta_shard(
        &mut self,
        requests: Vec<ObjectCacheRangeRequest>,
        max_items: usize,
        max_prewarm_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectBatchCacheControlReport, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let report = self.batch_prewarm_object_range_cache_routed_from_meta_shard_fenced(
            requests,
            max_items,
            max_prewarm_bytes,
            target_replica_index,
            include_disabled_replicas,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        let proof = report.read_proof.clone();
        Ok(ProvenObjectBatchCacheControlReport { report, proof })
    }
}
