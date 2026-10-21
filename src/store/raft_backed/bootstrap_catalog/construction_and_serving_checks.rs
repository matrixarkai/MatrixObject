// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    fn proven_cache_control_stats(
        &self,
        stats: CacheControlStats,
        readiness: &MetadataRaftReadiness,
        expected_route_control_checksum: &str,
    ) -> ProvenCacheControlStats {
        ProvenCacheControlStats {
            stats,
            proof: object_read_proof_from_readiness(readiness),
            route_control_proof: self.store.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum.to_string(),
        }
    }

    fn check_object_meta_shard_owner_serving(
        &self,
        bucket: &str,
        key: &str,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(bucket, key)),
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(())
    }

    fn check_multipart_meta_shard_owner_serving(
        &self,
        upload_id: &str,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        validate_upload_id(upload_id)?;
        self.check_object_meta_shard_owner_serving(
            "_multipart",
            upload_id,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    fn check_put_requests_meta_shard_owner_serving(
        &self,
        requests: &[ObjectPutRequest],
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        for request in requests {
            self.check_object_meta_shard_owner_serving(
                &request.bucket,
                &request.key,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
            )?;
        }
        Ok(())
    }

    fn put_requests_min_read_index(requests: &[ObjectPutRequest]) -> u64 {
        requests
            .iter()
            .map(|request| request.options.min_read_index)
            .max()
            .unwrap_or(0)
    }

    fn put_requests_min_commit_index(requests: &[ObjectPutRequest]) -> u64 {
        requests
            .iter()
            .map(|request| request.options.min_commit_index)
            .max()
            .unwrap_or(0)
    }

    pub fn new(raft: R, store: MatrixObjectStore) -> Self {
        Self { raft, store }
    }
}
