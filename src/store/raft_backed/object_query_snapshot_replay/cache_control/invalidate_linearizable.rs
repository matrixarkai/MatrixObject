// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn invalidate_object_range_cache_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        now_ms: u64,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.invalidate_object_range_cache_linearizable_fenced(
            bucket, key, offset, length, max_bytes, now_ms, "",
        )
    }

    pub fn invalidate_object_range_cache_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "object cache invalidation",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store.invalidate_object_range_cache_with_metadata_read(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn invalidate_object_range_cache_routed_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "routed object cache invalidation",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .invalidate_object_range_cache_routed_with_metadata_read(
                bucket,
                key,
                offset,
                length,
                max_bytes,
                target_replica_index,
                include_disabled_replicas,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn invalidate_object_range_cache_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self.invalidate_object_range_cache_linearizable_fenced(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            now_ms,
            &route_checksum,
        )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }

    pub fn invalidate_object_range_cache_routed_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self.invalidate_object_range_cache_routed_linearizable_fenced(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            target_replica_index,
            include_disabled_replicas,
            now_ms,
            &route_checksum,
        )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }

    pub fn invalidate_object_version_range_cache_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        now_ms: u64,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.invalidate_object_version_range_cache_linearizable_fenced(
            bucket, key, version, offset, length, max_bytes, now_ms, "",
        )
    }

    pub fn invalidate_object_version_range_cache_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "object version cache invalidation",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .invalidate_object_version_range_cache_with_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn invalidate_object_version_range_cache_routed_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "routed object version cache invalidation",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .invalidate_object_version_range_cache_routed_with_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_bytes,
                target_replica_index,
                include_disabled_replicas,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn invalidate_object_version_range_cache_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self.invalidate_object_version_range_cache_linearizable_fenced(
            bucket,
            key,
            version,
            offset,
            length,
            max_bytes,
            now_ms,
            &route_checksum,
        )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }

    pub fn invalidate_object_version_range_cache_routed_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self
            .invalidate_object_version_range_cache_routed_linearizable_fenced(
                bucket,
                key,
                version,
                offset,
                length,
                max_bytes,
                target_replica_index,
                include_disabled_replicas,
                now_ms,
                &route_checksum,
            )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }
}
