// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prewarm_object_range_cache_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.prewarm_object_range_cache_from_meta_shard_fenced(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn prewarm_object_range_cache_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "object cache prewarm from meta shard",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store.prewarm_object_range_cache_with_metadata_read(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn prewarm_object_range_cache_routed_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "routed object cache prewarm from meta shard",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .prewarm_object_range_cache_routed_with_metadata_read(
                bucket,
                key,
                offset,
                length,
                max_bytes,
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
    }

    pub fn prewarm_object_range_cache_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self.prewarm_object_range_cache_from_meta_shard_fenced(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }

    pub fn prewarm_object_range_cache_routed_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self.prewarm_object_range_cache_routed_from_meta_shard_fenced(
            bucket,
            key,
            offset,
            length,
            max_bytes,
            target_replica_index,
            include_disabled_replicas,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }

    pub fn prewarm_object_version_range_cache_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.prewarm_object_version_range_cache_from_meta_shard_fenced(
            bucket,
            key,
            version,
            offset,
            length,
            max_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            "",
        )
    }

    pub fn prewarm_object_version_range_cache_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "object version cache prewarm from meta shard",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .prewarm_object_version_range_cache_with_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
    }

    pub fn prewarm_object_version_range_cache_routed_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<(CacheControlStats, MetadataRaftReadiness), ObjectError> {
        self.ensure_page_route_control_ready(
            "routed object version cache prewarm from meta shard",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.store
            .prewarm_object_version_range_cache_routed_with_metadata_read(
                bucket,
                key,
                version,
                offset,
                length,
                max_bytes,
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
    }

    pub fn prewarm_object_version_range_cache_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self.prewarm_object_version_range_cache_from_meta_shard_fenced(
            bucket,
            key,
            version,
            offset,
            length,
            max_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
            &route_checksum,
        )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }

    pub fn prewarm_object_version_range_cache_routed_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        offset: u64,
        length: u64,
        max_bytes: u64,
        target_replica_index: Option<usize>,
        include_disabled_replicas: bool,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenCacheControlStats, ObjectError> {
        let route_checksum = self
            .store
            .chunk_service
            .route_control_proof()
            .state_checksum;
        let (stats, readiness) = self
            .prewarm_object_version_range_cache_routed_from_meta_shard_fenced(
                bucket,
                key,
                version,
                offset,
                length,
                max_bytes,
                target_replica_index,
                include_disabled_replicas,
                min_read_index,
                expected_shard_owner,
                expected_shard_epoch,
                now_ms,
                &route_checksum,
            )?;
        Ok(self.proven_cache_control_stats(stats, &readiness, &route_checksum))
    }
}
