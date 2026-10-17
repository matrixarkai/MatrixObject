impl MatrixObjectStore {
    pub fn prepare_metadata_read(
        &self,
        meta_key: &str,
        options: &MetadataReadOptions,
    ) -> Result<MetadataRaftReadiness, ObjectError> {
        validate_meta_key(meta_key)?;
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        if options.require_shard_owner_serving {
            let owner = options.expected_shard_owner.as_deref().ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "metadata read requires expected shard owner".to_string(),
                )
            })?;
            let epoch = options.expected_shard_epoch.ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "metadata read requires expected shard epoch".to_string(),
                )
            })?;
            self.meta_service
                .check_shard_owner_serving(meta_key, owner, epoch, options.now_ms)?;
        }
        Ok(readiness)
    }

    pub fn list_metadata_shard_inventory(
        &self,
        owner_node_id: Option<&str>,
        after_shard_id: Option<&str>,
        max_shards: usize,
        max_payload_bytes: u64,
        include_record_stats: bool,
    ) -> Result<MetadataShardInventoryPage, ObjectError> {
        self.meta_service.list_shard_inventory(
            owner_node_id,
            after_shard_id,
            max_shards,
            max_payload_bytes,
            include_record_stats,
        )
    }

    pub fn resolve_metadata_shard_routes(
        &self,
        prefix: &str,
        after_shard_id: Option<&str>,
        max_routes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<MetadataShardRoutePage, ObjectError> {
        self.resolve_metadata_shard_routes_fenced(
            prefix,
            after_shard_id,
            max_routes,
            max_payload_bytes,
            now_ms,
            None,
            None,
            None,
        )
    }

    pub fn resolve_metadata_shard_routes_fenced(
        &self,
        prefix: &str,
        after_shard_id: Option<&str>,
        max_routes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardRoutePage, ObjectError> {
        self.meta_service.resolve_shard_routes_fenced(
            prefix,
            after_shard_id,
            max_routes,
            max_payload_bytes,
            now_ms,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
        )
    }

    pub fn resolve_metadata_route_cached(
        &mut self,
        key: &str,
        now_ms: u64,
    ) -> Result<MetadataShardRoute, ObjectError> {
        validate_meta_key(key)?;
        self.ensure_metadata_route_cache_fresh();
        self.expire_metadata_route_cache(now_ms);
        self.metadata_route_cache_sequence = self.metadata_route_cache_sequence.saturating_add(1);
        let access_sequence = self.metadata_route_cache_sequence;
        if let Some(entry) = self.metadata_route_cache.get_mut(key) {
            self.metadata_route_cache_hits = self.metadata_route_cache_hits.saturating_add(1);
            entry.last_access_sequence = access_sequence;
            return Ok(entry.route.clone());
        }
        self.metadata_route_cache_misses = self.metadata_route_cache_misses.saturating_add(1);
        let page = self.resolve_metadata_shard_routes(key, None, 1, 0, now_ms)?;
        let route = page
            .routes
            .into_iter()
            .find(|route| metadata_shard_contains_key(&route.shard, key))
            .ok_or_else(|| ObjectError::PreconditionFailed("metadata shard missing".to_string()))?;
        let (topology_epoch, topology_checksum) =
            metadata_route_topology_fingerprint(&self.meta_service);
        let (_, shard_topology_checksum) = metadata_shard_topology_fingerprint(&self.meta_service);
        let session_checksum = metadata_route_session_fingerprint(&self.meta_service, now_ms);
        self.metadata_route_cache.insert(
            key.to_string(),
            MetadataRouteCacheEntry {
                route: route.clone(),
                topology_epoch,
                topology_checksum,
                shard_topology_checksum,
                session_checksum,
                expire_time_ms: metadata_route_cache_expire_time_ms(
                    &route,
                    now_ms,
                    self.options.metadata_route_cache_ttl_ms,
                ),
                last_access_sequence: access_sequence,
            },
        );
        self.prune_metadata_route_cache();
        Ok(route)
    }

    pub fn batch_resolve_metadata_routes_cached(
        &mut self,
        keys: &[String],
        max_payload_bytes: u64,
        now_ms: u64,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataShardRouteBatch, ObjectError> {
        if keys.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata route batch requires at least one key".to_string(),
            ));
        }
        if keys.len() > MAX_METADATA_ROUTE_BATCH_KEYS {
            return Err(ObjectError::InvalidArgument(format!(
                "metadata route batch key_count exceeds {MAX_METADATA_ROUTE_BATCH_KEYS}"
            )));
        }
        self.check_expected_metadata_shard_topology(
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )?;
        self.check_expected_metadata_coordinator_session(
            now_ms,
            expected_coordinator_session_checksum,
        )?;
        let (topology_epoch, shard_topology_checksum) =
            metadata_shard_topology_fingerprint(&self.meta_service);
        let session_checksum = metadata_route_session_fingerprint(&self.meta_service, now_ms);
        let hits_before = self.metadata_route_cache_hits;
        let misses_before = self.metadata_route_cache_misses;
        let invalidations_before = self.metadata_route_cache_invalidations;
        let expired_invalidations_before = self.metadata_route_cache_expired_invalidations;
        let mut batch = MetadataShardRouteBatch {
            key_count: keys.len() as u64,
            max_payload_bytes,
            metadata_shard_topology_epoch: topology_epoch,
            metadata_shard_topology_checksum: shard_topology_checksum,
            metadata_coordinator_session_checksum: session_checksum,
            expected_metadata_shard_topology_epoch: expected_shard_topology_epoch,
            expected_metadata_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            expected_metadata_coordinator_session_checksum: expected_coordinator_session_checksum
                .unwrap_or_default()
                .to_string(),
            ..MetadataShardRouteBatch::default()
        };
        for key in keys {
            let route = self.resolve_metadata_route_cached(key, now_ms)?;
            let route_payload = metadata_shard_route_payload_bytes(&route);
            if max_payload_bytes > 0
                && batch.payload_bytes.saturating_add(route_payload) > max_payload_bytes
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata route batch exceeds payload budget".to_string(),
                ));
            }
            batch.payload_bytes = batch.payload_bytes.saturating_add(route_payload);
            if route.shard.draining {
                batch.draining_count = batch.draining_count.saturating_add(1);
            }
            if !route.owner_serving {
                batch.owner_inactive_count = batch.owner_inactive_count.saturating_add(1);
            }
            batch.routes.push(route);
        }
        batch.route_cache_entries = self.metadata_route_cache.len() as u64;
        batch.route_cache_hits = self.metadata_route_cache_hits.saturating_sub(hits_before);
        batch.route_cache_misses = self
            .metadata_route_cache_misses
            .saturating_sub(misses_before);
        batch.route_cache_invalidations = self
            .metadata_route_cache_invalidations
            .saturating_sub(invalidations_before);
        batch.route_cache_expired_invalidations = self
            .metadata_route_cache_expired_invalidations
            .saturating_sub(expired_invalidations_before);
        Ok(batch)
    }

}
