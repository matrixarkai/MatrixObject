impl MatrixMetaService {
    pub fn metadata_shard(&self, shard_id: &str) -> Result<MetadataShard, ObjectError> {
        validate_meta_key(shard_id)?;
        self.shards
            .get(shard_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata shard missing".to_string()))
    }

    pub fn list_shard_inventory(
        &self,
        owner_node_id: Option<&str>,
        after_shard_id: Option<&str>,
        max_shards: usize,
        max_payload_bytes: u64,
        include_record_stats: bool,
    ) -> Result<MetadataShardInventoryPage, ObjectError> {
        if let Some(owner_node_id) = owner_node_id {
            validate_meta_key(owner_node_id)?;
        }
        if let Some(after_shard_id) = after_shard_id {
            validate_meta_key(after_shard_id)?;
        }
        if max_shards == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata shard inventory max_shards must be positive".to_string(),
            ));
        }
        let mut page = MetadataShardInventoryPage {
            owner_node_id: owner_node_id.map(str::to_string),
            ..MetadataShardInventoryPage::default()
        };
        for (shard_id, shard) in &self.shards {
            if after_shard_id.map_or(false, |marker| shard_id.as_str() <= marker) {
                continue;
            }
            page.scanned_shards = page.scanned_shards.saturating_add(1);
            if owner_node_id.map_or(false, |owner| shard.owner_node_id != owner) {
                page.skipped_shards = page.skipped_shards.saturating_add(1);
                continue;
            }
            if page.items.len() >= max_shards {
                page.has_more = true;
                break;
            }
            let item = MetadataShardInventoryItem {
                shard: shard.clone(),
                record_stats: include_record_stats.then(|| self.metadata_shard_record_stats(shard)),
            };
            let item_payload = metadata_shard_inventory_item_payload_bytes(&item);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                page.has_more = true;
                if page.items.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata shard inventory item exceeds payload budget".to_string(),
                    ));
                }
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_shard_marker = Some(shard_id.clone());
            page.items.push(item);
        }
        Ok(page)
    }

    fn metadata_shard_record_stats(&self, shard: &MetadataShard) -> MetadataShardRecordStats {
        let mut stats = MetadataShardRecordStats::default();
        for (key, record) in self.records.range(shard.start_key.clone()..) {
            if !shard.end_key.is_empty() && key >= &shard.end_key {
                break;
            }
            let payload_bytes = metadata_record_payload_bytes(record);
            stats.total_payload_bytes = stats.total_payload_bytes.saturating_add(payload_bytes);
            if record.deleted {
                stats.deleted_records = stats.deleted_records.saturating_add(1);
                stats.deleted_value_bytes = stats
                    .deleted_value_bytes
                    .saturating_add(record.value.len() as u64);
            } else {
                stats.live_records = stats.live_records.saturating_add(1);
                stats.live_value_bytes = stats
                    .live_value_bytes
                    .saturating_add(record.value.len() as u64);
            }
        }
        stats
    }

    pub fn resolve_shard_routes(
        &self,
        prefix: &str,
        after_shard_id: Option<&str>,
        max_routes: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<MetadataShardRoutePage, ObjectError> {
        self.resolve_shard_routes_fenced(
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

    pub fn resolve_shard_routes_fenced(
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
        validate_meta_key(prefix)?;
        if let Some(after_shard_id) = after_shard_id {
            validate_meta_key(after_shard_id)?;
        }
        if max_routes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata shard route max_routes must be positive".to_string(),
            ));
        }
        let (topology_epoch, shard_topology_checksum) = metadata_shard_topology_fingerprint(self);
        let session_checksum = metadata_route_session_fingerprint(self, now_ms);
        if let Some(expected_epoch) = expected_shard_topology_epoch {
            if expected_epoch != topology_epoch {
                return Err(ObjectError::InvalidArgument(format!(
                    "metadata shard topology epoch mismatch: expected {} got {}",
                    expected_epoch, topology_epoch
                )));
            }
        }
        if let Some(expected_checksum) = expected_shard_topology_checksum {
            if expected_checksum.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard topology checksum expectation is empty".to_string(),
                ));
            }
            if expected_checksum != shard_topology_checksum {
                return Err(ObjectError::InvalidArgument(format!(
                    "metadata shard topology checksum mismatch: expected {} got {}",
                    expected_checksum, shard_topology_checksum
                )));
            }
        }
        if let Some(expected_checksum) = expected_coordinator_session_checksum {
            if expected_checksum.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata coordinator session checksum expectation is empty".to_string(),
                ));
            }
            if expected_checksum != session_checksum {
                return Err(ObjectError::InvalidArgument(format!(
                    "metadata coordinator session checksum mismatch: expected {} got {}",
                    expected_checksum, session_checksum
                )));
            }
        }
        let mut page = MetadataShardRoutePage {
            prefix: prefix.to_string(),
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
            ..MetadataShardRoutePage::default()
        };
        for (shard_id, shard) in &self.shards {
            if after_shard_id.map_or(false, |marker| shard_id.as_str() <= marker) {
                continue;
            }
            page.scanned_shards = page.scanned_shards.saturating_add(1);
            if !metadata_shard_overlaps_prefix(shard, prefix) {
                page.skipped_shards = page.skipped_shards.saturating_add(1);
                continue;
            }
            if page.routes.len() >= max_routes {
                page.has_more = true;
                break;
            }
            let active_owner_session = self
                .active_fenced_owner_session(&shard.owner_node_id, now_ms)
                .cloned();
            let preferred_endpoint = active_owner_session
                .as_ref()
                .map_or_else(String::new, |session| session.endpoint.clone());
            let fallback_endpoints =
                self.metadata_route_fallback_endpoints(&preferred_endpoint, now_ms);
            let route = MetadataShardRoute {
                shard: shard.clone(),
                active_owner_session,
                preferred_endpoint,
                fallback_endpoints,
                contains_prefix_start: metadata_shard_contains_key(shard, prefix),
                owner_serving: false,
            };
            let mut route = route;
            route.owner_serving = !route.shard.draining && route.active_owner_session.is_some();
            let route_payload = metadata_shard_route_payload_bytes(&route);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(route_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                page.has_more = true;
                if page.routes.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata shard route exceeds payload budget".to_string(),
                    ));
                }
                break;
            }
            page.payload_bytes = page.payload_bytes.saturating_add(route_payload);
            page.next_shard_marker = Some(shard_id.clone());
            page.routes.push(route);
        }
        Ok(page)
    }

    pub fn shard_count(&self) -> u64 {
        self.shards.len() as u64
    }

    fn metadata_route_fallback_endpoints(
        &self,
        preferred_endpoint: &str,
        now_ms: u64,
    ) -> Vec<String> {
        let mut seen = BTreeSet::new();
        let mut endpoints = Vec::new();
        endpoints.reserve(self.coordinator_sessions.len().saturating_sub(1));
        for session in self.coordinator_sessions.values() {
            if session.expire_time_ms <= now_ms || session.endpoint == preferred_endpoint {
                continue;
            }
            if !self.coordinator_session_endpoint_is_fenced(&session.endpoint) {
                continue;
            }
            if seen.insert(session.endpoint.as_str()) {
                endpoints.push(session.endpoint.clone());
            }
        }
        endpoints
    }
}
