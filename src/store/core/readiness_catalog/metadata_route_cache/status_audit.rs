// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {

    pub fn metadata_route_cache_status(&self) -> MetadataRouteCacheStatus {
        self.metadata_route_cache_status_at(0)
    }

    pub fn metadata_route_cache_status_at(&self, now_ms: u64) -> MetadataRouteCacheStatus {
        let (topology_epoch, topology_checksum) =
            metadata_route_topology_fingerprint(&self.meta_service);
        let (_, shard_topology_checksum) = metadata_shard_topology_fingerprint(&self.meta_service);
        let session_checksum = metadata_route_session_fingerprint(&self.meta_service, now_ms);
        let mut fresh_entries = 0u64;
        let mut expired_entries = 0u64;
        let mut stale_topology_entries = 0u64;
        let mut stale_session_entries = 0u64;
        let mut stale_entries = 0u64;
        for entry in self.metadata_route_cache.values() {
            let expired = now_ms > 0 && entry.expire_time_ms <= now_ms;
            let stale_topology = entry.shard_topology_checksum != shard_topology_checksum;
            let stale_session = entry.session_checksum != session_checksum;
            if expired {
                expired_entries = expired_entries.saturating_add(1);
            }
            if stale_topology {
                stale_topology_entries = stale_topology_entries.saturating_add(1);
            }
            if stale_session {
                stale_session_entries = stale_session_entries.saturating_add(1);
            }
            if expired || stale_topology || stale_session {
                stale_entries = stale_entries.saturating_add(1);
            } else {
                fresh_entries = fresh_entries.saturating_add(1);
            }
        }
        MetadataRouteCacheStatus {
            entries: self.metadata_route_cache.len() as u64,
            capacity: self.options.metadata_route_cache_entries as u64,
            ttl_ms: self.options.metadata_route_cache_ttl_ms,
            hits: self.metadata_route_cache_hits,
            misses: self.metadata_route_cache_misses,
            invalidations: self.metadata_route_cache_invalidations,
            expired_invalidations: self.metadata_route_cache_expired_invalidations,
            capacity_evictions: self.metadata_route_cache_capacity_evictions,
            pressure: self.metadata_route_cache.len() >= self.options.metadata_route_cache_entries,
            topology_epoch,
            topology_checksum,
            shard_topology_checksum,
            session_checksum,
            fresh_entries,
            expired_entries,
            stale_topology_entries,
            stale_session_entries,
            stale_entries,
            fresh: stale_entries == 0,
        }
    }

    pub fn audit_metadata_route_cache(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
    ) -> MetadataRouteCacheAudit {
        self.audit_metadata_route_cache_fenced_sampled(now_ms, evict_stale, None, None, None, 0)
            .expect("unfenced metadata route cache audit cannot fail")
    }

    pub fn audit_metadata_route_cache_fenced(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
    ) -> Result<MetadataRouteCacheAudit, ObjectError> {
        self.audit_metadata_route_cache_fenced_sampled(
            now_ms,
            evict_stale,
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
            expected_coordinator_session_checksum,
            0,
        )
    }

    pub fn audit_metadata_route_cache_fenced_sampled(
        &mut self,
        now_ms: u64,
        evict_stale: bool,
        expected_shard_topology_epoch: Option<u64>,
        expected_shard_topology_checksum: Option<&str>,
        expected_coordinator_session_checksum: Option<&str>,
        max_stale_samples: usize,
    ) -> Result<MetadataRouteCacheAudit, ObjectError> {
        self.check_expected_metadata_shard_topology(
            expected_shard_topology_epoch,
            expected_shard_topology_checksum,
        )?;
        self.check_expected_metadata_coordinator_session(
            now_ms,
            expected_coordinator_session_checksum,
        )?;
        let entries_before = self.metadata_route_cache.len() as u64;
        let status_before = self.metadata_route_cache_status_at(now_ms);
        let mut stale_samples = Vec::new();
        if max_stale_samples > 0 && status_before.stale_entries > 0 {
            let shard_topology_checksum = status_before.shard_topology_checksum.clone();
            let session_checksum = status_before.session_checksum.clone();
            for (key, entry) in self.metadata_route_cache.iter() {
                let expired = now_ms > 0 && entry.expire_time_ms <= now_ms;
                let stale_topology = entry.shard_topology_checksum != shard_topology_checksum;
                let stale_session = entry.session_checksum != session_checksum;
                if !(expired || stale_topology || stale_session) {
                    continue;
                }
                let mut reasons = Vec::new();
                if expired {
                    reasons.push("expired");
                }
                if stale_topology {
                    reasons.push("stale_topology");
                }
                if stale_session {
                    reasons.push("stale_session");
                }
                stale_samples.push(MetadataRouteCacheStaleSample {
                    key: key.clone(),
                    shard_id: entry.route.shard.shard_id.clone(),
                    reason: reasons.join(","),
                    expire_time_ms: entry.expire_time_ms,
                    entry_topology_checksum: entry.shard_topology_checksum.to_string(),
                    entry_session_checksum: entry.session_checksum.to_string(),
                    current_topology_checksum: shard_topology_checksum.clone(),
                    current_session_checksum: session_checksum.clone(),
                });
                if stale_samples.len() >= max_stale_samples {
                    break;
                }
            }
        }
        let mut evicted_entries = 0u64;
        if evict_stale && status_before.stale_entries > 0 {
            let shard_topology_checksum = status_before.shard_topology_checksum.clone();
            let session_checksum = status_before.session_checksum.clone();
            let stale_keys: Vec<String> = self
                .metadata_route_cache
                .iter()
                .filter(|(_, entry)| {
                    (now_ms > 0 && entry.expire_time_ms <= now_ms)
                        || entry.shard_topology_checksum != shard_topology_checksum
                        || entry.session_checksum != session_checksum
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in stale_keys {
                if let Some(entry) = self.metadata_route_cache.remove(&key) {
                    evicted_entries = evicted_entries.saturating_add(1);
                    self.metadata_route_cache_invalidations =
                        self.metadata_route_cache_invalidations.saturating_add(1);
                    if now_ms > 0 && entry.expire_time_ms <= now_ms {
                        self.metadata_route_cache_expired_invalidations = self
                            .metadata_route_cache_expired_invalidations
                            .saturating_add(1);
                    }
                }
            }
        }
        let status = self.metadata_route_cache_status_at(now_ms);
        Ok(MetadataRouteCacheAudit {
            entries_before,
            entries_after: self.metadata_route_cache.len() as u64,
            evicted_entries,
            eviction_requested: evict_stale,
            expected_metadata_shard_topology_epoch: expected_shard_topology_epoch,
            expected_metadata_shard_topology_checksum: expected_shard_topology_checksum
                .unwrap_or_default()
                .to_string(),
            expected_metadata_coordinator_session_checksum: expected_coordinator_session_checksum
                .unwrap_or_default()
                .to_string(),
            status,
            stale_samples,
        })
    }

}
