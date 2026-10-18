impl MatrixObjectStore {
    fn check_expected_metadata_shard_topology(
        &self,
        expected_epoch: Option<u64>,
        expected_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        if expected_epoch.is_none() && expected_checksum.is_none() {
            return Ok(());
        }
        let (actual_epoch, actual_checksum) =
            metadata_shard_topology_fingerprint(&self.meta_service);
        if let Some(expected_epoch) = expected_epoch {
            if expected_epoch != actual_epoch {
                return Err(ObjectError::InvalidArgument(format!(
                    "metadata shard topology epoch mismatch: expected {} got {}",
                    expected_epoch, actual_epoch
                )));
            }
        }
        if let Some(expected_checksum) = expected_checksum {
            if expected_checksum.is_empty() {
                return Err(ObjectError::InvalidArgument(
                    "metadata shard topology checksum expectation is empty".to_string(),
                ));
            }
            if expected_checksum != actual_checksum {
                return Err(ObjectError::InvalidArgument(format!(
                    "metadata shard topology checksum mismatch: expected {} got {}",
                    expected_checksum, actual_checksum
                )));
            }
        }
        Ok(())
    }

    fn check_expected_metadata_coordinator_session(
        &self,
        now_ms: u64,
        expected_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        let Some(expected_checksum) = expected_checksum else {
            return Ok(());
        };
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata coordinator session checksum expectation is empty".to_string(),
            ));
        }
        let actual_checksum = metadata_route_session_fingerprint(&self.meta_service, now_ms);
        if expected_checksum != actual_checksum {
            return Err(ObjectError::InvalidArgument(format!(
                "metadata coordinator session checksum mismatch: expected {} got {}",
                expected_checksum, actual_checksum
            )));
        }
        Ok(())
    }

    fn ensure_metadata_route_cache_fresh(&mut self) {
        let (_, topology_checksum) = metadata_route_topology_fingerprint(&self.meta_service);
        let stale = self
            .metadata_route_cache
            .values()
            .any(|entry| entry.topology_checksum != topology_checksum);
        if stale {
            self.invalidate_metadata_route_cache();
        }
    }

    fn invalidate_metadata_route_cache(&mut self) {
        if !self.metadata_route_cache.is_empty() {
            self.metadata_route_cache_invalidations =
                self.metadata_route_cache_invalidations.saturating_add(1);
        }
        self.metadata_route_cache.clear();
        self.metadata_route_shared.clear();
        self.metadata_route_checksums.clear();
    }

    fn expire_metadata_route_cache(&mut self, now_ms: u64) {
        let expired_keys: Vec<String> = self
            .metadata_route_cache
            .iter()
            .filter(|(_, entry)| entry.expire_time_ms <= now_ms)
            .map(|(key, _)| key.clone())
            .collect();
        if !expired_keys.is_empty() {
            for key in &expired_keys {
                self.metadata_route_cache.remove(key);
            }
            let expired = expired_keys.len() as u64;
            self.metadata_route_cache_invalidations = self
                .metadata_route_cache_invalidations
                .saturating_add(expired);
            self.metadata_route_cache_expired_invalidations = self
                .metadata_route_cache_expired_invalidations
                .saturating_add(expired);
        }
    }

    fn prune_metadata_route_cache(&mut self) {
        if self.metadata_route_cache.len() <= self.options.metadata_route_cache_entries {
            return;
        }
        let mut eviction_candidates: Vec<_> = self
            .metadata_route_cache
            .iter()
            .map(|(key, entry)| (entry.last_access_sequence, key.clone()))
            .collect();
        eviction_candidates.sort_by_key(|(last_access_sequence, _)| *last_access_sequence);
        let mut candidate_iter = eviction_candidates.into_iter();
        while self.metadata_route_cache.len() > self.options.metadata_route_cache_entries {
            let Some((_, evict_key)) = candidate_iter.next() else {
                break;
            };
            self.metadata_route_cache.remove(&evict_key);
            self.metadata_route_cache_invalidations =
                self.metadata_route_cache_invalidations.saturating_add(1);
            self.metadata_route_cache_capacity_evictions = self
                .metadata_route_cache_capacity_evictions
                .saturating_add(1);
        }
    }

}
