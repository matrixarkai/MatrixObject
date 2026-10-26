// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn campaign_leader(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<PlacementLeaderLease, ObjectError> {
        self.validate_leader_candidate(node_id, ttl_ms)?;
        if let Some(existing) = &self.leader_lease {
            if existing.expire_time_ms > now_ms && existing.leader_node_id != node_id {
                return Err(ObjectError::PreconditionFailed(
                    "root leader lease is held by another node".to_string(),
                ));
            }
        }
        let epoch = self
            .leader_lease
            .as_ref()
            .map_or(1, |lease| lease.epoch.saturating_add(1));
        let lease = PlacementLeaderLease {
            leader_node_id: node_id.to_string(),
            epoch,
            acquire_time_ms: now_ms,
            expire_time_ms: now_ms.saturating_add(ttl_ms),
            ttl_ms,
        };
        self.leader_lease = Some(lease.clone());
        self.advance_commit_index();
        Ok(lease)
    }

    pub fn campaign_leader_idempotent(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(PlacementLeaderLease, PlacementLeaderLeaseReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = root_leader_campaign_request_checksum(node_id, ttl_ms, now_ms);
        if let Some(existing) = self.leader_lease_receipts.get(idempotency_key) {
            if existing.operation != "campaign"
                || existing.request_checksum != request_checksum
                || existing.lease.leader_node_id != node_id
            {
                return Err(ObjectError::PreconditionFailed(
                    "root leader lease idempotency key reused with different campaign".to_string(),
                ));
            }
            return Ok((existing.lease.clone(), existing.clone()));
        }
        let lease = self.campaign_leader(node_id, ttl_ms, now_ms)?;
        let receipt = PlacementLeaderLeaseReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: "campaign".to_string(),
            request_checksum,
            lease: lease.clone(),
            commit_index: self.placement_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            receipt_proof_checksum: String::new(),
        };
        let receipt = receipt.with_proof_checksum();
        self.leader_lease_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok((lease, receipt))
    }

    pub fn renew_leader(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<PlacementLeaderLease, ObjectError> {
        self.validate_leader_candidate(node_id, ttl_ms)?;
        let lease = self
            .leader_lease
            .as_mut()
            .ok_or_else(|| ObjectError::NotFound("root leader lease missing".to_string()))?;
        if lease.leader_node_id != node_id || lease.epoch != epoch || lease.expire_time_ms <= now_ms
        {
            return Err(ObjectError::PreconditionFailed(
                "root leader lease renew fence mismatch".to_string(),
            ));
        }
        lease.expire_time_ms = now_ms.saturating_add(ttl_ms);
        lease.ttl_ms = ttl_ms;
        let renewed_lease = lease.clone();
        self.advance_commit_index();
        Ok(renewed_lease)
    }

    pub fn renew_leader_idempotent(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(PlacementLeaderLease, PlacementLeaderLeaseReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = root_leader_renew_request_checksum(node_id, epoch, ttl_ms, now_ms);
        if let Some(existing) = self.leader_lease_receipts.get(idempotency_key) {
            if existing.operation != "renew"
                || existing.request_checksum != request_checksum
                || existing.lease.leader_node_id != node_id
                || existing.lease.epoch != epoch
            {
                return Err(ObjectError::PreconditionFailed(
                    "root leader lease idempotency key reused with different renew".to_string(),
                ));
            }
            return Ok((existing.lease.clone(), existing.clone()));
        }
        let lease = self.renew_leader(node_id, epoch, ttl_ms, now_ms)?;
        let receipt = PlacementLeaderLeaseReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: "renew".to_string(),
            request_checksum,
            lease: lease.clone(),
            commit_index: self.placement_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            receipt_proof_checksum: String::new(),
        };
        let receipt = receipt.with_proof_checksum();
        self.leader_lease_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok((lease, receipt))
    }

    pub fn leader_lease(&self) -> Option<PlacementLeaderLease> {
        self.leader_lease.clone()
    }

    pub fn list_leader_lease_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        leader_node_id: Option<&str>,
    ) -> Result<PlacementLeaderLeaseReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(operation) = operation {
            if operation != "campaign" && operation != "renew" {
                return Err(ObjectError::InvalidArgument(
                    "root leader lease receipt operation filter must be campaign or renew"
                        .to_string(),
                ));
            }
        }
        if let Some(node_id) = leader_node_id {
            validate_node_id(node_id)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = PlacementLeaderLeaseReceiptPage {
            total_receipt_count: self.leader_lease_receipts.len() as u64,
            operation: operation.map(str::to_string),
            leader_node_id: leader_node_id.map(str::to_string),
            limit: limit as u64,
            ..PlacementLeaderLeaseReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &PlacementLeaderLeaseReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.leader_lease_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.leader_lease_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if operation.is_some_and(|expected| receipt.operation != expected) {
                page.skipped_operation_count = page.skipped_operation_count.saturating_add(1);
                continue;
            }
            if leader_node_id.is_some_and(|expected| receipt.lease.leader_node_id != expected) {
                page.skipped_leader_count = page.skipped_leader_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                page.next_marker = Some(idempotency_key.clone());
                break;
            }
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn export_placement_snapshot(&self) -> PlacementSnapshot {
        let mut snapshot = PlacementSnapshot {
            nodes: self.nodes.clone(),
            placements: self.placements.clone(),
            leader_lease: self.leader_lease.clone(),
            leader_lease_receipts: self.leader_lease_receipts.clone(),
            coordinator_config: self.placement_coordinator_config.clone(),
            next_commit_index: self.placement_commit_index,
            checksum: String::new(),
        };
        snapshot.checksum = snapshot.checksum_material_hash();
        snapshot
    }

    pub fn root_state_fingerprint(&self) -> String {
        if let Some(fingerprint) = self.root_state_fingerprint_cache.borrow().as_ref() {
            return fingerprint.clone();
        }
        let snapshot = self.export_placement_snapshot();
        let leader_node_id = snapshot
            .leader_lease
            .as_ref()
            .map(|lease| lease.leader_node_id.as_str())
            .unwrap_or("");
        let mut material = Vec::with_capacity(
            encoded_string_len("MOSRSROOT-FINGERPRINT1")
                + encoded_string_len(&snapshot.checksum)
                + encoded_string_len(leader_node_id)
                + std::mem::size_of::<u64>() * 6,
        );
        append_string(&mut material, "MOSRSROOT-FINGERPRINT1");
        append_string(&mut material, &snapshot.checksum);
        append_u64(&mut material, snapshot.nodes.len() as u64);
        append_u64(&mut material, snapshot.placements.len() as u64);
        append_u64(&mut material, snapshot.leader_lease_receipts.len() as u64);
        append_u64(&mut material, snapshot.next_commit_index);
        if let Some(lease) = &snapshot.leader_lease {
            append_string(&mut material, leader_node_id);
            append_u64(&mut material, lease.epoch);
            append_u64(&mut material, lease.expire_time_ms);
        } else {
            append_string(&mut material, leader_node_id);
            append_u64(&mut material, 0);
            append_u64(&mut material, 0);
        }
        let fingerprint = etag(&material);
        *self.root_state_fingerprint_cache.borrow_mut() = Some(fingerprint.clone());
        fingerprint
    }

    fn placement_snapshot_cache_stamp(&self) -> String {
        format!(
            "root-commit:{}:{}:{}:{}",
            self.placement_commit_index,
            self.nodes.len(),
            self.placements.len(),
            self.leader_lease_receipts.len()
        )
    }

    fn export_placement_snapshot_image(&self) -> Result<PlacementSnapshotExportCache, ObjectError> {
        let stamp = self.placement_snapshot_cache_stamp();
        if let Some(cache) = self.placement_snapshot_export_cache.borrow().as_ref() {
            if cache.stamp == stamp {
                return Ok(cache.clone());
            }
        }
        let snapshot = self.export_placement_snapshot();
        let bytes = snapshot.to_bytes()?;
        let cache = PlacementSnapshotExportCache {
            stamp,
            checksum: etag(&bytes),
            bytes: std::sync::Arc::from(bytes.into_boxed_slice()),
        };
        *self.placement_snapshot_export_cache.borrow_mut() = Some(cache.clone());
        Ok(cache)
    }

    pub fn export_placement_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<PlacementSnapshotChunk, ObjectError> {
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "root snapshot chunk budget must be positive".to_string(),
            ));
        }
        let image = self.export_placement_snapshot_image()?;
        let chunk = export_root_snapshot_chunk_bytes_with_checksum(
            image.bytes.as_ref(),
            &image.checksum,
            offset,
            max_payload_bytes,
        )?;
        if chunk.eof {
            let mut cache = self.placement_snapshot_export_cache.borrow_mut();
            if cache
                .as_ref()
                .map(|cached| cached.stamp == image.stamp)
                .unwrap_or(false)
            {
                *cache = None;
            }
        }
        Ok(chunk)
    }

    /// Take the roster out of the metadata snapshot that now carries it.
    ///
    /// # WHY THIS IS NOT `install_placement_snapshot`
    ///
    /// That one takes a snapshot of its own, which is still how a roster arrives over the
    /// wire. This one reads the fields the metadata snapshot carries, so loading a store does
    /// not have to build a placement snapshot it would immediately take apart -- and would
    /// have to stamp a checksum for, purely to satisfy a validator.
    ///
    /// It validates with the same shared rules either way.
    pub fn install_placement_roster(&mut self, snapshot: &MetaSnapshot) -> Result<(), ObjectError> {
        validate_placement_roster(
            &snapshot.placement_nodes,
            &snapshot.placements,
            &snapshot.placement_leader_lease,
            &snapshot.placement_leader_lease_receipts,
            &snapshot.placement_coordinator_config,
            snapshot.placement_commit_index,
        )?;
        self.invalidate_snapshot_export_cache();
        self.nodes = snapshot.placement_nodes.clone();
        self.placements = snapshot.placements.clone();
        self.leader_lease = snapshot.placement_leader_lease.clone();
        self.leader_lease_receipts = snapshot.placement_leader_lease_receipts.clone();
        self.placement_coordinator_config = snapshot.placement_coordinator_config.clone();
        self.placement_commit_index = snapshot.placement_commit_index;
        Ok(())
    }

    pub fn install_placement_snapshot(&mut self, snapshot: PlacementSnapshot) -> Result<(), ObjectError> {
        snapshot.validate()?;
        self.invalidate_snapshot_export_cache();
        self.nodes = snapshot.nodes;
        self.placements = snapshot.placements;
        self.leader_lease = snapshot.leader_lease;
        self.leader_lease_receipts = snapshot.leader_lease_receipts;
        self.placement_coordinator_config = snapshot.coordinator_config;
        self.placement_commit_index = snapshot.next_commit_index;
        Ok(())
    }

    pub fn install_placement_snapshot_bytes(&mut self, bytes: &[u8]) -> Result<PlacementSnapshot, ObjectError> {
        let snapshot = PlacementSnapshot::from_bytes(bytes)?;
        self.install_placement_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    pub fn install_placement_snapshot_bytes_in_place(&mut self, bytes: &[u8]) -> Result<(), ObjectError> {
        self.install_placement_snapshot(PlacementSnapshot::from_bytes(bytes)?)
    }
}
