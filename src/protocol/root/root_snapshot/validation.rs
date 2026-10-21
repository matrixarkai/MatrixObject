// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl RootSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.next_commit_index == 0 {
            return Err(ObjectError::Corruption(
                "root snapshot next commit index is invalid".to_string(),
            ));
        }
        validate_metadata_coordinator_config(&self.coordinator_config)?;
        for (node_id, node) in &self.nodes {
            validate_node_id(node_id)?;
            if node.node_id != *node_id
                || node.used_bytes > node.capacity_bytes
                || node.committed_bytes > node.capacity_bytes
            {
                return Err(ObjectError::Corruption(
                    "root snapshot node is invalid".to_string(),
                ));
            }
            MatrixMetaService::validate_page_allocator_shard(node)?;
        }
        let mut committed: BTreeMap<String, u64> = BTreeMap::new();
        for (object_id, placement) in &self.placements {
            if placement.object_id != *object_id || placement.node_ids.is_empty() {
                return Err(ObjectError::Corruption(
                    "root snapshot placement is invalid".to_string(),
                ));
            }
            for node_id in &placement.node_ids {
                if !self.nodes.contains_key(node_id) {
                    return Err(ObjectError::Corruption(
                        "root snapshot placement references missing node".to_string(),
                    ));
                }
                let entry = committed.entry(node_id.clone()).or_insert(0);
                *entry = entry.saturating_add(placement.length);
            }
        }
        for (node_id, bytes) in committed {
            if self
                .nodes
                .get(&node_id)
                .map_or(0, |node| node.committed_bytes)
                != bytes
            {
                return Err(ObjectError::Corruption(
                    "root snapshot committed bytes mismatch".to_string(),
                ));
            }
        }
        for (node_id, node) in &self.nodes {
            if !self.placements.values().any(|placement| {
                placement
                    .node_ids
                    .iter()
                    .any(|placement_node| placement_node == node_id)
            }) && node.committed_bytes != 0
            {
                return Err(ObjectError::Corruption(
                    "root snapshot node has unowned committed bytes".to_string(),
                ));
            }
        }
        if let Some(lease) = &self.leader_lease {
            validate_node_id(&lease.leader_node_id)?;
            if lease.epoch == 0
                || lease.ttl_ms == 0
                || lease.expire_time_ms <= lease.acquire_time_ms
                || lease.expire_time_ms < lease.acquire_time_ms.saturating_add(lease.ttl_ms)
            {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease is invalid".to_string(),
                ));
            }
            let node = self.nodes.get(&lease.leader_node_id).ok_or_else(|| {
                ObjectError::Corruption(
                    "root snapshot leader lease references missing node".to_string(),
                )
            })?;
            if node.service_kind != "rootserver" || !node.online || node.draining {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease references inactive rootserver".to_string(),
                ));
            }
            if lease.ttl_ms > self.coordinator_config.session_ttl_ms {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease exceeds coordinator session ttl".to_string(),
                ));
            }
            if !is_embedded_coordinator_backend(&self.coordinator_config.backend)
                && !self
                    .coordinator_config
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint == &node.endpoint)
            {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease endpoint is outside configured coordinator endpoints"
                        .to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.leader_lease_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || (receipt.operation != "campaign" && receipt.operation != "renew")
                || receipt.lease.epoch == 0
                || receipt.lease.ttl_ms == 0
                || receipt.commit_index == 0
            {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease receipt is invalid".to_string(),
                ));
            }
            let expected = if receipt.operation == "campaign" {
                root_leader_campaign_request_checksum(
                    &receipt.lease.leader_node_id,
                    receipt.lease.ttl_ms,
                    receipt.lease.acquire_time_ms,
                )
            } else {
                root_leader_renew_request_checksum(
                    &receipt.lease.leader_node_id,
                    receipt.lease.epoch,
                    receipt.lease.ttl_ms,
                    receipt
                        .lease
                        .expire_time_ms
                        .saturating_sub(receipt.lease.ttl_ms),
                )
            };
            if receipt.request_checksum != expected {
                return Err(ObjectError::Corruption(
                    "root snapshot leader lease receipt checksum mismatch".to_string(),
                ));
            }
            receipt.verify_proof_checksum()?;
        }
        let mut checksum_compatibility_names = vec![
            "current", "v26", "v22", "v21", "v20", "v18", "v19", "v16", "v11", "v10", "v9", "v8",
            "v7", "v2",
        ];
        let mut accepted_checksums = vec![
            self.checksum_material_hash(),
            self.v26_checksum_material_hash(),
            self.v22_checksum_material_hash(),
            self.v21_checksum_material_hash(),
            self.v20_checksum_material_hash(),
            self.v18_checksum_material_hash(),
            self.v19_checksum_material_hash(),
            self.v16_checksum_material_hash(),
            self.v11_checksum_material_hash(),
            self.v10_checksum_material_hash(),
            self.v9_checksum_material_hash(),
            self.v8_checksum_material_hash(),
            self.v7_checksum_material_hash(),
            self.v2_checksum_material_hash(),
        ];
        if self.leader_lease_receipts.is_empty() {
            accepted_checksums.push(self.legacy_checksum_material_hash());
            checksum_compatibility_names.push("legacy");
        }
        if !accepted_checksums
            .iter()
            .any(|checksum| checksum == &self.checksum)
        {
            return Err(ObjectError::Corruption(format!(
                "root snapshot checksum mismatch: stored {}, current {}, accepted formats {}",
                self.checksum,
                accepted_checksums.first().cloned().unwrap_or_default(),
                checksum_compatibility_names.join(",")
            )));
        }
        Ok(())
    }
}
