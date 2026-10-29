// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PlacementSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        validate_placement_roster(
            &self.nodes,
            &self.placements,
            &self.leader_lease,
            &self.leader_lease_receipts,
            &self.coordinator_config,
            self.next_commit_index,
        )?;
        // ONE LIST OF PAIRS, NOT TWO LISTS CORRELATED BY INDEX. The names and the hashes
        // used to be separate `vec![..]`s that had to stay in the same order, and nothing
        // checked that they did -- so a format inserted into one and not the other would
        // have made the error message name the wrong format while still validating
        // correctly, which is the quietest kind of wrong.
        let accepted = self.accepted_checksum_formats();
        if !accepted.iter().any(|(_, checksum)| checksum == &self.checksum) {
            return Err(ObjectError::Corruption(format!(
                "root snapshot checksum mismatch: stored {}, current {}, accepted formats {}",
                self.checksum,
                accepted
                    .first()
                    .map(|(_, checksum)| checksum.clone())
                    .unwrap_or_default(),
                accepted
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(",")
            )));
        }
        Ok(())
    }

    /// Every checksum format this build will accept for a stored snapshot, newest first.
    ///
    /// # WHY THIS IS A FUNCTION AND NOT A LIST INSIDE `validate`
    ///
    /// The whole read-old-forever discipline rests on this list: a snapshot written by any
    /// earlier build stored one of these hashes, and dropping an entry makes that store fail
    /// to load. Measured, not assumed -- removing one and running the entire library suite
    /// left it green, so nothing anywhere asserted that an older snapshot still validates.
    /// Exposing the list is what lets a guard walk it and check each one.
    pub(crate) fn accepted_checksum_formats(&self) -> Vec<(&'static str, String)> {
        let mut accepted = vec![
            ("current", self.checksum_material_hash()),
            // The format current until the page-tier field joined the material.
            ("pre-page-tier", self.pre_page_tier_checksum_material_hash()),
            // The format that was current until the page raft endpoint counts joined the
            // material. Every snapshot written by an earlier build stored this one.
            ("v32", self.v32_checksum_material_hash()),
            ("v26", self.v26_checksum_material_hash()),
            ("v22", self.v22_checksum_material_hash()),
            ("v21", self.v21_checksum_material_hash()),
            ("v20", self.v20_checksum_material_hash()),
            ("v18", self.v18_checksum_material_hash()),
            ("v19", self.v19_checksum_material_hash()),
            ("v16", self.v16_checksum_material_hash()),
            ("v11", self.v11_checksum_material_hash()),
            ("v10", self.v10_checksum_material_hash()),
            ("v9", self.v9_checksum_material_hash()),
            ("v8", self.v8_checksum_material_hash()),
            ("v7", self.v7_checksum_material_hash()),
            ("v2", self.v2_checksum_material_hash()),
        ];
        if self.leader_lease_receipts.is_empty() {
            accepted.push(("legacy", self.legacy_checksum_material_hash()));
        }
        accepted
    }
}

/// Every rule a control-plane roster has to satisfy, whichever snapshot is carrying it.
///
/// # WHY THESE RULES LEFT THE SNAPSHOT THEY STARTED IN
///
/// The roster is moving into the metadata snapshot. Re-stating thirteen rules there would
/// create exactly the divergence this project keeps finding: two copies of one rule, and
/// nothing failing when they disagree. The caller being retired still calls this, so retiring
/// it becomes a deletion rather than a migration.
///
/// The checksum rule is NOT here. It is the only one about the snapshot envelope rather than
/// about the roster, and each snapshot stamps a checksum of its own.
///
/// All thirteen are exercised on both the write and the read path by
/// `a_rule_the_write_path_refuses_is_refused_on_the_read_path_too`, which also counts the
/// refusal sites in this file, so a rule added here owes a case there.
pub(crate) fn validate_placement_roster(
    nodes: &BTreeMap<String, PlacementNode>,
    placements: &BTreeMap<ObjectId, ObjectPlacement>,
    leader_lease: &Option<PlacementLeaderLease>,
    leader_lease_receipts: &BTreeMap<String, PlacementLeaderLeaseReceipt>,
    coordinator_config: &MetadataCoordinatorConfig,
    next_commit_index: u64,
) -> Result<(), ObjectError> {
    validate_placement_roster_with_manifests(nodes, placements, leader_lease,
        leader_lease_receipts, coordinator_config, next_commit_index, None)
}

pub(crate) fn validate_placement_roster_with_manifests(
    nodes: &BTreeMap<String, PlacementNode>,
    placements: &BTreeMap<ObjectId, ObjectPlacement>,
    leader_lease: &Option<PlacementLeaderLease>,
    leader_lease_receipts: &BTreeMap<String, PlacementLeaderLeaseReceipt>,
    coordinator_config: &MetadataCoordinatorConfig,
    next_commit_index: u64,
    manifests: Option<&crate::object_manifest::ManifestCatalog>,
) -> Result<(), ObjectError> {
    if next_commit_index == 0 {
        return Err(ObjectError::Corruption(
            "root snapshot next commit index is invalid".to_string(),
        ));
    }
    validate_metadata_coordinator_config(coordinator_config)?;
    for (node_id, node) in nodes {
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
    for (object_id, placement) in placements {
        let empty_manifest = manifests.and_then(|catalog| catalog.get(object_id))
            .is_some_and(|manifest| manifest.length == 0
                && placement.length == 0 && manifest.generation == placement.version);
        if placement.object_id != *object_id || (placement.node_ids.is_empty() && !empty_manifest) {
            return Err(ObjectError::Corruption(
                "root snapshot placement is invalid".to_string(),
            ));
        }
        for node_id in &placement.node_ids {
            if !nodes.contains_key(node_id) {
                return Err(ObjectError::Corruption(
                    "root snapshot placement references missing node".to_string(),
                ));
            }
            let entry = committed.entry(node_id.clone()).or_insert(0);
            *entry = entry.saturating_add(placement.length);
        }
    }
    for (node_id, bytes) in committed {
        if nodes
            .get(&node_id)
            .map_or(0, |node| node.committed_bytes)
            != bytes
        {
            return Err(ObjectError::Corruption(
                "root snapshot committed bytes mismatch".to_string(),
            ));
        }
    }
    for (node_id, node) in nodes {
        if !placements.values().any(|placement| {
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
    if let Some(lease) = leader_lease {
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
        let node = nodes.get(&lease.leader_node_id).ok_or_else(|| {
            ObjectError::Corruption(
                "root snapshot leader lease references missing node".to_string(),
            )
        })?;
        // THE SAME PREDICATE THE LIVE GATE USES. These two disagreeing is the worst
        // case available: it means a store writes a snapshot it cannot read back.
        if let Some(refusal) = control_plane_leader_refusal(node) {
            return Err(ObjectError::Corruption(format!(
                "a stored leader lease names a node that cannot hold one: {refusal}"
            )));
        }
        if lease.ttl_ms > coordinator_config.session_ttl_ms {
            return Err(ObjectError::Corruption(
                "root snapshot leader lease exceeds coordinator session ttl".to_string(),
            ));
        }
        if !is_embedded_coordinator_backend(&coordinator_config.backend)
            && !coordinator_config
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
    for (idempotency_key, receipt) in leader_lease_receipts {
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
            placement_leader_campaign_request_checksum(
                &receipt.lease.leader_node_id,
                receipt.lease.ttl_ms,
                receipt.lease.acquire_time_ms,
            )
        } else {
            placement_leader_renew_request_checksum(
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
    Ok(())
}
