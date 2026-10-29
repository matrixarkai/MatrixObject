// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Binary outcomes for the metaserver placement API.
use crate::object_manifest::{ManifestOutcome, ObjectManifest};
use crate::{MatrixMetaService, ObjectError, ObjectId, ObjectPlacement, PlacementNode};
use prost::Message;
use std::collections::BTreeSet;
pub const PLACEMENT_OUTCOME_MAGIC: &[u8; 4] = b"MOMP";
const LIMIT: usize = 8 * 1024 * 1024;
#[derive(Clone, PartialEq, Message)]
pub struct NodeUsage {
    #[prost(string, tag = "1")]
    pub node_id: String,
    #[prost(uint64, tag = "2")]
    pub used_bytes: u64,
    #[prost(uint64, tag = "3")]
    pub capacity_bytes: u64,
    #[prost(uint64, tag = "4")]
    pub committed_bytes: u64,
    #[prost(bool, tag = "5")]
    pub draining: bool,
    #[prost(bool, tag = "6")]
    pub online: bool,
    #[prost(uint64, tag = "7")]
    pub last_heartbeat_ms: u64,
}
impl NodeUsage {
    fn from_node(n: &PlacementNode) -> Self {
        Self {
            node_id: n.node_id.clone(),
            used_bytes: n.used_bytes,
            capacity_bytes: n.capacity_bytes,
            committed_bytes: n.committed_bytes,
            draining: n.draining,
            online: n.online,
            last_heartbeat_ms: n.last_heartbeat_ms,
        }
    }
}
#[derive(Clone, PartialEq, Message)]
pub struct PlacementValue {
    #[prost(string, tag = "1")]
    pub bucket: String,
    #[prost(string, tag = "2")]
    pub key: String,
    #[prost(uint64, tag = "3")]
    pub version: u64,
    #[prost(uint64, tag = "4")]
    pub length: u64,
    #[prost(string, repeated, tag = "5")]
    pub node_ids: Vec<String>,
    #[prost(uint64, tag = "6")]
    pub update_time_ms: u64,
}
impl PlacementValue {
    fn from_value(v: &ObjectPlacement) -> Self {
        Self {
            bucket: v.object_id.bucket.to_string(),
            key: v.object_id.key.to_string(),
            version: v.version,
            length: v.length,
            node_ids: v.node_ids.clone(),
            update_time_ms: v.update_time_ms,
        }
    }
    fn id(&self) -> ObjectId {
        ObjectId::new(self.bucket.clone(), self.key.clone())
    }
    fn value(&self) -> ObjectPlacement {
        ObjectPlacement {
            object_id: self.id(),
            version: self.version,
            length: self.length,
            node_ids: self.node_ids.clone(),
            update_time_ms: self.update_time_ms,
        }
    }
}
#[derive(Clone, PartialEq, Message)]
pub struct NodeChange {
    #[prost(message, optional, tag = "1")]
    pub before: Option<NodeUsage>,
    #[prost(message, optional, tag = "2")]
    pub after: Option<NodeUsage>,
}
#[derive(Clone, PartialEq, Message)]
pub struct PlacementChange {
    #[prost(message, optional, tag = "1")]
    pub before: Option<PlacementValue>,
    #[prost(message, optional, tag = "2")]
    pub after: Option<PlacementValue>,
}
#[derive(Clone, PartialEq, Message)]
pub struct PlacementOutcome {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub before_index: u64,
    #[prost(uint64, tag = "3")]
    pub after_index: u64,
    #[prost(message, repeated, tag = "4")]
    pub nodes: Vec<NodeChange>,
    #[prost(message, repeated, tag = "5")]
    pub placements: Vec<PlacementChange>,
    #[prost(message, optional, tag = "6")]
    pub manifest: Option<ManifestOutcome>,
}
fn invalid(s: &str) -> ObjectError {
    ObjectError::Corruption(s.to_string())
}

fn manifest_groups(manifest: &ObjectManifest) -> Vec<String> {
    manifest
        .extents
        .iter()
        .map(|e| e.group.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
fn matches_manifest(placement: &PlacementValue, manifest: &ObjectManifest) -> bool {
    placement.id() == manifest.object_id()
        && placement.version == manifest.generation
        && placement.length == manifest.length
        && placement.node_ids == manifest_groups(manifest)
}
impl PlacementOutcome {
    fn validate_manifest_binding(&self) -> Result<(), ObjectError> {
        match (self.version, &self.manifest) {
            (1, None) => Ok(()),
            (2, Some(manifest)) => {
                manifest.validate()?;
                if self.placements.len() != 1 {
                    return Err(invalid("manifest publication needs one placement change"));
                }
                let change = &self.placements[0];
                let after = manifest.after.as_ref().unwrap();
                if !change
                    .after
                    .as_ref()
                    .is_some_and(|p| matches_manifest(p, after))
                    || match (&change.before, &manifest.before) {
                        (None, None) => false,
                        (Some(p), Some(m)) => !matches_manifest(p, m),
                        _ => true,
                    }
                {
                    return Err(invalid("manifest publication placement binding mismatch"));
                }
                Ok(())
            }
            _ => Err(invalid("unsupported placement outcome version or manifest")),
        }
    }

    /// Decide one atomic placement and manifest transition without changing service state.
    /// The caller must verify configured group identities and durable data extents before
    /// proposing this outcome; structural validation alone cannot establish either fact.
    pub fn publish_manifest(
        service: &MatrixMetaService,
        manifest: ManifestOutcome,
        update_time_ms: u64,
    ) -> Result<Self, ObjectError> {
        manifest.validate()?;
        let after = manifest.after.as_ref().unwrap();
        let before = service
            .placements
            .get(&after.object_id())
            .map(PlacementValue::from_value);
        let placement = PlacementValue {
            bucket: after.bucket.clone(),
            key: after.key.clone(),
            version: after.generation,
            length: after.length,
            node_ids: manifest_groups(after),
            update_time_ms,
        };
        let ids: BTreeSet<_> = before
            .iter()
            .flat_map(|p| p.node_ids.iter())
            .chain(placement.node_ids.iter())
            .cloned()
            .collect();
        let mut nodes = Vec::new();
        for id in ids {
            let current = service
                .nodes
                .get(&id)
                .ok_or_else(|| invalid("manifest group is missing"))?;
            let prior = NodeUsage::from_node(current);
            let mut next = prior.clone();
            let released = before
                .as_ref()
                .filter(|p| p.node_ids.contains(&id))
                .map_or(0, |p| p.length);
            let added = if placement.node_ids.contains(&id) {
                placement.length
            } else {
                0
            };
            next.committed_bytes = prior
                .committed_bytes
                .checked_sub(released)
                .and_then(|v| v.checked_add(added))
                .ok_or_else(|| invalid("manifest capacity accounting overflow"))?;
            if next != prior {
                nodes.push(NodeChange {
                    before: Some(prior),
                    after: Some(next),
                });
            }
        }
        let outcome = Self {
            version: 2,
            before_index: service.placement_commit_index,
            after_index: service
                .placement_commit_index
                .checked_add(1)
                .ok_or_else(|| invalid("placement index exhausted"))?,
            nodes,
            placements: vec![PlacementChange {
                before,
                after: Some(placement),
            }],
            manifest: Some(manifest),
        };
        outcome.validate_manifest_binding()?;
        outcome.validate_publication(service, true)?;
        outcome.to_payload()?;
        Ok(outcome)
    }

    fn validate_publication(
        &self,
        service: &MatrixMetaService,
        admit: bool,
    ) -> Result<(), ObjectError> {
        let manifest = self.manifest.as_ref().unwrap();
        let after = manifest.after.as_ref().unwrap();
        if service.object_manifests.get(&after.object_id()) != manifest.before.as_ref() {
            return Err(invalid("manifest publication prior state mismatch"));
        }
        service.object_manifests.check_outcome(manifest)?;
        let change = &self.placements[0];
        if service
            .placements
            .get(&after.object_id())
            .map(PlacementValue::from_value)
            != change.before
        {
            return Err(invalid("manifest publication prior placement mismatch"));
        }
        let placement = change.after.as_ref().unwrap();
        let ids: BTreeSet<_> = change
            .before
            .iter()
            .flat_map(|p| p.node_ids.iter())
            .chain(placement.node_ids.iter())
            .cloned()
            .collect();
        let mut expected = Vec::new();
        for id in ids {
            let node = service
                .nodes
                .get(&id)
                .ok_or_else(|| invalid("manifest group is missing"))?;
            let before = NodeUsage::from_node(node);
            let mut after = before.clone();
            let released = change
                .before
                .as_ref()
                .filter(|p| p.node_ids.contains(&id))
                .map_or(0, |p| p.length);
            let added = if placement.node_ids.contains(&id) {
                placement.length
            } else {
                0
            };
            after.committed_bytes = before
                .committed_bytes
                .checked_sub(released)
                .and_then(|v| v.checked_add(added))
                .ok_or_else(|| invalid("manifest capacity accounting overflow"))?;
            if admit
                && placement.node_ids.contains(&id)
                && (!MatrixMetaService::node_accepts_new_placement(node)
                    || after.committed_bytes
                        > MatrixMetaService::node_effective_capacity_bytes(node))
            {
                return Err(invalid("manifest group is not writable or lacks capacity"));
            }
            if before != after {
                expected.push(NodeChange {
                    before: Some(before),
                    after: Some(after),
                });
            }
        }
        if self.nodes != expected {
            return Err(invalid("manifest publication capacity changes mismatch"));
        }
        Ok(())
    }
}
impl PlacementOutcome {
    /// Capture only records the operation can change, before executing it once.
    pub fn capture(
        service: &MatrixMetaService,
        nodes: Vec<String>,
        object: Option<ObjectId>,
    ) -> Result<Self, ObjectError> {
        let mut changes = Vec::new();
        for id in nodes.into_iter().collect::<BTreeSet<_>>() {
            let before = NodeUsage::from_node(
                service
                    .nodes
                    .get(&id)
                    .ok_or_else(|| ObjectError::NotFound("outcome node missing".to_string()))?,
            );
            changes.push(NodeChange {
                before: Some(before.clone()),
                after: Some(before),
            });
        }
        let placements = object
            .into_iter()
            .map(|id| {
                let before = service.placements.get(&id).map(PlacementValue::from_value);
                let after = before.clone().unwrap_or(PlacementValue {
                    bucket: id.bucket.to_string(),
                    key: id.key.to_string(),
                    ..Default::default()
                });
                PlacementChange {
                    before,
                    after: Some(after),
                }
            })
            .collect();
        Ok(Self {
            version: 1,
            before_index: service.placement_commit_index,
            after_index: service.placement_commit_index,
            nodes: changes,
            placements,
            manifest: None,
        })
    }
    pub fn finish(mut self, service: &MatrixMetaService) -> Result<Self, ObjectError> {
        self.after_index = service.placement_commit_index;
        for change in &mut self.nodes {
            let id = &change
                .before
                .as_ref()
                .ok_or_else(|| ObjectError::NotFound("outcome node missing".to_string()))?
                .node_id;
            change.after = Some(NodeUsage::from_node(
                service
                    .nodes
                    .get(id)
                    .ok_or_else(|| invalid("outcome node removed"))?,
            ));
        }
        for change in &mut self.placements {
            let id = change
                .after
                .as_ref()
                .ok_or_else(|| invalid("outcome placement missing"))?
                .id();
            change.after = Some(PlacementValue::from_value(
                service
                    .placements
                    .get(&id)
                    .ok_or_else(|| invalid("outcome placement missing"))?,
            ));
        }
        self.nodes.retain(|c| c.before != c.after);
        self.placements.retain(|c| c.before != c.after);
        self.to_payload()?;
        Ok(self)
    }
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate_manifest_binding()?;
        if self.encoded_len() > LIMIT - 12 {
            return Err(invalid("placement outcome exceeds size limit"));
        }
        let mut bytes = PLACEMENT_OUTCOME_MAGIC.to_vec();
        self.encode(&mut bytes)
            .map_err(|_| invalid("placement outcome encoding failed"))?;
        bytes.extend_from_slice(&crate::fnv1a64(&bytes).to_be_bytes());
        Ok(bytes)
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() < 12 || bytes.len() > LIMIT || !bytes.starts_with(PLACEMENT_OUTCOME_MAGIC) {
            return Err(invalid("invalid placement outcome envelope"));
        }
        let end = bytes.len() - 8;
        if crate::fnv1a64(&bytes[..end]) != u64::from_be_bytes(bytes[end..].try_into().unwrap()) {
            return Err(invalid("placement outcome checksum mismatch"));
        }
        let outcome = Self::decode(&bytes[4..end])
            .map_err(|_| invalid("invalid placement outcome protobuf"))?;
        if outcome.to_payload()? != bytes {
            return Err(invalid("unsupported placement outcome encoding"));
        }
        Ok(outcome)
    }
    pub fn apply(&self, service: &mut MatrixMetaService) -> Result<(), ObjectError> {
        self.validate_manifest_binding()?;
        if self.before_index != service.placement_commit_index
            || !(self.before_index.checked_add(1) == Some(self.after_index)
                || (self.after_index == self.before_index
                    && self.nodes.is_empty()
                    && self.placements.is_empty()
                    && self.manifest.is_none()))
        {
            return Err(invalid("placement outcome index mismatch"));
        }
        let mut nodes = BTreeSet::new();
        for change in &self.nodes {
            let before = change
                .before
                .as_ref()
                .ok_or_else(|| invalid("placement outcome lacks prior node"))?;
            let after = change
                .after
                .as_ref()
                .ok_or_else(|| invalid("placement outcome lacks resulting node"))?;
            let existing = service
                .nodes
                .get(&before.node_id)
                .ok_or_else(|| invalid("placement outcome node missing"))?;
            if before.node_id != after.node_id
                || !nodes.insert(before.node_id.clone())
                || NodeUsage::from_node(existing) != *before
                || after.used_bytes > after.capacity_bytes
                || after.last_heartbeat_ms < before.last_heartbeat_ms
            {
                return Err(invalid("placement outcome node state mismatch"));
            }
        }
        let mut objects = BTreeSet::new();
        for change in &self.placements {
            let after = change
                .after
                .as_ref()
                .ok_or_else(|| invalid("placement outcome lacks resulting placement"))?;
            let id = after.id();
            if (service.object_manifests.get(&id).is_some() && self.manifest.is_none())
                || !objects.insert(id.clone())
                || service.placements.get(&id).map(PlacementValue::from_value) != change.before
                || change.before.as_ref().is_some_and(|v| v.id() != id)
                || after
                    .node_ids
                    .iter()
                    .any(|id| !service.nodes.contains_key(id))
            {
                return Err(invalid("placement outcome object state mismatch"));
            }
        }
        if let Some(manifest) = &self.manifest {
            self.validate_publication(service, false)?;
            // Catalog capacity and expected-state checks happen before any other writes.
            service.object_manifests.apply(manifest)?;
        }
        // All checks precede mutation; replay never invokes placement planning or heartbeat expiry.
        for change in &self.nodes {
            let after = change.after.as_ref().unwrap();
            let node = service.nodes.get_mut(&after.node_id).unwrap();
            node.used_bytes = after.used_bytes;
            node.capacity_bytes = after.capacity_bytes;
            node.committed_bytes = after.committed_bytes;
            node.draining = after.draining;
            node.online = after.online;
            node.last_heartbeat_ms = after.last_heartbeat_ms;
        }
        for change in &self.placements {
            let after = change.after.as_ref().unwrap();
            service.placements.insert(after.id(), after.value());
        }
        service.placement_commit_index = self.after_index;
        service.invalidate_snapshot_export_cache();
        Ok(())
    }
}
