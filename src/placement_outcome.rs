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
    /// Aligned with `node_ids`; empty keeps the older full-length charge.
    #[prost(uint64, repeated, tag = "7")]
    pub group_bytes: Vec<u64>,
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
            group_bytes: v.group_bytes.clone(),
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
            group_bytes: self.group_bytes.clone(),
            update_time_ms: self.update_time_ms,
        }
    }

    fn charges(&self) -> std::collections::BTreeMap<&str, u64> {
        self.node_ids
            .iter()
            .enumerate()
            .map(|(index, id)| {
                (id.as_str(), self.group_bytes.get(index).copied().unwrap_or(self.length))
            })
            .collect()
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
    #[prost(message, optional, tag = "7")]
    pub retention: Option<crate::manifest_retention::ManifestRetentionChange>,
    #[prost(message, optional, tag = "8")]
    pub staging: Option<crate::staged_allocation::StagedAllocationChange>,
    #[prost(message, optional, tag = "9")]
    pub publication_fence: Option<crate::staged_allocation::PublicationFenceChange>,
    #[prost(message, optional, tag = "10")]
    pub copy_source: Option<crate::manifest_copy::LiveCopySource>,
    #[prost(message, optional, tag = "11")]
    pub retention_deadline: Option<crate::manifest_retention::RetentionDeadlineChange>,
    #[prost(message, optional, tag = "12")]
    pub read_session_open: Option<crate::read_session_outcome::ReadSessionOpen>,
    #[prost(message, optional, tag = "13")]
    pub read_session_removal: Option<crate::read_session_outcome::ReadSessionRemoval>,
    #[prost(message, optional, tag = "14")]
    pub manifest_append: Option<crate::manifest_append::ManifestAppend>,
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
fn manifest_group_bytes(manifest: &ObjectManifest, groups: &[String]) -> Vec<u64> {
    let mut totals = std::collections::BTreeMap::<&str, u64>::new();
    for extent in &manifest.extents {
        *totals.entry(extent.group.as_str()).or_default() += extent.length;
    }
    groups
        .iter()
        .map(|group| totals.get(group.as_str()).copied().unwrap_or(0))
        .collect()
}
fn matches_manifest(placement: &PlacementValue, manifest: &ObjectManifest) -> bool {
    placement.id() == manifest.object_id()
        && placement.version == manifest.generation
        && placement.length == manifest.length
        && placement.node_ids == manifest_groups(manifest)
        && (placement.group_bytes.is_empty()
            || placement.group_bytes == manifest_group_bytes(manifest, &placement.node_ids))
}
impl PlacementOutcome {
    fn validate_manifest_binding(&self) -> Result<(), ObjectError> {
        if let Some(append) = &self.manifest_append {
            append.validate()?;
            let record = self.staging.as_ref().and_then(|change|
                if change.after.is_none() { change.before.as_ref() } else { None })
                .ok_or_else(|| invalid("compact append must consume staged ownership"))?;
            record.validate()?;
            if self.version != 11 || self.before_index.checked_add(1) != Some(self.after_index)
                || self.manifest.is_some() || self.retention.is_some() || self.retention_deadline.is_some()
                || self.publication_fence.is_some() || self.copy_source.is_some()
                || self.read_session_open.is_some() || self.read_session_removal.is_some()
                || !record.append || record.bucket != append.bucket || record.key != append.key
                || record.expected_generation != append.before_generation
                || record.allocation_id > self.before_index || self.placements.len() != 1 {
                return Err(invalid("invalid compact append outcome binding"));
            }
            let placement = &self.placements[0];
            let before = placement.before.as_ref().ok_or_else(|| invalid("compact append lacks prior placement"))?;
            let after = placement.after.as_ref().ok_or_else(|| invalid("compact append lacks resulting placement"))?;
            if before.bucket != append.bucket || before.key != append.key
                || before.version != append.before_generation || before.length != append.before_length
                || after.bucket != append.bucket || after.key != append.key
                || after.version != append.before_generation + 1 || after.length != append.after_length
                || append.suffix.len() > record.page_count as usize {
                return Err(invalid("compact append placement differs from suffix decision"));
            }
            let allocation = record.allocation()?;
            for (index, extent) in append.suffix.iter().enumerate() {
                let page = allocation.page(index as u32)?;
                if extent.volume_id != page.chunk_id.volume_id() || extent.chunk_id != page.chunk_id.id()
                    || extent.page_index != page.page_index {
                    return Err(invalid("compact append uses an unowned page"));
                }
            }
            return Ok(());
        } else if self.version == 11 { return Err(invalid("compact append outcome lacks suffix")); }
        if self.read_session_open.is_some() || self.read_session_removal.is_some() {
            if self.before_index.checked_add(1) != Some(self.after_index)
                || self.manifest.is_some() || self.retention.is_some() || self.staging.is_some()
                || self.retention_deadline.is_some() || self.copy_source.is_some()
                || self.publication_fence.is_some() || !self.nodes.is_empty() || !self.placements.is_empty() {
                return Err(invalid("compact read session changes unrelated state"));
            }
            match (self.version, &self.read_session_open, &self.read_session_removal) {
                (9, Some(open), None) if open.retention_id == self.after_index => open.validate()?,
                (10, None, Some(removal)) if removal.retention_id <= self.before_index => removal.validate()?,
                _ => return Err(invalid("invalid compact read session outcome binding")),
            }
            return Ok(());
        } else if matches!(self.version, 9 | 10) { return Err(invalid("compact read session outcome lacks a decision")); }
        if let Some(change) = &self.retention_deadline {
            change.validate()?;
            if self.version != 8 || change.retention_id > self.before_index
                || self.before_index.checked_add(1) != Some(self.after_index)
                || self.manifest.is_some() || self.retention.is_some() || self.staging.is_some()
                || self.copy_source.is_some() || self.publication_fence.is_some()
                || !self.nodes.is_empty() || !self.placements.is_empty() {
                return Err(invalid("invalid read session renewal outcome"));
            }
            return Ok(());
        } else if self.version == 8 { return Err(invalid("read session renewal lacks deadline")); }
        if let Some(fence) = &self.publication_fence {
            if self.version != 5 || fence.before != 0 || fence.after != self.after_index || fence.after == 0
                || self.before_index.checked_add(1) != Some(self.after_index)
                || self.manifest.is_some() || self.staging.is_some() || self.retention.is_some()
                || self.copy_source.is_some() || !self.nodes.is_empty() || !self.placements.is_empty() {
                return Err(invalid("invalid publication fence outcome"));
            }
            return Ok(());
        } else if self.version == 5 { return Err(invalid("publication fence outcome lacks transition")); }
        if let Some(source) = &self.copy_source {
            source.validate()?;
            let after = self.manifest.as_ref().and_then(|outcome| outcome.after.as_ref())
                .ok_or_else(|| invalid("copy outcome lacks publication"))?;
            if self.version != 6 || after.deleted || self.staging.is_some() || self.retention.is_some() {
                return Err(invalid("invalid live copy source binding"));
            }
        } else if self.version == 6 { return Err(invalid("copy outcome lacks live source")); }
        if let Some(change) = &self.staging {
            let renewal = change.before.is_some() && change.after.is_some();
            if self.version != if renewal { 7 } else { 4 } || self.retention.is_some()
                || self.before_index.checked_add(1) != Some(self.after_index) {
                return Err(invalid("invalid staged outcome binding"));
            }
            match (&change.before, &change.after, &self.manifest) {
                (None, Some(record), None) if record.allocation_id == self.after_index => record.validate()?,
                (Some(record), None, None) if record.allocation_id <= self.before_index => record.validate()?,
                (Some(before), Some(after), None) if before.allocation_id <= self.before_index && after.renews(before) => {
                    before.validate()?;
                    after.validate()?;
                }
                (Some(record), None, Some(manifest)) if record.allocation_id <= self.before_index => record.check_publication(manifest)?,
                _ => return Err(invalid("invalid staged outcome transition")),
            }
            if self.manifest.is_none() {
                if !self.nodes.is_empty() || !self.placements.is_empty() { return Err(invalid("staging changed unrelated records")); }
                return Ok(());
            }
        } else if matches!(self.version, 4 | 7) { return Err(invalid("staged outcome lacks ownership change")); }
        if let Some(change) = &self.retention {
            if self.version != 3 || self.staging.is_some() || self.manifest.is_some() || !self.nodes.is_empty() || !self.placements.is_empty()
                || self.before_index.checked_add(1) != Some(self.after_index) {
                return Err(invalid("invalid retention outcome binding"));
            }
            match (&change.before, &change.after) {
                (None, Some(after)) if after.retention_id == self.after_index => after.validate(),
                (Some(before), None) if before.retention_id <= self.before_index => before.validate(),
                _ => Err(invalid("invalid retention outcome transition")),
            }?;
            return Ok(());
        }
        match (self.version, &self.manifest) {
            (1, None) => Ok(()),
            (2 | 4 | 6, Some(manifest)) => {
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
        Self::publish_manifest_with_staging(service, manifest, update_time_ms, None, None, None)
    }
    pub(crate) fn publish_staged_manifest(
        service: &MatrixMetaService,
        manifest: ManifestOutcome,
        update_time_ms: u64,
        allocation: crate::staged_allocation::StagedAllocation,
    ) -> Result<Self, ObjectError> {
        Self::publish_manifest_with_staging(service, manifest, update_time_ms,
            Some(crate::staged_allocation::StagedAllocationChange { before: Some(allocation), after: None }), None, None)
    }
    pub(crate) fn publish_copied_manifest(service: &MatrixMetaService, manifest: ManifestOutcome,
        update_time_ms: u64, source: crate::manifest_copy::LiveCopySource) -> Result<Self, ObjectError> {
        Self::publish_manifest_with_staging(service, manifest, update_time_ms, None, Some(source), None)
    }
    pub(crate) fn publish_staged_append(
        service: &MatrixMetaService, manifest: ManifestOutcome, update_time_ms: u64,
        allocation: crate::staged_allocation::StagedAllocation,
        append: crate::manifest_append::ManifestAppend,
    ) -> Result<Self, ObjectError> {
        Self::publish_manifest_with_staging(service, manifest, update_time_ms,
            Some(crate::staged_allocation::StagedAllocationChange { before: Some(allocation), after: None }),
            None, Some(append))
    }
    fn publish_manifest_with_staging(
        service: &MatrixMetaService,
        manifest: ManifestOutcome,
        update_time_ms: u64,
        staging: Option<crate::staged_allocation::StagedAllocationChange>,
        copy_source: Option<crate::manifest_copy::LiveCopySource>,
        compact_append: Option<crate::manifest_append::ManifestAppend>,
    ) -> Result<Self, ObjectError> {
        manifest.validate()?;
        let after = manifest.after.as_ref().unwrap();
        let before = service
            .placements
            .get(&after.object_id())
            .map(PlacementValue::from_value);
        let node_ids = manifest_groups(after);
        let placement = PlacementValue {
            bucket: after.bucket.clone(),
            key: after.key.clone(),
            version: after.generation,
            length: after.length,
            group_bytes: manifest_group_bytes(after, &node_ids),
            node_ids,
            update_time_ms,
        };
        let ids: BTreeSet<_> = before
            .iter()
            .flat_map(|p| p.node_ids.iter())
            .chain(placement.node_ids.iter())
            .cloned()
            .collect();
        let before_charges = before.as_ref().map(PlacementValue::charges).unwrap_or_default();
        let after_charges = placement.charges();
        let mut nodes = Vec::new();
        for id in ids {
            let current = service
                .nodes
                .get(&id)
                .ok_or_else(|| invalid("manifest group is missing"))?;
            let prior = NodeUsage::from_node(current);
            let mut next = prior.clone();
            let released = before_charges.get(id.as_str()).copied().unwrap_or(0);
            let added = after_charges.get(id.as_str()).copied().unwrap_or(0);
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
        let mut outcome = Self {
            version: if copy_source.is_some() { 6 } else if staging.is_some() { 4 } else { 2 },
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
            retention: None,
            staging,
            publication_fence: None,
            copy_source,
            retention_deadline: None,
            read_session_open: None,
            read_session_removal: None,
            manifest_append: None,
        };
        outcome.validate_manifest_binding()?;
        outcome.validate_publication(service, true)?;
        if let Some(append) = compact_append {
            if crate::manifest_append::ManifestAppend::decide(outcome.manifest.as_ref().unwrap())? != append {
                return Err(invalid("compact append differs from validated publication"));
            }
            outcome.version = 11;
            outcome.manifest = None;
            outcome.manifest_append = Some(append);
            outcome.validate_manifest_binding()?;
        }
        outcome.to_payload()?;
        Ok(outcome)
    }

    fn validate_publication(
        &self,
        service: &MatrixMetaService,
        admit: bool,
    ) -> Result<Option<usize>, ObjectError> {
        let manifest = self.manifest.as_ref().unwrap();
        let after = manifest.after.as_ref().unwrap();
        if service.object_manifests.get(&after.object_id()) != manifest.before.as_ref() {
            return Err(invalid("manifest publication prior state mismatch"));
        }
        let body_bytes = service.object_manifests.check_outcome_with_admission(manifest,
            self.staging.as_ref().and_then(|change| change.before.as_ref()).map(|record| record.allocation_id),
            self.copy_source.as_ref())?;
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
        let before_charges = change.before.as_ref().map(PlacementValue::charges).unwrap_or_default();
        let after_charges = placement.charges();
        let mut expected = Vec::new();
        for id in ids {
            let node = service
                .nodes
                .get(&id)
                .ok_or_else(|| invalid("manifest group is missing"))?;
            let before = NodeUsage::from_node(node);
            let mut after = before.clone();
            let released = before_charges.get(id.as_str()).copied().unwrap_or(0);
            let added = after_charges.get(id.as_str()).copied().unwrap_or(0);
            after.committed_bytes = before
                .committed_bytes
                .checked_sub(released)
                .and_then(|v| v.checked_add(added))
                .ok_or_else(|| invalid("manifest capacity accounting overflow"))?;
            if admit
                && placement.node_ids.contains(&id)
                && ((if self.copy_source.is_some() { node.draining } else { !MatrixMetaService::node_accepts_new_placement(node) })
                    // Verified staged pages already consume reported free space.
                    || after.committed_bytes > node.capacity_bytes)
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
        Ok(body_bytes)
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
            retention: None,
            staging: None,
            publication_fence: None,
            copy_source: None,
            retention_deadline: None,
            read_session_open: None,
            read_session_removal: None,
            manifest_append: None,
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
        if let Some(append) = &self.manifest_append {
            let before = service.object_manifests.get(&ObjectId::new(append.bucket.clone(), append.key.clone()))
                .ok_or_else(|| invalid("compact append prior manifest missing"))?;
            let mut expanded = self.clone();
            expanded.version = 4;
            expanded.manifest = Some(append.expand(before)?);
            expanded.manifest_append = None;
            return expanded.apply(service);
        }
        if self.before_index != service.placement_commit_index
            || !(self.before_index.checked_add(1) == Some(self.after_index)
                || (self.after_index == self.before_index
                    && self.nodes.is_empty()
                    && self.placements.is_empty()
                    && self.manifest.is_none()
                    && self.retention.is_none()
                    && self.staging.is_none()
                    && self.publication_fence.is_none()
                    && self.copy_source.is_none()
                    && self.retention_deadline.is_none()
                    && self.read_session_open.is_none() && self.read_session_removal.is_none()))
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
        if let Some(staging) = &self.staging {
            service.object_manifests.check_staging(staging)?;
        }
        if let Some(fence) = &self.publication_fence {
            service.object_manifests.check_publication_fence(fence.before, fence.after)?;
        }
        if let Some(change) = &self.retention_deadline {
            service.object_manifests.check_retention_deadline(change)?;
        }
        let read_session = if let Some(open) = &self.read_session_open {
            let (manifest, bytes) = service.object_manifests.check_read_session_open(open)?;
            Some((crate::manifest_retention::RetainedManifest { retention_id: open.retention_id,
                manifest: Some(manifest.clone()), expires_at_ms: open.expires_at_ms }, bytes))
        } else { None };
        let read_session_removal = if let Some(removal) = &self.read_session_removal {
            Some((removal.retention_id, service.object_manifests.check_read_session_removal(removal)?))
        } else { None };
        let publication_bytes = if self.manifest.is_some() { self.validate_publication(service, false)? } else { None };
        if let Some(staging) = &self.staging {
            service.object_manifests.apply_staging(staging)?;
        }
        if let Some(retention) = &self.retention {
            service.object_manifests.apply_retention(retention)?;
        }
        if let Some(change) = &self.retention_deadline {
            service.object_manifests.apply_retention_deadline(change)?;
        }
        if let Some((record, bytes)) = read_session { service.object_manifests.commit_checked_read_session(record, bytes); }
        if let Some((id, bytes)) = read_session_removal { service.object_manifests.commit_checked_read_session_removal(id, bytes); }
        if let Some(fence) = &self.publication_fence {
            service.object_manifests.restore_publication_fence(fence.after);
        }
        if let Some(manifest) = &self.manifest {
            // Catalog capacity and expected-state checks happen before any other writes.
            if let Some(bytes) = publication_bytes { service.object_manifests.commit_checked_manifest(manifest, bytes); }
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
