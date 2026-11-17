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
    /// The next append's allocation, created by the commit that publishes this one.
    #[prost(message, optional, tag = "15")]
    pub reserved: Option<crate::staged_allocation::StagedAllocation>,
}
fn invalid(s: &str) -> ObjectError {
    ObjectError::Corruption(s.to_string())
}

/// The manifest side of an outcome being applied.
enum Published<'a> {
    /// The outcome publishes nothing.
    None,
    /// It carries the manifests (before and after) of a full publication.
    Outcome(&'a ManifestOutcome),
    /// It is a compact append whose resulting manifest has been built from the stored one and
    /// checked; `bytes` is the catalog size once it is stored.
    Append { after: crate::extent_list::StoredManifest, bytes: usize },
}

/// Whether `placement` describes exactly the stored manifest.
fn matches_stored(placement: &PlacementValue, manifest: &crate::extent_list::StoredManifest) -> bool {
    placement.id() == manifest.object_id()
        && placement.version == manifest.generation
        && placement.length == manifest.length
        && placement.node_ids == groups_of(manifest.extents.iter())
        && (placement.group_bytes.is_empty()
            || placement.group_bytes == group_bytes_of(manifest.extents.iter(), &placement.node_ids))
}

#[cfg(test)]
thread_local! {
    static FULL_DERIVATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
/// Placement bookkeeping passes over a whole extent list on this thread (tests only).
#[cfg(all(test, feature = "metadata-consensus"))]
pub(crate) fn full_derivations() -> usize {
    FULL_DERIVATIONS.with(|count| count.get())
}

fn manifest_groups(manifest: &ObjectManifest) -> Vec<String> {
    groups_of(manifest.extents.iter())
}
fn groups_of<'a>(extents: impl Iterator<Item = &'a crate::object_manifest::ObjectExtent>) -> Vec<String> {
    #[cfg(test)]
    FULL_DERIVATIONS.with(|count| count.set(count.get() + 1));
    extents
        .map(|e| e.group.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
fn manifest_group_bytes(manifest: &ObjectManifest, groups: &[String]) -> Vec<u64> {
    group_bytes_of(manifest.extents.iter(), groups)
}
fn group_bytes_of<'a>(extents: impl Iterator<Item = &'a crate::object_manifest::ObjectExtent>, groups: &[String]) -> Vec<u64> {
    #[cfg(test)]
    FULL_DERIVATIONS.with(|count| count.set(count.get() + 1));
    let mut totals = std::collections::BTreeMap::<&str, u64>::new();
    for extent in extents {
        *totals.entry(extent.group.as_str()).or_default() += extent.length;
    }
    groups
        .iter()
        .map(|group| totals.get(group.as_str()).copied().unwrap_or(0))
        .collect()
}
/// The groups and per-group bytes of a manifest that extends the one `before` describes by
/// `suffix`: the prior record's totals plus the new extents, groups sorted without duplicates, as
/// `manifest_groups` and `manifest_group_bytes` give for the whole manifest. O(groups + suffix)
/// instead of O(extents). `None` when the prior record carries no per-group bytes (an older
/// record, or an object without extents yet): the caller then derives from the whole manifest.
fn extended_placement(before: &PlacementValue, suffix: &[crate::object_manifest::ObjectExtent]) -> Option<(Vec<String>, Vec<u64>)> {
    if before.group_bytes.is_empty() || before.group_bytes.len() != before.node_ids.len() {
        return None;
    }
    let mut totals: std::collections::BTreeMap<&str, u64> = before
        .node_ids
        .iter()
        .map(String::as_str)
        .zip(before.group_bytes.iter().copied())
        .collect();
    for extent in suffix {
        *totals.entry(extent.group.as_str()).or_default() += extent.length;
    }
    Some((totals.keys().map(|group| group.to_string()).collect(), totals.values().copied().collect()))
}

/// Whether the placement change binds the publication's manifests. When the publication extends
/// the stored manifest by a validated suffix, the resulting record is checked against the prior
/// record extended by that suffix, and the prior record by identity, generation and length: it must
/// equal the record the state already holds, which the publication that wrote it bound to its
/// manifest in full. Otherwise both records are derived from their manifests.
fn placement_binds_manifest(change: &PlacementChange, manifest: &ManifestOutcome, extends_prior: bool) -> bool {
    let (Some(after_manifest), Some(after_placement)) = (manifest.after.as_ref(), change.after.as_ref()) else {
        return false;
    };
    if extends_prior {
        if let (Some(before_placement), Some(before_manifest)) = (&change.before, &manifest.before) {
            let prefix = before_manifest.extents.len();
            if after_manifest.extents.len() >= prefix {
                if let Some((groups, bytes)) = extended_placement(before_placement, &after_manifest.extents[prefix..]) {
                    return before_placement.id() == before_manifest.object_id()
                        && before_placement.version == before_manifest.generation
                        && before_placement.length == before_manifest.length
                        && after_placement.id() == after_manifest.object_id()
                        && after_placement.version == after_manifest.generation
                        && after_placement.length == after_manifest.length
                        && after_placement.node_ids == groups
                        && (after_placement.group_bytes.is_empty() || after_placement.group_bytes == bytes);
                }
            }
        }
    }
    matches_manifest(after_placement, after_manifest)
        && match (&change.before, &manifest.before) {
            (None, None) => true,
            (Some(placement), Some(prior)) => matches_manifest(placement, prior),
            _ => false,
        }
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
        self.validate_manifest_binding_with(false)
    }
    /// `manifest_validated` says the publication's manifests were validated by the caller
    /// (the expansion of a compact append), so they are not walked again.
    fn validate_manifest_binding_with(&self, manifest_validated: bool) -> Result<(), ObjectError> {
        if self.reserved.is_some() && !(self.version == 13 || (self.version == 4 && self.manifest.is_some())) {
            return Err(invalid("a reservation belongs to a publication"));
        }
        if let Some(append) = &self.manifest_append {
            append.validate()?;
            let record = self.staging.as_ref().and_then(|change|
                if change.after.is_none() { change.before.as_ref() } else { None })
                .ok_or_else(|| invalid("compact append must consume staged ownership"))?;
            record.validate()?;
            if !matches!(self.version, 12 | 13) || (self.version == 13) != self.reserved.is_some()
                || self.before_index.checked_add(1) != Some(self.after_index)
                || self.manifest.is_some() || self.retention.is_some() || self.retention_deadline.is_some()
                || self.publication_fence.is_some() || self.copy_source.is_some()
                || self.read_session_open.is_some() || self.read_session_removal.is_some()
                || !record.append || record.bucket != append.bucket || record.key != append.key
                || record.expected_generation != append.before_generation
                || record.allocation_id > self.before_index || self.placements.len() != 1 {
                return Err(invalid("invalid compact append outcome binding"));
            }
            if let Some(reserved) = &self.reserved {
                reserved.validate()?;
                if reserved.version != 2 || !reserved.append || reserved.allocation_id != self.after_index
                    || reserved.bucket != append.bucket || reserved.key != append.key
                    || reserved.expected_generation != append.before_generation.wrapping_add(1) {
                    return Err(invalid("invalid compact append reservation binding"));
                }
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
        } else if matches!(self.version, 12 | 13) { return Err(invalid("compact append outcome lacks suffix")); }
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
                (Some(record), None, Some(manifest)) if record.allocation_id <= self.before_index => {
                    record.check_publication(manifest)?
                }
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
                if !manifest_validated {
                    manifest.validate()?;
                }
                if let Some(reserved) = &self.reserved {
                    let after = manifest.after.as_ref().unwrap();
                    reserved.validate()?;
                    if reserved.version != 2 || !reserved.append || reserved.allocation_id != self.after_index
                        || after.deleted || after.format_version != 2 || reserved.bucket != after.bucket
                        || reserved.key != after.key || reserved.expected_generation != after.generation {
                        return Err(invalid("invalid reservation binding"));
                    }
                }
                if self.placements.len() != 1 {
                    return Err(invalid("manifest publication needs one placement change"));
                }
                if !placement_binds_manifest(&self.placements[0], manifest, manifest_validated) {
                    return Err(invalid("manifest publication placement binding mismatch"));
                }
                Ok(())
            }
            _ => Err(invalid("unsupported placement outcome version or manifest")),
        }
    }

    /// A compact append outcome that also reserves the next append's allocation: one durable
    /// record publishes the append, removes the allocation it consumed and creates the
    /// reservation, so they become durable together or not at all.
    pub(crate) fn with_reservation(mut self, reserved: crate::staged_allocation::StagedAllocation) -> Result<Self, ObjectError> {
        if self.version != 12 || self.manifest_append.is_none() {
            return Err(invalid("only a compact append outcome can reserve the next allocation"));
        }
        self.version = 13;
        self.reserved = Some(reserved);
        self.validate_manifest_binding()?;
        Ok(self)
    }
    /// Decide one atomic placement and manifest transition without changing service state.
    /// The caller must verify configured group identities and durable data extents before
    /// proposing this outcome; structural validation alone cannot establish either fact.
    pub fn publish_manifest(
        service: &MatrixMetaService,
        manifest: ManifestOutcome,
        update_time_ms: u64,
    ) -> Result<Self, ObjectError> {
        Self::publish_manifest_with_staging(service, manifest, update_time_ms, None, None)
    }
    pub(crate) fn publish_staged_manifest(
        service: &MatrixMetaService,
        manifest: ManifestOutcome,
        update_time_ms: u64,
        allocation: crate::staged_allocation::StagedAllocation,
    ) -> Result<Self, ObjectError> {
        Self::publish_manifest_with_staging(service, manifest, update_time_ms,
            Some(crate::staged_allocation::StagedAllocationChange { before: Some(allocation), after: None }), None)
    }
    pub(crate) fn publish_copied_manifest(service: &MatrixMetaService, manifest: ManifestOutcome,
        update_time_ms: u64, source: crate::manifest_copy::LiveCopySource) -> Result<Self, ObjectError> {
        Self::publish_manifest_with_staging(service, manifest, update_time_ms, None, Some(source))
    }
    /// The outcome that publishes a compact append: `after` is `before`, the stored manifest,
    /// plus the new extents of `append` (see `ManifestAppend::expand_shared`). The outcome
    /// carries the decision, not the manifests; everything it must be bound to is read from
    /// `before` and `after` here, and again when it is applied.
    pub(crate) fn publish_staged_append(
        service: &MatrixMetaService,
        before_manifest: &crate::extent_list::StoredManifest,
        after_manifest: &crate::extent_list::StoredManifest,
        update_time_ms: u64,
        allocation: crate::staged_allocation::StagedAllocation,
        append: crate::manifest_append::ManifestAppend,
    ) -> Result<Self, ObjectError> {
        let before = service
            .placements
            .get(&after_manifest.object_id())
            .map(PlacementValue::from_value);
        // The placement is the prior record extended by the suffix, not a pass over every extent.
        let extended = before
            .as_ref()
            .and_then(|prior| extended_placement(prior, &append.suffix));
        let (node_ids, group_bytes) = extended.unwrap_or_else(|| {
            let node_ids = groups_of(after_manifest.extents.iter());
            let group_bytes = group_bytes_of(after_manifest.extents.iter(), &node_ids);
            (node_ids, group_bytes)
        });
        let placement = PlacementValue {
            bucket: after_manifest.bucket.clone(),
            key: after_manifest.key.clone(),
            version: after_manifest.generation,
            length: after_manifest.length,
            group_bytes,
            node_ids,
            update_time_ms,
        };
        let nodes = Self::capacity_changes(service, &before, &placement)?;
        let outcome = Self {
            version: 12,
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
            manifest: None,
            retention: None,
            staging: Some(crate::staged_allocation::StagedAllocationChange { before: Some(allocation), after: None }),
            publication_fence: None,
            copy_source: None,
            retention_deadline: None,
            read_session_open: None,
            read_session_removal: None,
            manifest_append: Some(append),
            reserved: None,
        };
        outcome.validate_manifest_binding()?;
        outcome.validate_append_publication(service, true, before_manifest, after_manifest)?;
        outcome.to_payload()?;
        Ok(outcome)
    }
    /// The capacity changes the move from `before` to `placement` makes on the groups it names.
    fn capacity_changes(
        service: &MatrixMetaService,
        before: &Option<PlacementValue>,
        placement: &PlacementValue,
    ) -> Result<Vec<NodeChange>, ObjectError> {
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
        Ok(nodes)
    }
    fn publish_manifest_with_staging(
        service: &MatrixMetaService,
        manifest: ManifestOutcome,
        update_time_ms: u64,
        staging: Option<crate::staged_allocation::StagedAllocationChange>,
        copy_source: Option<crate::manifest_copy::LiveCopySource>,
    ) -> Result<Self, ObjectError> {
        manifest.validate()?;
        let after = manifest.after.as_ref().unwrap();
        let before = service
            .placements
            .get(&after.object_id())
            .map(PlacementValue::from_value);
        let node_ids = manifest_groups(after);
        let group_bytes = manifest_group_bytes(after, &node_ids);
        let placement = PlacementValue {
            bucket: after.bucket.clone(),
            key: after.key.clone(),
            version: after.generation,
            length: after.length,
            group_bytes,
            node_ids,
            update_time_ms,
        };
        let nodes = Self::capacity_changes(service, &before, &placement)?;
        let outcome = Self {
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
            reserved: None,
        };
        outcome.validate_manifest_binding()?;
        outcome.validate_publication(service, true)?;
        outcome.to_payload()?;
        Ok(outcome)
    }

    /// The checks `validate_publication` makes for a compact append, against `before`, the
    /// stored manifest, and `after`, which extends it by the decision's suffix. Returns the
    /// catalog size once `after` is stored.
    fn validate_append_publication(
        &self,
        service: &MatrixMetaService,
        admit: bool,
        before: &crate::extent_list::StoredManifest,
        after: &crate::extent_list::StoredManifest,
    ) -> Result<usize, ObjectError> {
        let allocation_id = self.staging.as_ref().and_then(|change| change.before.as_ref()).map(|record| record.allocation_id);
        let body_bytes = service.object_manifests.check_append_publication(before, after, allocation_id)?;
        let change = &self.placements[0];
        let (Some(before_placement), Some(after_placement)) = (&change.before, &change.after) else {
            return Err(invalid("manifest publication placement binding mismatch"));
        };
        let suffix = after.extents.tail_from(before.extents.len());
        let bound = match extended_placement(before_placement, &suffix) {
            Some((groups, bytes)) => {
                before_placement.id() == before.object_id()
                    && before_placement.version == before.generation
                    && before_placement.length == before.length
                    && after_placement.id() == after.object_id()
                    && after_placement.version == after.generation
                    && after_placement.length == after.length
                    && after_placement.node_ids == groups
                    && (after_placement.group_bytes.is_empty() || after_placement.group_bytes == bytes)
            }
            None => matches_stored(after_placement, after) && matches_stored(before_placement, before),
        };
        if !bound {
            return Err(invalid("manifest publication placement binding mismatch"));
        }
        self.check_placement_and_capacity(service, &after.object_id(), admit)?;
        Ok(body_bytes)
    }

    fn validate_publication(
        &self,
        service: &MatrixMetaService,
        admit: bool,
    ) -> Result<Option<usize>, ObjectError> {
        let manifest = self.manifest.as_ref().unwrap();
        let after = manifest.after.as_ref().unwrap();
        if !service.object_manifests.matches_expected(&after.object_id(), manifest.before.as_ref()) {
            return Err(invalid("manifest publication prior state mismatch"));
        }
        let allocation_id = self.staging.as_ref().and_then(|change| change.before.as_ref()).map(|record| record.allocation_id);
        let body_bytes = service.object_manifests.check_outcome_with_admission(manifest, allocation_id, self.copy_source.as_ref())?;
        self.check_placement_and_capacity(service, &after.object_id(), admit)?;
        Ok(body_bytes)
    }

    /// The placement change must start from the placement the state holds, and the capacity
    /// changes must be exactly those the move makes; with `admit`, the groups it adds bytes to
    /// must be writable.
    fn check_placement_and_capacity(
        &self,
        service: &MatrixMetaService,
        object: &ObjectId,
        admit: bool,
    ) -> Result<(), ObjectError> {
        let change = &self.placements[0];
        if service
            .placements
            .get(object)
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
            // Only a group this publication adds bytes to or newly names must be writable.
            // An append that moves on to another group keeps the older extents on a group
            // that may since have stopped, and must not be refused for them.
            let newly_charged = added > released
                || !change.before.as_ref().is_some_and(|p| p.node_ids.contains(&id));
            if admit
                && placement.node_ids.contains(&id)
                && newly_charged
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
            retention: None,
            staging: None,
            publication_fence: None,
            copy_source: None,
            retention_deadline: None,
            read_session_open: None,
            read_session_removal: None,
            manifest_append: None,
            reserved: None,
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
        self.apply_with(service, false)
    }
    /// `manifest_validated`: the publication's manifests come from the expansion of a compact
    /// append against the stored manifest, which validated them.
    fn apply_with(&self, service: &mut MatrixMetaService, manifest_validated: bool) -> Result<(), ObjectError> {
        self.validate_manifest_binding_with(manifest_validated)?;
        if let Some(append) = &self.manifest_append {
            let (before, stored) = service.object_manifests.get_with_digest(&ObjectId::new(append.bucket.clone(), append.key.clone()))?
                .ok_or_else(|| invalid("compact append prior manifest missing"))?;
            let after = append.expand_shared(before, stored)?;
            let record = self.staging.as_ref().and_then(|change| change.before.as_ref())
                .ok_or_else(|| invalid("compact append must consume staged ownership"))?;
            record.check_append_publication(before, &after)?;
            let bytes = self.validate_append_publication(service, false, before, &after)?;
            return self.apply_checked(service, Published::Append { after, bytes });
        }
        let published = match &self.manifest {
            Some(manifest) => Published::Outcome(manifest),
            None => Published::None,
        };
        self.apply_checked(service, published)
    }
    /// Apply the outcome once `published`, its manifest side, has been decided and checked.
    fn apply_checked(
        &self,
        service: &mut MatrixMetaService,
        published: Published<'_>,
    ) -> Result<(), ObjectError> {
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
            if (service.object_manifests.get(&id).is_some() && matches!(published, Published::None))
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
                manifest: Some(manifest), expires_at_ms: open.expires_at_ms }, bytes))
        } else { None };
        let read_session_removal = if let Some(removal) = &self.read_session_removal {
            Some((removal.retention_id, service.object_manifests.check_read_session_removal(removal)?))
        } else { None };
        let publication_bytes = match &published {
            Published::Outcome(_) => self.validate_publication(service, false)?,
            Published::Append { bytes, .. } => Some(*bytes),
            Published::None => None,
        };
        if let (Some(reserved), Some(bytes)) = (&self.reserved, publication_bytes) {
            let consumed = self.staging.as_ref().and_then(|change| change.before.as_ref())
                .ok_or_else(|| invalid("reservation without the consumed allocation"))?;
            match &published {
                Published::Outcome(manifest) => {
                    service.object_manifests.check_reservation(reserved, manifest.after.as_ref().unwrap(), bytes, consumed)?;
                }
                Published::Append { after, .. } => {
                    service.object_manifests.check_reservation_for(
                        reserved, &after.bucket, &after.key, after.generation, after.deleted, after.format_version,
                        bytes, consumed,
                    )?;
                }
                Published::None => return Err(invalid("reservation without a publication")),
            }
        }
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
        // Catalog capacity and expected-state checks happen before any other writes.
        match published {
            Published::Outcome(manifest) => {
                if let Some(bytes) = publication_bytes { service.object_manifests.commit_checked_manifest(manifest, bytes); }
            }
            Published::Append { after, bytes } => service.object_manifests.commit_append(after, bytes),
            Published::None => {}
        }
        if let (Some(reserved), Some(_)) = (&self.reserved, publication_bytes) {
            service.object_manifests.restore_staging(reserved.allocation_id, Some(reserved.clone()));
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

#[cfg(test)]
mod incremental_placement_tests {
    use super::*;
    use crate::object_manifest::ObjectExtent;

    fn next(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state >> 33
    }

    fn extent(group: &str, length: u64) -> ObjectExtent {
        ObjectExtent { group: group.into(), length, ..Default::default() }
    }

    /// For random sequences of appends of 0..4 extents over five groups, each given a random length
    /// (zero included), the record extended from the previous one equals the record derived from the
    /// whole manifest: the same groups in the same order and the same bytes, so the encoded record
    /// (and the snapshot that carries it) is unchanged.
    #[test]
    fn the_extended_record_equals_the_record_derived_from_the_whole_manifest() {
        for seed in 1..=20u64 {
            let mut state = seed;
            let mut manifest = ObjectManifest { format_version: 2, bucket: "b".into(), key: "k".into(), generation: 1, ..Default::default() };
            let mut record: Option<PlacementValue> = None;
            let mut extended_steps = 0;
            for step in 0..300 {
                let count = (next(&mut state) % 5) as usize;
                let suffix: Vec<ObjectExtent> = (0..count)
                    .map(|_| extent(&format!("group-{}", next(&mut state) % 5), next(&mut state) % 10))
                    .collect();
                manifest.extents.extend(suffix.iter().cloned());
                manifest.generation += 1;
                let whole_groups = manifest_groups(&manifest);
                let whole_bytes = manifest_group_bytes(&manifest, &whole_groups);
                let derived = record.as_ref().and_then(|prior| extended_placement(prior, &suffix));
                if let Some((groups, bytes)) = &derived {
                    assert_eq!(groups, &whole_groups, "seed {seed} step {step}: groups");
                    assert_eq!(bytes, &whole_bytes, "seed {seed} step {step}: bytes");
                    extended_steps += 1;
                }
                record = Some(PlacementValue {
                    bucket: "b".into(),
                    key: "k".into(),
                    version: manifest.generation,
                    length: manifest.length,
                    node_ids: whole_groups,
                    update_time_ms: 0,
                    group_bytes: whole_bytes,
                });
            }
            assert!(extended_steps > 250, "seed {seed}: the extension ran on {extended_steps} of 300 steps");
        }
    }

    /// A prior record without per-group bytes (an object without extents, or an older record) is not
    /// extended: the caller derives it from the whole manifest.
    #[test]
    fn a_prior_record_without_group_bytes_is_not_extended() {
        let prior = PlacementValue { bucket: "b".into(), key: "k".into(), node_ids: vec!["g".into()], group_bytes: vec![], ..Default::default() };
        assert!(extended_placement(&prior, &[extent("g", 1)]).is_none());
        let shaped_wrong = PlacementValue { node_ids: vec!["g".into()], group_bytes: vec![1, 2], ..prior };
        assert!(extended_placement(&shaped_wrong, &[extent("g", 1)]).is_none());
    }
}
