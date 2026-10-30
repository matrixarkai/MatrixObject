// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Immutable chunk inventories, retained membership and placement health projection.
use super::{ChunkGroupIdentity, ChunkMembershipHealth, MetaRequest};
use crate::{MatrixMetaService, ObjectError};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// One logical placement target bound to a permitted Raft member inventory.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkGroupConfig {
    #[prost(string, tag = "1")]
    pub placement_id: String,
    #[prost(string, tag = "2")]
    pub failure_domain: String,
    #[prost(message, optional, tag = "3")]
    pub identity: Option<ChunkGroupIdentity>,
    /// API sockets in the same order as identity.voters.
    #[prost(string, repeated, tag = "4")]
    pub api_endpoints: Vec<String>,
    /// Metadata health identities in the same order as identity.voters.
    #[prost(string, repeated, tag = "5")]
    pub member_ids: Vec<String>,
}
#[derive(Clone, PartialEq, Message)]
struct CatalogHealth {
    #[prost(string, tag = "1")]
    placement_id: String,
    #[prost(bytes = "vec", tag = "2")]
    state: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
struct CatalogImage {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(message, repeated, tag = "2")]
    groups: Vec<ChunkGroupConfig>,
    #[prost(message, repeated, tag = "3")]
    health: Vec<CatalogHealth>,
}
#[derive(Debug, Default)]
pub struct ChunkGroupCatalog {
    groups: BTreeMap<String, ChunkGroupConfig>,
    members: BTreeMap<String, String>,
    health: Mutex<BTreeMap<String, ChunkMembershipHealth>>,
}
impl Clone for ChunkGroupCatalog {
    fn clone(&self) -> Self {
        Self {
            groups: self.groups.clone(),
            members: self.members.clone(),
            health: Mutex::new(
                self.health
                    .lock()
                    .expect("catalog health lock poisoned")
                    .clone(),
            ),
        }
    }
}
fn invalid(s: &str) -> ObjectError {
    ObjectError::InvalidArgument(s.into())
}
fn name_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}
#[derive(Clone, Copy)]
pub(super) struct VoterHealth {
    pub(super) online: bool,
    pub(super) draining: bool,
    pub(super) used_bytes: u64,
    pub(super) heartbeat_ms: u64,
}

// During a joint configuration, each voter set needs its own majority. Inventory
// entries that are absent from both sets have no effect on availability or usage.
pub(super) fn project_voter_health(
    incoming: &[u64],
    outgoing: &[u64],
    report: impl Fn(u64) -> Option<VoterHealth>,
) -> (bool, u64, Option<u64>) {
    let quorum_time = |voters: &[u64]| {
        if voters.is_empty() || voters[0] == 0 || voters.windows(2).any(|w| w[0] >= w[1]) {
            return None;
        }
        let mut times: Vec<_> = voters
            .iter()
            .filter_map(|id| report(*id))
            .filter(|health| health.online && !health.draining)
            .map(|health| health.heartbeat_ms)
            .collect();
        times.sort_unstable_by(|a, b| b.cmp(a));
        times.get(voters.len() / 2).copied()
    };
    let timestamp = quorum_time(incoming).and_then(|time| {
        if outgoing.is_empty() {
            Some(time)
        } else {
            quorum_time(outgoing).map(|other| time.min(other))
        }
    });
    let used = incoming
        .iter()
        .chain(outgoing)
        .filter_map(|id| report(*id))
        .map(|health| health.used_bytes)
        .max()
        .unwrap_or(0);
    (timestamp.is_some(), used, timestamp)
}

impl ChunkGroupCatalog {
    pub fn new(groups: Vec<ChunkGroupConfig>) -> Result<Self, ObjectError> {
        Self::with_membership_health(groups, BTreeMap::new())
    }
    /// Build a catalog with separately retained membership observations. The
    /// immutable inventory remains in each identity; health never expands it.
    pub fn with_membership_health(
        groups: Vec<ChunkGroupConfig>,
        health: BTreeMap<String, ChunkMembershipHealth>,
    ) -> Result<Self, ObjectError> {
        let mut result = Self::inventory(groups)?;
        for group in result.groups.values() {
            let identity = group.identity.as_ref().unwrap();
            match (identity.version, health.get(&group.placement_id)) {
                (1, None) => {}
                (2, Some(state)) => {
                    ChunkMembershipHealth::from_bytes(&state.to_bytes(), identity)?;
                }
                _ => {
                    return Err(invalid(
                        "chunk catalog membership state does not match identity",
                    ))
                }
            }
        }
        if health.keys().any(|id| !result.groups.contains_key(id)) {
            return Err(invalid("chunk catalog health names an unknown group"));
        }
        result.health = Mutex::new(health);
        Ok(result)
    }
    // Structural decoding is also used for read replies. It never supplies a
    // membership observation or creates a usable dynamic catalog by itself.
    fn inventory(groups: Vec<ChunkGroupConfig>) -> Result<Self, ObjectError> {
        if groups.len() > 512 {
            return Err(invalid("chunk catalog exceeds 512 groups"));
        }
        let mut result = Self::default();
        let mut names = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut peer_endpoints = BTreeSet::new();
        for mut group in groups {
            if !name_ok(&group.placement_id)
                || !name_ok(&group.failure_domain)
                || !names.insert(group.placement_id.clone())
            {
                return Err(invalid("invalid or duplicate chunk group name"));
            }
            let identity = group
                .identity
                .as_mut()
                .ok_or_else(|| invalid("chunk group identity missing"))?;
            identity.validate()?;
            identity.node_id = identity.voters[0];
            for endpoint in &identity.peer_endpoints {
                if !peer_endpoints.insert(endpoint.clone()) {
                    return Err(invalid("duplicate chunk peer endpoint across groups"));
                }
            }
            if identity.capacity_bytes == 0
                || !ids.insert(identity.group_id)
                || group.api_endpoints.len() != identity.voters.len()
                || group.member_ids.len() != identity.voters.len()
            {
                return Err(invalid("invalid chunk group membership"));
            }
            for (member, endpoint) in group.member_ids.iter().zip(&group.api_endpoints) {
                if !name_ok(member) || !names.insert(member.clone()) {
                    return Err(invalid("duplicate or invalid chunk member identity"));
                }
                let addr = endpoint
                    .parse::<std::net::SocketAddr>()
                    .map_err(|_| invalid("chunk API endpoint must be an IP socket"))?;
                if addr.port() == 0
                    || addr.ip().is_unspecified()
                    || addr.ip().is_multicast()
                    || addr.to_string() != *endpoint
                    || matches!(addr.ip(),std::net::IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some())
                    || !endpoints.insert(addr)
                {
                    return Err(invalid("invalid or duplicate chunk API endpoint"));
                }
                result
                    .members
                    .insert(member.clone(), group.placement_id.clone());
            }
            result.groups.insert(group.placement_id.clone(), group);
        }
        if endpoints
            .iter()
            .any(|endpoint| peer_endpoints.contains(&endpoint.to_string()))
        {
            return Err(invalid("chunk API and peer sockets overlap"));
        }
        Ok(result)
    }
    /// Check only immutable configuration when restoring a newer committed catalog.
    #[cfg(any(test, feature = "metadata-consensus"))]
    pub(super) fn same_inventory(&self, other: &Self) -> bool {
        self.groups == other.groups && self.members == other.members
    }
    pub fn membership_health(
        &self,
        id: &str,
    ) -> Result<Option<ChunkMembershipHealth>, ObjectError> {
        Ok(self
            .health
            .lock()
            .map_err(|_| invalid("catalog health lock poisoned"))?
            .get(id)
            .cloned())
    }
    /// Replace exactly the state that was used to prepare a committed outcome.
    /// This is internal until consensus dispatch also commits its placement effects.
    #[cfg(test)]
    pub(super) fn replace_membership_health(
        &self,
        id: &str,
        expected: &[u8],
        next: &[u8],
    ) -> Result<(), ObjectError> {
        self.replace_membership_health_with(id, expected, next, || Ok(()))
    }
    pub(super) fn replace_membership_health_with(
        &self,
        id: &str,
        expected: &[u8],
        next: &[u8],
        apply_placement: impl FnOnce() -> Result<(), ObjectError>,
    ) -> Result<(), ObjectError> {
        let group = self
            .groups
            .get(id)
            .ok_or_else(|| invalid("chunk group missing"))?;
        let identity = group.identity.as_ref().unwrap();
        let next = ChunkMembershipHealth::from_bytes(next, identity)?;
        let mut states = self
            .health
            .lock()
            .map_err(|_| invalid("catalog health lock poisoned"))?;
        let current = states
            .get(id)
            .ok_or_else(|| invalid("chunk group has fixed membership"))?;
        if current.to_bytes() != expected {
            return Err(invalid("chunk membership outcome precondition changed"));
        }
        // Matching the prior bytes does not authorize an older configuration.
        current.check_successor(&next, identity)?;
        apply_placement()?;
        states.insert(id.to_string(), next);
        Ok(())
    }
    #[cfg(any(test, feature = "metadata-consensus"))]
    pub(super) fn install_membership_health(&self, restored: &Self) -> Result<(), ObjectError> {
        if !self.same_inventory(restored) {
            return Err(invalid("chunk catalog inventory changed during recovery"));
        }
        let next = restored
            .health
            .lock()
            .map_err(|_| invalid("catalog health lock poisoned"))?
            .clone();
        let mut current = self
            .health
            .lock()
            .map_err(|_| invalid("catalog health lock poisoned"))?;
        if current.keys().ne(next.keys()) {
            return Err(invalid(
                "chunk catalog membership modes changed during recovery",
            ));
        }
        for (id, state) in current.iter() {
            state.check_successor(&next[id], self.groups[id].identity.as_ref().unwrap())?;
        }
        *current = next;
        Ok(())
    }
    pub fn groups(&self) -> impl Iterator<Item = &ChunkGroupConfig> {
        self.groups.values()
    }
    pub fn get(&self, id: &str) -> Option<&ChunkGroupConfig> {
        self.groups.get(id)
    }
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        let health = self
            .health
            .lock()
            .map_err(|_| invalid("catalog health lock poisoned"))?;
        let image = CatalogImage {
            version: if health.is_empty() { 1 } else { 2 },
            groups: self.groups.values().cloned().collect(),
            health: health
                .iter()
                .map(|(id, state)| CatalogHealth {
                    placement_id: id.clone(),
                    state: state.to_bytes(),
                })
                .collect(),
        };
        if image.encoded_len() > 2 * 1024 * 1024 {
            return Err(invalid("chunk group catalog exceeds size bound"));
        }
        let mut bytes = b"MOGC".to_vec();
        bytes.extend(image.encode_to_vec());
        Ok(bytes)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 2 * 1024 * 1024 + 4 || !bytes.starts_with(b"MOGC") {
            return Err(invalid("invalid chunk group catalog envelope"));
        }
        let image = CatalogImage::decode(&bytes[4..])
            .map_err(|_| invalid("invalid chunk group catalog protobuf"))?;
        if !matches!(image.version, 1 | 2) {
            return Err(invalid("unsupported chunk group catalog version"));
        }
        let mut health = BTreeMap::new();
        for state in image.health {
            let identity = image
                .groups
                .iter()
                .find(|g| g.placement_id == state.placement_id)
                .and_then(|g| g.identity.as_ref())
                .ok_or_else(|| invalid("chunk catalog health names an unknown group"))?;
            let decoded = ChunkMembershipHealth::from_bytes(&state.state, identity)?;
            if health.insert(state.placement_id, decoded).is_some() {
                return Err(invalid("duplicate chunk catalog health state"));
            }
        }
        let catalog = Self::with_membership_health(image.groups, health)?;
        if catalog.to_bytes()? != bytes {
            return Err(invalid("noncanonical chunk group catalog"));
        }
        Ok(catalog)
    }
    pub(super) fn affected_nodes(&self, mut nodes: Vec<String>) -> Vec<String> {
        let groups: Vec<_> = nodes
            .iter()
            .filter_map(|id| self.members.get(id).cloned())
            .collect();
        nodes.extend(groups);
        nodes.sort();
        nodes.dedup();
        nodes
    }
    pub(super) fn check_request(&self, request: &MetaRequest) -> Result<(), ObjectError> {
        match request {
            MetaRequest::PublishManifest { outcome, .. } => {
                outcome.validate()?;
                for manifest in outcome.before.iter().chain(outcome.after.iter()) {
                    for extent in &manifest.extents {
                        if !self.groups.contains_key(&extent.group) {
                            return Err(invalid("manifest names an unconfigured chunk group"));
                        }
                    }
                }
            }
            MetaRequest::HeartbeatRootNode {
                node_id,
                capacity_bytes,
                ..
            }
            | MetaRequest::HeartbeatChunkNode {
                node_id,
                capacity_bytes,
                ..
            } => {
                if self.groups.contains_key(node_id) {
                    return Err(invalid("chunk group health is derived from voter reports"));
                }
                if let Some(group) = self.members.get(node_id).and_then(|id| self.groups.get(id)) {
                    if group.identity.as_ref().unwrap().version == 2 {
                        return Err(invalid(
                            "dynamic chunk groups require fenced health reports",
                        ));
                    }
                    if *capacity_bytes != group.identity.as_ref().unwrap().capacity_bytes {
                        return Err(invalid(
                            "chunk voter capacity differs from group configuration",
                        ));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(super) fn project(&self, service: &mut MatrixMetaService, affected: &[String]) {
        let ids: BTreeSet<_> = affected
            .iter()
            .filter_map(|id| self.members.get(id))
            .collect();
        for id in ids {
            let group = &self.groups[id];
            let voters = &group.identity.as_ref().unwrap().voters;
            let dynamic = self
                .membership_health(id)
                .expect("catalog health available");
            let (online, used, timestamp) = if let Some(health) = dynamic {
                health.project_with_status(0, |voter| {
                    voters
                        .binary_search(&voter)
                        .ok()
                        .and_then(|index| service.nodes.get(&group.member_ids[index]))
                        .map(|node| (node.online, node.draining))
                        .unwrap_or((false, false))
                })
            } else {
                project_voter_health(voters, &[], |voter| {
                    let index = voters.binary_search(&voter).ok()?;
                    service
                        .nodes
                        .get(&group.member_ids[index])
                        .map(|node| VoterHealth {
                            online: node.online,
                            draining: node.draining,
                            used_bytes: node.used_bytes,
                            heartbeat_ms: node.last_heartbeat_ms,
                        })
                })
            };
            let target = service.nodes.get_mut(id).unwrap();
            target.online = online;
            target.used_bytes = used;
            if let Some(timestamp) = timestamp {
                target.last_heartbeat_ms = target.last_heartbeat_ms.max(timestamp);
            }
        }
        service.invalidate_snapshot_export_cache();
    }
    pub(super) fn validate_roster(&self, service: &MatrixMetaService) -> Result<(), ObjectError> {
        for group in self.groups.values() {
            let target = service
                .nodes
                .get(&group.placement_id)
                .ok_or_else(|| invalid("chunk group placement node missing"))?;
            if !target.data_eligible
                || target.endpoint != group.api_endpoints[0]
                || target.capacity_bytes != group.identity.as_ref().unwrap().capacity_bytes
            {
                return Err(invalid("chunk group placement node differs from catalog"));
            }
            for (member, endpoint) in group.member_ids.iter().zip(&group.api_endpoints) {
                let node = service
                    .nodes
                    .get(member)
                    .ok_or_else(|| invalid("chunk group voter node missing"))?;
                if node.data_eligible
                    || node.endpoint != *endpoint
                    || node.capacity_bytes != target.capacity_bytes
                {
                    return Err(invalid("chunk voter roster differs from catalog"));
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, PartialEq, Message)]
struct Bootstrap {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    snapshot: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    catalog: Vec<u8>,
}
fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}
pub(super) fn encode_bootstrap(
    service: &MatrixMetaService,
    catalog: &ChunkGroupCatalog,
) -> Result<Vec<u8>, ObjectError> {
    let snapshot = service.export_snapshot().to_bytes()?;
    if catalog.is_empty() {
        return Ok(snapshot);
    }
    let image = Bootstrap {
        version: 1,
        snapshot,
        catalog: catalog.to_bytes()?,
    };
    let mut bytes = b"MOGB".to_vec();
    bytes.extend(image.encode_to_vec());
    bytes.extend(digest(&bytes).to_le_bytes());
    Ok(bytes)
}
pub(super) fn decode_bootstrap(bytes: &[u8]) -> Result<(Vec<u8>, ChunkGroupCatalog), ObjectError> {
    if !bytes.starts_with(b"MOGB") {
        return Ok((bytes.to_vec(), ChunkGroupCatalog::default()));
    }
    if bytes.len() < 12 || bytes.len() > 64 * 1024 * 1024 {
        return Err(invalid("invalid metadata group bootstrap size"));
    }
    let end = bytes.len() - 8;
    if digest(&bytes[..end]).to_le_bytes() != bytes[end..] {
        return Err(invalid("metadata group bootstrap checksum mismatch"));
    }
    let image = Bootstrap::decode(&bytes[4..end])
        .map_err(|_| invalid("invalid metadata group bootstrap protobuf"))?;
    if image.version != 1 || image.encode_to_vec() != bytes[4..end] {
        return Err(invalid("noncanonical metadata group bootstrap"));
    }
    Ok((
        image.snapshot,
        ChunkGroupCatalog::from_bytes(&image.catalog)?,
    ))
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    pub(crate) fn config() -> ChunkGroupConfig {
        ChunkGroupConfig {
            placement_id: "group-a".into(),
            failure_domain: "rack-a".into(),
            identity: Some(ChunkGroupIdentity {
                bootstrap_voters: Vec::new(),
                bootstrap_id: 0,
                version: 1,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 4096,
                max_pages: 128,
                voters: vec![1, 2, 3],
                peer_endpoints: vec![
                    "127.0.0.1:9101".into(),
                    "127.0.0.1:9102".into(),
                    "127.0.0.1:9103".into(),
                ],
            }),
            api_endpoints: vec![
                "127.0.0.1:9201".into(),
                "127.0.0.1:9202".into(),
                "127.0.0.1:9203".into(),
            ],
            member_ids: vec!["voter-a".into(), "voter-b".into(), "voter-c".into()],
        }
    }
    pub(crate) fn dynamic_config() -> (ChunkGroupConfig, ChunkMembershipHealth) {
        let mut group = config();
        let identity = group.identity.as_mut().unwrap();
        identity.version = 2;
        identity.bootstrap_id = 91;
        identity.bootstrap_voters = vec![1, 2, 3];
        identity.voters.push(4);
        identity.peer_endpoints.push("127.0.0.1:9104".into());
        group.api_endpoints.push("127.0.0.1:9204".into());
        group.member_ids.push("voter-d".into());
        let status = super::super::MembershipStatus {
            version: 1,
            node_id: 1,
            term: 2,
            applied_index: 10,
            voters: vec![1, 2, 3],
            outgoing_voters: vec![],
            learners: vec![4],
        };
        let health = ChunkMembershipHealth::new(
            super::super::ChunkMembershipFence::from_status(identity, &status).unwrap(),
        );
        (group, health)
    }
    #[test]
    fn live_catalog_state_roundtrips_without_replacing_inventory_or_regressing_reports() {
        let (group, health) = dynamic_config();
        let identity = group.identity.as_ref().unwrap();
        let catalog = ChunkGroupCatalog::with_membership_health(
            vec![group.clone()],
            [(group.placement_id.clone(), health.clone())].into(),
        )
        .unwrap();
        let initial = catalog.to_bytes().unwrap();
        let mut next = health.clone();
        let status = super::super::MembershipStatus {
            version: 1,
            node_id: 1,
            term: 2,
            applied_index: 11,
            voters: vec![1, 2, 3],
            outgoing_voters: vec![],
            learners: vec![4],
        };
        next.record(identity, 1, 91, &status, 100, 100, true, false)
            .unwrap();
        let mut same_time = next.clone();
        let mut later = status.clone();
        later.applied_index = 12;
        same_time
            .record(identity, 1, 91, &later, 100, 101, true, false)
            .unwrap();
        next.check_successor(&same_time, identity).unwrap();
        catalog
            .replace_membership_health("group-a", &health.to_bytes(), &next.to_bytes())
            .unwrap();
        assert!(catalog
            .replace_membership_health("group-a", &health.to_bytes(), &next.to_bytes())
            .is_err());
        assert!(catalog
            .replace_membership_health("group-a", &next.to_bytes(), &health.to_bytes())
            .is_err());
        let encoded = catalog.to_bytes().unwrap();
        assert_ne!(encoded, initial);
        let restored = ChunkGroupCatalog::from_bytes(&encoded).unwrap();
        assert_eq!(restored.to_bytes().unwrap(), encoded);
        let bootstrap = ChunkGroupCatalog::from_bytes(&initial).unwrap();
        assert!(bootstrap.same_inventory(&restored));
        bootstrap.install_membership_health(&restored).unwrap();
        assert_eq!(bootstrap.to_bytes().unwrap(), encoded);
        assert!(bootstrap
            .install_membership_health(&ChunkGroupCatalog::from_bytes(&initial).unwrap())
            .is_err());
        let mut changed = group.clone();
        changed.api_endpoints[0] = "127.0.0.1:9301".into();
        let wrong = ChunkGroupCatalog::with_membership_health(
            vec![changed],
            [("group-a".into(), next.clone())].into(),
        )
        .unwrap();
        assert!(!catalog.same_inventory(&wrong));
        assert!(catalog.install_membership_health(&wrong).is_err());
        let mut image = CatalogImage::decode(&encoded[4..]).unwrap();
        image.health.push(image.health[0].clone());
        let mut duplicate = b"MOGC".to_vec();
        duplicate.extend(image.encode_to_vec());
        assert!(ChunkGroupCatalog::from_bytes(&duplicate).is_err());
        assert_eq!(
            catalog.to_bytes().unwrap(),
            encoded,
            "rejected replacements must preserve state"
        );
        assert!(
            catalog
                .check_request(&MetaRequest::HeartbeatChunkNode {
                    node_id: "voter-a".into(),
                    expected_endpoint: group.api_endpoints[0].clone(),
                    used_bytes: 100,
                    capacity_bytes: 4096,
                    online: true,
                    now_ms: 100,
                })
                .is_err(),
            "unfenced reports cannot update dynamic placement"
        );
    }

    #[test]
    fn joint_health_requires_both_majorities_and_excludes_learners() {
        let incoming = [1, 2, 3];
        let outgoing = [3, 4, 5];
        for mask in 0u64..64 {
            let report = |id: u64| {
                Some(VoterHealth {
                    online: mask & (1 << (id - 1)) != 0,
                    draining: false,
                    used_bytes: if id == 6 { 999 } else { id * 10 },
                    heartbeat_ms: id * 100,
                })
            };
            let (online, used, _) = project_voter_health(&incoming, &outgoing, report);
            let current_count = (mask & 0b000111).count_ones();
            let previous_count = (mask & 0b011100).count_ones();
            assert_eq!(
                online,
                current_count >= 2 && previous_count >= 2,
                "mask={mask}"
            );
            assert_eq!(used, 50, "learner usage must not enter voter accounting");
        }
        let report = |id| {
            Some(VoterHealth {
                online: true,
                draining: id == 3,
                used_bytes: id * 10,
                heartbeat_ms: id * 100,
            })
        };
        assert_eq!(
            project_voter_health(&incoming, &outgoing, report),
            (true, 50, Some(100))
        );
        let missing = |id| if id == 1 { None } else { report(id) };
        assert!(!project_voter_health(&incoming, &outgoing, missing).0);
        assert!(!project_voter_health(&[1, 1, 2], &[], report).0);
        assert!(!project_voter_health(&[], &outgoing, report).0);
        assert_eq!(
            project_voter_health(&[1, 2, 3], &[], report),
            (true, 30, Some(100))
        );
    }

    #[test]
    fn catalog_canonical_binary_roundtrip_and_corruption() {
        let mut changing = config();
        let identity = changing.identity.as_mut().unwrap();
        identity.version = 2;
        identity.bootstrap_voters = identity.voters.clone();
        identity.bootstrap_id = 9;
        identity.validate().unwrap();
        assert!(ChunkGroupCatalog::new(vec![changing]).is_err());
        let catalog = ChunkGroupCatalog::new(vec![config()]).unwrap();
        let bytes = catalog.to_bytes().unwrap();
        assert_eq!(
            ChunkGroupCatalog::from_bytes(&bytes)
                .unwrap()
                .to_bytes()
                .unwrap(),
            bytes
        );
        let empty = ChunkGroupCatalog::default().to_bytes().unwrap();
        for end in 0..bytes.len() {
            // A complete version-only image represents an empty catalog.
            if bytes[..end] != empty {
                assert!(ChunkGroupCatalog::from_bytes(&bytes[..end]).is_err());
            }
        }
        let mut unknown = bytes.clone();
        unknown.extend([0x78, 1]);
        assert!(ChunkGroupCatalog::from_bytes(&unknown).is_err());
        let mut duplicate = config();
        duplicate.member_ids[1] = duplicate.member_ids[0].clone();
        assert!(ChunkGroupCatalog::new(vec![duplicate]).is_err());
        let mut overlap = config();
        overlap.api_endpoints[0] = overlap.identity.as_ref().unwrap().peer_endpoints[0].clone();
        assert!(ChunkGroupCatalog::new(vec![overlap]).is_err());
    }
    #[test]
    fn catalog_rejects_cross_group_peer_reuse() {
        let first = config();
        let mut second = config();
        second.placement_id = "group-b".into();
        second.identity.as_mut().unwrap().group_id = 2;
        second.member_ids = vec!["voter-d".into(), "voter-e".into(), "voter-f".into()];
        second.api_endpoints = vec![
            "127.0.0.1:9301".into(),
            "127.0.0.1:9302".into(),
            "127.0.0.1:9303".into(),
        ];
        assert!(ChunkGroupCatalog::new(vec![first, second]).is_err());
    }
}

impl ChunkGroupConfig {
    pub(super) fn normalized_inventory(self) -> Result<Self, ObjectError> {
        let catalog = ChunkGroupCatalog::inventory(vec![self])?;
        Ok(catalog.groups.into_values().next().unwrap())
    }
}
