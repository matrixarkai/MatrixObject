// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Quorum-confirmed inventory and committed membership for chunk client routing.
use super::{ChunkGroupCatalog, ChunkGroupConfig, ChunkMembershipFence};
use crate::ObjectError;
use prost::Message;
use std::collections::BTreeSet;

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkGroupRoute {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(message, optional, tag = "2")]
    pub group: Option<ChunkGroupConfig>,
    /// Empty only for fixed membership; dynamic membership requires a retained fence.
    #[prost(bytes = "vec", tag = "3")]
    pub fence: Vec<u8>,
}
impl ChunkGroupRoute {
    pub fn membership_fence(&self) -> Result<Option<ChunkMembershipFence>, ObjectError> {
        if self.version != 1 || self.encoded_len() > 65536 {
            return Err(invalid("invalid chunk route bounds"));
        }
        let group = self
            .group
            .as_ref()
            .ok_or_else(|| invalid("chunk route inventory missing"))?;
        if group.clone().normalized_inventory()? != *group {
            return Err(invalid("noncanonical chunk route inventory"));
        }
        let identity = group.identity.as_ref().unwrap();
        match identity.version {
            1 if self.fence.is_empty() => Ok(None),
            2 if !self.fence.is_empty() => {
                ChunkMembershipFence::from_bytes(&self.fence, identity).map(Some)
            }
            _ => Err(invalid("chunk route membership does not match inventory")),
        }
    }
    /// API sockets and node IDs for current voters; joint membership includes both sets.
    pub fn active_members(&self) -> Result<Vec<(String, u64)>, ObjectError> {
        let fence = self.membership_fence()?;
        let group = self.group.as_ref().unwrap();
        let identity = group.identity.as_ref().unwrap();
        let ids: BTreeSet<u64> = match fence {
            Some(fence) => fence
                .voters()
                .iter()
                .chain(fence.outgoing_voters())
                .copied()
                .collect(),
            None => identity.voters.iter().copied().collect(),
        };
        if ids.is_empty() || ids.len() > 14 {
            return Err(invalid("chunk route voter count exceeds bound"));
        }
        ids.into_iter()
            .map(|id| {
                let index = identity
                    .voters
                    .binary_search(&id)
                    .map_err(|_| invalid("chunk route voter outside inventory"))?;
                Ok((group.api_endpoints[index].clone(), id))
            })
            .collect()
    }
    pub(super) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 65536 {
            return Err(invalid("chunk route exceeds size bound"));
        }
        let route = Self::decode(bytes).map_err(|_| invalid("invalid chunk route protobuf"))?;
        route.membership_fence()?;
        if route.encode_to_vec() != bytes {
            return Err(invalid("noncanonical chunk route protobuf"));
        }
        Ok(route)
    }
}
impl ChunkGroupCatalog {
    pub(super) fn routing_view(&self, placement_id: &str) -> Result<ChunkGroupRoute, ObjectError> {
        let group = self
            .get(placement_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("chunk group missing".into()))?;
        let fence = self
            .membership_health(placement_id)?
            .map(|health| health.fence().to_bytes())
            .unwrap_or_default();
        let route = ChunkGroupRoute {
            version: 1,
            group: Some(group),
            fence,
        };
        route.membership_fence()?;
        Ok(route)
    }
}
#[derive(Clone, PartialEq, Message)]
pub(super) struct RouteRead {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub placement_id: String,
}
impl RouteRead {
    pub(super) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 256 {
            return Err(invalid("chunk route request exceeds bound"));
        }
        let request =
            Self::decode(bytes).map_err(|_| invalid("invalid chunk route request protobuf"))?;
        if request.version != 1
            || request.placement_id.is_empty()
            || request.placement_id.len() > 128
            || !request
                .placement_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || request.encode_to_vec() != bytes
        {
            return Err(invalid("invalid chunk route request"));
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ChunkGroupIdentity, MembershipStatus};
    fn route() -> ChunkGroupRoute {
        let identity = ChunkGroupIdentity {
            version: 2,
            group_id: 42,
            node_id: 1,
            capacity_bytes: 4096,
            max_pages: 16,
            voters: (1..=64).collect(),
            peer_endpoints: (1..=64)
                .map(|i| format!("127.0.0.1:{}", 18000 + i))
                .collect(),
            bootstrap_id: 91,
            bootstrap_voters: vec![1, 2, 3],
        };
        let status = MembershipStatus {
            version: 1,
            node_id: 1,
            term: 2,
            applied_index: 20,
            voters: (8..=14).collect(),
            outgoing_voters: (1..=7).collect(),
            learners: vec![15],
        };
        let fence = ChunkMembershipFence::from_status(&identity, &status)
            .unwrap()
            .to_bytes();
        ChunkGroupRoute {
            version: 1,
            fence,
            group: Some(ChunkGroupConfig {
                placement_id: "group-a".into(),
                failure_domain: "zone-a".into(),
                identity: Some(identity),
                api_endpoints: (1..=64)
                    .map(|i| format!("127.0.0.1:{}", 19000 + i))
                    .collect(),
                member_ids: (1..=64).map(|i| format!("member-{i}")).collect(),
            }),
        }
    }
    #[test]
    fn routes_only_joint_voters_from_large_inventory() {
        let route = route();
        assert_eq!(
            route.active_members().unwrap(),
            (1..=14)
                .map(|id| (format!("127.0.0.1:{}", 19000 + id), id))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ChunkGroupRoute::from_payload(&route.encode_to_vec()).unwrap(),
            route
        );
        let group = route.group.as_ref().unwrap();
        let status = MembershipStatus {
            version: 1,
            node_id: 8,
            term: 3,
            applied_index: 30,
            voters: vec![8, 9, 10],
            outgoing_voters: vec![],
            learners: vec![15],
        };
        let mut next = route.clone();
        next.fence = ChunkMembershipFence::from_status(group.identity.as_ref().unwrap(), &status)
            .unwrap()
            .to_bytes();
        assert_eq!(
            next.active_members()
                .unwrap()
                .iter()
                .map(|(_, id)| *id)
                .collect::<Vec<_>>(),
            vec![8, 9, 10]
        );
    }
    #[test]
    fn route_rejects_missing_conflicting_and_noncanonical_membership() {
        let route = route();
        for field in 0..6 {
            let mut bad = route.clone();
            match field {
                0 => bad.fence.clear(),
                1 => bad.group = None,
                2 => bad.version = 2,
                3 => {
                    bad.group
                        .as_mut()
                        .unwrap()
                        .identity
                        .as_mut()
                        .unwrap()
                        .bootstrap_id += 1
                }
                4 => {
                    bad.group
                        .as_mut()
                        .unwrap()
                        .identity
                        .as_mut()
                        .unwrap()
                        .group_id += 1
                }
                _ => bad
                    .group
                    .as_mut()
                    .unwrap()
                    .api_endpoints
                    .pop()
                    .map(|_| ())
                    .unwrap(),
            }
            assert!(ChunkGroupRoute::from_payload(&bad.encode_to_vec()).is_err());
        }
        let mut bytes = route.encode_to_vec();
        bytes.extend([0x78, 1]);
        assert!(ChunkGroupRoute::from_payload(&bytes).is_err());
        assert!(ChunkGroupRoute::from_payload(&vec![0; 65537]).is_err());
    }
    #[test]
    fn route_requests_are_bounded_and_canonical() {
        let request = RouteRead {
            version: 1,
            placement_id: "group-a".into(),
        };
        let bytes = request.encode_to_vec();
        assert_eq!(RouteRead::from_payload(&bytes).unwrap(), request);
        for id in [String::new(), "a".repeat(129), "group/a".into()] {
            assert!(RouteRead::from_payload(
                &RouteRead {
                    version: 1,
                    placement_id: id
                }
                .encode_to_vec()
            )
            .is_err());
        }
        let mut bytes = bytes;
        bytes.extend([0x78, 1]);
        assert!(RouteRead::from_payload(&bytes).is_err());
        assert!(RouteRead::from_payload(&vec![0; 257]).is_err());
    }
}
