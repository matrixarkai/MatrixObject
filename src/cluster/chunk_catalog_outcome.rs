// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! One committed update to membership health and its placement projection.
use super::ChunkGroupCatalog;
use crate::{placement_outcome::PlacementOutcome, MatrixMetaService, ObjectError};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub(super) struct ChunkCatalogOutcome {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    placement_id: String,
    #[prost(bytes = "vec", tag = "3")]
    before: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    after: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    placement: Vec<u8>,
}
impl ChunkCatalogOutcome {
    pub(super) fn new(
        placement_id: String,
        before: Vec<u8>,
        after: Vec<u8>,
        placement: &PlacementOutcome,
    ) -> Result<Self, ObjectError> {
        let outcome = Self {
            version: 1,
            placement_id,
            before,
            after,
            placement: placement.to_payload()?,
        };
        outcome.validate()?;
        Ok(outcome)
    }
    fn validate(&self) -> Result<(), ObjectError> {
        if self.version != 1
            || self.placement_id.is_empty()
            || self.placement_id.len() > 128
            || self.before.is_empty()
            || self.after.is_empty()
            || self.before.len() > 16 * 1024
            || self.after.len() > 16 * 1024
            || self.encoded_len() > 128 * 1024
        {
            return Err(ObjectError::Corruption(
                "invalid chunk catalog outcome bounds".into(),
            ));
        }
        let placement = PlacementOutcome::from_payload(&self.placement)?;
        if !placement.placements.is_empty() || placement.manifest.is_some() {
            return Err(ObjectError::Corruption(
                "chunk catalog outcome changes object state".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        Ok(self.encode_to_vec())
    }
    pub(super) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 128 * 1024 {
            return Err(ObjectError::Corruption(
                "chunk catalog outcome exceeds bound".into(),
            ));
        }
        let value = Self::decode(bytes).map_err(|_| {
            ObjectError::Corruption("invalid chunk catalog outcome protobuf".into())
        })?;
        value.validate()?;
        if value.encode_to_vec() != bytes {
            return Err(ObjectError::Corruption(
                "noncanonical chunk catalog outcome".into(),
            ));
        }
        Ok(value)
    }
    pub(super) fn apply(
        &self,
        service: &mut MatrixMetaService,
        catalog: &ChunkGroupCatalog,
    ) -> Result<(), ObjectError> {
        self.validate()?;
        let placement = PlacementOutcome::from_payload(&self.placement)?;
        let group = catalog
            .get(&self.placement_id)
            .ok_or_else(|| ObjectError::Corruption("chunk catalog outcome group missing".into()))?;
        let capacity = group.identity.as_ref().unwrap().capacity_bytes;
        for change in &placement.nodes {
            let (Some(before), Some(after)) = (&change.before, &change.after) else {
                return Err(ObjectError::Corruption(
                    "chunk catalog outcome lacks node state".into(),
                ));
            };
            if (before.node_id != self.placement_id && !group.member_ids.contains(&before.node_id))
                || before.node_id != after.node_id
                || after.capacity_bytes != capacity
                || before.capacity_bytes != capacity
                || after.committed_bytes != before.committed_bytes
            {
                return Err(ObjectError::Corruption(
                    "chunk catalog outcome exceeds group scope".into(),
                ));
            }
        }
        let identity = group.identity.as_ref().unwrap();
        let health = super::ChunkMembershipHealth::from_bytes(&self.after, identity)?;
        let post_node = |id: &str| {
            let updated = placement
                .nodes
                .iter()
                .filter_map(|c| c.after.as_ref())
                .find(|node| node.node_id == id);
            updated
                .map(|node| super::chunk_groups::VoterHealth {
                    online: node.online,
                    draining: node.draining,
                    used_bytes: node.used_bytes,
                    heartbeat_ms: node.last_heartbeat_ms,
                })
                .or_else(|| {
                    service
                        .nodes
                        .get(id)
                        .map(|node| super::chunk_groups::VoterHealth {
                            online: node.online,
                            draining: node.draining,
                            used_bytes: node.used_bytes,
                            heartbeat_ms: node.last_heartbeat_ms,
                        })
                })
        };
        for (id, member) in identity.voters.iter().zip(&group.member_ids) {
            let node = post_node(member)
                .ok_or_else(|| ObjectError::Corruption("chunk outcome member missing".into()))?;
            if let Some(report) = health.accepted_report(*id) {
                if node.used_bytes != report.used_bytes
                    || node.heartbeat_ms != report.heartbeat_ms
                    || (node.online && !report.online)
                    || (report.draining && !node.draining)
                {
                    return Err(ObjectError::Corruption(
                        "chunk outcome member differs from accepted report".into(),
                    ));
                }
            }
        }
        let (online, used, time) = health.project_with_status(0, |id| {
            identity
                .voters
                .binary_search(&id)
                .ok()
                .and_then(|index| post_node(&group.member_ids[index]))
                .map(|node| (node.online, node.draining))
                .unwrap_or((false, false))
        });
        let before = service.nodes.get(&self.placement_id).ok_or_else(|| {
            ObjectError::Corruption("chunk outcome placement target missing".into())
        })?;
        let after = post_node(&self.placement_id).unwrap();
        if after.online != online
            || after.used_bytes != used
            || after.heartbeat_ms != before.last_heartbeat_ms.max(time.unwrap_or(0))
        {
            return Err(ObjectError::Corruption(
                "chunk outcome projection differs from membership health".into(),
            ));
        }
        // Hold the catalog state through placement validation/application. The
        // placement decoder checks every prior value before mutating service state.
        // Once it succeeds, installing the already validated health cannot fail.
        catalog.replace_membership_health_with(
            &self.placement_id,
            &self.before,
            &self.after,
            || placement.apply(service),
        )
    }
}
