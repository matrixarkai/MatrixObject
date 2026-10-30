// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded commands that prepare catalog outcomes without changing live state.
use super::chunk_catalog_outcome::ChunkCatalogOutcome;
use super::{ChunkGroupCatalog, MembershipStatus, MetaReply, PlacementNodeSummary};
use crate::{placement_outcome::PlacementOutcome, MatrixMetaService, ObjectError, PlacementNode};
use prost::Message;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkCatalogCommand {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// 1 advances a quorum-confirmed membership observation; 2 reports member health.
    #[prost(uint32, tag = "2")]
    pub operation: u32,
    #[prost(string, tag = "3")]
    pub placement_id: String,
    #[prost(bytes = "vec", tag = "4")]
    pub expected_fence: Vec<u8>,
    #[prost(uint64, tag = "5")]
    pub bootstrap_id: u64,
    #[prost(message, optional, tag = "6")]
    pub status: Option<MembershipStatus>,
    #[prost(string, tag = "7")]
    pub member_id: String,
    #[prost(string, tag = "8")]
    pub expected_endpoint: String,
    #[prost(uint64, tag = "9")]
    pub used_bytes: u64,
    #[prost(uint64, tag = "10")]
    pub capacity_bytes: u64,
    #[prost(bool, tag = "11")]
    pub online: bool,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
impl ChunkCatalogCommand {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.version != 1
            || self.bootstrap_id == 0
            || self.placement_id.is_empty()
            || self.placement_id.len() > 128
            || self.encoded_len() > 4096
        {
            return Err(invalid("invalid chunk catalog command bounds"));
        }
        self.status
            .as_ref()
            .ok_or_else(|| invalid("chunk membership status missing"))?
            .validate()?;
        match self.operation {
            1 if !self.expected_fence.is_empty()
                && self.expected_fence.len() <= 2048
                && self.member_id.is_empty()
                && self.expected_endpoint.is_empty()
                && self.used_bytes == 0
                && self.capacity_bytes == 0
                && !self.online =>
            {
                Ok(())
            }
            2 if self.expected_fence.is_empty()
                && !self.member_id.is_empty()
                && self.member_id.len() <= 128
                && !self.expected_endpoint.is_empty()
                && self.expected_endpoint.len() <= 128
                && self.capacity_bytes != 0
                && self.used_bytes <= self.capacity_bytes =>
            {
                Ok(())
            }
            _ => Err(invalid("invalid chunk catalog command fields")),
        }
    }
    pub(super) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 4096 {
            return Err(invalid("chunk catalog command exceeds bound"));
        }
        let value =
            Self::decode(bytes).map_err(|_| invalid("invalid chunk catalog command protobuf"))?;
        value.validate()?;
        if value.encode_to_vec() != bytes {
            return Err(invalid("noncanonical chunk catalog command"));
        }
        Ok(value)
    }
}
struct Draft<'a> {
    service: &'a mut MatrixMetaService,
    nodes: Vec<PlacementNode>,
    index: u64,
}
impl Drop for Draft<'_> {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            self.service.nodes.insert(node.node_id.clone(), node);
        }
        self.service.placement_commit_index = self.index;
        self.service.invalidate_snapshot_export_cache();
    }
}
pub(super) fn prepare(
    service: &mut MatrixMetaService,
    catalog: &ChunkGroupCatalog,
    command: &ChunkCatalogCommand,
    limit: usize,
) -> Result<(ChunkCatalogOutcome, MetaReply), ObjectError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| invalid("metadata clock precedes epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| invalid("metadata clock exceeds health range"))?;
    prepare_at(service, catalog, command, limit, now)
}
pub(super) fn prepare_at(
    service: &mut MatrixMetaService,
    catalog: &ChunkGroupCatalog,
    command: &ChunkCatalogCommand,
    limit: usize,
    now: u64,
) -> Result<(ChunkCatalogOutcome, MetaReply), ObjectError> {
    command.validate()?;
    let group = catalog
        .get(&command.placement_id)
        .ok_or_else(|| invalid("chunk group missing"))?;
    let identity = group.identity.as_ref().unwrap();
    if identity.bootstrap_id != command.bootstrap_id || identity.version != 2 {
        return Err(invalid("chunk command bootstrap identity mismatch"));
    }
    let status = command.status.as_ref().unwrap();
    status.validate_inventory(&identity.voters)?;
    let mut health = catalog
        .membership_health(&command.placement_id)?
        .ok_or_else(|| invalid("chunk group has fixed membership"))?;
    let before = health.to_bytes();
    let mut member_update = None;
    if command.operation == 1 {
        if health.fence().to_bytes() != command.expected_fence {
            return Err(invalid("chunk membership fence changed"));
        }
        health.advance(identity, status)?;
    } else {
        let index = identity
            .voters
            .binary_search(&status.node_id)
            .map_err(|_| invalid("report node outside inventory"))?;
        if group.member_ids[index] != command.member_id
            || group.api_endpoints[index] != command.expected_endpoint
            || identity.capacity_bytes != command.capacity_bytes
        {
            return Err(invalid("chunk report differs from inventory"));
        }
        let node = service
            .nodes
            .get(&command.member_id)
            .ok_or_else(|| invalid("chunk report member missing"))?;
        let decided_time = now.max(node.last_heartbeat_ms).max(1);
        health.record(
            identity,
            status.node_id,
            command.bootstrap_id,
            status,
            decided_time,
            command.used_bytes,
            command.online,
            node.draining,
        )?;
        member_update = Some((command.member_id.clone(), decided_time));
    }
    let ids = catalog.affected_nodes(group.member_ids.clone());
    let captured = PlacementOutcome::capture(service, ids.clone(), None)?;
    let nodes = ids
        .iter()
        .map(|id| {
            service
                .nodes
                .get(id)
                .cloned()
                .ok_or_else(|| invalid("catalog roster missing"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let index = service.placement_commit_index;
    let draft = Draft {
        service,
        nodes,
        index,
    };
    if let Some((member, time)) = member_update {
        let node = draft.service.nodes.get_mut(&member).unwrap();
        node.used_bytes = command.used_bytes;
        node.online = command.online;
        node.last_heartbeat_ms = time;
    }
    let (online, used, time) = health.project_with_status(0, |id| {
        let index = identity.voters.binary_search(&id).unwrap();
        let node = &draft.service.nodes[&group.member_ids[index]];
        (node.online, node.draining)
    });
    let node = draft.service.nodes.get_mut(&command.placement_id).unwrap();
    node.online = online;
    node.used_bytes = used;
    if let Some(time) = time {
        node.last_heartbeat_ms = node.last_heartbeat_ms.max(time);
    }
    let reply = MetaReply::PlacementNode(PlacementNodeSummary::from_placement_node(node));
    super::message::check_reply_size(reply.payload_len(), limit)?;
    draft.service.placement_commit_index = index
        .checked_add(1)
        .ok_or_else(|| invalid("placement index exhausted"))?;
    let placement = captured.finish(draft.service)?;
    let outcome = ChunkCatalogOutcome::new(
        command.placement_id.clone(),
        before,
        health.to_bytes(),
        &placement,
    )?;
    super::message::check_reply_size(outcome.to_payload()?.len(), limit)?;
    drop(draft);
    Ok((outcome, reply))
}
