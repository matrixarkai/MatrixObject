// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One encoder and one decoder per metaserver message, shared by both ends.
//!
//! # WHY THE CLIENT AND THE SERVER DO NOT EACH HAVE THEIR OWN
//!
//! A wire format written twice is two formats that happen to agree today. So the request is
//! built here and parsed here, the reply is built here and parsed here, and
//! [`crate::cluster::meta_client`] and [`crate::cluster::meta_server`] are both callers.
//!
//! # THE VERBS ARE THE ONES THE CONTROL PLANE ALREADY HAS
//!
//! Every request here names a method that already exists on this store's control plane:
//! `heartbeat_placement_node`, `expire_placement_node_heartbeats`, `plan_object_placement`,
//! `commit_object_placement`. The facade in `src/store/core/readiness_catalog/` has been
//! request-shaped since before there was a socket -- register, heartbeat, plan, commit,
//! resolve, discover -- and inventing a parallel vocabulary beside it would leave two names
//! for every operation and a reader with no way to tell which one the store believes.
//!
//! # WHAT A NODE LOOKS LIKE ON THE WIRE, AND WHAT IS LEFT OFF
//!
//! A node in the roster is a `PlacementNode`: nineteen fields, three of which are health
//! structures carrying about a hundred fields of telemetry between them -- `NodePageHealth`
//! alone has thirty-eight, and `NodeMetaHealth` nests a control proof and a route-cache
//! status.
//!
//! [`PlacementNodeSummary`] carries the sixteen that are not telemetry, and **registration is not
//! a verb here at all**. Both of those are deliberate:
//!
//! * Writing a hundred-and-ten-field wire codec by hand is a hundred and ten chances to drop
//!   a field silently, and the only existing byte encoding for `PlacementNode` is inlined in
//!   `PlacementSnapshot::to_bytes` -- a durable format with thirty-two readable versions and
//!   fourteen checksum-material functions behind it. Sharing that is the right move and it is
//!   a change to a durable encoder, which is its own piece of work.
//! * So a node joins this metaserver's roster in the metaserver's own process, at startup,
//!   and the socket carries the operations that keep it current and place objects on it.
//!   That is the honest shape of this slice rather than a registration verb that drops the
//!   telemetry placement decides on.
//!
//! The narrowing is enforced by the compiler, not by this comment:
//! [`PlacementNodeSummary::from_placement_node`] destructures `PlacementNode` naming **every** field, so a
//! field added to it does not compile until it is either carried or discarded by name.

use crate::cluster::meta_wire::{put_bool, take_bool, MetaRequestCode};
use crate::cluster::wire::{put_str, put_u64, Cursor, StatusCode};
use crate::{ObjectError, ObjectId, ObjectPlacement, PlacementNode, PlacementPlan};

/// A node in the roster, as the wire reports it: everything except the telemetry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlacementNodeSummary {
    pub node_id: String,
    pub endpoint: String,
    pub service_kind: String,
    pub failure_domain: String,
    pub metadata_eligible: bool,
    pub data_eligible: bool,
    pub gateway_eligible: bool,
    pub draining: bool,
    pub online: bool,
    pub metadata_value_bytes: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    /// Bytes charged to this node by placements the metaserver has committed.
    pub committed_bytes: u64,
    pub last_heartbeat_ms: u64,
    pub allocator_shard_index: u64,
    pub allocator_shard_count: u64,
}

impl PlacementNodeSummary {
    /// The wire view of a roster node.
    ///
    /// DESTRUCTURED WITHOUT `..` ON PURPOSE. A field added to `PlacementNode` breaks this
    /// function, which is the only form of this narrowing that cannot quietly become an
    /// accident.
    pub fn from_placement_node(node: &PlacementNode) -> Self {
        let PlacementNode {
            node_id,
            endpoint,
            service_kind,
            failure_domain,
            metadata_eligible,
            data_eligible,
            gateway_eligible,
            metadata_value_bytes,
            capacity_bytes,
            used_bytes,
            committed_bytes,
            draining,
            online,
            last_heartbeat_ms,
            page_allocator_shard_index,
            page_allocator_shard_count,
            // DISCARDED ON PURPOSE. The wire view keeps its shape: this field is not sent,
            // and a receiver derives it from the kind in the same way the snapshot decoder
            // does for a record that predates the field. Named rather than swept into `..`
            // so that the next field added here has to be thought about too.
            serves_page_tier: _,
            // DISCARDED, NAMED, AND THE REASON IS THE MODULE NOTE. About a hundred fields of
            // telemetry between the three. A reader of this wire gets a node's identity,
            // eligibility, capacity and clock; it does not get the health the placement
            // policy consults, because that is read where the policy runs.
            meta_health: _,
            page_health: _,
            chunk_health: _,
        } = node;
        Self {
            node_id: node_id.clone(),
            endpoint: endpoint.clone(),
            service_kind: service_kind.clone(),
            failure_domain: failure_domain.clone(),
            metadata_eligible: *metadata_eligible,
            data_eligible: *data_eligible,
            gateway_eligible: *gateway_eligible,
            draining: *draining,
            online: *online,
            metadata_value_bytes: *metadata_value_bytes,
            capacity_bytes: *capacity_bytes,
            used_bytes: *used_bytes,
            committed_bytes: *committed_bytes,
            last_heartbeat_ms: *last_heartbeat_ms,
            allocator_shard_index: *page_allocator_shard_index,
            allocator_shard_count: *page_allocator_shard_count,
        }
    }
}

/// Nodes chosen for an object, and what was skipped choosing them.
///
/// The three skip counts are carried rather than dropped because they are the difference
/// between "this store has nowhere to put your object" and "this store has nowhere to put
/// your object *because four nodes are offline*". A planner that returned only a refusal
/// would make the second question a log-reading exercise on the metaserver's host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementAssignment {
    pub bucket: String,
    pub key: String,
    pub length: u64,
    pub node_ids: Vec<String>,
    pub skipped_draining: u64,
    pub skipped_offline: u64,
    pub skipped_capacity: u64,
    /// What a commit must hand back to prove it is committing this plan.
    pub plan_checksum: String,
}

impl PlacementAssignment {
    pub fn from_plan(plan: &PlacementPlan) -> Self {
        Self {
            bucket: plan.object_id.bucket.as_str().to_string(),
            key: plan.object_id.key.as_str().to_string(),
            length: plan.length,
            node_ids: plan.node_ids.clone(),
            skipped_draining: plan.skipped_draining,
            skipped_offline: plan.skipped_offline,
            skipped_capacity: plan.skipped_capacity,
            plan_checksum: plan.plan_checksum.clone(),
        }
    }
}

/// A placement the metaserver has committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedPlacement {
    pub bucket: String,
    pub key: String,
    pub version: u64,
    pub length: u64,
    pub node_ids: Vec<String>,
    pub update_time_ms: u64,
}

impl CommittedPlacement {
    pub fn from_placement(placement: &ObjectPlacement) -> Self {
        Self {
            bucket: placement.object_id.bucket.as_str().to_string(),
            key: placement.object_id.key.as_str().to_string(),
            version: placement.version,
            length: placement.length,
            node_ids: placement.node_ids.clone(),
            update_time_ms: placement.update_time_ms,
        }
    }

    pub fn into_placement(self) -> ObjectPlacement {
        ObjectPlacement {
            object_id: object_id(&self.bucket, &self.key),
            version: self.version,
            length: self.length,
            node_ids: self.node_ids,
            update_time_ms: self.update_time_ms,
        }
    }
}

/// An object's identity from its two names.
pub fn object_id(bucket: &str, key: &str) -> ObjectId {
    ObjectId {
        bucket: bucket.into(),
        key: key.into(),
    }
}

/// A request as both ends understand it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaRequest {
    HeartbeatChunkNode {
        node_id: String,
        expected_endpoint: String,
        used_bytes: u64,
        capacity_bytes: u64,
        online: bool,
        now_ms: u64,
    },
    HeartbeatRootNode {
        node_id: String,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    },
    ExpireRootNodeHeartbeats {
        ttl_ms: u64,
        now_ms: u64,
    },
    ListRootNodes,
    PlanObjectPlacement {
        bucket: String,
        key: String,
        length: u64,
        replica_count: u64,
    },
    CommitObjectPlacement {
        placement: CommittedPlacement,
        expected_plan_checksum: String,
    },
    ReadObjectRootPlacement {
        bucket: String,
        key: String,
    },
    Ping,
}

impl MetaRequest {
    pub fn code(&self) -> MetaRequestCode {
        match self {
            MetaRequest::HeartbeatRootNode { .. } => MetaRequestCode::HeartbeatRootNode,
            MetaRequest::HeartbeatChunkNode { .. } => MetaRequestCode::HeartbeatChunkNode,
            MetaRequest::ExpireRootNodeHeartbeats { .. } => {
                MetaRequestCode::ExpireRootNodeHeartbeats
            }
            MetaRequest::ListRootNodes => MetaRequestCode::ListRootNodes,
            MetaRequest::PlanObjectPlacement { .. } => MetaRequestCode::PlanObjectPlacement,
            MetaRequest::CommitObjectPlacement { .. } => MetaRequestCode::CommitObjectPlacement,
            MetaRequest::ReadObjectRootPlacement { .. } => MetaRequestCode::ReadObjectRootPlacement,
            MetaRequest::Ping => MetaRequestCode::Ping,
        }
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            MetaRequest::HeartbeatChunkNode {
                node_id,
                expected_endpoint,
                used_bytes,
                capacity_bytes,
                online,
                now_ms,
            } => {
                put_str(&mut out, node_id);
                put_str(&mut out, expected_endpoint);
                put_u64(&mut out, *used_bytes);
                put_u64(&mut out, *capacity_bytes);
                put_bool(&mut out, *online);
                put_u64(&mut out, *now_ms);
            }
            MetaRequest::HeartbeatRootNode {
                node_id,
                used_bytes,
                capacity_bytes,
                draining,
                online,
                now_ms,
            } => {
                put_str(&mut out, node_id);
                put_u64(&mut out, *used_bytes);
                put_u64(&mut out, *capacity_bytes);
                put_bool(&mut out, *draining);
                put_bool(&mut out, *online);
                put_u64(&mut out, *now_ms);
            }
            MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => {
                put_u64(&mut out, *ttl_ms);
                put_u64(&mut out, *now_ms);
            }
            MetaRequest::ListRootNodes | MetaRequest::Ping => {}
            MetaRequest::PlanObjectPlacement {
                bucket,
                key,
                length,
                replica_count,
            } => {
                put_str(&mut out, bucket);
                put_str(&mut out, key);
                put_u64(&mut out, *length);
                put_u64(&mut out, *replica_count);
            }
            MetaRequest::CommitObjectPlacement {
                placement,
                expected_plan_checksum,
            } => {
                put_placement(&mut out, placement);
                put_str(&mut out, expected_plan_checksum);
            }
            MetaRequest::ReadObjectRootPlacement { bucket, key } => {
                put_str(&mut out, bucket);
                put_str(&mut out, key);
            }
        }
        out
    }

    /// Read a request from a payload, REFUSING a payload with bytes left over.
    ///
    /// Bytes to spare mean the sender laid the fields out differently from this reader, and
    /// every field this reader did take came out of the wrong place -- a node id read one
    /// field short still parses into a valid string, naming a node nobody asked about.
    pub fn decode(code: MetaRequestCode, payload: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = Cursor::new(payload);
        let request = match code {
            MetaRequestCode::HeartbeatChunkNode => MetaRequest::HeartbeatChunkNode {
                node_id: cursor.take_str()?,
                expected_endpoint: cursor.take_str()?,
                used_bytes: cursor.take_u64()?,
                capacity_bytes: cursor.take_u64()?,
                online: take_bool(&mut cursor)?,
                now_ms: cursor.take_u64()?,
            },
            MetaRequestCode::HeartbeatRootNode => MetaRequest::HeartbeatRootNode {
                node_id: cursor.take_str()?,
                used_bytes: cursor.take_u64()?,
                capacity_bytes: cursor.take_u64()?,
                draining: take_bool(&mut cursor)?,
                online: take_bool(&mut cursor)?,
                now_ms: cursor.take_u64()?,
            },
            MetaRequestCode::ExpireRootNodeHeartbeats => MetaRequest::ExpireRootNodeHeartbeats {
                ttl_ms: cursor.take_u64()?,
                now_ms: cursor.take_u64()?,
            },
            MetaRequestCode::ListRootNodes => MetaRequest::ListRootNodes,
            MetaRequestCode::PlanObjectPlacement => MetaRequest::PlanObjectPlacement {
                bucket: cursor.take_str()?,
                key: cursor.take_str()?,
                length: cursor.take_u64()?,
                replica_count: cursor.take_u64()?,
            },
            MetaRequestCode::CommitObjectPlacement => MetaRequest::CommitObjectPlacement {
                placement: take_placement(&mut cursor)?,
                expected_plan_checksum: cursor.take_str()?,
            },
            MetaRequestCode::ReadObjectRootPlacement => MetaRequest::ReadObjectRootPlacement {
                bucket: cursor.take_str()?,
                key: cursor.take_str()?,
            },
            MetaRequestCode::Ping => MetaRequest::Ping,
        };
        if cursor.remaining() != 0 {
            return Err(ObjectError::Corruption(format!(
                "a metaserver request of kind {code:?} left {} byte(s) unread, so the sender \
                 laid its fields out differently from this reader",
                cursor.remaining()
            )));
        }
        Ok(request)
    }
}

/// Reply contents, including a pre-admission leader-routing response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaReply {
    /// A routing response: this request was not proposed or applied.
    NotLeader,
    PlacementNode(PlacementNodeSummary),
    RootNodes(Vec<PlacementNodeSummary>),
    /// How many nodes this call took offline.
    Expired {
        offline_count: u64,
    },
    Plan(PlacementAssignment),
    Placement(CommittedPlacement),
    /// The metaserver is answering, and this is what it holds.
    Pong {
        node_count: u64,
        placement_count: u64,
        commit_index: u64,
    },
}

impl MetaReply {
    pub(crate) fn payload_len(&self) -> usize {
        let strings = |values: &[String]| {
            values
                .iter()
                .fold(0usize, |n, v| n.saturating_add(4).saturating_add(v.len()))
        };
        match self {
            Self::NotLeader => 0,
            Self::PlacementNode(node) => node_payload_len(
                &node.node_id,
                &node.endpoint,
                &node.service_kind,
                &node.failure_domain,
            ),
            Self::RootNodes(nodes) => nodes.iter().fold(8usize, |n, node| {
                n.saturating_add(node_payload_len(
                    &node.node_id,
                    &node.endpoint,
                    &node.service_kind,
                    &node.failure_domain,
                ))
            }),
            Self::Expired { .. } => 8,
            Self::Plan(plan) => 52usize
                .saturating_add(plan.bucket.len())
                .saturating_add(plan.key.len())
                .saturating_add(plan.plan_checksum.len())
                .saturating_add(strings(&plan.node_ids)),
            Self::Placement(placement) => placement_payload_len(placement),
            Self::Pong { .. } => 24,
        }
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            MetaReply::NotLeader => {}
            MetaReply::PlacementNode(node) => put_node(&mut out, node),
            MetaReply::RootNodes(nodes) => {
                put_u64(&mut out, nodes.len() as u64);
                for node in nodes {
                    put_node(&mut out, node);
                }
            }
            MetaReply::Expired { offline_count } => put_u64(&mut out, *offline_count),
            MetaReply::Plan(plan) => {
                put_str(&mut out, &plan.bucket);
                put_str(&mut out, &plan.key);
                put_u64(&mut out, plan.length);
                put_u64(&mut out, plan.node_ids.len() as u64);
                for node_id in &plan.node_ids {
                    put_str(&mut out, node_id);
                }
                put_u64(&mut out, plan.skipped_draining);
                put_u64(&mut out, plan.skipped_offline);
                put_u64(&mut out, plan.skipped_capacity);
                put_str(&mut out, &plan.plan_checksum);
            }
            MetaReply::Placement(placement) => put_placement(&mut out, placement),
            MetaReply::Pong {
                node_count,
                placement_count,
                commit_index,
            } => {
                put_u64(&mut out, *node_count);
                put_u64(&mut out, *placement_count);
                put_u64(&mut out, *commit_index);
            }
        }
        out
    }

    /// Read a reply, given the request it answers.
    ///
    /// The request kind is an argument rather than a byte on the reply, because a reply whose
    /// shape is read from the reply itself can be read as the wrong shape. The caller knows
    /// what it asked.
    pub fn decode(code: MetaRequestCode, payload: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = Cursor::new(payload);
        let reply = match code {
            MetaRequestCode::HeartbeatRootNode | MetaRequestCode::HeartbeatChunkNode => {
                MetaReply::PlacementNode(take_node(&mut cursor)?)
            }
            MetaRequestCode::ListRootNodes => {
                let count = cursor.take_u64()?;
                // BOUNDED BEFORE ANYTHING IS RESERVED. A node is at least this many bytes on
                // the wire even with every string empty, so a count past what is left is a
                // frame to refuse rather than a `Vec` to try to allocate.
                let floor = (count as usize).saturating_mul(MIN_NODE_BYTES);
                if floor > cursor.remaining() {
                    return Err(ObjectError::Corruption(format!(
                        "a metaserver roster claims {count} node(s), which need at least \
                         {floor} byte(s), with only {} left in the frame",
                        cursor.remaining()
                    )));
                }
                let mut nodes = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    nodes.push(take_node(&mut cursor)?);
                }
                MetaReply::RootNodes(nodes)
            }
            MetaRequestCode::ExpireRootNodeHeartbeats => MetaReply::Expired {
                offline_count: cursor.take_u64()?,
            },
            MetaRequestCode::PlanObjectPlacement => {
                let bucket = cursor.take_str()?;
                let key = cursor.take_str()?;
                let length = cursor.take_u64()?;
                let count = cursor.take_u64()?;
                let floor = (count as usize).saturating_mul(4);
                if floor > cursor.remaining() {
                    return Err(ObjectError::Corruption(format!(
                        "a placement plan claims {count} node(s), which need at least {floor} \
                         byte(s), with only {} left in the frame",
                        cursor.remaining()
                    )));
                }
                let mut node_ids = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    node_ids.push(cursor.take_str()?);
                }
                MetaReply::Plan(PlacementAssignment {
                    bucket,
                    key,
                    length,
                    node_ids,
                    skipped_draining: cursor.take_u64()?,
                    skipped_offline: cursor.take_u64()?,
                    skipped_capacity: cursor.take_u64()?,
                    plan_checksum: cursor.take_str()?,
                })
            }
            MetaRequestCode::CommitObjectPlacement | MetaRequestCode::ReadObjectRootPlacement => {
                MetaReply::Placement(take_placement(&mut cursor)?)
            }
            MetaRequestCode::Ping => MetaReply::Pong {
                node_count: cursor.take_u64()?,
                placement_count: cursor.take_u64()?,
                commit_index: cursor.take_u64()?,
            },
        };
        if cursor.remaining() != 0 {
            return Err(ObjectError::Corruption(format!(
                "a metaserver reply to {code:?} left {} byte(s) unread",
                cursor.remaining()
            )));
        }
        Ok(reply)
    }
}

/// The smallest a node summary can be on the wire: four empty strings and the rest.
///
/// A floor, not an estimate, and used only to bound a roster count before a `Vec` is reserved
/// for it: four 4-byte string headers, five 8-byte booleans and seven 8-byte numbers.
const MIN_NODE_BYTES: usize = 4 * 4 + 5 * 8 + 7 * 8;

pub(crate) fn node_payload_len(id: &str, endpoint: &str, kind: &str, domain: &str) -> usize {
    [id, endpoint, kind, domain]
        .iter()
        .fold(MIN_NODE_BYTES, |n, text| n.saturating_add(text.len()))
}

pub(crate) fn placement_payload_len(placement: &CommittedPlacement) -> usize {
    placement.node_ids.iter().fold(
        40usize
            .saturating_add(placement.bucket.len())
            .saturating_add(placement.key.len()),
        |n, id| n.saturating_add(4).saturating_add(id.len()),
    )
}

fn put_node(out: &mut Vec<u8>, node: &PlacementNodeSummary) {
    put_str(out, &node.node_id);
    put_str(out, &node.endpoint);
    put_str(out, &node.service_kind);
    put_str(out, &node.failure_domain);
    put_bool(out, node.metadata_eligible);
    put_bool(out, node.data_eligible);
    put_bool(out, node.gateway_eligible);
    put_bool(out, node.draining);
    put_bool(out, node.online);
    put_u64(out, node.metadata_value_bytes);
    put_u64(out, node.capacity_bytes);
    put_u64(out, node.used_bytes);
    put_u64(out, node.committed_bytes);
    put_u64(out, node.last_heartbeat_ms);
    put_u64(out, node.allocator_shard_index);
    put_u64(out, node.allocator_shard_count);
}

fn take_node(cursor: &mut Cursor<'_>) -> Result<PlacementNodeSummary, ObjectError> {
    Ok(PlacementNodeSummary {
        node_id: cursor.take_str()?,
        endpoint: cursor.take_str()?,
        service_kind: cursor.take_str()?,
        failure_domain: cursor.take_str()?,
        metadata_eligible: take_bool(cursor)?,
        data_eligible: take_bool(cursor)?,
        gateway_eligible: take_bool(cursor)?,
        draining: take_bool(cursor)?,
        online: take_bool(cursor)?,
        metadata_value_bytes: cursor.take_u64()?,
        capacity_bytes: cursor.take_u64()?,
        used_bytes: cursor.take_u64()?,
        committed_bytes: cursor.take_u64()?,
        last_heartbeat_ms: cursor.take_u64()?,
        allocator_shard_index: cursor.take_u64()?,
        allocator_shard_count: cursor.take_u64()?,
    })
}

fn put_placement(out: &mut Vec<u8>, placement: &CommittedPlacement) {
    put_str(out, &placement.bucket);
    put_str(out, &placement.key);
    put_u64(out, placement.version);
    put_u64(out, placement.length);
    put_u64(out, placement.node_ids.len() as u64);
    for node_id in &placement.node_ids {
        put_str(out, node_id);
    }
    put_u64(out, placement.update_time_ms);
}

fn take_placement(cursor: &mut Cursor<'_>) -> Result<CommittedPlacement, ObjectError> {
    let bucket = cursor.take_str()?;
    let key = cursor.take_str()?;
    let version = cursor.take_u64()?;
    let length = cursor.take_u64()?;
    let count = cursor.take_u64()?;
    let floor = (count as usize).saturating_mul(4);
    if floor > cursor.remaining() {
        return Err(ObjectError::Corruption(format!(
            "a placement claims {count} node(s), which need at least {floor} byte(s), with \
             only {} left in the frame",
            cursor.remaining()
        )));
    }
    let mut node_ids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        node_ids.push(cursor.take_str()?);
    }
    Ok(CommittedPlacement {
        bucket,
        key,
        version,
        length,
        node_ids,
        update_time_ms: cursor.take_u64()?,
    })
}

/// Lay out a complete request frame: header then payload.
pub fn encode_meta_request(request: &MetaRequest) -> Vec<u8> {
    let payload = request.encode_payload();
    let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload.len());
    crate::cluster::wire::put_header(&mut frame, request.code() as u8, payload.len());
    frame.extend_from_slice(&payload);
    frame
}

/// Lay out a complete success frame.
pub fn encode_meta_reply(reply: &MetaReply) -> Vec<u8> {
    let payload = reply.encode_payload();
    let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload.len());
    let status = if matches!(reply, MetaReply::NotLeader) {
        StatusCode::NotLeader
    } else {
        StatusCode::Ok
    };
    crate::cluster::wire::put_header(&mut frame, status as u8, payload.len());
    frame.extend_from_slice(&payload);
    frame
}
