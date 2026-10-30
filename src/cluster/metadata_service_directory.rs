// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Metadata location and behavior when the control plane is unavailable.
//!
//! An empty catalog selects an in-process service. A non-empty catalog supplies
//! one through seven metadata API endpoints. The client follows typed pre-admission
//! leader refusals, and may reconnect reads after transport failure. It does not
//! retry a mutation whose response was lost or whose admission is uncertain.
//!
//! The metaserver owns the roster, placements, leases, shards, transactions, and
//! metadata digests. The store owns object names and their chunk extents; replica
//! nodes own the data. Existing object data reads therefore remain independent of
//! metadata availability, while new placement requires a working control plane.
//!
//! Single-node state can be persisted with `MATRIXOBJECT_METASERVER_STATE_DIR`.
//! With the optional `metadata-consensus` feature and Raft configuration, metadata
//! requests instead require a voting majority. Catalog endpoints must name the
//! metadata API ports, not the separate consensus ports. A catalog does not itself
//! turn independent single-node servers into a replicated group.

use crate::cluster::meta_client::{MetaCallOutcome, MetaClientOptions, RemoteMetaClient};
use crate::cluster::meta_message::{
    CommittedPlacement, MetaReply, MetaRequest, PlacementAssignment, PlacementNodeSummary,
};
use crate::{MatrixMetaService, ObjectError};

/// What a round trip to the control plane just found.
///
/// Every field is filled from the answer to one request made when [`MetaDirectory::status`]
/// was called. Nothing here is carried over from a previous call, which is the property that
/// makes `reachable` worth reading at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlaneStatus {
    /// Whether the control plane is in another process.
    pub remote: bool,
    /// Whether it answered, just now.
    pub reachable: bool,
    pub node_count: u64,
    pub placement_count: u64,
    /// The control plane's own commit index, which advances on every roster or placement
    /// change. Taken from the control plane, never from the caller's own bookkeeping -- a
    /// figure a checker reads out of its own memory is a comparison with itself.
    pub commit_index: u64,
    /// Why it did not answer. Empty when it did.
    pub detail: String,
}

/// The control plane, wherever the endpoint list put it.
///
/// Two arms, one set of calls. A caller written against this does not know whether it is
/// addressing a `BTreeMap` on this heap or a process on another machine.
///
/// # WHY EVERY METHOD TAKES `&mut self`
///
/// `plan_object_placement` reads the roster and takes `&self` locally. Its remote form takes
/// `&mut self`, because sending a request frame is a write to a socket. The asymmetry is
/// recorded rather than smoothed over, exactly as [`crate::cluster::client`] records it for
/// the replica path: hiding it behind interior mutability would make a socket write look like
/// a read to every caller.
#[derive(Debug)]
pub enum MetaDirectory {
    /// BOXED because the two arms are wildly different sizes: `MatrixMetaService` holds
    /// around twenty maps, and a client holds a socket and a buffer. Without the box every
    /// directory, including every remote one, is as large as the whole control plane.
    InProcess(Box<MatrixMetaService>),
    Remote(RemoteMetaClient),
}

impl MetaDirectory {
    /// Open the control plane the endpoint list describes.
    ///
    /// An empty list is the in-process arrangement. A non-empty list forms a fixed
    /// catalog of metadata API endpoints. Typed pre-admission refusals select another
    /// voter; transport failures never cause a submitted mutation to be retried.
    pub fn open(
        metaserver_catalog_endpoints: &str,
        options: &MetaClientOptions,
    ) -> Result<Self, ObjectError> {
        let endpoints = crate::parse_catalog_endpoint_list(Some(metaserver_catalog_endpoints))?;
        match endpoints.first() {
            None => Ok(MetaDirectory::InProcess(Box::new(MatrixMetaService::new()))),
            Some(_) => Ok(MetaDirectory::Remote(RemoteMetaClient::connect_catalog(
                &endpoints, options,
            )?)),
        }
    }

    /// An in-process control plane holding a service that already exists.
    ///
    /// For a caller that has one -- the store owns a `MatrixMetaService` by value -- so that
    /// adopting this type does not mean building a second, empty control plane beside the
    /// real one.
    pub fn in_process(meta: MatrixMetaService) -> Self {
        MetaDirectory::InProcess(Box::new(meta))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, MetaDirectory::Remote(_))
    }

    /// Ask the control plane what it is holding, RIGHT NOW.
    ///
    /// For the remote arm this is one round trip, made on every call. A failure produces
    /// `reachable: false` with the transport's own message, and the counts are zero -- not
    /// the counts from a previous successful call, which would be the one shape of this
    /// answer that cannot be trusted.
    pub fn status(&mut self) -> ControlPlaneStatus {
        match self {
            MetaDirectory::InProcess(meta) => ControlPlaneStatus {
                remote: false,
                reachable: true,
                node_count: meta.nodes.len() as u64,
                placement_count: meta.placements.len() as u64,
                commit_index: meta.placement_commit_index,
                detail: String::new(),
            },
            MetaDirectory::Remote(client) => match client.probe() {
                MetaCallOutcome::Answered(MetaReply::Pong {
                    node_count,
                    placement_count,
                    commit_index,
                }) => ControlPlaneStatus {
                    remote: true,
                    reachable: true,
                    node_count,
                    placement_count,
                    commit_index,
                    detail: String::new(),
                },
                MetaCallOutcome::Answered(other) => ControlPlaneStatus {
                    remote: true,
                    reachable: false,
                    node_count: 0,
                    placement_count: 0,
                    commit_index: 0,
                    detail: format!("a ping was answered with {other:?}"),
                },
                MetaCallOutcome::Refused(err) | MetaCallOutcome::Unreachable(err) => {
                    ControlPlaneStatus {
                        remote: true,
                        reachable: false,
                        node_count: 0,
                        placement_count: 0,
                        commit_index: 0,
                        detail: err.to_string(),
                    }
                }
            },
        }
    }

    pub fn heartbeat_placement_node(
        &mut self,
        node_id: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    ) -> Result<PlacementNodeSummary, ObjectError> {
        match self {
            MetaDirectory::InProcess(meta) => {
                let node = meta.heartbeat_node(
                    node_id,
                    used_bytes,
                    capacity_bytes,
                    draining,
                    online,
                    now_ms,
                )?;
                Ok(PlacementNodeSummary::from_placement_node(&node))
            }
            MetaDirectory::Remote(client) => {
                match client.try_call(MetaRequest::HeartbeatRootNode {
                    node_id: node_id.to_string(),
                    used_bytes,
                    capacity_bytes,
                    draining,
                    online,
                    now_ms,
                }) {
                    MetaCallOutcome::Answered(MetaReply::PlacementNode(node)) => Ok(node),
                    outcome => Err(control_plane_error("heartbeat a node", outcome)),
                }
            }
        }
    }

    pub fn expire_placement_node_heartbeats(
        &mut self,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<u64, ObjectError> {
        match self {
            MetaDirectory::InProcess(meta) => meta.expire_node_heartbeats(ttl_ms, now_ms),
            MetaDirectory::Remote(client) => {
                match client.try_call(MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms }) {
                    MetaCallOutcome::Answered(MetaReply::Expired { offline_count }) => {
                        Ok(offline_count)
                    }
                    outcome => Err(control_plane_error("expire node heartbeats", outcome)),
                }
            }
        }
    }

    pub fn list_placement_nodes(&mut self) -> Result<Vec<PlacementNodeSummary>, ObjectError> {
        match self {
            MetaDirectory::InProcess(meta) => Ok(meta
                .nodes
                .values()
                .map(PlacementNodeSummary::from_placement_node)
                .collect()),
            MetaDirectory::Remote(client) => match client.try_call(MetaRequest::ListRootNodes) {
                MetaCallOutcome::Answered(MetaReply::RootNodes(nodes)) => Ok(nodes),
                outcome => Err(control_plane_error("list the roster", outcome)),
            },
        }
    }

    /// Choose nodes for an object.
    ///
    /// THIS IS THE CALL THAT STOPS when the control plane is unreachable, and the one the
    /// module note is about. There is no local fallback, because choosing without the roster
    /// means choosing nodes whose liveness and free capacity are unknown.
    pub fn plan_object_placement(
        &mut self,
        bucket: &str,
        key: &str,
        length: u64,
        replica_count: u64,
    ) -> Result<PlacementAssignment, ObjectError> {
        match self {
            MetaDirectory::InProcess(meta) => {
                let width = usize::try_from(replica_count).map_err(|_| {
                    ObjectError::InvalidArgument(format!(
                        "a placement width of {replica_count} does not fit this build's \
                         address size"
                    ))
                })?;
                let plan = meta.plan_object_placement(
                    crate::cluster::meta_message::object_id(bucket, key),
                    length,
                    width,
                )?;
                Ok(PlacementAssignment::from_plan(&plan))
            }
            MetaDirectory::Remote(client) => {
                match client.try_call(MetaRequest::PlanObjectPlacement {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    length,
                    replica_count,
                }) {
                    MetaCallOutcome::Answered(MetaReply::Plan(plan)) => Ok(plan),
                    outcome => Err(control_plane_error("plan a placement", outcome)),
                }
            }
        }
    }

    pub fn commit_object_placement(
        &mut self,
        placement: CommittedPlacement,
        expected_plan_checksum: &str,
    ) -> Result<CommittedPlacement, ObjectError> {
        match self {
            MetaDirectory::InProcess(meta) => {
                let committed = meta.commit_object_placement(
                    placement.into_placement(),
                    expected_plan_checksum,
                )?;
                Ok(CommittedPlacement::from_placement(&committed))
            }
            MetaDirectory::Remote(client) => {
                match client.try_call(MetaRequest::CommitObjectPlacement {
                    placement,
                    expected_plan_checksum: expected_plan_checksum.to_string(),
                }) {
                    MetaCallOutcome::Answered(MetaReply::Placement(placement)) => Ok(placement),
                    outcome => Err(control_plane_error("commit a placement", outcome)),
                }
            }
        }
    }

    pub fn object_placement(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<CommittedPlacement, ObjectError> {
        match self {
            MetaDirectory::InProcess(meta) => {
                let object_id = crate::cluster::meta_message::object_id(bucket, key);
                let placement = meta.placements.get(&object_id).ok_or_else(|| {
                    ObjectError::NotFound(format!("no placement is committed for {bucket}/{key}"))
                })?;
                Ok(CommittedPlacement::from_placement(placement))
            }
            MetaDirectory::Remote(client) => {
                match client.try_call(MetaRequest::ReadObjectRootPlacement {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                }) {
                    MetaCallOutcome::Answered(MetaReply::Placement(placement)) => Ok(placement),
                    outcome => Err(control_plane_error("read a placement", outcome)),
                }
            }
        }
    }
}

/// Turn a non-answer into an error whose message says which of the three things happened.
///
/// An unreachable control plane and a refusal from a reachable one arrive here as different
/// outcomes and leave as different sentences. Collapsing them would make "no node has room
/// for this object" and "the metaserver process is gone" the same operational event, which
/// they are not: one is a capacity problem and the other is an outage, and they are fixed by
/// different people.
///
/// A `NotFound` from a reachable metaserver passes straight through, so a caller asking
/// whether a placement exists can tell "it does not" from "I could not ask".
fn control_plane_error(doing: &str, outcome: MetaCallOutcome) -> ObjectError {
    match outcome {
        MetaCallOutcome::Unreachable(err) => ObjectError::PreconditionFailed(format!(
            "this store cannot {doing} because its control plane is unreachable: {err}. \
             Reads of objects this store already knows are unaffected -- their extents are \
             held by the store and their bytes by the replica nodes, neither of which is \
             behind the control plane"
        )),
        MetaCallOutcome::Refused(err) => err,
        MetaCallOutcome::Answered(reply) => ObjectError::Corruption(format!(
            "an attempt to {doing} was answered with {reply:?}, which does not belong to \
             that request"
        )),
    }
}
