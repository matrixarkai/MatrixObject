// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Where this store's control plane is, and what the store does without one.
//!
//! # THERE IS NO NEW SETTING HERE, AND THAT IS THE POINT
//!
//! `StoreOptions.metaserver_catalog_endpoints` has existed for a long time. It is parsed by
//! `parse_catalog_endpoint_list`, normalised, validated, hashed into the store snapshot's
//! checksum and written to the journal by
//! `append_metaserver_catalog_endpoints_journal_record` -- and **nothing has ever opened a
//! socket to it**. `discover_metaserver_endpoints_from_catalog` reports a constant,
//! `EMBEDDED_PLACEMENT_CATALOG_ENDPOINT`, because the control plane has always been a field of the
//! store.
//!
//! So this module does not add a placement enum or a mode. The setting that was recorded and
//! never dialled is the setting that is dialled: **an empty endpoint list is the in-process
//! control plane this store has always had, and a non-empty one is a metaserver in another
//! process.** A first draft of this module did invent a `MetaPlacement` enum beside it, which
//! would have left two ways to say the same thing and a reader with no way to tell which one
//! the store believes.
//!
//! `ControlPlaneMode` is likewise untouched. Its two arms, `Auto` and `MetaServer`, both
//! already mean "the metaserver is this store's control plane", and they are hashed into a
//! checksum format; a third arm would be a durable format change to say something the
//! endpoint list already says.
//!
//! # WHAT THE METASERVER OWNS, READ FROM THE CODE RATHER THAN ASSUMED
//!
//! | state | who holds it | lost with the metaserver node? |
//! |---|---|---|
//! | the node roster (`nodes`) | metaserver | **yes** |
//! | object placement onto nodes (`placements`) | metaserver | **yes** |
//! | the leader lease, metadata leases, shards, transactions | metaserver | **yes** |
//! | metadata records (`records`) | metaserver -- **a digest only** | the digest, yes |
//! | **object name to chunk and byte range** (`objects`, each `ObjectMetadata.extents`) | **the store** | **no** |
//! | **which bytes a chunk page holds** | **the replica nodes** | **no** |
//!
//! The fifth row is the one that decides the behaviour below. The authority on what an object
//! *is* -- its length, its digest, and the chunk and byte range of every extent -- is the
//! store's own `objects` collection. The metaserver holds a record carrying a name, two
//! stamps and an eight-byte digest of what that object would render: enough to prove a record
//! and an object agree, and not enough to reconstruct one from the other.
//!
//! So the metaserver is **not** the single holder of the mapping from an object to its bytes.
//! It is the single holder of the roster and of placement.
//!
//! # WHAT HAPPENS WHEN THE METASERVER NODE IS UNREACHABLE
//!
//! Given that table, the answer is not a matter of taste:
//!
//! * **Reads of objects the store already knows keep working.** They consult the metaserver
//!   on no path, so refusing them would refuse work the store can plainly still do.
//! * **New placement stops, and says so.** Choosing nodes for an object needs the roster,
//!   which is on the node that is gone. There is no safe local answer: placing without the
//!   roster means placing onto nodes whose liveness and free capacity are unknown, and
//!   charging `committed_bytes` that the metaserver would have to reconcile later.
//!
//! The refusal is [`ObjectError::PreconditionFailed`] and its message names the control plane
//! rather than the data, so an operator reading only the text is not told the store is damaged
//! when what happened is that one process stopped answering.
//!
//! **This is a single point of failure for placement and for the roster, and saying so plainly
//! is worth more than pretending otherwise.** What makes it survivable rather than fatal is
//! the fifth row: the data plane is not behind it.
//!
//! What recovery would cost, since nothing here is durable yet: `placements` is
//! reconstructible in principle, because a placement says which nodes hold an object and the
//! nodes can be asked what they hold -- at the cost of a full walk of every replica's page
//! map, which is a snapshot transfer per node rather than a query. The roster is **not**
//! recoverable that way: `capacity_bytes`, `failure_domain` and `draining` are configuration
//! an operator supplied, not facts a replica can be asked for, so a rebuilt roster would be
//! missing exactly the fields the placement policy decides on. `MatrixMetaService` already has
//! a durable form -- a `MetaSnapshot` of twenty-seven fields and a checksum, with a decoder
//! that reads seventeen versions -- and this node does not write it. That is the honest cost:
//! the recovery story is a durable log this node does not yet have, not a derivation from the
//! replicas.
//!
//! # REACHABILITY IS A ROUND TRIP, NOT A REMEMBERED FLAG
//!
//! [`MetaDirectory::status`] sends a frame and waits, every time it is called. There is no
//! cached `reachable` on this type, deliberately: a health answer taken from a value the
//! checker last wrote down is a comparison against itself and can never fire, so a node that
//! had stopped talking to its control plane would go on reporting that it had one.
//!
//! One source is still one source. This reports what a round trip just found and nothing
//! more; combining it with the node's own report and a second observer is a later change, and
//! until then `reachable` means "it answered me, now".

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
    /// An empty list is the in-process arrangement. A non-empty one connects to its first
    /// entry; the rest are the membership a later change will elect a leader among, and
    /// dialling only the first is recorded here rather than hidden, because a caller handed
    /// three endpoints and reaching one of them should be able to find that out by reading
    /// this.
    pub fn open(
        metaserver_catalog_endpoints: &str,
        options: &MetaClientOptions,
    ) -> Result<Self, ObjectError> {
        let endpoints = crate::parse_catalog_endpoint_list(Some(metaserver_catalog_endpoints))?;
        match endpoints.first() {
            None => Ok(MetaDirectory::InProcess(Box::new(MatrixMetaService::new()))),
            Some(endpoint) => Ok(MetaDirectory::Remote(RemoteMetaClient::connect(
                endpoint, options,
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
