// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The metaserver in its own process, reached over a socket.
//!
//! # WHAT THIS NODE OWNS, AND WHAT IT DOES NOT
//!
//! It owns one `MatrixMetaService` and nothing else can reach it except through this socket.
//! That service is the store's control plane: the node roster, the placement of objects onto
//! nodes, the leader lease, the metadata records, leases, shards and transactions.
//!
//! It does **not** own the object-name to chunk mapping. That lives in the store's own
//! `objects` collection, and the metaserver holds only an eight-byte digest of what each
//! record would render -- enough to prove a record and an object agree, not enough to
//! rebuild one from the other. The consequence is the thing worth knowing about this node:
//! **losing it does not lose the mapping from an object to its bytes.** See
//! [`crate::cluster::directory`] for what it does lose and what that means for a caller.
//!
//! # NOTHING SURVIVES THE PROCESS
//!
//! This metaserver is held entirely in memory. There is no snapshot path and no write to
//! disk, so a restarted metaserver is an empty one: an empty roster, no placements, and a
//! commit index back at its initial value. `MatrixMetaService` has a durable form already --
//! a `MetaSnapshot` of twenty-eight fields with a checksum -- and this node does not use it.
//! Giving this node a durable log is the next change and not this one; what is asserted here
//! is that the node is a separate process, not that it survives being one.
//!
//! # IT IS UNAUTHENTICATED
//!
//! No signing, no token, no TLS, the same as [`crate::net`] and [`crate::cluster::server`].
//! Anything that can open a socket to this port can register a node, move a placement, or
//! read the whole roster. The shipped binary binds loopback for that reason.

use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cluster::meta_message::{
    encode_meta_reply, CommittedPlacement, MetaReply, MetaRequest, PlacementAssignment,
    PlacementNodeSummary,
};
use crate::cluster::meta_wire::MetaRequestCode;
use crate::cluster::message::{check_stamp, encode_failure};
use crate::cluster::wire::{StatusCode, HEADER_BYTES, MAX_PAYLOAD_BYTES};
use crate::{
    MatrixMetaService, ObjectError, NodeChunkHealth, NodeMetaHealth, PlacementNode,
    NodePageHealth,
};

/// How a metaserver node is sized and bounded.
#[derive(Clone, Debug)]
pub struct MetaNodeOptions {
    /// Largest payload this node will read from a frame.
    ///
    /// A length field arrives before the bytes it describes, so this is the bound on what one
    /// frame can make the node allocate.
    pub max_payload_bytes: usize,
    /// Connections handled at once.
    pub worker_threads: usize,
    /// How long a connection may go without sending before it is dropped.
    pub read_timeout: Duration,
    /// How long a write to a peer may block.
    pub write_timeout: Duration,
}

impl Default for MetaNodeOptions {
    fn default() -> Self {
        Self {
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            // Fewer than the replica node's four. A control-plane call happens once per node
            // per heartbeat interval and once per object creation, not once per page write,
            // so the concurrency a replica needs is capacity this node would hold idle.
            worker_threads: 2,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
        }
    }
}

/// What a node has done since it started.
///
/// Read over the socket by `Ping` rather than only printed, because a count nothing can read
/// back is not an observation -- it is a line in a log.
#[derive(Debug, Default)]
struct MetaCounters {
    requests: AtomicU64,
    failures: AtomicU64,
}

/// A bound listener and the one metaserver behind it.
pub struct MetaNodeServer {
    listener: TcpListener,
    meta: Arc<Mutex<MatrixMetaService>>,
    options: MetaNodeOptions,
    counters: Arc<MetaCounters>,
    stop: Arc<AtomicBool>,
}

impl MetaNodeServer {
    /// Bind `addr` and serve a fresh, empty metaserver.
    ///
    /// There is deliberately no `bind_with_snapshot` taking a `MetaSnapshot`. It was written
    /// and removed before this landed: this node has no durable state to load from, so the
    /// only possible caller is a recovery path that does not exist yet. It belongs in the
    /// change that gives this node something to recover.
    pub fn bind(addr: &str, options: MetaNodeOptions) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        Ok(Self {
            listener,
            meta: Arc::new(Mutex::new(MatrixMetaService::new())),
            options,
            counters: Arc::new(MetaCounters::default()),
            stop: Arc::new(AtomicBool::new(false)),
        })
    }

    /// The address actually bound.
    ///
    /// Asked for rather than assumed, because a node told to bind port 0 does not know its
    /// own port until the kernel has chosen one.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Put `count` data nodes in this metaserver's roster before it starts serving.
    ///
    /// # WHY A SEED AND NOT A REGISTRATION VERB
    ///
    /// Placement needs a roster, and registration is deliberately not one of this service's
    /// verbs: a `PlacementNode` is nineteen fields of which three are health structures carrying
    /// about a hundred fields between them, and the only existing byte encoding for one is
    /// inlined in a durable snapshot format. See [`crate::cluster::meta_wire`].
    ///
    /// So the roster is populated in this process, and this is the stand-in for the
    /// membership a later change will agree between metaserver nodes rather than read from
    /// one node's environment. It is named `seed` so that nothing mistakes it for the real
    /// thing.
    ///
    /// # WHY THESE NODES DIFFER FROM EACH OTHER
    ///
    /// Each seeded node gets its own `failure_domain` and its own allocator shard index. A
    /// roster whose nodes are interchangeable cannot test a placement policy that spreads
    /// across failure domains -- it would pass whatever the policy did -- and the allocator
    /// shard index must be unique or `register_node` refuses the second node outright.
    ///
    /// They are `blockserver`s rather than `chunkserver`s, and that is forced rather than
    /// chosen: the placement policy asks a `chunkserver` for health telemetry it would only
    /// have after a health heartbeat, so a chunkserver seeded with default health is not a
    /// placement candidate. Fail-closed, and the asymmetry belongs in a comment where someone
    /// will read it rather than in a surprise.
    pub fn seed_data_nodes(
        &self,
        count: usize,
        capacity_bytes: u64,
    ) -> Result<usize, ObjectError> {
        let mut guard = self.meta.lock().map_err(|_| {
            ObjectError::Corruption("this metaserver's state is locked".to_string())
        })?;
        for index in 0..count {
            guard.register_node(PlacementNode {
                node_id: format!("data-{index}"),
                endpoint: format!("127.0.0.1:{}", 17_400 + index),
                service_kind: "blockserver".to_string(),
                // ONE DOMAIN PER NODE. The policy prefers one node per failure domain before
                // it backfills, so identical domains would make the first pass a no-op.
                failure_domain: format!("domain-{index}"),
                metadata_eligible: false,
                data_eligible: true,
                gateway_eligible: false,
                serves_page_tier: true,
                metadata_value_bytes: 0,
                capacity_bytes,
                used_bytes: 0,
                committed_bytes: 0,
                draining: false,
                online: true,
                last_heartbeat_ms: 1,
                page_allocator_shard_index: index as u64,
                page_allocator_shard_count: count.max(1) as u64,
                meta_health: NodeMetaHealth::default(),
                page_health: NodePageHealth::default(),
                chunk_health: NodeChunkHealth::default(),
            })?;
        }
        Ok(guard.nodes.len())
    }

    /// A handle that stops the accept loops.
    pub fn shutdown_handle(&self) -> MetaNodeShutdown {
        MetaNodeShutdown {
            stop: Arc::clone(&self.stop),
        }
    }

    /// Accept and serve until the shutdown handle is used.
    pub fn serve(self) -> std::io::Result<()> {
        let worker_threads = self.options.worker_threads.max(1);
        let mut handles = Vec::with_capacity(worker_threads);
        for _ in 0..worker_threads {
            let listener = self.listener.try_clone()?;
            let meta = Arc::clone(&self.meta);
            let counters = Arc::clone(&self.counters);
            let stop = Arc::clone(&self.stop);
            let options = self.options.clone();
            handles.push(std::thread::spawn(move || {
                accept_loop(listener, meta, counters, stop, options);
            }));
        }
        for handle in handles {
            // A worker that panicked has already stopped serving; joining it is how that
            // becomes visible rather than a thread count quietly dropping to zero.
            let _ = handle.join();
        }
        Ok(())
    }
}

/// Stops a running [`MetaNodeServer`].
#[derive(Clone, Debug)]
pub struct MetaNodeShutdown {
    stop: Arc<AtomicBool>,
}

impl MetaNodeShutdown {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

fn accept_loop(
    listener: TcpListener,
    meta: Arc<Mutex<MatrixMetaService>>,
    counters: Arc<MetaCounters>,
    stop: Arc<AtomicBool>,
    options: MetaNodeOptions,
) {
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                let _ = stream.set_read_timeout(Some(options.read_timeout));
                let _ = stream.set_write_timeout(Some(options.write_timeout));
                let _ = stream.set_nodelay(true);
                serve_connection(stream, &meta, &counters, &options);
            }
            Err(_) => {
                // An accept that failed is not a reason to stop accepting -- a single
                // refused connection would otherwise take the control plane down.
                continue;
            }
        }
    }
}

/// Serve one connection until the peer closes it or a frame cannot be read.
///
/// A connection is PERSISTENT: a coordinator opens one socket to the metaserver and sends
/// every request down it, which is what makes the established-socket count a usable
/// statement about the cluster.
fn serve_connection(
    stream: TcpStream,
    meta: &Arc<Mutex<MatrixMetaService>>,
    counters: &Arc<MetaCounters>,
    options: &MetaNodeOptions,
) {
    let mut writer = match stream.try_clone() {
        Ok(writer) => writer,
        Err(_) => return,
    };
    let mut reader = BufReader::new(stream);
    loop {
        let mut header = [0u8; HEADER_BYTES];
        if reader.read_exact(&mut header).is_err() {
            return;
        }
        counters.requests.fetch_add(1, Ordering::Relaxed);
        let (code, payload) = match read_frame(&mut reader, &header, options.max_payload_bytes) {
            Ok(frame) => frame,
            Err(err) => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                let _ = writer.write_all(&encode_failure(
                    StatusCode::for_error(&err),
                    &err.to_string(),
                ));
                let _ = writer.flush();
                // A frame that could not be read leaves the stream at an unknown offset, so
                // there is no honest way to look for the next header in it.
                return;
            }
        };
        let response = match MetaRequestCode::from_u8(code) {
            None => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                // THIS IS THE ANSWER A REPLICA'S FRAME GETS. The metaserver's codes start
                // past every replica code precisely so that a client pointed at the wrong
                // port is told so instead of having its page write applied as a node
                // registration.
                encode_failure(
                    StatusCode::UnknownRequest,
                    &format!("request code {code} is not one this metaserver implements"),
                )
            }
            Some(code) => match MetaRequest::decode(code, &payload) {
                Err(err) => {
                    counters.failures.fetch_add(1, Ordering::Relaxed);
                    encode_failure(StatusCode::for_error(&err), &err.to_string())
                }
                Ok(request) => match apply(meta, request) {
                    Ok(reply) => encode_meta_reply(&reply),
                    Err(err) => {
                        counters.failures.fetch_add(1, Ordering::Relaxed);
                        encode_failure(StatusCode::for_error(&err), &err.to_string())
                    }
                },
            },
        };
        if writer.write_all(&response).is_err() || writer.flush().is_err() {
            return;
        }
    }
}

fn read_frame(
    reader: &mut BufReader<TcpStream>,
    header: &[u8; HEADER_BYTES],
    max_payload_bytes: usize,
) -> Result<(u8, Vec<u8>), ObjectError> {
    check_stamp(header[0])?;
    let code = header[1];
    let payload_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    // CHECKED BEFORE THE ALLOCATION, not after the read. The length is four bytes from a
    // socket; without this the frame chooses how much memory this process asks for.
    if payload_len > max_payload_bytes {
        return Err(ObjectError::InvalidArgument(format!(
            "a metaserver frame declares a {payload_len}-byte payload, past the \
             {max_payload_bytes}-byte bound"
        )));
    }
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).map_err(|err| {
        ObjectError::Corruption(format!(
            "a metaserver frame declared {payload_len} byte(s) and the connection ended \
             first: {err}"
        ))
    })?;
    Ok((code, payload))
}

/// Run one request against the metaserver.
///
/// Every arm is a call into `MatrixMetaService`. Nothing here decides anything about
/// placement or membership -- a refusal from the control plane travels back as its own error,
/// with its own message, so a caller reading the failure learns what the metaserver said and
/// not what this file thought about it.
fn apply(
    meta: &Arc<Mutex<MatrixMetaService>>,
    request: MetaRequest,
) -> Result<MetaReply, ObjectError> {
    let mut guard = meta.lock().map_err(|_| {
        // A poisoned lock means a previous request panicked while holding the control plane,
        // so its state may be half-applied. Refusing is the only honest answer: a roster
        // whose invariants were never re-established would place objects on nodes chosen
        // from it.
        ObjectError::Corruption(
            "this metaserver's state was left locked by a request that panicked, so it can \
             no longer be trusted to answer"
                .to_string(),
        )
    })?;
    match request {
        MetaRequest::HeartbeatRootNode {
            node_id,
            used_bytes,
            capacity_bytes,
            draining,
            online,
            now_ms,
        } => {
            let node = guard.heartbeat_node(
                &node_id,
                used_bytes,
                capacity_bytes,
                draining,
                online,
                now_ms,
            )?;
            Ok(MetaReply::PlacementNode(PlacementNodeSummary::from_placement_node(&node)))
        }
        MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => Ok(MetaReply::Expired {
            offline_count: guard.expire_node_heartbeats(ttl_ms, now_ms)?,
        }),
        MetaRequest::ListRootNodes => Ok(MetaReply::RootNodes(
            guard
                .nodes
                .values()
                .map(PlacementNodeSummary::from_placement_node)
                .collect(),
        )),
        MetaRequest::PlanObjectPlacement {
            bucket,
            key,
            length,
            replica_count,
        } => {
            let width = usize::try_from(replica_count).map_err(|_| {
                ObjectError::InvalidArgument(format!(
                    "a placement width of {replica_count} does not fit this build's \
                     address size"
                ))
            })?;
            let plan = guard.plan_object_placement(
                crate::cluster::meta_message::object_id(&bucket, &key),
                length,
                width,
            )?;
            Ok(MetaReply::Plan(PlacementAssignment::from_plan(&plan)))
        }
        MetaRequest::CommitObjectPlacement {
            placement,
            expected_plan_checksum,
        } => {
            let committed = guard
                .commit_object_placement(placement.into_placement(), &expected_plan_checksum)?;
            Ok(MetaReply::Placement(CommittedPlacement::from_placement(
                &committed,
            )))
        }
        MetaRequest::ReadObjectRootPlacement { bucket, key } => {
            let object_id = crate::cluster::meta_message::object_id(&bucket, &key);
            let placement = guard.placements.get(&object_id).ok_or_else(|| {
                ObjectError::NotFound(format!("no placement is committed for {bucket}/{key}"))
            })?;
            Ok(MetaReply::Placement(CommittedPlacement::from_placement(
                placement,
            )))
        }
        MetaRequest::Ping => Ok(MetaReply::Pong {
            node_count: guard.nodes.len() as u64,
            placement_count: guard.placements.len() as u64,
            commit_index: guard.placement_commit_index,
        }),
    }
}
