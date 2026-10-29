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
//! # RESTART RECOVERY
//!
//! Set `MATRIXOBJECT_METASERVER_STATE_DIR` on the binary, or use
//! `MetaNodeServer::bind_with_snapshot`, to persist and restore the metadata state.
//! Bootstrap saves a snapshot; mutations sync binary protobuf outcomes before acknowledgment. The in-memory bind
//! remains available. This recovers a single node; it does not replicate the control plane.
//!
//! # IT IS UNAUTHENTICATED
//!
//! No signing, no token, no TLS, the same as [`crate::net`] and [`crate::cluster::server`].
//! Anything that can open a socket to this port can register a node, move a placement, or
//! read the whole roster. The shipped binary binds loopback for that reason.

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::meta_snapshot_file::MetaSnapshotFile;
use super::shutdown::ServerShutdown;

struct NodeMeta {
    groups: Arc<super::ChunkGroupCatalog>,
    service: MatrixMetaService,
    storage: Option<MetaSnapshotFile>,
    recovered: bool,
    failed: bool,
    #[cfg(feature = "metadata-consensus")]
    consensus: Option<Arc<super::meta_consensus::MetaConsensus>>,
}

impl std::ops::Deref for NodeMeta {
    type Target = MatrixMetaService;
    fn deref(&self) -> &Self::Target {
        &self.service
    }
}
impl std::ops::DerefMut for NodeMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.service
    }
}
impl NodeMeta {
    fn ensure_healthy(&self) -> Result<(), ObjectError> {
        if self.failed {
            Err(ObjectError::Corruption(
                "metadata persistence failed; restart before serving requests".to_string(),
            ))
        } else {
            Ok(())
        }
    }
    fn persist(&mut self) -> Result<(), ObjectError> {
        if let Some(storage) = &self.storage {
            if let Err(err) = storage.persist(&self.service, &self.groups) {
                self.failed = true;
                return Err(ObjectError::Corruption(format!(
                    "metadata persistence failed: {err}"
                )));
            }
        }
        Ok(())
    }
}

use crate::cluster::meta_message::{
    encode_meta_reply, CommittedPlacement, MetaReply, MetaRequest, PlacementAssignment,
    PlacementNodeSummary,
};
use crate::cluster::meta_wire::MetaRequestCode;
use crate::cluster::wire::{StatusCode, MAX_PAYLOAD_BYTES};
use crate::{
    MatrixMetaService, NodeChunkHealth, NodeMetaHealth, NodePageHealth, ObjectError, PlacementNode,
};

/// One operator-configured chunk endpoint. Seeds start offline until a heartbeat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataNodeSeed {
    pub node_id: String,
    pub endpoint: SocketAddr,
    pub failure_domain: String,
}

impl DataNodeSeed {
    /// Parse comma-separated `node-id@socket-address@failure-domain` entries.
    pub fn parse_roster(raw: &str) -> Result<Vec<Self>, ObjectError> {
        if raw.len() > 1024 * 1024 {
            return Err(ObjectError::InvalidArgument(
                "data roster exceeds size bound".into(),
            ));
        }
        let nodes = raw
            .split(',')
            .map(|entry| {
                let fields: Vec<_> = entry.trim().split('@').collect();
                if fields.len() != 3 {
                    return Err(ObjectError::InvalidArgument(
                        "expected node-id@socket-address@failure-domain".into(),
                    ));
                }
                Ok(Self {
                    node_id: fields[0].into(),
                    endpoint: fields[1].parse().map_err(|_| {
                        ObjectError::InvalidArgument("invalid data node socket address".into())
                    })?,
                    failure_domain: fields[2].into(),
                })
            })
            .collect::<Result<Vec<_>, ObjectError>>()?;
        Self::validated(&nodes)
    }

    fn validated(nodes: &[Self]) -> Result<Vec<Self>, ObjectError> {
        use std::collections::BTreeSet;
        if nodes.is_empty() || nodes.len() > 4096 {
            return Err(ObjectError::InvalidArgument(
                "data roster must contain 1..4096 nodes".into(),
            ));
        }
        let mut nodes = nodes.to_vec();
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for node in &mut nodes {
            let valid_name = |name: &str| {
                !name.is_empty()
                    && name.len() <= 128
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            };
            if let std::net::IpAddr::V6(ip) = node.endpoint.ip() {
                if let Some(ip) = ip.to_ipv4_mapped() {
                    node.endpoint = SocketAddr::new(ip.into(), node.endpoint.port());
                }
            }
            if !valid_name(&node.node_id)
                || !valid_name(&node.failure_domain)
                || node.endpoint.port() == 0
                || node.endpoint.ip().is_unspecified()
                || node.endpoint.ip().is_multicast()
                || !ids.insert(node.node_id.clone())
                || !addresses.insert(node.endpoint)
            {
                return Err(ObjectError::InvalidArgument(
                    "invalid or duplicate data node identity/address".into(),
                ));
            }
        }
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(nodes)
    }
}

/// How a metaserver node is sized and bounded.
#[derive(Clone, Debug)]
pub struct MetaNodeOptions {
    /// Largest payload this node will read from a frame.
    ///
    /// A length field arrives before the bytes it describes, so this is the bound on what one
    /// frame can make the node allocate.
    /// Also bounds outgoing payloads, including failure messages; minimum 24 bytes.
    pub max_payload_bytes: usize,
    /// Request workers; each has a companion that shares up to 128 connections.
    pub worker_threads: usize,
    /// Deadline for receiving a complete frame, including initial idle time.
    pub read_timeout: Duration,
    /// Deadline for sending a complete response.
    pub write_timeout: Duration,
}

impl Default for MetaNodeOptions {
    fn default() -> Self {
        Self {
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            // Idle sockets do not occupy a worker. Each worker shares up to 128 sockets
            // and bounds reserved frame buffers to twice the maximum payload size.
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
    meta: Arc<Mutex<NodeMeta>>,
    options: MetaNodeOptions,
    counters: Arc<MetaCounters>,
    stop: Arc<ServerShutdown>,
    heartbeat_ttl_ms: Option<u64>,
}

impl MetaNodeServer {
    /// Bind `addr` and serve a fresh in-memory metaserver.
    pub fn bind(addr: &str, options: MetaNodeOptions) -> std::io::Result<Self> {
        Self::bind_storage(addr, options, None)
    }

    /// Restore the metadata and placement snapshot from an exclusively locked directory.
    /// The directory must already exist. Mutations are synced before acknowledgment.
    pub fn bind_with_snapshot(
        addr: &str,
        options: MetaNodeOptions,
        directory: &std::path::Path,
    ) -> std::io::Result<Self> {
        Self::bind_storage(addr, options, Some(directory))
    }

    fn bind_storage(
        addr: &str,
        options: MetaNodeOptions,
        directory: Option<&std::path::Path>,
    ) -> std::io::Result<Self> {
        crate::cluster::message::validate_payload_limit(options.max_payload_bytes)?;
        let mut service = MatrixMetaService::new();
        let (storage, recovered) = match directory {
            Some(directory) => {
                let (storage, recovered) = MetaSnapshotFile::open(directory, &mut service)?;
                (Some(storage), recovered)
            }
            None => (None, false),
        };
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            meta: Arc::new(Mutex::new(NodeMeta {
                groups: Arc::new(
                    storage
                        .as_ref()
                        .map(|s| s.groups.clone())
                        .unwrap_or_default(),
                ),
                service,
                storage,
                recovered,
                failed: false,
                #[cfg(feature = "metadata-consensus")]
                consensus: None,
            })),
            options,
            counters: Arc::new(MetaCounters::default()),
            stop: Arc::new(ServerShutdown::default()),
            heartbeat_ttl_ms: None,
        })
    }

    /// Periodically expire stale chunk health through the same durable mutation path.
    /// In consensus mode only the leader can commit expiry outcomes.
    pub fn enable_heartbeat_expiry(&mut self, ttl_ms: u64) -> std::io::Result<()> {
        if !(1000..=3_600_000).contains(&ttl_ms) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "heartbeat TTL must be 1000..3600000 ms",
            ));
        }
        self.heartbeat_ttl_ms = Some(ttl_ms);
        Ok(())
    }

    /// Enable network consensus after seeding and before serving. Local snapshot
    /// storage and replicated storage must use separate directories.
    #[cfg(feature = "metadata-consensus")]
    pub fn enable_raft(&mut self, options: super::MetaRaftOptions) -> std::io::Result<usize> {
        let mut meta = self
            .meta
            .lock()
            .map_err(|_| std::io::Error::other("metadata lock poisoned"))?;
        if meta.storage.is_some() || meta.consensus.is_some() {
            return Err(std::io::Error::other(
                "metadata storage mode already configured",
            ));
        }
        let consensus = super::meta_consensus::MetaConsensus::open(
            options,
            meta.service.clone(),
            (*meta.groups).clone(),
            Arc::clone(&self.stop),
            self.options.max_payload_bytes,
        )?;
        let node_count = consensus.node_count;
        meta.consensus = Some(Arc::new(consensus));
        meta.service = MatrixMetaService::new();
        Ok(node_count)
    }

    /// The address actually bound.
    ///
    /// Asked for rather than assumed, because a node told to bind port 0 does not know its
    /// own port until the kernel has chosen one.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Seed the legacy loopback roster for local development.
    pub fn seed_data_nodes(&self, count: usize, capacity_bytes: u64) -> Result<usize, ObjectError> {
        if count > 4096 {
            return Err(ObjectError::InvalidArgument(
                "data roster exceeds 4096 nodes".into(),
            ));
        }
        let nodes: Vec<_> = (0..count)
            .map(|index| DataNodeSeed {
                node_id: format!("data-{index}"),
                endpoint: SocketAddr::from(([127, 0, 0, 1], 17400 + index as u16)),
                failure_domain: format!("domain-{index}"),
            })
            .collect();
        self.seed_roster(&nodes, capacity_bytes, true)
    }

    /// Install a canonical roster before serving. Configured nodes remain offline
    /// until heartbeats arrive. Recovered storage takes precedence over new seeds.
    pub fn seed_data_roster(
        &self,
        nodes: &[DataNodeSeed],
        capacity_bytes: u64,
    ) -> Result<usize, ObjectError> {
        let nodes = DataNodeSeed::validated(nodes)?;
        if capacity_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "data node capacity must be positive".into(),
            ));
        }
        self.seed_roster(&nodes, capacity_bytes, false)
    }

    fn seed_roster(
        &self,
        nodes: &[DataNodeSeed],
        capacity_bytes: u64,
        online: bool,
    ) -> Result<usize, ObjectError> {
        let mut guard = self.meta.lock().map_err(|_| {
            ObjectError::Corruption("this metaserver's state is locked".to_string())
        })?;
        guard.ensure_healthy()?;
        #[cfg(feature = "metadata-consensus")]
        if guard.consensus.is_some() {
            return Err(ObjectError::InvalidArgument(
                "seed before enabling consensus".into(),
            ));
        }
        if guard.recovered {
            return Ok(guard.nodes.len());
        }
        if !guard.nodes.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "data roster is already initialized".into(),
            ));
        }
        let mut staged = guard.service.clone();
        for (index, node) in nodes.iter().enumerate() {
            staged.register_node(PlacementNode {
                node_id: node.node_id.clone(),
                endpoint: node.endpoint.to_string(),
                service_kind: "chunkserver".to_string(),
                failure_domain: node.failure_domain.clone(),
                metadata_eligible: false,
                data_eligible: true,
                gateway_eligible: false,
                serves_page_tier: true,
                metadata_value_bytes: 0,
                capacity_bytes,
                used_bytes: 0,
                committed_bytes: 0,
                draining: false,
                online,
                last_heartbeat_ms: u64::from(online),
                page_allocator_shard_index: index as u64,
                page_allocator_shard_count: nodes.len().max(1) as u64,
                meta_health: NodeMetaHealth::default(),
                page_health: NodePageHealth::default(),
                chunk_health: NodeChunkHealth::default(),
            })?;
        }
        guard.service = staged;
        guard.persist()?;
        guard.recovered = guard.storage.is_some();
        Ok(guard.nodes.len())
    }

    /// Bootstrap logical Raft groups and their non-placeable health members.
    /// Persisted membership wins over later startup configuration.
    pub fn seed_chunk_groups(
        &self,
        catalog: super::ChunkGroupCatalog,
    ) -> Result<usize, ObjectError> {
        let mut guard = self
            .meta
            .lock()
            .map_err(|_| ObjectError::Corruption("metadata state locked".into()))?;
        guard.ensure_healthy()?;
        #[cfg(feature = "metadata-consensus")]
        if guard.consensus.is_some() {
            return Err(ObjectError::InvalidArgument(
                "seed before enabling consensus".into(),
            ));
        }
        if guard.recovered {
            return Ok(guard.nodes.len());
        }
        if !guard.nodes.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "data roster is already initialized".into(),
            ));
        }
        let count: usize = catalog.groups().map(|g| 1 + g.member_ids.len()).sum();
        let mut staged = guard.service.clone();
        let mut index = 0;
        for group in catalog.groups() {
            let capacity = group.identity.as_ref().unwrap().capacity_bytes;
            for (id, endpoint, eligible) in
                std::iter::once((&group.placement_id, &group.api_endpoints[0], true)).chain(
                    group
                        .member_ids
                        .iter()
                        .zip(&group.api_endpoints)
                        .map(|(id, ep)| (id, ep, false)),
                )
            {
                staged.register_node(PlacementNode {
                    node_id: id.clone(),
                    endpoint: endpoint.clone(),
                    service_kind: "chunkserver".into(),
                    failure_domain: group.failure_domain.clone(),
                    metadata_eligible: false,
                    data_eligible: eligible,
                    gateway_eligible: false,
                    serves_page_tier: true,
                    metadata_value_bytes: 0,
                    capacity_bytes: capacity,
                    used_bytes: 0,
                    committed_bytes: 0,
                    draining: false,
                    online: false,
                    last_heartbeat_ms: 0,
                    page_allocator_shard_index: index,
                    page_allocator_shard_count: count.max(1) as u64,
                    meta_health: NodeMetaHealth::default(),
                    page_health: NodePageHealth::default(),
                    chunk_health: NodeChunkHealth::default(),
                })?;
                index += 1;
            }
        }
        catalog.validate_roster(&staged)?;
        guard.service = staged;
        guard.groups = Arc::new(catalog);
        guard.persist()?;
        guard.recovered = guard.storage.is_some();
        Ok(count)
    }

    /// A handle that stops accept loops and closes active connections.
    pub fn shutdown_handle(&self) -> MetaNodeShutdown {
        MetaNodeShutdown {
            stop: Arc::clone(&self.stop),
        }
    }

    /// Accept and serve until the shutdown handle is used.
    pub fn serve(self) -> std::io::Result<()> {
        let worker_threads = self.options.worker_threads.max(1);
        let dispatch = super::connections::Dispatch::new(worker_threads);
        let mut handles = Vec::with_capacity(worker_threads);
        for _ in 0..worker_threads {
            let dispatch = dispatch.clone();
            let listener = self.listener.try_clone()?;
            let meta = Arc::clone(&self.meta);
            let counters = Arc::clone(&self.counters);
            let stop = Arc::clone(&self.stop);
            let options = self.options.clone();
            handles.push(std::thread::spawn(move || {
                accept_loop(listener, meta, counters, stop, options, dispatch);
            }));
        }
        if let Some(ttl_ms) = self.heartbeat_ttl_ms {
            let meta = Arc::clone(&self.meta);
            let stop = Arc::clone(&self.stop);
            let limit = self.options.max_payload_bytes;
            handles.push(std::thread::spawn(move || {
                let interval = Duration::from_millis((ttl_ms / 3).min(1000));
                while !stop.is_stopped() {
                    if let Ok(elapsed) =
                        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                    {
                        if let Ok(now_ms) = u64::try_from(elapsed.as_millis()) {
                            let _ = apply(
                                &meta,
                                MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms },
                                limit,
                            );
                        }
                    }
                    if stop.wait_for(interval) {
                        break;
                    }
                }
            }));
        }
        for handle in handles {
            // A worker that panicked has already stopped serving; joining it is how that
            // becomes visible rather than a thread count quietly dropping to zero.
            let _ = handle.join();
        }
        #[cfg(feature = "metadata-consensus")]
        if self
            .meta
            .lock()
            .map_err(|_| std::io::Error::other("metadata lock poisoned"))?
            .consensus
            .as_ref()
            .is_some_and(|c| c.is_failed())
        {
            return Err(std::io::Error::other("metadata consensus worker failed"));
        }
        Ok(())
    }
}

/// Stops a running [`MetaNodeServer`].
#[derive(Clone, Debug)]
pub struct MetaNodeShutdown {
    stop: Arc<ServerShutdown>,
}

impl MetaNodeShutdown {
    pub fn stop(&self) {
        self.stop.stop();
    }
}

fn accept_loop(
    listener: TcpListener,
    meta: Arc<Mutex<NodeMeta>>,
    counters: Arc<MetaCounters>,
    stop: Arc<ServerShutdown>,
    options: MetaNodeOptions,
    dispatch: super::connections::Dispatch,
) {
    super::connections::accept_loop(
        listener,
        &counters.requests,
        &counters.failures,
        |code, payload| frame_response(code, payload, &meta, &counters, &options),
        stop,
        super::connections::Options {
            max_payload_bytes: options.max_payload_bytes,
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
        },
        dispatch,
    );
}

fn frame_response(
    code: u8,
    payload: &[u8],
    meta: &Arc<Mutex<NodeMeta>>,
    counters: &Arc<MetaCounters>,
    options: &MetaNodeOptions,
) -> Vec<u8> {
    let encode_failure = |status, message: &str| {
        crate::cluster::message::encode_bounded_failure(status, message, options.max_payload_bytes)
    };
    match MetaRequestCode::from_u8(code) {
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
        Some(code) => match MetaRequest::decode(code, payload) {
            Err(err) => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                encode_failure(StatusCode::for_error(&err), &err.to_string())
            }
            Ok(request) => {
                match apply(meta, request, options.max_payload_bytes).and_then(|reply| {
                    crate::cluster::message::check_reply_size(
                        reply.payload_len(),
                        options.max_payload_bytes,
                    )?;
                    Ok(reply)
                }) {
                    Ok(reply) => encode_meta_reply(&reply),
                    Err(err) => {
                        counters.failures.fetch_add(1, Ordering::Relaxed);
                        encode_failure(StatusCode::for_error(&err), &err.to_string())
                    }
                }
            }
        },
    }
}

/// Run one request against the metaserver.
///
/// Every arm is a call into `MatrixMetaService`. Nothing here decides anything about
/// placement or membership -- a refusal from the control plane travels back as its own error,
/// with its own message, so a caller reading the failure learns what the metaserver said and
/// not what this file thought about it.
fn apply(
    meta: &Arc<Mutex<NodeMeta>>,
    request: MetaRequest,
    max_payload_bytes: usize,
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
    guard.ensure_healthy()?;
    #[cfg(feature = "metadata-consensus")]
    if let Some(consensus) = guard.consensus.clone() {
        drop(guard);
        return consensus.request(request);
    }
    let request = super::health_report::stamp(request)?;
    let decision = match &request {
        MetaRequest::AllocateObjectPages { page_count } => {
            if guard.storage.is_none() {
                return Err(ObjectError::PreconditionFailed(
                    "object allocation requires durable metadata storage".into(),
                ));
            }
            Some(decide_allocation(
                &guard.service,
                *page_count,
                max_payload_bytes,
            )?)
        }
        MetaRequest::PublishManifest {
            outcome,
            update_time_ms,
        } => {
            guard.groups.check_request(&request)?;
            Some(decide_manifest(
                &guard.service,
                outcome.clone(),
                *update_time_ms,
                max_payload_bytes,
            )?)
        }
        _ => None,
    };
    if let Some((decided, reply)) = decision {
        if guard.storage.is_some() {
            // Preflight the local snapshot bound before persisting an outcome.
            if decided.manifest.is_some() {
                let mut preview = guard.service.clone();
                decided.apply(&mut preview)?;
                if super::chunk_groups::encode_bootstrap(&preview, &guard.groups)?.len()
                    > 64 * 1024 * 1024
                {
                    return Err(ObjectError::InvalidArgument(
                        "metadata snapshot exceeds 64 MiB".into(),
                    ));
                }
            }
            if !guard.recovered {
                guard.persist()?;
                guard.recovered = true;
            }
            if let Err(error) = guard.storage.as_mut().unwrap().append_outcome(&decided) {
                guard.failed = true;
                return Err(ObjectError::Corruption(error.to_string()));
            }
        }
        if let Err(error) = decided.apply(&mut guard.service) {
            guard.failed = true;
            return Err(error);
        }
        return Ok(reply);
    }
    if let MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } = &request {
        if expired_node_ids(&guard.service, *ttl_ms, *now_ms)?.is_empty() {
            return Ok(MetaReply::Expired { offline_count: 0 });
        }
    }
    let mutating = matches!(
        request,
        MetaRequest::HeartbeatRootNode { .. }
            | MetaRequest::HeartbeatChunkNode { .. }
            | MetaRequest::ExpireRootNodeHeartbeats { .. }
            | MetaRequest::CommitObjectPlacement { .. }
            | MetaRequest::CommitObjectPlacementIfCurrent { .. }
    );
    guard.groups.check_request(&request)?;
    let outcome = if mutating && guard.storage.is_some() {
        if !guard.recovered {
            guard.persist()?;
            guard.recovered = true;
        }
        let (nodes, object) = match &request {
            MetaRequest::HeartbeatRootNode { node_id, .. }
            | MetaRequest::HeartbeatChunkNode { node_id, .. } => (vec![node_id.clone()], None),
            MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => {
                (expired_node_ids(&guard.service, *ttl_ms, *now_ms)?, None)
            }
            MetaRequest::CommitObjectPlacement { placement, .. }
            | MetaRequest::CommitObjectPlacementIfCurrent { placement, .. } => {
                let object =
                    crate::cluster::meta_message::object_id(&placement.bucket, &placement.key);
                let mut nodes = placement.node_ids.clone();
                if let Some(previous) = guard.placements.get(&object) {
                    nodes.extend(previous.node_ids.iter().cloned());
                }
                (nodes, Some(object))
            }
            _ => unreachable!(),
        };
        Some(crate::placement_outcome::PlacementOutcome::capture(
            &guard.service,
            guard.groups.affected_nodes(nodes),
            object,
        )?)
    } else {
        None
    };
    let groups = guard.groups.clone();
    let reply = apply_with_groups(&mut guard.service, request, max_payload_bytes, &groups)?;
    if let Some(outcome) = outcome {
        let persisted = outcome.finish(&guard.service).and_then(|outcome| {
            if outcome.before_index == outcome.after_index {
                return Ok(());
            }
            guard
                .storage
                .as_mut()
                .unwrap()
                .append_outcome(&outcome)
                .map_err(|err| ObjectError::Corruption(err.to_string()))
        });
        if let Err(err) = persisted {
            guard.failed = true;
            return Err(err);
        }
    }
    Ok(reply)
}

/// Restrict an expiry outcome to the nodes whose state actually changes.
pub(super) fn expired_node_ids(
    service: &MatrixMetaService,
    ttl_ms: u64,
    now_ms: u64,
) -> Result<Vec<String>, ObjectError> {
    if ttl_ms == 0 {
        return Err(ObjectError::InvalidArgument(
            "root heartbeat expiry requires ttl_ms".into(),
        ));
    }
    Ok(service
        .nodes
        .values()
        .filter(|node| {
            node.online
                && node.last_heartbeat_ms != 0
                && now_ms > node.last_heartbeat_ms
                && now_ms.saturating_sub(node.last_heartbeat_ms) > ttl_ms
        })
        .map(|node| node.node_id.clone())
        .collect())
}

pub(super) fn decide_allocation(
    service: &MatrixMetaService,
    page_count: u32,
    limit: usize,
) -> Result<(crate::placement_outcome::PlacementOutcome, MetaReply), ObjectError> {
    let allocation_id = service
        .placement_commit_index
        .checked_add(1)
        .ok_or_else(|| {
            ObjectError::PreconditionFailed("object allocation sequence exhausted".into())
        })?;
    let allocation = crate::object_manifest::ObjectPageAllocation {
        format_version: 1,
        allocation_id,
        page_count,
    };
    allocation.validate()?;
    let reply = MetaReply::ObjectPages(allocation);
    crate::cluster::message::check_reply_size(reply.payload_len(), limit)?;
    let outcome = crate::placement_outcome::PlacementOutcome {
        version: 1,
        before_index: service.placement_commit_index,
        after_index: allocation_id,
        nodes: vec![],
        placements: vec![],
        manifest: None,
    };
    outcome.to_payload()?;
    Ok((outcome, reply))
}

pub(super) fn decide_manifest(
    service: &MatrixMetaService,
    outcome: crate::object_manifest::ManifestOutcome,
    update_time_ms: u64,
    limit: usize,
) -> Result<(crate::placement_outcome::PlacementOutcome, MetaReply), ObjectError> {
    let decided = crate::placement_outcome::PlacementOutcome::publish_manifest(
        service,
        outcome,
        update_time_ms,
    )?;
    let reply = MetaReply::Manifest(decided.manifest.as_ref().unwrap().after.clone().unwrap());
    crate::cluster::message::check_reply_size(reply.payload_len(), limit)?;
    Ok((decided, reply))
}

pub(super) fn apply_with_groups(
    service: &mut MatrixMetaService,
    request: MetaRequest,
    limit: usize,
    groups: &super::ChunkGroupCatalog,
) -> Result<MetaReply, ObjectError> {
    groups.check_request(&request)?;
    if let MetaRequest::AllocateObjectPages { page_count } = &request {
        let (decided, reply) = decide_allocation(service, *page_count, limit)?;
        decided.apply(service)?;
        return Ok(reply);
    }
    if let MetaRequest::PublishManifest {
        outcome,
        update_time_ms,
    } = &request
    {
        let (decided, reply) = decide_manifest(service, outcome.clone(), *update_time_ms, limit)?;
        decided.apply(service)?;
        return Ok(reply);
    }

    if let MetaRequest::ReadChunkGroup { placement_id } = &request {
        let reply = groups
            .get(placement_id)
            .cloned()
            .map(MetaReply::ChunkGroup)
            .ok_or_else(|| ObjectError::NotFound("chunk group missing".into()))?;
        crate::cluster::message::check_reply_size(reply.payload_len(), limit)?;
        return Ok(reply);
    }
    let affected = match &request {
        MetaRequest::HeartbeatRootNode { node_id, .. }
        | MetaRequest::HeartbeatChunkNode { node_id, .. } => vec![node_id.clone()],
        MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => {
            expired_node_ids(service, *ttl_ms, *now_ms)?
        }
        _ => Vec::new(),
    };
    let reply = apply_to_meta(service, request, limit)?;
    if !affected.is_empty() {
        groups.project(service, &affected);
    }
    Ok(reply)
}

pub(super) fn apply_to_meta(
    guard: &mut MatrixMetaService,
    request: MetaRequest,
    max_payload_bytes: usize,
) -> Result<MetaReply, ObjectError> {
    match request {
        MetaRequest::AllocateObjectPages { .. } => Err(ObjectError::InvalidArgument(
            "allocation requires durable dispatch".into(),
        )),
        MetaRequest::PublishManifest { .. } => Err(ObjectError::InvalidArgument(
            "publication requires a chunk catalog".into(),
        )),
        MetaRequest::ReadManifest { bucket, key } => {
            crate::validate_bucket(&bucket)?;
            crate::validate_key(&key)?;
            let value = guard
                .object_manifests
                .get(&super::meta_message::object_id(&bucket, &key))
                .cloned()
                .ok_or_else(|| ObjectError::NotFound("object manifest missing".into()))?;
            let reply = MetaReply::Manifest(value);
            crate::cluster::message::check_reply_size(reply.payload_len(), max_payload_bytes)?;
            Ok(reply)
        }

        MetaRequest::ReadChunkGroup { .. } => {
            Err(ObjectError::NotFound("chunk group missing".into()))
        }
        MetaRequest::ReportChunkNode { .. } => Err(ObjectError::PreconditionFailed(
            "chunk health report requires a decided timestamp".into(),
        )),
        MetaRequest::HeartbeatChunkNode {
            node_id,
            expected_endpoint,
            used_bytes,
            capacity_bytes,
            online,
            now_ms,
        } => {
            let node = guard
                .nodes
                .get(&node_id)
                .ok_or_else(|| ObjectError::NotFound("chunk heartbeat node missing".into()))?;
            if node.endpoint != expected_endpoint || node.service_kind != "chunkserver" {
                return Err(ObjectError::PreconditionFailed(
                    "chunk heartbeat endpoint does not match membership".into(),
                ));
            }
            let draining = node.draining;
            apply_to_meta(
                guard,
                MetaRequest::HeartbeatRootNode {
                    node_id,
                    used_bytes,
                    capacity_bytes,
                    draining,
                    online,
                    now_ms,
                },
                max_payload_bytes,
            )
        }
        MetaRequest::HeartbeatRootNode {
            node_id,
            used_bytes,
            capacity_bytes,
            draining,
            online,
            now_ms,
        } => {
            if let Some(node) = guard.nodes.get(&node_id) {
                let size = crate::cluster::meta_message::node_payload_len(
                    &node.node_id,
                    &node.endpoint,
                    &node.service_kind,
                    &node.failure_domain,
                );
                crate::cluster::message::check_reply_size(size, max_payload_bytes)?;
            }
            let node = guard.heartbeat_node(
                &node_id,
                used_bytes,
                capacity_bytes,
                draining,
                online,
                now_ms,
            )?;
            Ok(MetaReply::PlacementNode(
                PlacementNodeSummary::from_placement_node(&node),
            ))
        }
        MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => Ok(MetaReply::Expired {
            offline_count: guard.expire_node_heartbeats(ttl_ms, now_ms)?,
        }),
        MetaRequest::ListRootNodes => {
            let size = guard.nodes.values().fold(8usize, |n, node| {
                n.saturating_add(crate::cluster::meta_message::node_payload_len(
                    &node.node_id,
                    &node.endpoint,
                    &node.service_kind,
                    &node.failure_domain,
                ))
            });
            crate::cluster::message::check_reply_size(size, max_payload_bytes)?;
            Ok(MetaReply::RootNodes(
                guard
                    .nodes
                    .values()
                    .map(PlacementNodeSummary::from_placement_node)
                    .collect(),
            ))
        }
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
        MetaRequest::GroupIdentity => Err(ObjectError::InvalidArgument(
            "metadata node has no Raft group".into(),
        )),
        MetaRequest::CommitObjectPlacementIfCurrent {
            placement,
            expected,
            expected_plan_checksum,
        } => {
            if placement.version == 0
                || placement.node_ids.is_empty()
                || placement
                    .node_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != placement.node_ids.len()
                || expected.as_ref().is_some_and(|old| {
                    old.bucket != placement.bucket
                        || old.key != placement.key
                        || old.version >= placement.version
                })
            {
                return Err(ObjectError::InvalidArgument(
                    "invalid conditional placement identity or version".into(),
                ));
            }
            let current = guard
                .placements
                .get(&crate::cluster::meta_message::object_id(
                    &placement.bucket,
                    &placement.key,
                ))
                .map(CommittedPlacement::from_placement);
            if current != expected {
                return Err(ObjectError::PreconditionFailed(
                    "object placement changed".into(),
                ));
            }
            apply_to_meta(
                guard,
                MetaRequest::CommitObjectPlacement {
                    placement,
                    expected_plan_checksum,
                },
                max_payload_bytes,
            )
        }
        MetaRequest::CommitObjectPlacement {
            placement,
            expected_plan_checksum,
        } => {
            crate::cluster::message::check_reply_size(
                crate::cluster::meta_message::placement_payload_len(&placement),
                max_payload_bytes,
            )?;
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

#[cfg(test)]
mod chunk_group_tests {
    #[test]
    fn received_health_local_journal_keeps_timestamp_after_restart() {
        use super::{apply, MetaReply, MetaRequest};
        use crate::cluster::{DataNodeSeed, MetaNodeOptions, MetaNodeServer};
        let directory = std::env::temp_dir().join(format!(
            "health-clock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let server = MetaNodeServer::bind_with_snapshot(
            "127.0.0.1:0",
            MetaNodeOptions::default(),
            &directory,
        )
        .unwrap();
        server
            .seed_data_roster(
                &DataNodeSeed::parse_roster("chunk-1@127.0.0.1:9100@rack").unwrap(),
                1024,
            )
            .unwrap();
        let request = MetaRequest::ReportChunkNode {
            node_id: "chunk-1".into(),
            expected_endpoint: "127.0.0.1:9100".into(),
            used_bytes: 17,
            capacity_bytes: 1024,
            online: true,
        };
        let MetaReply::PlacementNode(reported) = apply(&server.meta, request, 65536).unwrap()
        else {
            panic!("missing report")
        };
        assert!(reported.last_heartbeat_ms > 0);
        drop(server);
        let recovered = MetaNodeServer::bind_with_snapshot(
            "127.0.0.1:0",
            MetaNodeOptions::default(),
            &directory,
        )
        .unwrap();
        let MetaReply::RootNodes(nodes) =
            apply(&recovered.meta, MetaRequest::ListRootNodes, 65536).unwrap()
        else {
            panic!("missing roster")
        };
        assert_eq!(nodes, vec![reported]);
        drop(recovered);
        std::fs::remove_dir_all(directory).unwrap();
    }

    use super::*;
    use crate::cluster::{chunk_groups::tests::config, ChunkGroupCatalog};
    #[test]
    fn group_catalog_and_projected_outcomes_survive_disk_recovery() {
        let dir = std::env::temp_dir().join(format!(
            "group-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let catalog = ChunkGroupCatalog::new(vec![config()]).unwrap();
        let server =
            MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir)
                .unwrap();
        server.seed_chunk_groups(catalog.clone()).unwrap();
        for (id, used) in [("voter-a", 100), ("voter-b", 200)] {
            apply(
                &server.meta,
                MetaRequest::HeartbeatRootNode {
                    node_id: id.into(),
                    used_bytes: used,
                    capacity_bytes: 4096,
                    draining: false,
                    online: true,
                    now_ms: 1000,
                },
                65536,
            )
            .unwrap();
        }
        let expected = server
            .meta
            .lock()
            .unwrap()
            .service
            .export_snapshot()
            .to_bytes()
            .unwrap();
        drop(server);
        assert!(std::fs::read(dir.join("metadata.snapshot"))
            .unwrap()
            .starts_with(b"MOGB"));
        let outcomes = std::fs::read(dir.join("metadata.outcomes")).unwrap();
        assert!(outcomes.starts_with(b"MOMJ"));
        assert!(outcomes.windows(4).any(|w| w == b"MOMP"));
        let restored =
            MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir)
                .unwrap();
        restored
            .seed_chunk_groups(ChunkGroupCatalog::default())
            .unwrap();
        {
            let state = restored.meta.lock().unwrap();
            assert_eq!(
                state.groups.to_bytes().unwrap(),
                catalog.to_bytes().unwrap()
            );
            assert_eq!(
                state.service.export_snapshot().to_bytes().unwrap(),
                expected
            );
            assert!(state.service.nodes["group-a"].online);
            assert_eq!(state.service.nodes["group-a"].used_bytes, 200);
        }
        drop(restored);
        let mut bytes = std::fs::read(dir.join("metadata.snapshot")).unwrap();
        bytes[10] ^= 1;
        std::fs::write(dir.join("metadata.snapshot"), bytes).unwrap();
        assert!(MetaNodeServer::bind_with_snapshot(
            "127.0.0.1:0",
            MetaNodeOptions::default(),
            &dir
        )
        .is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn group_health_counts_one_capacity_and_requires_majority() {
        let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
        let catalog = ChunkGroupCatalog::new(vec![config()]).unwrap();
        assert_eq!(server.seed_chunk_groups(catalog.clone()).unwrap(), 4);
        let mut state = server.meta.lock().unwrap();
        assert_eq!(
            state
                .service
                .nodes
                .values()
                .filter(|n| n.data_eligible)
                .count(),
            1
        );
        assert!(!state.service.nodes["group-a"].online);
        let heartbeat = |id: &str, used, now| MetaRequest::HeartbeatRootNode {
            node_id: id.into(),
            used_bytes: used,
            capacity_bytes: 4096,
            draining: false,
            online: true,
            now_ms: now,
        };
        apply_with_groups(
            &mut state.service,
            heartbeat("voter-a", 100, 1000),
            65536,
            &catalog,
        )
        .unwrap();
        assert!(!state.service.nodes["group-a"].online);
        apply_with_groups(
            &mut state.service,
            heartbeat("voter-b", 200, 1100),
            65536,
            &catalog,
        )
        .unwrap();
        let group = &state.service.nodes["group-a"];
        assert!(group.online);
        assert_eq!(group.used_bytes, 200);
        assert_eq!(group.capacity_bytes, 4096);
        assert_eq!(group.last_heartbeat_ms, 1000);
        let before = state.service.export_snapshot().to_bytes().unwrap();
        assert!(apply_with_groups(
            &mut state.service,
            heartbeat("group-a", 0, 1200),
            65536,
            &catalog
        )
        .is_err());
        assert_eq!(state.service.export_snapshot().to_bytes().unwrap(), before);
        apply_with_groups(
            &mut state.service,
            MetaRequest::ExpireRootNodeHeartbeats {
                ttl_ms: 1000,
                now_ms: 2050,
            },
            65536,
            &catalog,
        )
        .unwrap();
        assert!(!state.service.nodes["group-a"].online);
        let bytes =
            crate::cluster::chunk_groups::encode_bootstrap(&state.service, &catalog).unwrap();
        let (snapshot, restored) = crate::cluster::chunk_groups::decode_bootstrap(&bytes).unwrap();
        assert_eq!(
            snapshot,
            state.service.export_snapshot().to_bytes().unwrap()
        );
        assert_eq!(restored.to_bytes().unwrap(), catalog.to_bytes().unwrap());
        let mut corrupt = bytes;
        corrupt[10] ^= 1;
        assert!(crate::cluster::chunk_groups::decode_bootstrap(&corrupt).is_err());
        assert!(apply_with_groups(
            &mut state.service,
            MetaRequest::ReadChunkGroup {
                placement_id: "group-a".into()
            },
            1,
            &catalog
        )
        .is_err());
    }
}

#[cfg(all(test, feature = "metadata-consensus"))]
#[path = "meta_catalog_recovery_tests.rs"]
mod catalog_recovery_tests;
