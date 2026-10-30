// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! One durable metadata voter with bounded TCP delivery and committed read barriers.
use super::meta_message::{MetaReply, MetaRequest};
use super::meta_server::apply_with_groups;
use super::shutdown::ServerShutdown;
#[cfg(test)]
use crate::cluster::consensus_checkpoint::Image;
use crate::cluster::consensus_checkpoint::{storage, Policy};
use crate::cluster::consensus_inbox::{
    ConsensusChannels, ConsensusInbox, RequestPhase, RequestProgress, WorkerPhase, WorkerProgress,
};
use crate::cluster::socket_readiness::Notify;
use crate::placement_outcome::PlacementOutcome;
use crate::{MatrixMetaService, MetaSnapshot, ObjectError};
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions},
    LogId, RaftError,
};
use prost::Message;
use std::cell::{Cell, RefCell};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
const PACKET_LIMIT: usize = 8 * 1024 * 1024;
const PROPOSALS_PER_BATCH: usize = 8;
const TICK: Duration = Duration::from_millis(100);
const NETWORK_TIMEOUT: Duration = Duration::from_millis(100);

/// Fixed voter endpoints. Each process uses its own existing state directory.
#[derive(Clone, Debug)]
pub struct MetaRaftOptions {
    /// Opt in to membership changes. peers then contains all permitted endpoints.
    pub bootstrap_voters: Option<Vec<u64>>,
    /// Mutual TLS for peer ports; this does not protect client API ports.
    pub peer_tls: Option<crate::cluster::PeerTlsOptions>,
    pub node_id: u64,
    pub group_id: u64,
    pub peers: BTreeMap<u64, SocketAddr>,
    pub directory: PathBuf,
    pub request_timeout: Duration,
    /// Reclaim this many newly persisted WAL bytes; zero disables capture.
    /// Enable only after every voter supports application checkpoint restoration.
    pub checkpoint_after_bytes: u64,
}
#[derive(Clone, PartialEq, Message)]
struct Entry {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    outcome: Vec<u8>,
}
struct Request {
    request: MetaRequest,
    reply: SyncSender<Result<MetaReply, ObjectError>>,
    deadline: Instant,
    progress: RequestProgress,
}
struct Pending {
    id: LogId,
    request: Request,
    reply: Option<Result<MetaReply, ObjectError>>,
}
pub(super) struct MetaConsensus {
    progress: WorkerProgress,
    pub(super) node_count: usize,
    identity: super::MetaGroupIdentity,
    reply_limit: usize,
    requests: SyncSender<Request>,
    wake: Option<Notify>,
    timeout: Duration,
    stop: Arc<ServerShutdown>,
    failed: Arc<AtomicBool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}
fn invalid(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}
fn refused(s: impl ToString) -> ObjectError {
    ObjectError::InvalidArgument(s.to_string())
}
// A failed read confirmation has not executed the application request. Only
// typed leadership loss allows catalog routing to another voter; other failures
// stay authoritative. Never use this conversion for accepted writes.
fn read_failure(error: &RaftError) -> Result<MetaReply, ObjectError> {
    match error {
        RaftError::NotLeader(_) => Ok(MetaReply::NotLeader),
        _ => Err(refused(error)),
    }
}
fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}
impl MetaConsensus {
    pub(super) fn open(
        options: MetaRaftOptions,
        mut service: MatrixMetaService,
        groups: super::ChunkGroupCatalog,
        stop: Arc<ServerShutdown>,
        limit: usize,
    ) -> io::Result<Self> {
        if options.group_id == 0
            || options.node_id == 0
            || options.peers.is_empty()
            || options.peers.len()
                > if options.bootstrap_voters.is_some() {
                    64
                } else {
                    7
                }
            || options.peers.keys().any(|id| *id == 0)
            || !options.peers.contains_key(&options.node_id)
            || options.request_timeout < Duration::from_millis(100)
            || options.request_timeout > Duration::from_secs(30)
            || options.peers.values().any(|a| a.port() == 0)
            || options
                .peers
                .values()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != options.peers.len()
        {
            return Err(invalid("invalid metadata voter configuration"));
        }
        let voters = options
            .bootstrap_voters
            .clone()
            .unwrap_or_else(|| options.peers.keys().copied().collect());
        if voters.is_empty()
            || voters.len() > 7
            || voters.windows(2).any(|w| w[0] >= w[1])
            || voters.iter().any(|id| !options.peers.contains_key(id))
        {
            return Err(invalid("invalid metadata bootstrap voters"));
        }
        let transport = crate::cluster::peer_tls::PeerTransport::load(options.peer_tls.as_ref())?;
        let directory = options.directory.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("metadata.peer.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        transport.pin_directory(&directory)?;
        if directory.join("metadata.snapshot").exists()
            || directory.join("metadata.outcomes").exists()
        {
            return Err(invalid(
                "local metadata files cannot be opened as a Raft group",
            ));
        }
        let base = directory.join("metadata.peer.base");
        let wal = directory.join("metadata.peer.wal");
        let checkpoint =
            Policy::new(1, options.checkpoint_after_bytes).map_err(invalid)?;
        if base.exists() != wal.exists() {
            return Err(invalid("incomplete metadata peer storage"));
        }
        if base.exists() && wal.metadata()?.len() <= 24 {
            return Err(invalid("metadata peer WAL is empty or truncated"));
        }
        let bytes = if base.exists() {
            let mut file = File::open(&base)?;
            if file.metadata()?.len() > 64 * 1024 * 1024 {
                return Err(invalid("metadata bootstrap too large"));
            }
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            bytes
        } else {
            super::chunk_groups::encode_bootstrap(&service, &groups).map_err(invalid)?
        };
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(invalid("metadata bootstrap too large"));
        }
        let (snapshot_bytes, groups) =
            super::chunk_groups::decode_bootstrap(&bytes).map_err(invalid)?;
        let snapshot = MetaSnapshot::from_bytes(&snapshot_bytes).map_err(invalid)?;
        service = MatrixMetaService::new();
        service
            .install_placement_roster(&snapshot)
            .map_err(invalid)?;
        service
            .install_raft_snapshot_bytes(&snapshot_bytes, 0)
            .map_err(invalid)?;
        groups.validate_roster(&service).map_err(invalid)?;
        // Different bootstrap images cannot form one voting group.
        let mut identity = bytes.clone();
        identity.extend_from_slice(&options.group_id.to_be_bytes());
        if options.bootstrap_voters.is_some() {
            identity.extend_from_slice(b"membership-v1");
            identity.extend_from_slice(&(voters.len() as u64).to_be_bytes());
            for id in &voters {
                identity.extend_from_slice(&id.to_be_bytes());
            }
            for (id, endpoint) in &options.peers {
                identity.extend_from_slice(&id.to_be_bytes());
                let endpoint = endpoint.to_string();
                identity.extend_from_slice(&(endpoint.len() as u64).to_be_bytes());
                identity.extend_from_slice(endpoint.as_bytes());
            }
        }
        let group_id = hash(&identity).max(1);
        let group_identity = super::MetaGroupIdentity {
            bootstrap_voters: options.bootstrap_voters.clone().unwrap_or_default(),
            version: if options.bootstrap_voters.is_some() {
                2
            } else {
                1
            },
            group_id: options.group_id,
            node_id: options.node_id,
            voters: options.peers.keys().copied().collect(),
            peer_endpoints: options.peers.values().map(ToString::to_string).collect(),
            bootstrap_id: group_id,
        };
        group_identity.validate().map_err(invalid)?;
        let peer_options = PeerOptions {
            node_id: options.node_id,
            group_id,
            voters,
            wal_path: wal,
        };
        let mut peer = if options.bootstrap_voters.is_some() {
            DurablePeer::open_with_membership(peer_options, options.peers.keys().copied().collect())
        } else {
            DurablePeer::open(peer_options)
        }
        .map_err(invalid)?;
        if !base.exists() {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&base)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            #[cfg(unix)]
            File::open(&directory)?.sync_all()?;
        }
        let listener = TcpListener::bind(options.peers[&options.node_id])?;
        listener.set_nonblocking(true)?;
        let (requests, rx) = ConsensusInbox::new(&stop);
        let wake = rx.notifier();
        let inbound_wake = wake.clone();
        let (incoming, packets) = mpsc::sync_channel(8);
        let mut handles = Vec::new();
        let inbound_stop = Arc::clone(&stop);
        let inbound_transport = transport.clone();
        handles.push(thread::spawn(move || {
            crate::cluster::peer_receiver::receive_with_transport(
                listener,
                incoming,
                &inbound_stop,
                NETWORK_TIMEOUT,
                inbound_wake,
                &inbound_transport,
            );
        }));
        let mut outgoing = BTreeMap::new();
        for (id, address) in options
            .peers
            .iter()
            .filter(|(id, _)| **id != options.node_id)
        {
            let (tx, rx) = mpsc::sync_channel::<crate::cluster::peer_delivery::PeerDelivery>(8);
            outgoing.insert(*id, tx);
            let address = *address;
            let transport = transport.clone();
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                while !stop.is_stopped() {
                    let delivery = match rx.recv_timeout(NETWORK_TIMEOUT) {
                        Ok(bytes) => bytes,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    let _ = delivery.send_with_transport(&address, &transport);
                }
            }));
        }
        let node_count = service.nodes.len();
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let worker_stop = Arc::clone(&stop);
        let progress = WorkerProgress::new();
        let worker_progress = progress.clone();
        handles.push(thread::spawn(move || {
            let _lock = lock;
            if let Err(error) = run(
                &mut peer,
                &mut service,
                &groups,
                ConsensusChannels {
                    progress: worker_progress.clone(),
                    requests: rx,
                    packets,
                    outgoing,
                },
                &worker_stop,
                limit,
                checkpoint,
            ) {
                worker_failed.store(true, Ordering::SeqCst);
                eprintln!("metadata consensus stopped: {error}");
                worker_stop.stop();
            }
            worker_progress.mark(WorkerPhase::Stopped);
        }));
        Ok(Self {
            progress,
            identity: group_identity,
            reply_limit: limit,
            node_count,
            requests,
            wake,
            timeout: options.request_timeout,
            stop,
            failed,
            handles: Mutex::new(handles),
        })
    }
    pub(super) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }
    pub(super) fn request(&self, request: MetaRequest) -> Result<MetaReply, ObjectError> {
        if let MetaRequest::Membership(command) = &request {
            command.validate()?;
            if self.identity.version != 2 || command.bootstrap_id != self.identity.bootstrap_id {
                return Err(refused("membership mode or group identity mismatch"));
            }
        }
        if self.is_failed() || self.stop.is_stopped() {
            return Err(refused("metadata consensus stopped"));
        }
        if matches!(request, MetaRequest::GroupIdentity) {
            // Bootstrap identity is immutable. Catalog state belongs to the worker
            // and must be read after quorum confirmation, like placement and health.
            let reply = MetaReply::GroupIdentity(self.identity.clone());
            super::message::check_reply_size(reply.payload_len(), self.reply_limit)?;
            return Ok(reply);
        }
        let progress = RequestProgress::new(read_only(&request));
        let (reply, rx) = mpsc::sync_channel(1);
        let deadline = Instant::now() + self.timeout;
        self.requests
            .try_send(Request {
                request,
                reply,
                deadline,
                progress: progress.clone(),
            })
            .map_err(|_| refused("metadata consensus unavailable or request queue full"))?;
        if let Some(wake) = &self.wake {
            wake.notify();
        }
        progress.wait_observed(&rx, self.timeout, "metadata", Some(&self.progress))
    }
}
impl Drop for MetaConsensus {
    fn drop(&mut self) {
        self.stop.stop();
        for handle in self
            .handles
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            let _ = handle.join();
        }
    }
}
fn run(
    peer: &mut DurablePeer,
    service: &mut MatrixMetaService,
    groups: &super::ChunkGroupCatalog,
    channels: ConsensusChannels<Request>,
    stop: &ServerShutdown,
    limit: usize,
    mut checkpoint: Policy,
) -> Result<(), RaftError> {
    let ConsensusChannels {
        progress,
        requests,
        packets,
        outgoing,
    } = channels;
    let mut tick = Instant::now();
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut queued = VecDeque::new();
    let mut pending_read: Option<(u64, Vec<Request>)> = None;
    let applied_term = Cell::new(0);
    let mut recovered = false;
    let mut pending_membership: Option<(LogId, Request)> = None;
    while !stop.is_stopped() {
        progress.mark(WorkerPhase::PeerPackets);
        let mut admitted = false;
        for _ in 0..if recovered { 32 } else { 0 } {
            match packets.try_recv() {
                Ok(bytes) => {
                    let _ = peer.step(&bytes);
                }
                Err(_) => break,
            }
        }
        if recovered && tick.elapsed() >= TICK {
            peer.tick()?;
            tick = Instant::now();
        }
        let application = RefCell::new(&mut *service);
        progress.mark(WorkerPhase::ApplyReady);
        let output = peer.process_ready_with_snapshot_reader(
            |id, bytes| {
                applied_term.set(id.term);
                apply_committed(
                    &mut application.borrow_mut(),
                    groups,
                    limit,
                    &mut pending,
                    id,
                    bytes,
                )?;
                Ok(())
            },
            |id, reader, length| {
                progress.mark(WorkerPhase::RestoreCheckpoint);
                let snapshot = super::meta_checkpoint::restore(
                    reader,
                    length,
                    id.clone(),
                    &groups.to_bytes().map_err(storage)?,
                )?;
                let restored = restore_snapshot(snapshot, groups)?;
                **application.borrow_mut() = restored;
                applied_term.set(id.term);
                checkpoint.restored(id.index);
                progress.mark(WorkerPhase::ApplyReady);
                Ok(())
            },
        )?;
        progress.mark(WorkerPhase::Requests);
        drop(application);
        recovered = true;
        if let Some((id, _)) = &pending_membership {
            if peer.term() == id.term && peer.applied_index() >= id.index {
                let (_, request) = pending_membership.take().unwrap();
                let _ = request.reply.try_send(Ok(MetaReply::Membership(
                    super::MembershipStatus::from_peer(peer),
                )));
            } else if !peer.is_leader() || peer.term() != id.term {
                let (_, request) = pending_membership.take().unwrap();
                let _ = request.reply.try_send(Err(refused(
                    "metadata leadership changed; membership outcome may be committed",
                )));
            }
        }
        for packet in output {
            if let Some(tx) = outgoing.get(&packet.to) {
                let _ = tx.try_send(crate::cluster::peer_delivery::PeerDelivery::new(
                    packet.bytes,
                    NETWORK_TIMEOUT,
                ));
            }
        }
        if pending
            .front()
            .is_some_and(|p| !peer.is_leader() || p.id.term != peer.term())
        {
            for p in pending.drain(..) {
                let _ = p.request.reply.try_send(Err(refused(
                    format!("metadata leadership changed; write outcome may be committed{}", progress.describe()),
                )));
            }
        }
        if let Some((token, _)) = &pending_read {
            match peer.poll_read(*token) {
                Ok(false) => {}
                result => {
                    let (_, requests) = pending_read.take().unwrap();
                    for request in requests {
                        let reply = match &result {
                            Ok(true) => match request.request {
                                MetaRequest::Membership(_) => Ok(MetaReply::Membership(
                                    super::MembershipStatus::from_peer(peer),
                                )),
                                other => apply_with_groups(service, other, limit, groups),
                            },
                            Err(error) => read_failure(error),
                            Ok(false) => unreachable!(),
                        };
                        let _ = request.reply.try_send(reply);
                    }
                }
            }
        }
        // No new batch is prepared until all earlier accepted outcomes apply or
        // leadership changes. A client timeout alone does not remove a proposal.
        if pending.is_empty() && pending_read.is_none() && pending_membership.is_none() {
            progress.mark(WorkerPhase::PublishCheckpoint);
            checkpoint.maybe_checkpoint_stream(peer, applied_term.get(), |id| {
                let mut snapshot = service.export_snapshot_unsealed();
                let catalog = groups.to_bytes().map_err(storage)?;
                Ok(move || {
                    snapshot.checksum = snapshot.checksum_material_hash();
                    super::meta_checkpoint::capture(snapshot, catalog, id).map(|stream| {
                        stream.map(|s| {
                            let length = s.length;
                            (s, length)
                        })
                    })
                })
            })?;
            progress.mark(WorkerPhase::Requests);
            while queued.len() < PROPOSALS_PER_BATCH {
                match requests.try_recv() {
                    Ok(request) => queued.push_back(request),
                    Err(_) => break,
                }
            }
            admitted = !queued.is_empty();
            if !peer.is_leader() || applied_term.get() != peer.term() {
                for request in queued.drain(..) {
                    let _ = request.reply.try_send(Ok(MetaReply::NotLeader));
                }
            } else {
                if queued.front().is_some_and(|r| matches!(&r.request, MetaRequest::Membership(command) if !command.is_read())) {
                    let request = queued.pop_front().unwrap();
                    if request.deadline <= Instant::now() {
                        let _ = request.reply.try_send(Err(refused("membership request expired before proposal")));
                    } else {
                        let MetaRequest::Membership(command) = &request.request else { unreachable!() };
                        request.progress.mark(RequestPhase::ProposalCompletion);
                        match command.change().and_then(|change| peer.propose_membership(change).map_err(refused)) {
                            Ok(id) => pending_membership = Some((id, request)),
                            Err(error) => { let _ = request.reply.try_send(Err(error)); }
                        }
                    }
                } else if queued.front().is_some_and(|r| read_only(&r.request)) {
                    let mut reads = Vec::new();
                    while queued.front().is_some_and(|r| read_only(&r.request)) {
                        let request = queued.pop_front().unwrap();
                        if request.deadline <= Instant::now() {
                            let _ = request.reply.try_send(Err(refused(
                                "metadata request expired before read confirmation",
                            )));
                        } else {
                            reads.push(request);
                        }
                    }
                    if !reads.is_empty() {
                        for request in &reads {
                            request.progress.mark(RequestPhase::ReadConfirmation);
                        }
                        match peer.begin_read() {
                            Ok(token) => pending_read = Some((token, reads)),
                            Err(error) => {
                                for request in reads {
                                    let _ = request.reply.try_send(read_failure(&error));
                                }
                            }
                        }
                    }
                } else {
                    // A later write must not pass reads already admitted to this queue.
                    let mut writes = VecDeque::new();
                    while queued.front().is_some_and(|r| !read_only(&r.request) && !matches!(r.request, MetaRequest::Membership(_))) {
                        writes.push_back(queued.pop_front().unwrap());
                    }
                    propose_batch(peer, service, groups, limit, &mut writes, &mut pending)?;
                    writes.append(&mut queued);
                    queued = writes;
                }
            }
        }
        // Process newly proposed local work before sleeping. Peer packets and
        // requests signal the inbox; the next Raft tick bounds all idle waits.
        if !admitted {
            progress.mark(WorkerPhase::Idle);
            requests.wait(stop, TICK.saturating_sub(tick.elapsed()));
        }
    }
    Ok(())
}

#[cfg(test)]
fn restore_checkpoint(
    bytes: &[u8],
    id: LogId,
    groups: &super::ChunkGroupCatalog,
) -> Result<MatrixMetaService, RaftError> {
    let snapshot = super::meta_checkpoint::restore(
        &mut std::io::Cursor::new(bytes),
        bytes.len() as u64,
        id,
        &groups.to_bytes().map_err(storage)?,
    )?;
    restore_snapshot(snapshot, groups)
}
fn restore_snapshot(
    snapshot: MetaSnapshot,
    groups: &super::ChunkGroupCatalog,
) -> Result<MatrixMetaService, RaftError> {
    let mut service = MatrixMetaService::new();
    service
        .install_placement_roster(&snapshot)
        .map_err(storage)?;
    service.install_snapshot(snapshot).map_err(storage)?;
    groups.validate_roster(&service).map_err(storage)?;
    Ok(service)
}
fn read_only(request: &MetaRequest) -> bool {
    match request {
        MetaRequest::Membership(command) => command.is_read(),
        MetaRequest::ReadManifest { .. }
        | MetaRequest::ListRootNodes
        | MetaRequest::ReadChunkGroup { .. }
        | MetaRequest::PlanObjectPlacement { .. }
        | MetaRequest::ReadObjectRootPlacement { .. }
        | MetaRequest::GroupIdentity
        | MetaRequest::Ping => true,
        MetaRequest::AllocateObjectPages { .. }
        | MetaRequest::PublishManifest { .. }
        | MetaRequest::HeartbeatRootNode { .. }
        | MetaRequest::ReportChunkNode { .. }
        | MetaRequest::HeartbeatChunkNode { .. }
        | MetaRequest::ExpireRootNodeHeartbeats { .. }
        | MetaRequest::CommitObjectPlacement { .. }
        | MetaRequest::CommitObjectPlacementIfCurrent { .. } => false,
    }
}
fn apply_committed(
    service: &mut MatrixMetaService,
    groups: &super::ChunkGroupCatalog,
    limit: usize,
    pending: &mut VecDeque<Pending>,
    id: LogId,
    bytes: &[u8],
) -> Result<(), RaftError> {
    if !bytes.is_empty() {
        let entry = Entry::decode(bytes).map_err(|e| RaftError::Storage(e.to_string()))?;
        if entry.version != 1 || entry.encode_to_vec() != bytes {
            return Err(RaftError::Storage(
                "invalid metadata consensus entry".into(),
            ));
        }
        if !entry.outcome.is_empty() {
            PlacementOutcome::from_payload(&entry.outcome)
                .and_then(|o| o.apply(service))
                .map_err(|e| RaftError::Storage(e.to_string()))?;
        }
    }
    if pending.front().is_some_and(|p| p.id == id) {
        let p = pending.pop_front().unwrap();
        let reply = match p.reply {
            Some(reply) => reply,
            None => apply_with_groups(service, p.request.request, limit, groups),
        };
        let _ = p.request.reply.try_send(reply);
    }
    Ok(())
}

fn propose_batch(
    peer: &mut DurablePeer,
    service: &mut MatrixMetaService,
    groups: &super::ChunkGroupCatalog,
    limit: usize,
    queued: &mut VecDeque<Request>,
    pending: &mut VecDeque<Pending>,
) -> Result<(), RaftError> {
    if queued.is_empty() {
        return Ok(());
    }
    for request in queued.iter() {
        request.progress.mark(RequestPhase::Preparing);
    }
    let mut nodes = BTreeSet::new();
    let mut objects = BTreeSet::new();
    for request in queued.iter() {
        match &request.request {
            MetaRequest::PublishManifest { outcome, .. } => {
                for manifest in outcome.before.iter().chain(outcome.after.iter()) {
                    nodes.extend(manifest.extents.iter().map(|e| e.group.clone()));
                    objects.insert(manifest.object_id());
                }
            }

            MetaRequest::HeartbeatRootNode { node_id, .. }
            | MetaRequest::ReportChunkNode { node_id, .. }
            | MetaRequest::HeartbeatChunkNode { node_id, .. } => {
                nodes.insert(node_id.clone());
            }
            MetaRequest::ExpireRootNodeHeartbeats { .. } => {
                // Earlier heartbeats in this batch can make an offline node expire.
                nodes.extend(service.nodes.keys().cloned());
            }
            MetaRequest::CommitObjectPlacement { placement, .. }
            | MetaRequest::CommitObjectPlacementIfCurrent { placement, .. } => {
                let object = super::meta_message::object_id(&placement.bucket, &placement.key);
                nodes.extend(placement.node_ids.iter().cloned());
                if let Some(previous) = service.placements.get(&object) {
                    nodes.extend(previous.node_ids.iter().cloned());
                }
                objects.insert(object);
            }
            _ => {}
        }
    }
    let nodes = groups
        .affected_nodes(nodes.into_iter().collect())
        .into_iter()
        .filter(|id| service.nodes.contains_key(id))
        .collect();
    let draft = PlacementDraft::with_records(service, nodes, objects.into_iter().collect());
    let mut bytes = 0usize;
    let mut payloads = Vec::new();
    let mut prepared = Vec::new();
    for _ in 0..PROPOSALS_PER_BATCH {
        let Some(request) = queued.pop_front() else {
            break;
        };
        if request.deadline <= Instant::now() {
            let _ = request
                .reply
                .try_send(Err(refused("metadata request expired before proposal")));
            continue;
        }
        let (outcome, reply) = match prepare(draft.service, &request.request, limit, groups) {
            Ok((outcome, reply)) => (outcome, reply.map(Ok)),
            Err(error) => (Vec::new(), Some(Err(error))),
        };
        let payload = Entry {
            version: 1,
            outcome: outcome.clone(),
        }
        .encode_to_vec();
        if !prepared.is_empty() && bytes.saturating_add(payload.len()) > PACKET_LIMIT / 2 {
            queued.push_front(request);
            break;
        }
        let length = payload.len();
        if length > PACKET_LIMIT / 2 {
            let _ = request
                .reply
                .try_send(Err(refused("metadata proposal exceeds batch byte limit")));
            break;
        }
        request.progress.mark(RequestPhase::ProposalCompletion);
        // Prepare the next outcome against this prefix, but restore the live
        // service before admission, Ready processing, or any client reply.
        if !outcome.is_empty() {
            PlacementOutcome::from_payload(&outcome)
                .and_then(|o| o.apply(draft.service))
                .map_err(|e| RaftError::Storage(e.to_string()))?;
        }
        payloads.push(payload);
        prepared.push((request, reply));
        bytes += length;
    }
    drop(draft);
    if !payloads.is_empty() {
        match peer.propose_batch(payloads) {
            Ok(ids) => {
                for (id, (request, reply)) in ids.into_iter().zip(prepared) {
                    pending.push_back(Pending { id, request, reply });
                }
            }
            Err(error) => {
                for (request, _) in prepared {
                    let _ = request.reply.try_send(Err(refused(&error)));
                }
            }
        }
    }
    Ok(())
}

fn prepare(
    service: &mut MatrixMetaService,
    request: &MetaRequest,
    limit: usize,
    groups: &super::ChunkGroupCatalog,
) -> Result<(Vec<u8>, Option<MetaReply>), ObjectError> {
    let stamped = if matches!(request, MetaRequest::ReportChunkNode { .. }) {
        Some(super::health_report::stamp(request.clone(), service)?)
    } else {
        None
    };
    let request = stamped.as_ref().unwrap_or(request);
    groups.check_request(request)?;
    if let MetaRequest::AllocateObjectPages { page_count } = request {
        let (decided, reply) = super::meta_server::decide_allocation(service, *page_count, limit)?;
        return Ok((decided.to_payload()?, Some(reply)));
    }
    if let MetaRequest::PublishManifest {
        outcome,
        update_time_ms,
    } = request
    {
        let (decided, reply) =
            super::meta_server::decide_manifest(service, outcome.clone(), *update_time_ms, limit)?;
        return Ok((decided.to_payload()?, Some(reply)));
    }

    let (nodes, object) = match request {
        MetaRequest::HeartbeatRootNode { node_id, .. }
        | MetaRequest::HeartbeatChunkNode { node_id, .. } => (vec![node_id.clone()], None),
        MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms } => {
            let nodes = super::meta_server::expired_node_ids(service, *ttl_ms, *now_ms)?;
            if nodes.is_empty() {
                // Keep the read barrier, without cloning or logging the whole roster.
                return Ok((Vec::new(), Some(MetaReply::Expired { offline_count: 0 })));
            }
            (nodes, None)
        }
        MetaRequest::CommitObjectPlacement { placement, .. }
        | MetaRequest::CommitObjectPlacementIfCurrent { placement, .. } => {
            let object = super::meta_message::object_id(&placement.bucket, &placement.key);
            let mut nodes = placement.node_ids.clone();
            if let Some(previous) = service.placements.get(&object) {
                nodes.extend(previous.node_ids.iter().cloned());
            }
            (nodes, Some(object))
        }
        _ => return Ok((Vec::new(), None)),
    };
    let nodes = groups.affected_nodes(nodes);
    let before = PlacementOutcome::capture(service, nodes.clone(), object.clone())?;
    // The worker exclusively owns this service. Keep the entire roster available to
    // validation, but save only records these three mutation kinds can change.
    let draft = PlacementDraft::new(service, nodes, object);
    let reply = apply_with_groups(draft.service, request.clone(), limit, groups)?;
    let payload = before.finish(draft.service)?.to_payload()?;
    // Restore before the caller can propose, acknowledge, or handle another request.
    drop(draft);
    Ok((payload, Some(reply)))
}

/// Undo state for the closed set of placement mutations admitted by `prepare`.
/// Never use this for membership, health reports, or other metadata operations.
struct PlacementDraft<'a> {
    service: &'a mut MatrixMetaService,
    nodes: Vec<crate::PlacementNode>,
    objects: Vec<(
        crate::ObjectId,
        Option<crate::ObjectPlacement>,
        Option<crate::object_manifest::ObjectManifest>,
    )>,
    index: u64,
}
impl<'a> PlacementDraft<'a> {
    fn new(
        service: &'a mut MatrixMetaService,
        nodes: Vec<String>,
        object: Option<crate::ObjectId>,
    ) -> Self {
        Self::with_records(service, nodes, object.into_iter().collect())
    }
    fn with_records(
        service: &'a mut MatrixMetaService,
        nodes: Vec<String>,
        objects: Vec<crate::ObjectId>,
    ) -> Self {
        let nodes = nodes.iter().map(|id| service.nodes[id].clone()).collect();
        let objects = objects
            .into_iter()
            .map(|id| {
                let previous = service.placements.get(&id).cloned();
                let manifest = service.object_manifests.get(&id).cloned();
                (id, previous, manifest)
            })
            .collect();
        let index = service.placement_commit_index;
        Self {
            service,
            nodes,
            objects,
            index,
        }
    }
}
impl Drop for PlacementDraft<'_> {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            self.service.nodes.insert(node.node_id.clone(), node);
        }
        for (id, previous, manifest) in self.objects.drain(..) {
            self.service
                .object_manifests
                .restore_entry(id.clone(), manifest);
            if let Some(previous) = previous {
                self.service.placements.insert(id, previous);
            } else {
                self.service.placements.remove(&id);
            }
        }
        self.service.placement_commit_index = self.index;
        self.service.invalidate_snapshot_export_cache();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ChunkGroupCatalog, ChunkGroupConfig, ChunkGroupIdentity};
    use crate::{ObjectId, ObjectPlacement, PlacementNode};
    use std::sync::mpsc::Receiver;
    fn fixture(count: usize, objects: usize) -> (MatrixMetaService, ChunkGroupCatalog) {
        let groups = ChunkGroupCatalog::new(
            (0..count)
                .map(|g| {
                    let port = 10000 + g * 6;
                    ChunkGroupConfig {
                        placement_id: format!("group-{g}"),
                        failure_domain: format!("rack-{g}"),
                        identity: Some(ChunkGroupIdentity {
                            bootstrap_voters: Vec::new(),
                            bootstrap_id: 0,
                            version: 1,
                            group_id: g as u64 + 1,
                            node_id: 1,
                            capacity_bytes: 1 << 30,
                            max_pages: 1024,
                            voters: vec![1, 2, 3],
                            peer_endpoints: (0..3)
                                .map(|v| format!("127.0.0.1:{}", port + v))
                                .collect(),
                        }),
                        api_endpoints: (3..6).map(|v| format!("127.0.0.1:{}", port + v)).collect(),
                        member_ids: (0..3).map(|v| format!("member-{g}-{v}")).collect(),
                    }
                })
                .collect(),
        )
        .unwrap();
        let mut service = MatrixMetaService::new();
        for group in groups.groups() {
            for (id, endpoint, eligible) in
                std::iter::once((&group.placement_id, &group.api_endpoints[0], true)).chain(
                    group
                        .member_ids
                        .iter()
                        .zip(&group.api_endpoints)
                        .map(|(id, ep)| (id, ep, false)),
                )
            {
                let index = service.nodes.len() as u64;
                service.nodes.insert(
                    id.clone(),
                    PlacementNode {
                        node_id: id.clone(),
                        endpoint: endpoint.clone(),
                        service_kind: "chunkserver".into(),
                        failure_domain: group.failure_domain.clone(),
                        metadata_eligible: false,
                        data_eligible: eligible,
                        gateway_eligible: false,
                        serves_page_tier: true,
                        metadata_value_bytes: 0,
                        capacity_bytes: 1 << 30,
                        used_bytes: 0,
                        committed_bytes: 0,
                        draining: false,
                        online: true,
                        last_heartbeat_ms: 1,
                        page_allocator_shard_index: index,
                        page_allocator_shard_count: (count * 4) as u64,
                        meta_health: Default::default(),
                        page_health: Default::default(),
                        chunk_health: Default::default(),
                    },
                );
            }
        }
        for n in 0..objects {
            let id = ObjectId::new("bucket", format!("object-{n}"));
            service.placements.insert(
                id.clone(),
                ObjectPlacement {
                    object_id: id,
                    version: 1,
                    length: 128,
                    node_ids: vec!["group-0".into()],
                    update_time_ms: 1,
                },
            );
        }
        service.nodes.get_mut("group-0").unwrap().committed_bytes = objects as u64 * 128;
        service.placement_commit_index = (service.nodes.len() + objects) as u64;
        groups.validate_roster(&service).unwrap();
        (service, groups)
    }

    #[test]
    fn catalog_reads_queue_while_only_bootstrap_identity_bypasses_consensus() {
        let (requests, receiver) = mpsc::sync_channel(1);
        let mut consensus = MetaConsensus {
            progress: WorkerProgress::new(),
            node_count: 4,
            identity: super::super::MetaGroupIdentity {
                bootstrap_voters: Vec::new(),
                version: 1,
                group_id: 1,
                node_id: 1,
                voters: vec![1],
                peer_endpoints: vec!["127.0.0.1:9999".into()],
                bootstrap_id: 1,
            },
            reply_limit: 4096,
            requests,
            wake: None,
            timeout: Duration::from_millis(1),
            stop: Arc::new(ServerShutdown::default()),
            failed: Arc::new(AtomicBool::new(false)),
            handles: Mutex::new(vec![]),
        };
        let request = MetaRequest::ReadChunkGroup {
            placement_id: "group-0".into(),
        };
        // With no worker, catalog reads must time out instead of returning a
        // bootstrap copy. The request is queued for the worker's read barrier.
        assert!(consensus.request(request.clone()).is_err());
        assert_eq!(receiver.try_recv().unwrap().request, request);
        assert!(matches!(consensus.request(MetaRequest::GroupIdentity),
            Ok(MetaReply::GroupIdentity(_))));
        assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
        consensus.reply_limit = 1;
        assert!(consensus.request(MetaRequest::GroupIdentity).is_err());
        consensus.reply_limit = 4096;
        consensus.failed.store(true, Ordering::SeqCst);
        assert!(consensus.request(request.clone()).is_err());
        consensus.failed.store(false, Ordering::SeqCst);
        consensus.stop.stop();
        assert!(consensus.request(request).is_err());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn automatic_health_survives_clock_rollback_and_snapshot_recovery() {
        let (mut service, groups) = fixture(1, 0);
        service
            .nodes
            .get_mut("member-0-0")
            .unwrap()
            .last_heartbeat_ms = u64::MAX;
        service.invalidate_snapshot_export_cache();
        let request = MetaRequest::ReportChunkNode {
            node_id: "member-0-0".into(),
            expected_endpoint: "127.0.0.1:10003".into(),
            used_bytes: 256,
            capacity_bytes: 1 << 30,
            online: true,
        };
        for used in [256, 257] {
            let mut request = request.clone();
            if let MetaRequest::ReportChunkNode { used_bytes, .. } = &mut request {
                *used_bytes = used;
            }
            let before = state(&service);
            let (payload, reply) = prepare(&mut service, &request, 65536, &groups).unwrap();
            assert_eq!(
                state(&service),
                before,
                "preparation must remain speculative"
            );
            let Some(MetaReply::PlacementNode(node)) = reply else {
                panic!("missing health reply")
            };
            assert_eq!(node.last_heartbeat_ms, u64::MAX);
            assert_eq!(node.used_bytes, used);
            PlacementOutcome::from_payload(&payload)
                .unwrap()
                .apply(&mut service)
                .unwrap();
            assert_eq!(service.nodes["member-0-0"].last_heartbeat_ms, u64::MAX);
            assert_eq!(service.nodes["member-0-0"].used_bytes, used);
            let snapshot = service.export_snapshot();
            service = restore_snapshot(snapshot, &groups).unwrap();
        }
        let before = state(&service);
        let mut explicit = heartbeat();
        if let MetaRequest::HeartbeatChunkNode { now_ms, .. } = &mut explicit {
            *now_ms = u64::MAX - 1;
        }
        assert!(prepare(&mut service, &explicit, 65536, &groups).is_err());
        assert_eq!(state(&service), before);
        let mut other = request;
        if let MetaRequest::ReportChunkNode {
            node_id,
            expected_endpoint,
            ..
        } = &mut other
        {
            *node_id = "member-0-1".into();
            *expected_endpoint = "127.0.0.1:10004".into();
        }
        let (_, reply) = prepare(&mut service, &other, 65536, &groups).unwrap();
        let Some(MetaReply::PlacementNode(node)) = reply else {
            panic!("missing other report")
        };
        assert!(
            node.last_heartbeat_ms < u64::MAX,
            "the floor belongs to one node"
        );
    }

    #[test]
    fn received_health_preparation_restores_state_and_replays_decided_time() {
        let (mut service, groups) = fixture(1, 0);
        let before = state(&service);
        let request = MetaRequest::ReportChunkNode {
            node_id: "member-0-0".into(),
            expected_endpoint: "127.0.0.1:10003".into(),
            used_bytes: 256,
            capacity_bytes: 1 << 30,
            online: true,
        };
        let earliest = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let (payload, reply) = prepare(&mut service, &request, 65536, &groups).unwrap();
        let latest = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(state(&service), before);
        let Some(MetaReply::PlacementNode(node)) = reply else {
            panic!("missing health reply")
        };
        assert!((earliest..=latest).contains(&node.last_heartbeat_ms));
        let mut recovered = service.clone();
        PlacementOutcome::from_payload(&payload)
            .unwrap()
            .apply(&mut recovered)
            .unwrap();
        assert_eq!(
            recovered.nodes["member-0-0"].last_heartbeat_ms,
            node.last_heartbeat_ms
        );
        assert_eq!(recovered.nodes["member-0-0"].used_bytes, 256);
        assert!(apply_with_groups(&mut service, request.clone(), 65536, &groups).is_err());
        assert_eq!(state(&service), before);
        let mut invalid = request.clone();
        if let MetaRequest::ReportChunkNode {
            expected_endpoint, ..
        } = &mut invalid
        {
            *expected_endpoint = "127.0.0.1:1".into();
        }
        assert!(prepare(&mut service, &invalid, 65536, &groups).is_err());
        assert_eq!(state(&service), before);
        if let MetaRequest::ReportChunkNode { capacity_bytes, .. } = &mut invalid {
            *capacity_bytes = 512;
        }
        assert!(prepare(&mut service, &invalid, 65536, &groups).is_err());
        assert_eq!(state(&service), before);
    }

    fn heartbeat() -> MetaRequest {
        MetaRequest::HeartbeatChunkNode {
            node_id: "member-0-0".into(),
            expected_endpoint: "127.0.0.1:10003".into(),
            used_bytes: 256,
            capacity_bytes: 1 << 30,
            online: true,
            now_ms: 2,
        }
    }
    fn placement(service: &MatrixMetaService, key: &str) -> MetaRequest {
        let plan = service
            .plan_object_placement(ObjectId::new("bucket", key), 256, 1)
            .unwrap();
        MetaRequest::CommitObjectPlacement {
            placement: super::super::meta_message::CommittedPlacement {
                bucket: "bucket".into(),
                key: key.into(),
                version: 2,
                length: 256,
                node_ids: plan.node_ids,
                update_time_ms: 2,
            },
            expected_plan_checksum: plan.plan_checksum,
        }
    }
    fn state(service: &MatrixMetaService) -> (Vec<u8>, String) {
        (
            service.export_snapshot().to_bytes().unwrap(),
            service.placement_state_fingerprint(),
        )
    }
    fn compare_preparation(
        service: &mut MatrixMetaService,
        groups: &ChunkGroupCatalog,
        request: MetaRequest,
    ) {
        let saved = state(service);
        let mut candidate = service.clone();
        let object = match &request {
            MetaRequest::CommitObjectPlacement { placement, .. } => Some(ObjectId::new(
                placement.bucket.clone(),
                placement.key.clone(),
            )),
            _ => None,
        };
        let before =
            PlacementOutcome::capture(service, service.nodes.keys().cloned().collect(), object)
                .unwrap();
        let reply =
            apply_with_groups(&mut candidate, request.clone(), 16 * 1024 * 1024, groups).unwrap();
        let expected = before.finish(&candidate).unwrap().to_payload().unwrap();
        let (actual, actual_reply) = prepare(service, &request, 16 * 1024 * 1024, groups).unwrap();
        assert_eq!(state(service), saved, "preparation changed committed state");
        assert_eq!(actual_reply, Some(reply));
        assert_eq!(actual, expected);
        PlacementOutcome::from_payload(&actual)
            .unwrap()
            .apply(service)
            .unwrap();
        assert_eq!(state(service), state(&candidate));
    }

    #[test]
    fn deferred_metadata_sealing_preserves_captured_roster_and_placements() {
        let (mut service, groups) = fixture(1, 30);
        let expected = service.export_snapshot();
        let mut captured = service.export_snapshot_unsealed();
        assert!(captured.checksum.is_empty());
        apply_with_groups(&mut service, heartbeat(), 16 * 1024 * 1024, &groups).unwrap();
        let sealed = std::thread::spawn(move || {
            captured.checksum = captured.checksum_material_hash();
            captured
        })
        .join()
        .unwrap();
        assert_eq!(sealed, expected);
        assert_eq!(sealed.to_bytes().unwrap(), expected.to_bytes().unwrap());
        assert_ne!(sealed, service.export_snapshot());
        sealed.validate().unwrap();
    }
    #[test]
    fn application_checkpoint_restores_placement_health_and_checks_catalog() {
        let (mut service, groups) = fixture(4, 30);
        apply_with_groups(&mut service, heartbeat(), 16 * 1024 * 1024, &groups).unwrap();
        let saved = state(&service);
        let id = LogId {
            index: 150,
            term: 9,
        };
        let bytes =
            Image::new(1, id.clone(), saved.0.clone(), groups.to_bytes().unwrap()).encode_to_vec();
        let mut streamed = Vec::new();
        crate::cluster::meta_checkpoint::capture(
            service.export_snapshot(),
            groups.to_bytes().unwrap(),
            id.clone(),
        )
        .unwrap()
        .unwrap()
        .read_to_end(&mut streamed)
        .unwrap();
        for encoded in [&bytes, &streamed] {
            let restored = restore_checkpoint(encoded, id.clone(), &groups).unwrap();
            assert_eq!(state(&restored), saved);
            assert!(
                restore_checkpoint(encoded, id.clone(), &ChunkGroupCatalog::default()).is_err()
            );
        }
        assert!(restore_checkpoint(&bytes, id, &ChunkGroupCatalog::default()).is_err());
        assert_eq!(state(&service), saved);
    }
    #[test]
    fn preparation_restores_state_and_replays_identical_outcomes() {
        let (mut service, groups) = fixture(32, 1000);
        compare_preparation(&mut service, &groups, heartbeat());
        assert_eq!(service.nodes["group-0"].used_bytes, 256);
        let new = placement(&service, "new-object");
        compare_preparation(&mut service, &groups, new);
        let replacement = placement(&service, "object-0");
        compare_preparation(&mut service, &groups, replacement);
        compare_preparation(
            &mut service,
            &groups,
            MetaRequest::ExpireRootNodeHeartbeats {
                ttl_ms: 1,
                now_ms: 5,
            },
        );
        assert!(service.nodes.values().all(|n| !n.online));
        let saved = state(&service);
        let (payload, reply) = prepare(
            &mut service,
            &MetaRequest::ExpireRootNodeHeartbeats {
                ttl_ms: 1,
                now_ms: 6,
            },
            65536,
            &groups,
        )
        .unwrap();
        assert!(payload.is_empty());
        assert_eq!(reply, Some(MetaReply::Expired { offline_count: 0 }));
        assert_eq!(state(&service), saved);
    }
    #[test]
    fn preparation_restores_rejections_errors_and_unwinding() {
        let (mut service, groups) = fixture(2, 1);
        let saved = state(&service);
        let mut invalid = heartbeat();
        if let MetaRequest::HeartbeatChunkNode { capacity_bytes, .. } = &mut invalid {
            *capacity_bytes = 0;
        }
        assert!(prepare(&mut service, &invalid, 65536, &groups).is_err());
        assert_eq!(state(&service), saved);
        for panic in [false, true] {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || -> Result<(), ObjectError> {
                    let draft = PlacementDraft::new(
                        &mut service,
                        groups.affected_nodes(vec!["member-0-0".into()]),
                        None,
                    );
                    apply_with_groups(draft.service, heartbeat(), 65536, &groups)?;
                    assert_eq!(draft.service.nodes["group-0"].used_bytes, 256);
                    if panic {
                        panic!("injected preparation unwind");
                    }
                    Err(ObjectError::InvalidArgument(
                        "injected preparation failure".into(),
                    ))
                },
            ));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(state(&service), saved);
        }
        // Full-roster allocator checks must still see nodes outside the undo set.
        let groups = ChunkGroupCatalog::default();
        service
            .nodes
            .get_mut("group-1")
            .unwrap()
            .page_allocator_shard_index = service.nodes["group-0"].page_allocator_shard_index;
        service.invalidate_snapshot_export_cache();
        let saved = state(&service);
        let invalid = MetaRequest::HeartbeatRootNode {
            node_id: "group-0".into(),
            used_bytes: 256,
            capacity_bytes: 1 << 30,
            draining: false,
            online: true,
            now_ms: 2,
        };
        assert!(prepare(&mut service, &invalid, 65536, &groups).is_err());
        assert_eq!(state(&service), saved);
    }
    #[test]
    fn preparation_restores_placement_when_outcome_exceeds_limit() {
        let (mut service, groups) = fixture(1, 0);
        let key = "k".repeat(4 * 1024 * 1024);
        let request = placement(&service, &key);
        apply_with_groups(&mut service, request, 16 * 1024 * 1024, &groups).unwrap();
        let mut request = placement(&service, &key);
        if let MetaRequest::CommitObjectPlacement { placement, .. } = &mut request {
            placement.version = 3;
        }
        let saved = state(&service);
        let error = prepare(&mut service, &request, 16 * 1024 * 1024, &groups).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("placement outcome exceeds size limit"),
            "{error}"
        );
        assert_eq!(state(&service), saved);
    }

    struct PeerFiles(PathBuf);
    impl Drop for PeerFiles {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn batch_peer() -> (PeerFiles, DurablePeer) {
        let root = std::env::temp_dir().join(format!(
            "metadata-batch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let files = PeerFiles(root);
        let mut peer = DurablePeer::open(PeerOptions {
            node_id: 1,
            group_id: 19,
            voters: vec![1],
            wal_path: files.0.join("peer.wal"),
        })
        .unwrap();
        for _ in 0..30 {
            peer.tick().unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
            if peer.is_leader() {
                break;
            }
        }
        assert!(peer.is_leader());
        (files, peer)
    }
    fn queued(
        request: MetaRequest,
        deadline: Instant,
    ) -> (Request, Receiver<Result<MetaReply, ObjectError>>) {
        let (reply, rx) = mpsc::sync_channel(1);
        (
            Request {
                progress: RequestProgress::new(read_only(&request)),
                request,
                deadline,
                reply,
            },
            rx,
        )
    }
    #[test]
    fn proposal_batch_preserves_ordered_outcomes_and_commit_only_replies() {
        let (_files, mut peer) = batch_peer();
        let (mut service, groups) = fixture(2, 1);
        let first = placement(&service, "batch-object");
        let mut second = first.clone();
        if let MetaRequest::CommitObjectPlacement {
            placement,
            expected_plan_checksum,
        } = &mut second
        {
            placement.version = 3;
            placement.node_ids = vec!["group-0".into()];
            *expected_plan_checksum = crate::placement_plan_checksum(&crate::PlacementPlan {
                object_id: ObjectId::new(placement.bucket.clone(), placement.key.clone()),
                length: placement.length,
                node_ids: placement.node_ids.clone(),
                skipped_draining: 0,
                skipped_offline: 0,
                skipped_capacity: 0,
                plan_checksum: String::new(),
            });
        }
        let mut another = heartbeat();
        if let MetaRequest::HeartbeatChunkNode {
            node_id,
            expected_endpoint,
            ..
        } = &mut another
        {
            *node_id = "member-0-1".into();
            *expected_endpoint = "127.0.0.1:10004".into();
        }
        let mut invalid = heartbeat();
        if let MetaRequest::HeartbeatChunkNode { capacity_bytes, .. } = &mut invalid {
            *capacity_bytes = 0;
        }
        let operations = vec![
            heartbeat(),
            another,
            first,
            second,
            MetaRequest::ReadObjectRootPlacement {
                bucket: "bucket".into(),
                key: "batch-object".into(),
            },
            invalid,
            MetaRequest::ExpireRootNodeHeartbeats {
                ttl_ms: 1,
                now_ms: 5,
            },
            MetaRequest::ListRootNodes,
        ];
        let mut expected = service.clone();
        let replies: Vec<_> = operations
            .iter()
            .cloned()
            .map(|r| apply_with_groups(&mut expected, r, 65536, &groups).map_err(|e| e.to_string()))
            .collect();
        let mut requests = VecDeque::new();
        let mut receivers = Vec::new();
        for operation in operations {
            let (request, rx) = queued(operation, Instant::now() + Duration::from_secs(10));
            requests.push_back(request);
            receivers.push(rx);
        }
        let saved = state(&service);
        let mut pending = VecDeque::new();
        propose_batch(
            &mut peer,
            &mut service,
            &groups,
            65536,
            &mut requests,
            &mut pending,
        )
        .unwrap();
        assert_eq!(pending.len(), 8);
        assert!(requests.is_empty());
        assert_eq!(state(&service), saved);
        assert!(receivers
            .iter()
            .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
        peer.process_ready(|id, bytes| {
            apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
        })
        .unwrap();
        assert!(pending.is_empty());
        for (rx, expected_reply) in receivers.into_iter().zip(replies) {
            assert_eq!(
                rx.try_recv().unwrap().map_err(|e| e.to_string()),
                expected_reply
            );
        }
        assert_eq!(state(&service), state(&expected));
        drop(peer);
        let mut reopened = DurablePeer::open(PeerOptions {
            node_id: 1,
            group_id: 19,
            voters: vec![1],
            wal_path: _files.0.join("peer.wal"),
        })
        .unwrap();
        let (mut restored, _) = fixture(2, 1);
        reopened
            .process_ready(|id, bytes| {
                apply_committed(&mut restored, &groups, 65536, &mut pending, id, bytes)
            })
            .unwrap();
        assert_eq!(state(&restored), state(&expected));
    }
    #[test]
    fn rejected_proposal_group_restores_prepared_state_and_replies_to_every_request() {
        let (_files, mut peer) = batch_peer();
        let (mut service, groups) = fixture(2, 1);
        let operations = [heartbeat(), placement(&service, "refused-object")];
        let saved = state(&service);
        // Keep the core's admission budget occupied until Ready processing.
        peer.propose(vec![0; 4 * 1024 * 1024]).unwrap();
        let mut requests = VecDeque::new();
        let mut receivers = Vec::new();
        for operation in operations {
            let (request, rx) = queued(operation, Instant::now() + Duration::from_secs(30));
            requests.push_back(request);
            receivers.push(rx);
        }
        let mut pending = VecDeque::new();
        propose_batch(
            &mut peer,
            &mut service,
            &groups,
            65536,
            &mut requests,
            &mut pending,
        ).unwrap();
        assert!(requests.is_empty());
        assert!(pending.is_empty());
        assert_eq!(state(&service), saved);
        for rx in receivers {
            assert!(rx.try_recv().unwrap().is_err());
        }
    }

    #[test]
    fn proposal_batches_bound_bytes_and_keep_deferred_requests() {
        let (_files, mut peer) = batch_peer();
        let (mut service, groups) = fixture(1, 0);
        let (expired, expired_rx) = queued(MetaRequest::Ping, Instant::now());
        let mut requests = VecDeque::from([expired]);
        let mut receivers = Vec::new();
        for prefix in ["a", "b"] {
            let key = prefix.repeat(2 * 1024 * 1024);
            let request = placement(&service, &key);
            let (request, rx) = queued(request, Instant::now() + Duration::from_secs(30));
            requests.push_back(request);
            receivers.push(rx);
        }
        let saved = state(&service);
        let mut pending = VecDeque::new();
        propose_batch(
            &mut peer,
            &mut service,
            &groups,
            8 * 1024 * 1024,
            &mut requests,
            &mut pending,
        )
        .unwrap();
        assert!(expired_rx.try_recv().unwrap().is_err());
        assert_eq!(pending.len(), 1);
        assert_eq!(requests.len(), 1);
        assert_eq!(state(&service), saved);
        assert!(receivers
            .iter()
            .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
        peer.process_ready(|id, bytes| {
            apply_committed(
                &mut service,
                &groups,
                8 * 1024 * 1024,
                &mut pending,
                id,
                bytes,
            )
        })
        .unwrap();
        assert!(receivers[0].try_recv().unwrap().is_ok());
        assert_eq!(service.placements.len(), 1);
        propose_batch(
            &mut peer,
            &mut service,
            &groups,
            8 * 1024 * 1024,
            &mut requests,
            &mut pending,
        )
        .unwrap();
        assert!(requests.is_empty());
        assert_eq!(pending.len(), 1);
        assert_eq!(service.placements.len(), 1);
        peer.process_ready(|id, bytes| {
            apply_committed(
                &mut service,
                &groups,
                8 * 1024 * 1024,
                &mut pending,
                id,
                bytes,
            )
        })
        .unwrap();
        assert!(receivers[1].try_recv().unwrap().is_ok());
        assert_eq!(service.placements.len(), 2);
    }
    #[test]
    #[ignore = "bounded local preparation timing; run with --release and --nocapture"]
    fn measure_heartbeat_preparation_by_catalog_size() {
        println!("groups,nodes,placements,clone_ns,prepare_ns,outcome_bytes");
        for count in [1, 32, 512] {
            for objects in [0, 10000] {
                let (mut service, groups) = fixture(count, objects);
                let request = MetaRequest::HeartbeatChunkNode {
                    node_id: "member-0-0".into(),
                    expected_endpoint: "127.0.0.1:10003".into(),
                    used_bytes: 1,
                    capacity_bytes: 1 << 30,
                    online: true,
                    now_ms: 2,
                };
                let size = prepare(&mut service, &request, 65536, &groups)
                    .unwrap()
                    .0
                    .len();
                let mut clones = Vec::new();
                let mut preparations = Vec::new();
                for _ in 0..7 {
                    let start = Instant::now();
                    for _ in 0..16 {
                        std::hint::black_box(service.clone());
                    }
                    clones.push(start.elapsed().as_nanos() / 16);
                    let start = Instant::now();
                    for _ in 0..16 {
                        std::hint::black_box(
                            prepare(&mut service, &request, 65536, &groups).unwrap(),
                        );
                    }
                    preparations.push(start.elapsed().as_nanos() / 16);
                }
                clones.sort_unstable();
                preparations.sort_unstable();
                assert_eq!(service.nodes["member-0-0"].used_bytes, 0);
                println!(
                    "{count},{},{objects},{},{},{size}",
                    service.nodes.len(),
                    clones[3],
                    preparations[3]
                );
            }
        }
    }
    #[test]
    fn manifest_outcomes_recover_from_raft_checkpoint_and_committed_suffix() {
        use std::cell::RefCell;
        let (files, mut peer) = batch_peer();
        let (mut service, create, replace) = crate::tests::manifest_publication_fixture();
        let groups = super::super::ChunkGroupCatalog::default();
        let mut pending = VecDeque::new();
        let encode = |outcome: &PlacementOutcome| {
            Entry {
                version: 1,
                outcome: outcome.to_payload().unwrap(),
            }
            .encode_to_vec()
        };
        peer.propose(encode(&create)).unwrap();
        peer.process_ready(|id, bytes| {
            apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
        })
        .unwrap();
        let id = LogId {
            index: peer.applied_index(),
            term: peer.term(),
        };
        let mut stream = super::super::meta_checkpoint::capture(
            service.export_snapshot(),
            groups.to_bytes().unwrap(),
            id,
        )
        .unwrap()
        .unwrap();
        let length = stream.length;
        peer.checkpoint_from_reader(&mut stream, length).unwrap();
        drop(stream);
        peer.propose(encode(&replace)).unwrap();
        peer.process_ready(|id, bytes| {
            apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
        })
        .unwrap();
        let expected = service.export_snapshot();
        drop(peer);
        let mut peer = DurablePeer::open(PeerOptions {
            node_id: 1,
            group_id: 19,
            voters: vec![1],
            wal_path: files.0.join("peer.wal"),
        })
        .unwrap();
        let recovered = RefCell::new(MatrixMetaService::new());
        let mut installed = false;
        peer.process_ready_with_snapshot_reader(
            |id, bytes| {
                apply_committed(
                    &mut recovered.borrow_mut(),
                    &groups,
                    65536,
                    &mut pending,
                    id,
                    bytes,
                )
            },
            |id, reader, length| {
                let snapshot = super::super::meta_checkpoint::restore(
                    reader,
                    length,
                    id,
                    &groups.to_bytes().map_err(storage)?,
                )?;
                *recovered.borrow_mut() = restore_snapshot(snapshot, &groups)?;
                installed = true;
                Ok(())
            },
        )
        .unwrap();
        assert!(installed);
        assert_eq!(recovered.borrow().export_snapshot(), expected);
        drop(peer);
    }
    #[test]
    fn manifest_batch_preparation_restores_catalog_and_commits_in_order() {
        use crate::object_manifest::ManifestOutcome;
        let (_files, mut peer) = batch_peer();
        let (mut service, groups) = fixture(1, 0);
        let (_, create, _) = crate::tests::manifest_publication_fixture();
        let mut first = create.manifest.unwrap().after.unwrap();
        first.extents[0].group = "group-0".into();
        let mut second = first.clone();
        second.generation = 2;
        second.extents[0].page_index = 1;
        let create = MetaRequest::PublishManifest {
            outcome: ManifestOutcome::decide(None, first.clone()).unwrap(),
            update_time_ms: 10,
        };
        let replace = MetaRequest::PublishManifest {
            outcome: ManifestOutcome::decide(Some(first), second.clone()).unwrap(),
            update_time_ms: 20,
        };
        let saved = state(&service);
        assert!(prepare(&mut service, &create, 1, &groups).is_err());
        assert_eq!(state(&service), saved);
        let (_, reply) = prepare(&mut service, &create, 65536, &groups).unwrap();
        assert!(matches!(reply, Some(MetaReply::Manifest(_))));
        assert_eq!(state(&service), saved);
        assert!(prepare(&mut service, &create, 65536, &Default::default()).is_err());
        let mut pending = VecDeque::new();
        let mut requests = VecDeque::new();
        let mut receivers = Vec::new();
        for request in [create.clone(), replace, create] {
            let (request, rx) = queued(request, Instant::now() + Duration::from_secs(10));
            requests.push_back(request);
            receivers.push(rx);
        }
        propose_batch(
            &mut peer,
            &mut service,
            &groups,
            65536,
            &mut requests,
            &mut pending,
        )
        .unwrap();
        assert_eq!(state(&service), saved);
        assert!(receivers
            .iter()
            .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
        peer.process_ready(|id, bytes| {
            apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
        })
        .unwrap();
        assert!(receivers[0].try_recv().unwrap().is_ok());
        assert_eq!(
            receivers[1].try_recv().unwrap().unwrap(),
            MetaReply::Manifest(second.clone())
        );
        assert!(receivers[2].try_recv().unwrap().is_err());
        assert_eq!(
            service.object_manifests.get(&second.object_id()),
            Some(&second)
        );
        assert_eq!(service.nodes["group-0"].committed_bytes, 4);
    }
    #[test]
    fn object_allocations_commit_unique_namespaces_and_restore_speculative_batches() {
        let (_files, mut peer) = batch_peer();
        let (mut service, groups) = fixture(1, 0);
        let saved = state(&service);
        let base = service.placement_commit_index;
        for count in [0, 4097] {
            assert!(prepare(
                &mut service,
                &MetaRequest::AllocateObjectPages { page_count: count },
                65536,
                &groups
            )
            .is_err());
            assert_eq!(state(&service), saved);
        }
        assert!(prepare(
            &mut service,
            &MetaRequest::AllocateObjectPages { page_count: 1 },
            1,
            &groups
        )
        .is_err());
        assert_eq!(state(&service), saved);
        let mut pending = VecDeque::new();
        let mut requests = VecDeque::new();
        let mut receivers = Vec::new();
        for count in [1, 4096, 2] {
            let (request, receiver) = queued(
                MetaRequest::AllocateObjectPages { page_count: count },
                Instant::now() + Duration::from_secs(10),
            );
            requests.push_back(request);
            receivers.push(receiver);
        }
        // A caller disconnecting before apply must not make an accepted ID reusable.
        let (request, lost_reply) = queued(
            MetaRequest::AllocateObjectPages { page_count: 1 },
            Instant::now() + Duration::from_secs(10),
        );
        requests.push_back(request);
        drop(lost_reply);
        propose_batch(
            &mut peer,
            &mut service,
            &groups,
            65536,
            &mut requests,
            &mut pending,
        )
        .unwrap();
        assert_eq!(state(&service), saved);
        assert!(receivers
            .iter()
            .all(|rx| matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty))));
        peer.process_ready(|id, bytes| {
            apply_committed(&mut service, &groups, 65536, &mut pending, id, bytes)
        })
        .unwrap();
        for (index, rx) in receivers.into_iter().enumerate() {
            let MetaReply::ObjectPages(allocation) = rx.try_recv().unwrap().unwrap() else {
                panic!("allocation reply missing");
            };
            assert_eq!(allocation.allocation_id, base + index as u64 + 1);
        }
        assert_eq!(service.placement_commit_index, base + 4);
        let snapshot = service.export_snapshot();
        let mut restored = restore_snapshot(snapshot, &groups).unwrap();
        let (_, reply) = prepare(
            &mut restored,
            &MetaRequest::AllocateObjectPages { page_count: 1 },
            65536,
            &groups,
        )
        .unwrap();
        let Some(MetaReply::ObjectPages(allocation)) = reply else {
            panic!("allocation reply missing");
        };
        assert_eq!(allocation.allocation_id, base + 5);
        restored.placement_commit_index = u64::MAX;
        let saved = state(&restored);
        assert!(prepare(
            &mut restored,
            &MetaRequest::AllocateObjectPages { page_count: 1 },
            65536,
            &groups
        )
        .is_err());
        assert_eq!(state(&restored), saved);
    }
}

#[cfg(test)]
mod read_failure_tests {
    use super::*;

    #[test]
    fn metadata_lost_read_leadership_routes_without_masking_other_failures() {
        assert!(matches!(
            read_failure(&RaftError::NotLeader(1)),
            Ok(MetaReply::NotLeader)
        ));
        // Matching text cannot grant permission to route another request.
        for error in [
            RaftError::Storage("node 1 is not the leader".into()),
            RaftError::InvalidRequest("node 1 is not the leader".into()),
            RaftError::Transport("node 1 is not the leader".into()),
            RaftError::NoLeader,
        ] {
            assert!(matches!(
                read_failure(&error),
                Err(ObjectError::InvalidArgument(_))
            ));
        }
    }
}
