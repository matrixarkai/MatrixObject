// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! One durable chunk voter with bounded TCP delivery and committed read barriers.
use super::{apply_to_replica, NodeReplica};
#[cfg(test)]
use crate::cluster::consensus_checkpoint::Image;
use crate::cluster::consensus_checkpoint::{storage, Policy};
use crate::cluster::consensus_inbox::{
    ConsensusChannels, ConsensusInbox, RequestPhase, RequestProgress, WorkerPhase, WorkerProgress,
};
use crate::cluster::socket_readiness::Notify;
use crate::cluster::message::{ChunkGroupIdentity, ReplicaReply, ReplicaRequest};
use crate::cluster::shutdown::ServerShutdown;
use crate::outcome::PageOutcome;
use crate::{MatrixReplicaService, ObjectError};
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions},
    LogId, RaftError,
};
use prost::Message;
use std::cell::{Cell, RefCell};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
const TICK: Duration = Duration::from_millis(100);
const NETWORK_TIMEOUT: Duration = Duration::from_millis(100);

/// Fixed voter endpoints. Each process uses its own existing state directory.
#[derive(Clone, Debug)]
pub struct ChunkRaftOptions {
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
#[derive(Clone, PartialEq, Message)]
struct Bootstrap {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    capacity_bytes: u64,
    #[prost(uint64, tag = "3")]
    max_pages: u64,
}
struct Request {
    request: ReplicaRequest,
    reply: SyncSender<Result<ReplicaReply, ObjectError>>,
    deadline: Instant,
    progress: RequestProgress,
}
struct Pending {
    id: LogId,
    request: Request,
    reply: Option<Result<ReplicaReply, ObjectError>>,
}
pub(super) struct ChunkConsensus {
    progress: WorkerProgress,
    identity: ChunkGroupIdentity,
    requests: SyncSender<Request>,
    wake: Option<Notify>,
    timeout: Duration,
    stop: Arc<ServerShutdown>,
    failed: Arc<AtomicBool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}
fn canonical_address(address: SocketAddr) -> SocketAddr {
    match address {
        SocketAddr::V6(v) => v
            .ip()
            .to_ipv4_mapped()
            .map(|ip| SocketAddr::new(ip.into(), v.port()))
            .unwrap_or(address),
        _ => address,
    }
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
fn read_failure(error: &RaftError) -> Result<ReplicaReply, ObjectError> {
    match error {
        RaftError::NotLeader(_) => Ok(ReplicaReply::NotLeader),
        _ => Err(refused(error)),
    }
}
fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}
impl ChunkConsensus {
    pub(super) fn open(
        options: ChunkRaftOptions,
        state: Arc<Mutex<NodeReplica>>,
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
            || options.peers.values().any(|a| {
                let a = canonical_address(*a);
                a.port() == 0 || a.ip().is_unspecified() || a.ip().is_multicast()
            })
            || options
                .peers
                .values()
                .map(|a| canonical_address(*a))
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != options.peers.len()
        {
            return Err(invalid("invalid chunk voter configuration"));
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
            return Err(invalid("invalid chunk bootstrap voters"));
        }
        let transport = crate::cluster::peer_tls::PeerTransport::load(options.peer_tls.as_ref())?;
        let directory = options.directory.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("chunk.peer.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        transport.pin_directory(&directory)?;
        let configuration = {
            let node = state.lock().map_err(|_| invalid("chunk state poisoned"))?;
            if node.journal.is_some() || node.service.page_count() != 0 {
                return Err(invalid(
                    "Raft requires an empty replica without a local journal",
                ));
            }
            Bootstrap {
                version: 1,
                capacity_bytes: node.service.capacity_bytes(),
                max_pages: node.max_pages as u64,
            }
        };
        let base = directory.join("chunk.peer.base");
        let wal = directory.join("chunk.peer.wal");
        let checkpoint =
            Policy::new(2, options.checkpoint_after_bytes).map_err(invalid)?;
        if base.exists() != wal.exists() {
            return Err(invalid("incomplete chunk peer storage"));
        }
        if base.exists() && wal.metadata()?.len() <= 24 {
            return Err(invalid("chunk peer WAL is empty or truncated"));
        }
        let mut expected = b"MOCB".to_vec();
        expected.extend(configuration.encode_to_vec());
        let bytes = if base.exists() {
            let mut file = File::open(&base)?;
            if file.metadata()?.len() > 1024 {
                return Err(invalid("chunk bootstrap too large"));
            }
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            if bytes != expected {
                return Err(invalid("chunk bootstrap configuration changed or damaged"));
            }
            bytes
        } else {
            expected
        };
        let mut group_identity = ChunkGroupIdentity {
            bootstrap_voters: options.bootstrap_voters.clone().unwrap_or_default(),
            bootstrap_id: 0,
            version: if options.bootstrap_voters.is_some() {
                2
            } else {
                1
            },
            group_id: options.group_id,
            node_id: options.node_id,
            capacity_bytes: configuration.capacity_bytes,
            max_pages: configuration.max_pages,
            voters: options.peers.keys().copied().collect(),
            peer_endpoints: options
                .peers
                .values()
                .map(|a| canonical_address(*a).to_string())
                .collect(),
        };
        let mut identity = b"chunk-consensus".to_vec();
        identity.extend_from_slice(&bytes);
        identity.extend_from_slice(&options.group_id.to_be_bytes());
        if options.bootstrap_voters.is_some() {
            identity.extend_from_slice(b"membership-v1");
            identity.extend_from_slice(&(voters.len() as u64).to_be_bytes());
            for id in &voters {
                identity.extend_from_slice(&id.to_be_bytes());
            }
            for (id, endpoint) in options.peers.keys().zip(&group_identity.peer_endpoints) {
                identity.extend_from_slice(&id.to_be_bytes());
                identity.extend_from_slice(&(endpoint.len() as u64).to_be_bytes());
                identity.extend_from_slice(endpoint.as_bytes());
            }
        }
        let group_id = hash(&identity).max(1);
        if options.bootstrap_voters.is_some() {
            group_identity.bootstrap_id = group_id;
        }
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
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let worker_stop = Arc::clone(&stop);
        let progress = WorkerProgress::new();
        let worker_progress = progress.clone();
        #[cfg(feature = "meta-server")]
        let track_membership = group_identity.version == 2;
        handles.push(thread::spawn(move || {
            let _lock = lock;
            if let Err(error) = run(
                &mut peer,
                &state,
                ConsensusChannels {
                    progress: worker_progress.clone(),
                    requests: rx,
                    packets,
                    outgoing,
                },
                &worker_stop,
                limit,
                checkpoint,
                #[cfg(feature = "meta-server")]
                track_membership,
            ) {
                if let Ok(mut node) = state.lock() {
                    node.failed = true;
                }
                worker_failed.store(true, Ordering::SeqCst);
                eprintln!("chunk consensus stopped: {error}");
                worker_stop.stop();
            }
            worker_progress.mark(WorkerPhase::Stopped);
        }));
        Ok(Self {
            progress,
            identity: group_identity,
            requests,
            wake,
            timeout: options.request_timeout,
            stop,
            failed,
            handles: Mutex::new(handles),
        })
    }
    #[cfg(feature = "meta-server")]
    pub(super) fn identity(&self) -> &ChunkGroupIdentity {
        &self.identity
    }

    pub(super) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }
    pub(super) fn request(&self, request: ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        if let ReplicaRequest::Membership(command) = &request {
            command.validate()?;
            if self.identity.version != 2 || command.bootstrap_id != self.identity.bootstrap_id {
                return Err(refused("membership mode or group identity mismatch"));
            }
        }
        if matches!(request, ReplicaRequest::GroupIdentity) {
            if self.is_failed() {
                return Err(refused("chunk consensus unavailable"));
            }
            return Ok(ReplicaReply::GroupIdentity(self.identity.clone()));
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
            .map_err(|_| refused("chunk consensus unavailable or request queue full"))?;
        if let Some(wake) = &self.wake {
            wake.notify();
        }
        progress.wait_observed(&rx, self.timeout, "chunk", Some(&self.progress))
    }
}
impl Drop for ChunkConsensus {
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
    state: &Arc<Mutex<NodeReplica>>,
    channels: ConsensusChannels<Request>,
    stop: &ServerShutdown,
    limit: usize,
    mut checkpoint: Policy,
    #[cfg(feature = "meta-server")] track_membership: bool,
) -> Result<(), RaftError> {
    let ConsensusChannels {
        progress,
        requests,
        packets,
        outgoing,
    } = channels;
    let mut tick = Instant::now();
    let mut pending: Option<Pending> = None;
    let mut pending_read: Option<(u64, Vec<Request>)> = None;
    let mut deferred = None;
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
        progress.mark(WorkerPhase::StateLock);
        let mut node = state
            .lock()
            .map_err(|_| RaftError::Storage("chunk state poisoned".into()))?;
        let application = RefCell::new(&mut *node);
        progress.mark(WorkerPhase::ApplyReady);
        let output = peer.process_ready_with_snapshot_reader(
            |id, bytes| {
                let _observation = progress.applying_outcome();
                let mut node = application.borrow_mut();
                if !bytes.is_empty() {
                    let entry =
                        Entry::decode(bytes).map_err(|e| RaftError::Storage(e.to_string()))?;
                    if entry.version != 1 || entry.encode_to_vec() != bytes {
                        return Err(RaftError::Storage("invalid chunk consensus entry".into()));
                    }
                    if !entry.outcome.is_empty() {
                        let outcome = PageOutcome::from_payload(&entry.outcome)
                            .map_err(|e| RaftError::Storage(e.to_string()))?;
                        let page = outcome
                            .page_id()
                            .map_err(|e| RaftError::Storage(e.to_string()))?;
                        let before = node.service.page_physical_len(&page);
                        let max_pages = node.max_pages;
                        outcome
                            .apply(&mut node.service, max_pages)
                            .map_err(|e| RaftError::Storage(e.to_string()))?;
                        node.used_bytes = node
                            .used_bytes
                            .checked_sub(before)
                            .and_then(|n| n.checked_add(node.service.page_physical_len(&page)))
                            .ok_or_else(|| {
                                RaftError::Storage("chunk usage accounting overflow".into())
                            })?;
                    }
                }
                applied_term.set(id.term);
                if pending.as_ref().is_some_and(|p| p.id == id) {
                    let p = pending.take().unwrap();
                    let reply = match p.reply {
                        Some(reply) => reply,
                        None => {
                            let max_pages = node.max_pages;
                            checked_apply(&mut node.service, p.request.request, max_pages, limit)
                        }
                    };
                    let _ = p.request.reply.try_send(reply);
                }
                Ok(())
            },
            |id, reader, length| {
                progress.mark(WorkerPhase::RestoreCheckpoint);
                let mut node = application.borrow_mut();
                let snapshot = crate::cluster::chunk_checkpoint::restore(
                    reader,
                    length,
                    id.clone(),
                    node.service.capacity_bytes(),
                    node.max_pages,
                )?;
                restore_snapshot(&mut node, snapshot)?;
                applied_term.set(id.term);
                checkpoint.restored(id.index);
                progress.mark(WorkerPhase::ApplyReady);
                Ok(())
            },
        )?;
        progress.mark(WorkerPhase::Requests);
        drop(application);
        // Capture only after Ready has durably applied data and membership. The
        // heartbeat worker reads this view and usage under the same state lock.
        #[cfg(feature = "meta-server")]
        if track_membership {
            match &mut node.applied_membership {
                Some((status, refreshed))
                    if status.term == peer.term()
                        && status.applied_index == peer.applied_index() =>
                {
                    *refreshed = Instant::now();
                }
                _ => {
                    let status = crate::cluster::MembershipStatus::from_peer(peer);
                    node.applied_membership = if status.validate().is_ok() {
                        Some((status, Instant::now()))
                    } else {
                        None
                    };
                }
            }
        }
        recovered = true;
        if let Some((id, _)) = &pending_membership {
            if peer.term() == id.term && peer.applied_index() >= id.index {
                let (_, request) = pending_membership.take().unwrap();
                let _ = request.reply.try_send(Ok(ReplicaReply::Membership(
                    crate::cluster::MembershipStatus::from_peer(peer),
                )));
            } else if !peer.is_leader() || peer.term() != id.term {
                let (_, request) = pending_membership.take().unwrap();
                let _ = request.reply.try_send(Err(refused(
                    "chunk leadership changed; membership outcome may be committed",
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
            .as_ref()
            .is_some_and(|p| !peer.is_leader() || p.id.term != peer.term())
        {
            let p = pending.take().unwrap();
            let _ = p.request.reply.try_send(Err(refused(
                format!("chunk leadership changed; write outcome may be committed{}", progress.describe()),
            )));
        }
        if let Some((token, _)) = &pending_read {
            match peer.poll_read(*token) {
                Ok(false) => {}
                result => {
                    let (_, batch) = pending_read.take().unwrap();
                    for request in batch {
                        let reply = match &result {
                            Ok(true) => {
                                let max_pages = node.max_pages;
                                match request.request {
                                    ReplicaRequest::Membership(_) => Ok(ReplicaReply::Membership(
                                        crate::cluster::MembershipStatus::from_peer(peer),
                                    )),
                                    other => {
                                        checked_apply(&mut node.service, other, max_pages, limit)
                                    }
                                }
                            }
                            Err(error) => read_failure(error),
                            Ok(false) => unreachable!(),
                        };
                        let _ = request.reply.try_send(reply);
                    }
                }
            }
        }
        // A timed-out accepted proposal remains pending until applied or leadership changes.
        // Proposing a second outcome against the same old state would be invalid.
        if pending.is_none() && pending_read.is_none() && pending_membership.is_none() {
            progress.mark(WorkerPhase::PublishCheckpoint);
            checkpoint.maybe_checkpoint_stream(peer, applied_term.get(), |id| {
                let mut snapshot = node.service.export_snapshot_unsealed();
                Ok(move || {
                    snapshot.seal_for_checkpoint();
                    crate::cluster::chunk_checkpoint::capture(snapshot, id).map(|stream| {
                        stream.map(|s| {
                            let length = s.length;
                            (s, length)
                        })
                    })
                })
            })?;
            progress.mark(WorkerPhase::Requests);
            if let Some(request) = deferred.take().or_else(|| requests.try_recv().ok()) {
                admitted = true;
                if request.deadline <= Instant::now() {
                    let _ = request
                        .reply
                        .try_send(Err(refused("chunk request expired before proposal")));
                } else if !peer.is_leader() || applied_term.get() != peer.term() {
                    let _ = request.reply.try_send(Ok(ReplicaReply::NotLeader));
                } else {
                    if read_only(&request.request) {
                        let batch =
                            read_batch(request, requests.receiver(), &mut deferred, Instant::now());
                        for request in &batch {
                            request.progress.mark(RequestPhase::ReadConfirmation);
                        }
                        match peer.begin_read() {
                            Ok(token) => pending_read = Some((token, batch)),
                            Err(error) => {
                                for request in batch {
                                    let _ = request.reply.try_send(read_failure(&error));
                                }
                            }
                        }
                    } else if let ReplicaRequest::Membership(command) = &request.request {
                        request.progress.mark(RequestPhase::ProposalCompletion);
                        match command
                            .change()
                            .and_then(|change| peer.propose_membership(change).map_err(refused))
                        {
                            Ok(id) => pending_membership = Some((id, request)),
                            Err(error) => {
                                let _ = request.reply.try_send(Err(error));
                            }
                        }
                    } else {
                        request.progress.mark(RequestPhase::Preparing);
                        let (outcome, reply) =
                            match prepare(&node.service, &request.request, node.max_pages, limit) {
                                Ok((outcome, reply)) => (outcome, reply.map(Ok)),
                                Err(error) => (Vec::new(), Some(Err(error))),
                            };
                        let payload = Entry {
                            version: 1,
                            outcome,
                        }
                        .encode_to_vec();
                        request.progress.mark(RequestPhase::ProposalCompletion);
                        match peer.propose(payload) {
                            Ok(id) => pending = Some(Pending { id, request, reply }),
                            Err(e) => {
                                let _ = request.reply.try_send(Err(refused(e)));
                            }
                        }
                    }
                }
            }
        }
        drop(node);
        // Process newly proposed local work before sleeping. Peer packets and
        // requests signal the inbox; the next Raft tick bounds all idle waits.
        if !admitted {
            progress.mark(WorkerPhase::Idle);
            requests.wait(stop, TICK.saturating_sub(tick.elapsed()));
        }
    }
    Ok(())
}

// Admission ends before begin_read: later arrivals cannot reuse this confirmation.
// Keep at most one deferred mutation so reads never pass a queued write.
fn read_batch(
    first: Request,
    incoming: &Receiver<Request>,
    deferred: &mut Option<Request>,
    now: Instant,
) -> Vec<Request> {
    debug_assert!(read_only(&first.request));
    debug_assert!(deferred.is_none());
    let mut batch = vec![first];
    for _ in 1..32 {
        let Ok(request) = incoming.try_recv() else {
            break;
        };
        if !read_only(&request.request) {
            *deferred = Some(request);
            break;
        }
        if request.deadline <= now {
            let _ = request.reply.try_send(Err(refused(
                "chunk request expired before read confirmation",
            )));
        } else {
            batch.push(request);
        }
    }
    batch
}

#[cfg(test)]
fn restore_checkpoint(node: &mut NodeReplica, bytes: &[u8], id: LogId) -> Result<(), RaftError> {
    let snapshot = crate::cluster::chunk_checkpoint::restore(
        &mut std::io::Cursor::new(bytes),
        bytes.len() as u64,
        id,
        node.service.capacity_bytes(),
        node.max_pages,
    )?;
    restore_snapshot(node, snapshot)
}
fn restore_snapshot(
    node: &mut NodeReplica,
    snapshot: crate::ValidatedReplicaServiceSnapshot,
) -> Result<(), RaftError> {
    if snapshot.snapshot().capacity_bytes != node.service.capacity_bytes()
        || snapshot.snapshot().pages.len() > node.max_pages
    {
        return Err(storage(
            "chunk checkpoint exceeds or changes configured limits",
        ));
    }
    let mut service = MatrixReplicaService::new();
    service
        .install_validated_snapshot(snapshot)
        .map_err(storage)?;
    let used_bytes = service.physical_bytes();
    if used_bytes > node.service.capacity_bytes() {
        return Err(storage("chunk checkpoint exceeds configured byte capacity"));
    }
    node.service = service;
    node.used_bytes = used_bytes;
    Ok(())
}
fn read_only(request: &ReplicaRequest) -> bool {
    match request {
        ReplicaRequest::Membership(command) => command.is_read(),
        ReplicaRequest::ReadChunk { .. }
        | ReplicaRequest::ReadChunkStateRange { .. }
        | ReplicaRequest::HeadChunk { .. }
        | ReplicaRequest::PageInventory { .. }
        | ReplicaRequest::Ping
        | ReplicaRequest::GroupIdentity => true,
        ReplicaRequest::StateInstall(_)
        | ReplicaRequest::WriteChunk { .. }
        | ReplicaRequest::WriteChunkIfSequence { .. }
        | ReplicaRequest::DiscardChunk { .. }
        | ReplicaRequest::DiscardChunkIfSequence { .. } => false,
    }
}
fn checked_apply(
    service: &mut MatrixReplicaService,
    request: ReplicaRequest,
    max_pages: usize,
    limit: usize,
) -> Result<ReplicaReply, ObjectError> {
    if let ReplicaRequest::ReadChunkStateRange { length, .. } = &request {
        let size = usize::try_from(*length)
            .unwrap_or(usize::MAX)
            .saturating_add(crate::cluster::state_transfer::RANGE_OVERHEAD);
        crate::cluster::message::check_reply_size(size, limit)?;
    }
    if let ReplicaRequest::ReadChunk { length, .. } = &request {
        let size = usize::try_from(*length)
            .unwrap_or(usize::MAX)
            .saturating_add(40);
        crate::cluster::message::check_reply_size(size, limit)?;
    }
    let reply = apply_to_replica(service, request, max_pages)?;
    crate::cluster::message::check_reply_size(reply.payload_len(), limit)?;
    Ok(reply)
}
fn prepare(
    service: &MatrixReplicaService,
    request: &ReplicaRequest,
    max_pages: usize,
    limit: usize,
) -> Result<(Vec<u8>, Option<ReplicaReply>), ObjectError> {
    if matches!(request, ReplicaRequest::StateInstall(_)) {
        return Err(ObjectError::PreconditionFailed(
            "standalone state installation is unavailable on Raft nodes".into(),
        ));
    }
    let page = match request {
        ReplicaRequest::WriteChunk { page_id, .. }
        | ReplicaRequest::WriteChunkIfSequence { page_id, .. }
        | ReplicaRequest::DiscardChunkIfSequence { page_id, .. }
        | ReplicaRequest::DiscardChunk { page_id, .. } => page_id,
        _ => return Ok((Vec::new(), None)),
    };
    let before = match service.read_chunk_state(page) {
        Ok(state) => Some(state),
        Err(ObjectError::NotFound(_)) => None,
        Err(e) => return Err(e),
    };
    let counters = PageOutcome::counters(service);
    let mut candidate = service.clone_for_raft_preparation();
    let reply = checked_apply(&mut candidate, request.clone(), max_pages, limit)?;
    let after = match candidate.read_chunk_state(page) {
        Ok(state) => state,
        Err(ObjectError::NotFound(_)) if before.is_none() => return Ok((Vec::new(), Some(reply))),
        Err(e) => return Err(e),
    };
    let outcome = PageOutcome::between(page, before.as_ref(), &after)?
        .with_counters(counters, PageOutcome::counters(&candidate))
        .to_payload()?;
    Ok((outcome, Some(reply)))
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    fn node(capacity: u64, max_pages: usize) -> NodeReplica {
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(capacity).unwrap();
        NodeReplica {
            service,
            journal: None,
            journal_checkpoint_after_bytes: 0,
            staged: None,
            maintenance: false,
            max_pages,
            used_bytes: 0,
            #[cfg(all(feature = "chunk-consensus", feature = "meta-server"))]
            applied_membership: None,
            failed: false,
        }
    }
    #[test]
    fn checkpoint_restores_pages_discards_and_usage_without_changing_limits() {
        let mut original = node(8192, 4);
        for index in 0..2 {
            let page = crate::PageId {
                chunk_id: crate::ChunkId::new(1, 2).unwrap(),
                page_index: index,
            };
            apply_to_replica(
                &mut original.service,
                ReplicaRequest::WriteChunk {
                    page_id: page.clone(),
                    offset: 128,
                    data: vec![index as u8 + 1; 256],
                },
                4,
            )
            .unwrap();
            apply_to_replica(
                &mut original.service,
                ReplicaRequest::DiscardChunk {
                    page_id: page,
                    offset: 0,
                    length: 16,
                },
                4,
            )
            .unwrap();
        }
        let id = LogId { index: 17, term: 4 };
        let snapshot = original.service.export_snapshot().to_bytes().unwrap();
        let bytes = Image::new(2, id.clone(), snapshot.clone(), Vec::new()).encode_to_vec();
        let mut streamed = Vec::new();
        crate::cluster::chunk_checkpoint::capture(original.service.export_snapshot(), id.clone())
            .unwrap()
            .unwrap()
            .read_to_end(&mut streamed)
            .unwrap();
        for bytes in [&bytes, &streamed] {
            let mut restored = node(8192, 4);
            restore_checkpoint(&mut restored, bytes, id.clone()).unwrap();
            assert_eq!(
                restored.service.export_snapshot().to_bytes().unwrap(),
                snapshot
            );
            assert_eq!(restored.used_bytes, 768);
            for (capacity, pages) in [(4096, 4), (8192, 1)] {
                let mut refused_node = node(capacity, pages);
                let before = refused_node.service.export_snapshot().to_bytes().unwrap();
                assert!(restore_checkpoint(&mut refused_node, bytes, id.clone()).is_err());
                assert_eq!(
                    refused_node.service.export_snapshot().to_bytes().unwrap(),
                    before
                );
                assert_eq!(refused_node.used_bytes, 0);
            }
        }
        let mut restored = node(8192, 4);
        let unexpected = Image::new(2, id.clone(), snapshot, vec![1]).encode_to_vec();
        assert!(restore_checkpoint(&mut restored, &unexpected, id).is_err());
    }
}

#[cfg(test)]
mod read_batch_tests {
    use super::*;

    #[test]
    fn chunk_read_batches_bound_admission_preserve_write_order_and_expire_requests() {
        let now = Instant::now();
        let make = |request, deadline| {
            let (reply, response) = mpsc::sync_channel(1);
            (
                Request {
                    progress: RequestProgress::new(read_only(&request)),
                    request,
                    reply,
                    deadline,
                },
                response,
            )
        };
        let live = now + Duration::from_secs(5);
        let (tx, rx) = mpsc::sync_channel(64);
        let (expired, response) = make(ReplicaRequest::Ping, now);
        tx.send(expired).unwrap();
        tx.send(make(ReplicaRequest::Ping, live).0).unwrap();
        let mutation = ReplicaRequest::WriteChunk {
            page_id: crate::PageId {
                chunk_id: crate::ChunkId::new(1, 1).unwrap(),
                page_index: 0,
            },
            offset: 0,
            data: vec![7],
        };
        tx.send(make(mutation.clone(), live).0).unwrap();
        tx.send(make(ReplicaRequest::Ping, live).0).unwrap();
        let mut deferred = None;
        let batch = read_batch(make(ReplicaRequest::Ping, live).0, &rx, &mut deferred, now);
        assert_eq!(batch.len(), 2);
        assert!(response.try_recv().unwrap().is_err());
        assert_eq!(deferred.take().unwrap().request, mutation);
        assert!(matches!(
            rx.try_recv().unwrap().request,
            ReplicaRequest::Ping
        ));
        assert!(rx.try_recv().is_err());
        for _ in 0..40 {
            tx.send(make(ReplicaRequest::Ping, live).0).unwrap();
        }
        let batch = read_batch(make(ReplicaRequest::Ping, live).0, &rx, &mut deferred, now);
        assert_eq!(batch.len(), 32);
        assert!(deferred.is_none());
        // An arrival after admission stays queued for a later confirmation.
        tx.send(make(ReplicaRequest::Ping, live).0).unwrap();
        assert_eq!(rx.try_iter().count(), 10);
        assert_eq!(batch.len(), 32);
    }
}

#[cfg(test)]
mod read_failure_tests {
    use super::*;

    #[test]
    fn chunk_lost_read_leadership_routes_without_masking_other_failures() {
        assert!(matches!(
            read_failure(&RaftError::NotLeader(1)),
            Ok(ReplicaReply::NotLeader)
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

#[cfg(test)]
mod preparation_tests {
    use super::*;
    fn prepare_with_full_clone(
        service: &MatrixReplicaService,
        request: &ReplicaRequest,
        max_pages: usize,
        limit: usize,
    ) -> Result<(Vec<u8>, Option<ReplicaReply>), ObjectError> {
        if matches!(request, ReplicaRequest::StateInstall(_)) {
            return Err(ObjectError::PreconditionFailed(
                "standalone state installation is unavailable on Raft nodes".into(),
            ));
        }
        let page = match request {
            ReplicaRequest::WriteChunk { page_id, .. }
            | ReplicaRequest::WriteChunkIfSequence { page_id, .. }
            | ReplicaRequest::DiscardChunkIfSequence { page_id, .. }
            | ReplicaRequest::DiscardChunk { page_id, .. } => page_id,
            _ => return Ok((Vec::new(), None)),
        };
        let before = match service.read_chunk_state(page) {
            Ok(state) => Some(state),
            Err(ObjectError::NotFound(_)) => None,
            Err(e) => return Err(e),
        };
        let counters = PageOutcome::counters(service);
        let mut candidate = service.clone();
        let reply = checked_apply(&mut candidate, request.clone(), max_pages, limit)?;
        let after = match candidate.read_chunk_state(page) {
            Ok(state) => state,
            Err(ObjectError::NotFound(_)) if before.is_none() => {
                return Ok((Vec::new(), Some(reply)))
            }
            Err(e) => return Err(e),
        };
        let outcome = PageOutcome::between(page, before.as_ref(), &after)?
            .with_counters(counters, PageOutcome::counters(&candidate))
            .to_payload()?;
        Ok((outcome, Some(reply)))
    }

    #[test]
    fn raft_preparation_skips_runtime_caches_without_changing_outcomes() {
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(65536).unwrap();
        service.set_read_cache_max_bytes(65536);
        service.set_read_cache_max_cacheable_read_bytes(1024);
        let page = |index| crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: index,
        };
        for i in 0..32 {
            service
                .write_chunk(page(i), 0, &vec![i as u8; 1024])
                .unwrap();
            service.read_chunk(&page(i), 0, 1024).unwrap();
        }
        let requests = vec![
            ReplicaRequest::WriteChunk {
                page_id: page(0),
                offset: 17,
                data: vec![9; 31],
            },
            ReplicaRequest::WriteChunk {
                page_id: page(40),
                offset: 0,
                data: vec![7; 1024],
            },
            ReplicaRequest::WriteChunk {
                page_id: page(40),
                offset: 0,
                data: vec![7; 65536],
            },
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page(0),
                offset: 0,
                data: vec![8],
                expected_sequence: 1,
            },
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page(0),
                offset: 0,
                data: vec![8],
                expected_sequence: 0,
            },
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page(40),
                offset: 0,
                data: vec![8],
                expected_sequence: 0,
            },
            ReplicaRequest::DiscardChunk {
                page_id: page(0),
                offset: 0,
                length: 32,
            },
            ReplicaRequest::DiscardChunk {
                page_id: page(40),
                offset: 0,
                length: 32,
            },
            ReplicaRequest::DiscardChunkIfSequence {
                page_id: page(0),
                offset: 0,
                length: 32,
                expected_sequence: 1,
            },
            ReplicaRequest::DiscardChunkIfSequence {
                page_id: page(0),
                offset: 0,
                length: 32,
                expected_sequence: 9,
            },
        ];
        for mode in 0..5 {
            let frozen = mode == 1;
            for max_pages in [32, 64] {
                for limit in [16, 1 << 20] {
                    for request in &requests {
                        let mut actual = service.clone();
                        actual.configure_service_mode(None, None, Some(frozen));
                        match mode {
                            2 => actual.configure_service_mode(Some(false), None, None),
                            3 => {
                                actual
                                    .configure_mutation_qos(crate::ReplicaMutationQosConfig {
                                        max_write_bytes_per_window: 1,
                                        max_discard_bytes_per_window: 1,
                                        ..Default::default()
                                    })
                                    .unwrap();
                            }
                            4 => actual.corrupt_chunk_payload_for_repair(&page(0)).unwrap(),
                            _ => {}
                        }

                        let expected = actual.clone();
                        let left = prepare(&actual, request, max_pages, limit)
                            .map_err(|error| error.to_string());
                        let right = prepare_with_full_clone(&expected, request, max_pages, limit)
                            .map_err(|error| error.to_string());
                        assert_eq!(
                            left, right,
                            "{request:?} frozen={frozen} pages={max_pages} limit={limit}"
                        );
                        assert_eq!(
                            actual, expected,
                            "speculation must preserve identical live state"
                        );
                    }
                }
            }
        }
    }
    #[test]
    #[ignore = "bounded local write preparation timing; run explicitly in release mode"]
    fn measure_raft_preparation_with_warm_read_caches() {
        use std::time::Instant;
        for count in [64, 1024, 4096] {
            let mut service = MatrixReplicaService::new();
            service.set_capacity_bytes(128 * 1024 * 1024).unwrap();
            service.set_read_cache_max_bytes(32 * 1024 * 1024);
            service.set_read_cache_max_cacheable_read_bytes(4096);
            let bytes: Vec<u8> = (0..4096)
                .map(|n| ((n * 31 + n / 251) % 251) as u8)
                .collect();
            let page = |index| crate::PageId {
                chunk_id: crate::ChunkId::new(1, 1).unwrap(),
                page_index: index,
            };
            for i in 0..count {
                service.write_chunk(page(i), 0, &bytes).unwrap();
                service.read_chunk(&page(i), 0, 4096).unwrap();
            }
            assert_eq!(service.read_cache_stats().entries, count);
            let request = ReplicaRequest::WriteChunkIfSequence {
                page_id: page(0),
                offset: 17,
                data: vec![7; 32],
                expected_sequence: 1,
            };
            let mut full = Vec::new();
            let mut candidate = Vec::new();
            for _ in 0..30 {
                let began = Instant::now();
                let expected = prepare_with_full_clone(&service, &request, 8192, 1 << 20).unwrap();
                full.push(began.elapsed().as_nanos());
                let began = Instant::now();
                let actual = prepare(&service, &request, 8192, 1 << 20).unwrap();
                candidate.push(began.elapsed().as_nanos());
                assert_eq!(actual, expected);
            }
            full.sort();
            candidate.sort();
            println!("PREPARATION pages={count} cached_bytes={} full_p50_ns={} full_p95_ns={} candidate_p50_ns={} candidate_p95_ns={}",
                service.read_cache_stats().bytes, full[15], full[28], candidate[15], candidate[28]);
            assert_eq!(service.read_cache_stats().entries, count);
        }
    }
}
