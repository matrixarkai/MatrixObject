// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! One durable chunk voter with bounded TCP delivery and committed read barriers.
use super::{apply_to_replica, NodeReplica};
#[cfg(test)]
use crate::cluster::consensus_checkpoint::Image;
use crate::cluster::consensus_checkpoint::{storage, Policy};
use crate::cluster::message::{ChunkGroupIdentity, ReplicaReply, ReplicaRequest};
use crate::cluster::shutdown::ServerShutdown;
use crate::outcome::PageOutcome;
use crate::{MatrixReplicaService, ObjectError};
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions},
    LogId, RaftError,
};
use prost::Message;
use crate::cluster::consensus_inbox::{ConsensusChannels, ConsensusInbox};
use crate::cluster::socket_readiness::Notify;
use std::cell::{Cell, RefCell};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
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
}
struct Pending {
    id: LogId,
    request: Request,
    reply: Option<Result<ReplicaReply, ObjectError>>,
}
pub(super) struct ChunkConsensus {
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
            || options.peers.len() > 7
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
        let directory = options.directory.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("chunk.peer.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
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
            Policy::new(2, wal.clone(), options.checkpoint_after_bytes).map_err(invalid)?;
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
        let group_identity = ChunkGroupIdentity {
            version: 1,
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
        let group_id = hash(&identity).max(1);
        let mut peer = DurablePeer::open(PeerOptions {
            node_id: options.node_id,
            group_id,
            voters: options.peers.keys().copied().collect(),
            wal_path: wal,
        })
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
        handles.push(thread::spawn(move || {
            crate::cluster::peer_receiver::receive(
                listener,
                incoming,
                &inbound_stop,
                NETWORK_TIMEOUT,
                inbound_wake,
            );
        }));
        let mut outgoing = BTreeMap::new();
        for (id, address) in options
            .peers
            .iter()
            .filter(|(id, _)| **id != options.node_id)
        {
            let (tx, rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::sync_channel(8);
            outgoing.insert(*id, tx);
            let address = *address;
            let stop = Arc::clone(&stop);
            handles.push(thread::spawn(move || {
                while !stop.is_stopped() {
                    let bytes = match rx.recv_timeout(NETWORK_TIMEOUT) {
                        Ok(bytes) => bytes,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    {
                        if let Ok(mut stream) =
                            TcpStream::connect_timeout(&address, NETWORK_TIMEOUT)
                        {
                            let deadline = Instant::now() + NETWORK_TIMEOUT;
                            let _ = stream.set_nodelay(true);
                            if write_until(
                                &mut stream,
                                &(bytes.len() as u32).to_be_bytes(),
                                deadline,
                            )
                            .is_ok()
                            {
                                let _ = write_until(&mut stream, &bytes, deadline);
                            }
                        }
                    }
                }
            }));
        }
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let worker_stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            let _lock = lock;
            if let Err(error) = run(
                &mut peer,
                &state,
                ConsensusChannels {
                    requests: rx,
                    packets,
                    outgoing,
                },
                &worker_stop,
                limit,
                checkpoint,
            ) {
                if let Ok(mut node) = state.lock() {
                    node.failed = true;
                }
                worker_failed.store(true, Ordering::SeqCst);
                eprintln!("chunk consensus stopped: {error}");
                worker_stop.stop();
            }
        }));
        Ok(Self {
            identity: group_identity,
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
    pub(super) fn request(&self, request: ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        if matches!(request, ReplicaRequest::GroupIdentity) {
            if self.is_failed() {
                return Err(refused("chunk consensus unavailable"));
            }
            return Ok(ReplicaReply::GroupIdentity(self.identity.clone()));
        }
        let (reply, rx) = mpsc::sync_channel(1);
        let deadline = Instant::now() + self.timeout;
        self.requests
            .try_send(Request {
                request,
                reply,
                deadline,
            })
            .map_err(|_| refused("chunk consensus unavailable or request queue full"))?;
        if let Some(wake) = &self.wake {
            wake.notify();
        }
        rx.recv_timeout(self.timeout)
            .map_err(|_| refused("chunk consensus deadline; write outcome may be committed"))?
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
) -> Result<(), RaftError> {
    let ConsensusChannels {
        requests,
        packets,
        outgoing,
    } = channels;
    let mut tick = Instant::now();
    let mut pending: Option<Pending> = None;
    let mut pending_read: Option<(u64, Vec<Request>)> = None;
    let mut deferred = None;
    let applied_term = Cell::new(0);
    while !stop.is_stopped() {
        let mut admitted = false;
        for _ in 0..32 {
            match packets.try_recv() {
                Ok(bytes) => {
                    let _ = peer.step(&bytes);
                }
                Err(_) => break,
            }
        }
        if tick.elapsed() >= TICK {
            peer.tick()?;
            tick = Instant::now();
        }
        let mut node = state
            .lock()
            .map_err(|_| RaftError::Storage("chunk state poisoned".into()))?;
        let application = RefCell::new(&mut *node);
        let output = peer.process_ready_with_snapshot_reader(
            |id, bytes| {
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
                Ok(())
            },
        )?;
        drop(application);
        for packet in output {
            if let Some(tx) = outgoing.get(&packet.to) {
                let _ = tx.try_send(packet.bytes);
            }
        }
        if pending
            .as_ref()
            .is_some_and(|p| !peer.is_leader() || p.id.term != peer.term())
        {
            let p = pending.take().unwrap();
            let _ = p.request.reply.try_send(Err(refused(
                "chunk leadership changed; write outcome may be committed",
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
                                checked_apply(&mut node.service, request.request, max_pages, limit)
                            }
                            Err(error) => Err(refused(error)),
                            Ok(false) => unreachable!(),
                        };
                        let _ = request.reply.try_send(reply);
                    }
                }
            }
        }
        // A timed-out accepted proposal remains pending until applied or leadership changes.
        // Proposing a second outcome against the same old state would be invalid.
        if pending.is_none() && pending_read.is_none() {
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
                        match peer.begin_read() {
                            Ok(token) => pending_read = Some((token, batch)),
                            Err(error) => {
                                for request in batch {
                                    let _ = request.reply.try_send(Err(refused(&error)));
                                }
                            }
                        }
                    } else {
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
    snapshot: crate::ReplicaServiceSnapshot,
) -> Result<(), RaftError> {
    if snapshot.capacity_bytes != node.service.capacity_bytes()
        || snapshot.pages.len() > node.max_pages
    {
        return Err(storage(
            "chunk checkpoint exceeds or changes configured limits",
        ));
    }
    let mut service = MatrixReplicaService::new();
    service.install_snapshot(snapshot).map_err(storage)?;
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
        ReplicaRequest::ReadChunk { .. }
        | ReplicaRequest::ReadChunkStateRange { .. }
        | ReplicaRequest::HeadChunk { .. }
        | ReplicaRequest::PageInventory { .. }
        | ReplicaRequest::Ping
        | ReplicaRequest::GroupIdentity => true,
        ReplicaRequest::WriteChunk { .. }
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
        Err(ObjectError::NotFound(_)) if before.is_none() => return Ok((Vec::new(), Some(reply))),
        Err(e) => return Err(e),
    };
    let outcome = PageOutcome::between(page, before.as_ref(), &after)?
        .with_counters(counters, PageOutcome::counters(&candidate))
        .to_payload()?;
    Ok((outcome, Some(reply)))
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "peer packet deadline"))
}
fn write_until(stream: &mut TcpStream, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        let n = stream.write(bytes)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "incomplete peer packet",
            ));
        }
        bytes = &bytes[n..];
    }
    Ok(())
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
            max_pages,
            used_bytes: 0,
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
