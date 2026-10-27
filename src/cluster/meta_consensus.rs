// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! One durable metadata voter with bounded TCP delivery and committed read barriers.
use super::meta_message::{MetaReply, MetaRequest};
use super::meta_server::apply_to_meta;
use super::shutdown::ServerShutdown;
use crate::placement_outcome::PlacementOutcome;
use crate::{MatrixMetaService, MetaSnapshot, ObjectError};
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions},
    LogId, RaftError,
};
use prost::Message;
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
const PACKET_LIMIT: usize = 8 * 1024 * 1024;
const TICK: Duration = Duration::from_millis(100);
const NETWORK_TIMEOUT: Duration = Duration::from_millis(100);

/// Fixed voter endpoints. Each process uses its own existing state directory.
#[derive(Clone, Debug)]
pub struct MetaRaftOptions {
    pub node_id: u64,
    pub group_id: u64,
    pub peers: BTreeMap<u64, SocketAddr>,
    pub directory: PathBuf,
    pub request_timeout: Duration,
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
}
struct Pending {
    id: LogId,
    request: Request,
    reply: Option<Result<MetaReply, ObjectError>>,
}
pub(super) struct MetaConsensus {
    pub(super) node_count: usize,
    requests: SyncSender<Request>,
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
fn hash(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}
impl MetaConsensus {
    pub(super) fn open(
        options: MetaRaftOptions,
        mut service: MatrixMetaService,
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
        let directory = options.directory.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("metadata.peer.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        if directory.join("metadata.snapshot").exists()
            || directory.join("metadata.outcomes").exists()
        {
            return Err(invalid(
                "local metadata files cannot be opened as a Raft group",
            ));
        }
        let base = directory.join("metadata.peer.base");
        let wal = directory.join("metadata.peer.wal");
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
            service.export_snapshot().to_bytes().map_err(invalid)?
        };
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(invalid("metadata bootstrap too large"));
        }
        let snapshot = MetaSnapshot::from_bytes(&bytes).map_err(invalid)?;
        service = MatrixMetaService::new();
        service
            .install_placement_roster(&snapshot)
            .map_err(invalid)?;
        service
            .install_raft_snapshot_bytes(&bytes, 0)
            .map_err(invalid)?;
        // Different bootstrap images cannot form one voting group.
        let mut identity = bytes.clone();
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
        let (requests, rx) = mpsc::sync_channel(32);
        let (incoming, packets) = mpsc::sync_channel(8);
        let mut handles = Vec::new();
        let inbound_stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while !inbound_stop.is_stopped() {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let deadline = Instant::now() + NETWORK_TIMEOUT;
                        let mut header = [0; 4];
                        if read_until(&mut stream, &mut header, deadline).is_err() {
                            continue;
                        }
                        let len = u32::from_be_bytes(header) as usize;
                        if len == 0 || len > PACKET_LIMIT {
                            continue;
                        }
                        let mut bytes = vec![0; len];
                        if read_until(&mut stream, &mut bytes, deadline).is_ok() {
                            let _ = incoming.try_send(bytes);
                        }
                    }
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
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
        let node_count = service.nodes.len();
        let failed = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::clone(&failed);
        let worker_stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            let _lock = lock;
            if let Err(error) = run(
                &mut peer,
                &mut service,
                rx,
                packets,
                outgoing,
                &worker_stop,
                limit,
            ) {
                worker_failed.store(true, Ordering::SeqCst);
                eprintln!("metadata consensus stopped: {error}");
                worker_stop.stop();
            }
        }));
        Ok(Self {
            node_count,
            requests,
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
        let (reply, rx) = mpsc::sync_channel(1);
        let deadline = Instant::now() + self.timeout;
        self.requests
            .try_send(Request {
                request,
                reply,
                deadline,
            })
            .map_err(|_| refused("metadata consensus unavailable or request queue full"))?;
        rx.recv_timeout(self.timeout)
            .map_err(|_| refused("metadata consensus deadline; write outcome may be committed"))?
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
    requests: Receiver<Request>,
    packets: Receiver<Vec<u8>>,
    outgoing: BTreeMap<u64, SyncSender<Vec<u8>>>,
    stop: &ServerShutdown,
    limit: usize,
) -> Result<(), RaftError> {
    let mut tick = Instant::now();
    let mut pending: Option<Pending> = None;
    let mut applied_term = 0;
    while !stop.is_stopped() {
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
        let output = peer.process_ready(|id, bytes| {
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
            applied_term = id.term;
            if pending.as_ref().is_some_and(|p| p.id == id) {
                let p = pending.take().unwrap();
                let reply = match p.reply {
                    Some(reply) => reply,
                    None => apply_to_meta(service, p.request.request, limit),
                };
                let _ = p.request.reply.try_send(reply);
            }
            Ok(())
        })?;
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
                "metadata leadership changed; write outcome may be committed",
            )));
        }
        // A timed-out accepted proposal remains pending until applied or leadership changes.
        // Proposing a second outcome against the same old state would be invalid.
        if pending.is_none() {
            if let Ok(request) = requests.try_recv() {
                if request.deadline <= Instant::now() {
                    let _ = request
                        .reply
                        .try_send(Err(refused("metadata request expired before proposal")));
                } else if !peer.is_leader() || applied_term != peer.term() {
                    let _ = request.reply.try_send(Err(refused(format!(
                        "metadata leader unavailable; leader={:?}",
                        peer.leader_id()
                    ))));
                } else {
                    let (outcome, reply) = match prepare(service, &request.request, limit) {
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
        thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}
fn prepare(
    service: &MatrixMetaService,
    request: &MetaRequest,
    limit: usize,
) -> Result<(Vec<u8>, Option<MetaReply>), ObjectError> {
    let (nodes, object) = match request {
        MetaRequest::HeartbeatRootNode { node_id, .. } => (vec![node_id.clone()], None),
        MetaRequest::ExpireRootNodeHeartbeats { .. } => {
            (service.nodes.keys().cloned().collect(), None)
        }
        MetaRequest::CommitObjectPlacement { placement, .. } => {
            let object = super::meta_message::object_id(&placement.bucket, &placement.key);
            let mut nodes = placement.node_ids.clone();
            if let Some(previous) = service.placements.get(&object) {
                nodes.extend(previous.node_ids.iter().cloned());
            }
            (nodes, Some(object))
        }
        _ => return Ok((Vec::new(), None)),
    };
    let before = PlacementOutcome::capture(service, nodes, object)?;
    let mut candidate = service.clone();
    let reply = apply_to_meta(&mut candidate, request.clone(), limit)?;
    Ok((before.finish(&candidate)?.to_payload()?, Some(reply)))
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "peer packet deadline"))
}
fn read_until(stream: &mut TcpStream, mut bytes: &mut [u8], deadline: Instant) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_read_timeout(Some(remaining(deadline)?))?;
        let n = stream.read(bytes)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete peer packet",
            ));
        }
        bytes = &mut bytes[n..];
    }
    Ok(())
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
