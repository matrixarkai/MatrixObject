// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One replica, in its own process, behind a socket.
//!
//! # WHAT THIS CHANGES ABOUT THE STORE
//!
//! Every replica in this repository was an element of a `Vec` on the service that wrote to
//! it. Three replicas were three `BTreeMap`s on one heap, the fan-out was a `for` loop over
//! a borrowed slice, and a write quorum was a count of loop iterations that returned `Ok`.
//! That arrangement pays the full cost of three copies and buys none of what three copies
//! are for: one process ending takes all three with it.
//!
//! A [`ReplicaNodeServer`] is the other arrangement. It owns ONE [`MatrixReplicaService`]
//! and nothing else can reach it except over the socket. Three of them are three processes
//! with three address spaces, and one of them ending leaves the other two holding the data.
//!
//! # THE STATE MACHINE IS THE EXISTING ONE
//!
//! This server adds no storage logic. `write_chunk`, `read_chunk`, `head_chunk` and
//! `discard_chunk` are the replica's own methods, called with the arguments the frame
//! carried. The page layout, the write sequence, the checksum, the capacity refusals and the
//! read cache are all unchanged, which is the point: what moved is where the state lives,
//! not what it is.
//!
//! # ONE LOCK, AND WHY THAT IS NOT A LIMITATION HERE
//!
//! The replica sits behind a `Mutex`, so concurrent connections to one node serialise. For
//! this shape that costs nothing worth recovering: a replica node's work per request is a
//! `BTreeMap` lookup and a `copy_from_slice`, and the coordinator fanning out to it makes
//! one call at a time per page anyway. Accept threads still earn their keep -- they stop one
//! slow client from making the node look dead to the others.
//!
//! # IT IS UNAUTHENTICATED, AND THE DEFAULT BIND SAYS SO
//!
//! There is no signing, no token and no TLS, exactly as for [`crate::net`]. Anything that
//! can open a socket to this port can read and overwrite every page this replica holds.
//! [`ReplicaNodeServer::bind`] does not choose the address; the binary that calls it
//! defaults to loopback.

use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::shutdown::ServerShutdown;

use crate::cluster::message::{
    check_stamp, encode_failure, encode_reply, ReplicaReply, ReplicaRequest,
};
use crate::cluster::wire::{RequestCode, StatusCode, HEADER_BYTES, MAX_PAYLOAD_BYTES};
use crate::{MatrixReplicaService, ObjectError};
use super::replica_journal::ReplicaJournal;

struct NodeReplica {
    service: MatrixReplicaService,
    journal: Option<ReplicaJournal>,
    failed: bool,
}

/// How a replica node is sized and bounded.
#[derive(Clone, Debug)]
pub struct ReplicaNodeOptions {
    /// Largest payload this node will read from a frame.
    ///
    /// A length field arrives before the bytes it describes, so this is the bound on what
    /// one frame can make the node allocate.
    pub max_payload_bytes: usize,
    /// Connections handled at once.
    pub worker_threads: usize,
    /// How long a connection may go without sending before it is dropped.
    pub read_timeout: Duration,
    /// How long a write to a peer may block.
    pub write_timeout: Duration,
}

impl Default for ReplicaNodeOptions {
    fn default() -> Self {
        Self {
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            worker_threads: 4,
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
struct NodeCounters {
    requests: AtomicU64,
    failures: AtomicU64,
}

/// A bound listener and the one replica behind it.
pub struct ReplicaNodeServer {
    listener: TcpListener,
    replica: Arc<Mutex<NodeReplica>>,
    options: ReplicaNodeOptions,
    counters: Arc<NodeCounters>,
    stop: Arc<ServerShutdown>,
}

impl ReplicaNodeServer {
    /// Bind `addr` and serve a fresh in-memory replica.
    pub fn bind(addr: &str, options: ReplicaNodeOptions) -> std::io::Result<Self> {
        Self::bind_storage(addr, options, None)
    }

    /// Replay a locked journal and sync every successful mutation before replying.
    /// The parent directory must already exist. This does not repair missed writes
    /// from other nodes or establish membership after a restart.
    pub fn bind_with_journal(addr: &str, options: ReplicaNodeOptions, path: &std::path::Path) -> std::io::Result<Self> {
        Self::bind_storage(addr, options, Some(path))
    }

    fn bind_storage(addr: &str, options: ReplicaNodeOptions, path: Option<&std::path::Path>) -> std::io::Result<Self> {
        let mut service = MatrixReplicaService::new();
        let journal = path.map(|path| ReplicaJournal::open(path, &mut service)).transpose()?;
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            replica: Arc::new(Mutex::new(NodeReplica { service, journal, failed: false })),
            options,
            counters: Arc::new(NodeCounters::default()),
            stop: Arc::new(ServerShutdown::default()),
        })
    }

    /// The address actually bound.
    ///
    /// Asked for rather than assumed, because a node told to bind port 0 does not know its
    /// own port until the kernel has chosen one -- which is how a test stands three of these
    /// up without picking numbers and hoping.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// A handle that stops accept loops and closes active connections.
    pub fn shutdown_handle(&self) -> ReplicaNodeShutdown {
        ReplicaNodeShutdown {
            stop: Arc::clone(&self.stop),
        }
    }

    /// Accept and serve until the shutdown handle is used.
    pub fn serve(self) -> std::io::Result<()> {
        let worker_threads = self.options.worker_threads.max(1);
        let mut handles = Vec::with_capacity(worker_threads);
        for _ in 0..worker_threads {
            let listener = self.listener.try_clone()?;
            let replica = Arc::clone(&self.replica);
            let counters = Arc::clone(&self.counters);
            let stop = Arc::clone(&self.stop);
            let options = self.options.clone();
            handles.push(std::thread::spawn(move || {
                accept_loop(listener, replica, counters, stop, options);
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

/// Stops a running [`ReplicaNodeServer`].
#[derive(Clone, Debug)]
pub struct ReplicaNodeShutdown {
    stop: Arc<ServerShutdown>,
}

impl ReplicaNodeShutdown {
    pub fn stop(&self) {
        self.stop.stop();
    }
}

fn accept_loop(
    listener: TcpListener,
    replica: Arc<Mutex<NodeReplica>>,
    counters: Arc<NodeCounters>,
    stop: Arc<ServerShutdown>,
    options: ReplicaNodeOptions,
) {
    while !stop.is_stopped() {
        match listener.accept() {
            Ok((stream, _peer)) => {
                let _active = match stop.track(&stream) {
                    Ok(Some(active)) => active,
                    Ok(None) => return,
                    Err(_) => continue,
                };
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                let _ = stream.set_read_timeout(Some(options.read_timeout));
                let _ = stream.set_write_timeout(Some(options.write_timeout));
                // TCP_NODELAY: a replica call is one small frame and the answer is wanted
                // before the next one is sent, so Nagle has nothing to coalesce and only
                // adds latency to every write in the fan-out.
                let _ = stream.set_nodelay(true);
                serve_connection(stream, &replica, &counters, &options);
            }
            Err(_) => {
                // Nonblocking accept lets idle workers observe shutdown. Back off on
                // all accept errors so resource exhaustion cannot create a busy loop.
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

/// Serve one connection until the peer closes it or a frame cannot be read.
///
/// A connection is PERSISTENT: a coordinator opens one socket per replica and sends every
/// request down it. That is what makes the established-socket count a usable statement about
/// the cluster -- a connection per request would leave nothing to look at between calls.
fn serve_connection(
    stream: TcpStream,
    replica: &Arc<Mutex<NodeReplica>>,
    counters: &Arc<NodeCounters>,
    options: &ReplicaNodeOptions,
) {
    let mut writer = match stream.try_clone() {
        Ok(writer) => writer,
        Err(_) => return,
    };
    let mut reader = BufReader::new(stream);
    loop {
        let mut header = [0u8; HEADER_BYTES];
        if reader.read_exact(&mut header).is_err() {
            // The peer closed, or went away without closing. Either way this connection is
            // done and the node keeps serving the others.
            return;
        }
        counters.requests.fetch_add(1, Ordering::Relaxed);
        let frame = match read_frame(&mut reader, &header, options.max_payload_bytes) {
            Ok(frame) => frame,
            Err(err) => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                let _ = writer.write_all(&encode_failure(
                    StatusCode::for_error(&err),
                    &err.to_string(),
                ));
                let _ = writer.flush();
                // A frame that could not be read leaves the stream at an unknown offset, so
                // there is no honest way to look for the next header in it. The error is
                // reported and the connection ends.
                return;
            }
        };
        let (code, payload) = frame;
        let response = match RequestCode::from_u8(code) {
            None => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                encode_failure(
                    StatusCode::UnknownRequest,
                    &format!("request code {code} is not one this replica implements"),
                )
            }
            Some(code) => match ReplicaRequest::decode(code, &payload) {
                Err(err) => {
                    counters.failures.fetch_add(1, Ordering::Relaxed);
                    encode_failure(StatusCode::for_error(&err), &err.to_string())
                }
                Ok(request) => match apply(replica, request) {
                    Ok(reply) => encode_reply(&reply),
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
    let payload_len =
        u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    // CHECKED BEFORE THE ALLOCATION, not after the read. The length is four bytes from a
    // socket; without this the frame chooses how much memory this process asks for.
    if payload_len > max_payload_bytes {
        return Err(ObjectError::InvalidArgument(format!(
            "a replica frame declares a {payload_len}-byte payload, past the {max_payload_bytes}-byte bound"
        )));
    }
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).map_err(|err| {
        ObjectError::Corruption(format!(
            "a replica frame declared {payload_len} byte(s) and the connection ended first: {err}"
        ))
    })?;
    Ok((code, payload))
}

/// Run one request against the replica.
///
/// Every arm is a call into [`MatrixReplicaService`]. Nothing here decides anything about
/// storage -- a refusal from the replica travels back as the replica's own error, with its
/// own message, so a caller reading the failure learns what the store said and not what this
/// file thought about it.
fn apply(
    replica: &Arc<Mutex<NodeReplica>>,
    request: ReplicaRequest,
) -> Result<ReplicaReply, ObjectError> {
    let mut guard = replica.lock().map_err(|_| {
        // A poisoned lock means a previous request panicked while holding the replica, so
        // its state may be half-applied. Refusing is the only honest answer: continuing
        // would serve reads out of a page whose invariants were never re-established.
        ObjectError::Corruption(
            "this replica's state was left locked by a request that panicked, so it can no \
             longer be trusted to answer"
                .to_string(),
        )
    })?;
    if guard.failed {
        return Err(ObjectError::Corruption("replica journal failed; restart and recover before serving requests".to_string()));
    }
    let record = if guard.journal.is_some() && matches!(request, ReplicaRequest::WriteChunk { .. } | ReplicaRequest::DiscardChunk { .. }) {
        Some(ReplicaJournal::encode(&request).map_err(|err| ObjectError::InvalidArgument(err.to_string()))?)
    } else { None };
    let reply = apply_to_replica(&mut guard.service, request)?;
    if let Some(record) = record {
        if let Err(err) = guard.journal.as_mut().unwrap().append(&record) {
            // The mutation may be in memory or partly on disk. Never expose that
            // uncertain state to another request, and never acknowledge this write.
            guard.failed = true;
            return Err(ObjectError::Corruption(format!("replica journal append failed: {err}")));
        }
    }
    Ok(reply)
}

pub(super) fn apply_to_replica(guard: &mut MatrixReplicaService, request: ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
    match request {
        ReplicaRequest::WriteChunk {
            page_id,
            offset,
            data,
        } => {
            guard.write_chunk(page_id, offset, &data)?;
            Ok(ReplicaReply::Applied)
        }
        ReplicaRequest::ReadChunk {
            page_id,
            offset,
            length,
        } => Ok(ReplicaReply::Read(guard.read_chunk(
            &page_id, offset, length,
        )?)),
        ReplicaRequest::HeadChunk { page_id } => {
            Ok(ReplicaReply::Head(guard.head_chunk(&page_id)?))
        }
        ReplicaRequest::DiscardChunk {
            page_id,
            offset,
            length,
        } => {
            guard.discard_chunk(&page_id, offset, length)?;
            Ok(ReplicaReply::Applied)
        }
        ReplicaRequest::Ping => Ok(ReplicaReply::Pong {
            page_count: guard.page_count() as u64,
        }),
        ReplicaRequest::PageInventory { after, limit } => {
            crate::cluster::message::check_inventory_limit(limit)?;
            let mut entries = guard.page_write_sequences_after(after.as_ref(), limit as usize + 1);
            let has_more = entries.len() > limit as usize;
            entries.truncate(limit as usize);
            Ok(ReplicaReply::PageInventory(crate::cluster::message::PageInventory {
                entries, has_more,
            }))
        }
    }
}

#[cfg(test)]
mod journal_failure_tests {
    use super::*;
    use crate::{ChunkId, PageId};

    #[test]
    fn failed_persistence_blocks_acknowledgment_and_all_later_requests() {
        let path = std::env::temp_dir().join(format!("replica-readonly-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::write(&path, []).unwrap();
        let replica = Arc::new(Mutex::new(NodeReplica {
            service: MatrixReplicaService::new(),
            journal: Some(ReplicaJournal::read_only_for_test(&path)),
            failed: false,
        }));
        let page = PageId { chunk_id: ChunkId::new(1, 2).unwrap(), page_index: 3 };
        let write = ReplicaRequest::WriteChunk { page_id: page.clone(), offset: 0, data: b"uncommitted".to_vec() };
        assert!(apply(&replica, write.clone()).is_err());
        assert!(replica.lock().unwrap().failed);
        for request in [write, ReplicaRequest::Ping, ReplicaRequest::HeadChunk { page_id: page }] {
            assert!(apply(&replica, request).is_err());
        }
        drop(replica);
        let mut recovered = MatrixReplicaService::new();
        drop(ReplicaJournal::open(&path, &mut recovered).unwrap());
        assert_eq!(recovered.page_count(), 0);
        std::fs::remove_file(path).unwrap();
    }
}
