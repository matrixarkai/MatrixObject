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
//! Full-residency replicas delegate page operations to `MatrixReplicaService`.
//! Segmented replicas can bound resident page vectors while retaining complete
//! durable metadata. Cold read plans own their immutable source files before
//! payload I/O leaves the node mutex. Mutations remain serialized through sync.
//!
//!
//! # SERIAL MUTATION AND DURABLE COMPLETION
//!
//! One service lock protects mutation, accounting and the durable completion point.
//! Journal-backed standalone nodes can share a journal synchronization across
//! requests already offered by bounded executors. Readers and repair cannot see
//! those mutations before synchronization finishes. Checkpoints and general
//! mutation shapes can still hold this lock for work proportional to stored data.
//!
//! # IT IS UNAUTHENTICATED, AND THE DEFAULT BIND SAYS SO
//!
//! There is no signing, no token and no TLS, exactly as for [`crate::net`]. Anything that
//! can open a socket to this port can read and overwrite every page this replica holds.
//! [`ReplicaNodeServer::bind`] does not choose the address; the binary that calls it
//! defaults to loopback.

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::shutdown::ServerShutdown;
#[path = "replica_journal_maintenance.rs"]
mod journal_maintenance;
#[path = "replica_health.rs"]
mod replica_health;
use replica_health::{ReplicaHealth, Used};

use super::replica_journal::ReplicaJournal;
use super::replica_journal_backend::ReplicaJournalBackend;
use super::replica_segmented_journal::SegmentedReplicaJournal;
use crate::cluster::message::{encode_reply, ReplicaReply, ReplicaRequest};
use crate::cluster::wire::{RequestCode, StatusCode, MAX_PAYLOAD_BYTES};
use crate::{MatrixReplicaService, ObjectError};

#[path = "star_group_commit.rs"]
mod star_group_commit;

#[cfg(feature = "meta-server")]
#[path = "server_heartbeat.rs"]
mod heartbeat;
#[cfg(feature = "meta-server")]
pub use heartbeat::ReplicaHeartbeatOptions;

#[cfg(feature = "chunk-consensus")]
#[path = "chunk_consensus.rs"]
mod consensus;
#[cfg(feature = "chunk-consensus")]
pub use consensus::ChunkRaftOptions;

#[path = "replica_cold_reads.rs"]
mod cold_reads;
#[path = "replica_resident_cache.rs"]
mod resident_cache;
use resident_cache::ResidentCache;

struct NodeReplica {
    service: MatrixReplicaService,
    resident: Option<ResidentCache>,
    journal: Option<Box<dyn ReplicaJournalBackend>>,
    journal_checkpoint_after_bytes: u64,
    staged: Option<super::state_install::StagedState>,
    max_pages: usize,
    /// Every change is published to `health`, which the heartbeat reads without the lock.
    used_bytes: Used,
    /// What the heartbeat reports: failure, maintenance, capacity, usage and the applied
    /// membership view. Written under this lock, read without it.
    health: Arc<ReplicaHealth>,
    /// Log chunk files, beside the journal. None for a node without durable storage.
    log_chunks: Option<super::log_chunk_store::LogChunkStore>,
}

/// How a replica node is sized and bounded.
#[derive(Clone, Debug)]
pub struct ReplicaNodeOptions {
    /// Optional preloaded mutual-TLS identity required for API clients.
    pub tls: Option<super::ApiTlsConfig>,
    /// Standalone only: refuse ordinary writes/discards, retain reads and repair,
    /// and report offline for metadata placement. Fixed for this server lifetime.
    pub maintenance: bool,
    /// Keep local XOR recovery data. Defaults to true for standalone/single-copy
    /// callers. Configured replicated Star deployments can use peer repair instead;
    /// changing this does not change journal sync, checksums or client write quorum.
    pub local_recovery_sidecar: bool,
    /// Opt-in bounded deferred completion for standalone journal group commits.
    /// Frees request executors while durability is pending; defaults to false.
    pub journal_deferred_completion: bool,
    /// Largest payload this node will read from a frame.
    ///
    /// A length field arrives before the bytes it describes, so this is the bound on what
    /// one frame can make the node allocate.
    /// Also bounds outgoing payloads, including failure messages; minimum 24 bytes.
    pub max_payload_bytes: usize,
    /// Maximum total page payload bytes, including zero-filled space before write offsets.
    /// This excludes indexes, caches, sidecars, and transport buffers. Must be positive.
    pub capacity_bytes: u64,
    /// Resident page-vector allocation budget. Zero keeps full residency.
    /// Requires segmented standalone storage and peer recovery. Excludes bounded
    /// request, repair, response-cache and one-page mutation working buffers.
    pub resident_payload_bytes: u64,
    /// Resident identities, including empty pages. Must be positive.
    pub resident_page_entries: usize,
    /// Maximum page identities, including pages created by empty writes. Must be positive.
    pub max_pages: usize,
    /// Request executors. By default each has an I/O loop for up to 128 connections.
    pub worker_threads: usize,
    /// Deadline for receiving a complete frame, including initial idle time.
    pub read_timeout: Duration,
    /// Deadline for sending a complete response.
    pub write_timeout: Duration,
    /// Zero-fill chunk for the standalone journal, 4 KiB through 512 MiB; `None` keeps the
    /// original layout. See [`wal_zero_fill_from_env`](crate::cluster::wal_zero_fill_from_env).
    pub wal_zero_fill_bytes: Option<u64>,
}

impl Default for ReplicaNodeOptions {
    fn default() -> Self {
        Self {
            tls: None,
            maintenance: false,
            local_recovery_sidecar: true,
            journal_deferred_completion: false,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            capacity_bytes: 256 * 1024 * 1024,
            resident_payload_bytes: 0,
            resident_page_entries: 4096,
            max_pages: 65_536,
            worker_threads: 4,
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            wal_zero_fill_bytes: None,
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

/// Physical segment sizing and amortized background retirement for the opt-in journal.
#[derive(Debug, Clone, Copy)]
pub struct ReplicaSegmentedJournalOptions {
    /// Sector-aligned payload capacity from 24 through 512 MiB.
    pub segment_capacity_bytes: u64,
    /// Minimum retained segment count that starts background discovery.
    pub retention_segment_threshold: usize,
}
impl Default for ReplicaSegmentedJournalOptions {
    fn default() -> Self {
        Self {
            segment_capacity_bytes: crate::durable_payload::DEFAULT_CAPACITY_BYTES,
            retention_segment_threshold: 16,
        }
    }
}

/// A bound listener and the one replica behind it.
pub struct ReplicaNodeServer {
    listener: TcpListener,
    has_local_journal: bool,
    has_background_journal: bool,
    replica: Arc<Mutex<NodeReplica>>,
    /// The heartbeat's view of `replica`, readable while a request holds the replica lock.
    #[cfg_attr(not(feature = "meta-server"), allow(dead_code))]
    health: Arc<ReplicaHealth>,
    options: ReplicaNodeOptions,
    io_threads: Option<usize>,
    counters: Arc<NodeCounters>,
    stop: Arc<ServerShutdown>,
    #[cfg(feature = "meta-server")]
    heartbeat: Option<ReplicaHeartbeatOptions>,
    #[cfg(feature = "chunk-consensus")]
    consensus: Option<Arc<consensus::ChunkConsensus>>,
}

impl ReplicaNodeServer {
    /// Set I/O loops independently of request executors, retaining 128 connection
    /// slots and one payload byte budget per loop. Must be positive and no greater
    /// than the request executor count. The default keeps one loop per executor.
    pub fn set_io_threads(&mut self, count: usize) -> std::io::Result<()> {
        if count == 0 || count > self.options.worker_threads.max(1) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "I/O loops must be positive and no greater than request executors",
            ));
        }
        self.io_threads = Some(count);
        Ok(())
    }

    /// Bind `addr` and serve a fresh in-memory replica.
    pub fn bind(addr: &str, options: ReplicaNodeOptions) -> std::io::Result<Self> {
        Self::bind_storage_layout(addr, options, None, None)
    }

    /// Replay a locked journal and sync every successful mutation before replying.
    /// The parent directory must already exist. This does not repair missed writes
    /// from other nodes or establish membership after a restart.
    pub fn bind_with_journal(
        addr: &str,
        options: ReplicaNodeOptions,
        path: &std::path::Path,
    ) -> std::io::Result<Self> {
        Self::bind_storage_layout(addr, options, Some(path), None)
    }

    /// Use immutable outcome segments and asynchronous metadata-only checkpoints.
    /// This opt-in layout requires its own fresh path. Checkpoint maintenance retires unreachable sealed segments.
    pub fn bind_with_segmented_journal(
        addr: &str,
        options: ReplicaNodeOptions,
        path: &std::path::Path,
    ) -> std::io::Result<Self> {
        Self::bind_with_segmented_journal_options(
            addr,
            options,
            path,
            ReplicaSegmentedJournalOptions::default(),
        )
    }
    /// Use explicit segment capacity and the minimum collection trigger.
    pub fn bind_with_segmented_journal_options(
        addr: &str,
        options: ReplicaNodeOptions,
        path: &std::path::Path,
        journal: ReplicaSegmentedJournalOptions,
    ) -> std::io::Result<Self> {
        if options.wal_zero_fill_bytes.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "segmented outcomes cannot use zero-filled journal frames",
            ));
        }
        Self::bind_storage_layout(addr, options, Some(path), Some(journal))
    }

    /// Compact a standalone journal after this many new outcome bytes. Unix only.
    /// A checkpoint keeps complete page state; it does not repair remote replicas.
    pub fn enable_journal_checkpoints(&mut self, interval: u64) -> std::io::Result<()> {
        if !cfg!(unix) || !(1024..=512 * 1024 * 1024).contains(&interval) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "journal checkpoints require Unix and an interval of 1024..536870912 bytes",
            ));
        }
        let mut replica = self
            .replica
            .lock()
            .map_err(|_| std::io::Error::other("replica lock poisoned"))?;
        if replica.journal.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "journal checkpoints require standalone journal storage",
            ));
        }
        replica.journal_checkpoint_after_bytes = interval;
        Ok(())
    }

    fn bind_storage_layout(
        addr: &str,
        options: ReplicaNodeOptions,
        path: Option<&std::path::Path>,
        segmented: Option<ReplicaSegmentedJournalOptions>,
    ) -> std::io::Result<Self> {
        crate::cluster::message::validate_payload_limit(options.max_payload_bytes)?;
        if options.capacity_bytes == 0 || options.max_pages == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "replica capacity_bytes and max_pages must be positive",
            ));
        }
        if options.resident_payload_bytes != 0
            && (segmented.is_none()
                || options.local_recovery_sidecar
                || options.resident_page_entries == 0)
        {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                "bounded page residency requires segmented standalone storage, peer recovery and positive entry limit"));
        }
        if !options.local_recovery_sidecar && path.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "peer recovery requires a durable standalone replica journal",
            ));
        }
        let mut service = MatrixReplicaService::new();
        service.set_keep_ec_sidecar(options.local_recovery_sidecar);
        service
            .set_capacity_bytes(options.capacity_bytes)
            .map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string())
            })?;
        let mut journal: Option<Box<dyn ReplicaJournalBackend>> = path
            .map(|path| -> std::io::Result<Box<dyn ReplicaJournalBackend>> {
                if let Some(journal) = segmented {
                    let open = if options.resident_payload_bytes != 0 {
                        SegmentedReplicaJournal::open_with_page_backing
                    } else {
                        SegmentedReplicaJournal::open_with_settings
                    };
                    Ok(Box::new(open(
                        path,
                        &mut service,
                        options.max_pages,
                        journal.segment_capacity_bytes,
                        journal.retention_segment_threshold,
                    )?))
                } else {
                    Ok(Box::new(ReplicaJournal::open_with_options(
                        path,
                        &mut service,
                        options.max_pages,
                        options.wal_zero_fill_bytes.unwrap_or(0),
                    )?))
                }
            })
            .transpose()?;
        // A restored checkpoint also restores its old runtime recovery policy.
        // Apply this deployment's explicit policy before exposing the listener.
        service
            .configure_journal_recovery(options.local_recovery_sidecar)
            .map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
            })?;
        let resident = if options.resident_payload_bytes != 0 {
            let backing = journal.as_mut().unwrap().enable_page_backing()?;
            Some(ResidentCache::new(
                backing,
                options.resident_payload_bytes,
                options.resident_page_entries,
            )?)
        } else {
            None
        };
        let used = resident.as_ref().map_or_else(
            || Ok(service.physical_bytes()),
            |cache| cache.backing.totals().map(|(_, bytes)| bytes),
        )?;
        if used > service.capacity_bytes() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "replica catalog exceeds byte capacity",
            ));
        }
        if let Some(path) = path {
            super::api_tls::pin_file(&super::api_tls::journal_pin(path), options.tls.is_some())?;
        }
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let health = ReplicaHealth::new(options.maintenance, service.capacity_bytes(), used);
        health.watch_journal(
            journal
                .as_ref()
                .and_then(|journal| journal.failure_handle()),
        );
        Ok(Self {
            listener,
            has_local_journal: path.is_some(),
            has_background_journal: journal
                .as_ref()
                .is_some_and(|journal| journal.background_maintenance()),
            replica: Arc::new(Mutex::new(NodeReplica {
                used_bytes: Used::new(used, Arc::clone(&health)),
                service,
                resident,
                journal,
                journal_checkpoint_after_bytes: 0,
                staged: None,
                max_pages: options.max_pages,
                health: Arc::clone(&health),
                log_chunks: match path {
                    Some(path) => Some(super::log_chunk_store::LogChunkStore::open(
                        &path
                            .parent()
                            .unwrap_or(std::path::Path::new("."))
                            .join("logchunks"),
                    )?),
                    None => None,
                },
            })),
            health,
            options,
            io_threads: None,
            counters: Arc::new(NodeCounters::default()),
            stop: Arc::new(ServerShutdown::default()),
            #[cfg(feature = "meta-server")]
            heartbeat: None,
            #[cfg(feature = "chunk-consensus")]
            consensus: None,
        })
    }

    /// Enable one independently durable Raft voter before serving any requests.
    #[cfg(feature = "chunk-consensus")]
    pub fn enable_consensus(&mut self, options: ChunkRaftOptions) -> std::io::Result<()> {
        if self.options.resident_payload_bytes != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bounded standalone residency cannot use chunk consensus",
            ));
        }
        if self.options.maintenance {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "maintenance mode is only supported by standalone replicas",
            ));
        }
        if self.consensus.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunk consensus already configured",
            ));
        }
        let directory = options.directory.clone();
        let consensus = consensus::ChunkConsensus::open(
            options,
            Arc::clone(&self.replica),
            Arc::clone(&self.stop),
            self.options.max_payload_bytes,
        )?;
        super::api_tls::pin_file(&directory.join("api.transport"), self.options.tls.is_some())?;
        self.consensus = Some(Arc::new(consensus));
        Ok(())
    }

    /// Enable periodic health reports to an explicitly configured metaserver catalog.
    #[cfg(feature = "meta-server")]
    pub fn enable_heartbeats(&mut self, options: ReplicaHeartbeatOptions) -> std::io::Result<()> {
        options.validate()?;
        if self.heartbeat.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "heartbeats already configured",
            ));
        }
        self.heartbeat = Some(options);
        Ok(())
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
        #[cfg(feature = "meta-server")]
        if let Some(options) = &self.heartbeat {
            #[cfg(feature = "chunk-consensus")]
            let dynamic = self
                .consensus
                .as_ref()
                .is_some_and(|c| c.identity().version == 2);
            #[cfg(not(feature = "chunk-consensus"))]
            let dynamic = false;
            if options.placement_id.is_some() != dynamic {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "fenced heartbeat placement ID is required only for dynamic chunk consensus",
                ));
            }
        }
        #[cfg(feature = "chunk-consensus")]
        let standalone = self.consensus.is_none();
        #[cfg(not(feature = "chunk-consensus"))]
        let standalone = true;
        let group_commit =
            if standalone && self.has_local_journal {
                Some(star_group_commit::Committer::start_with_deferred(
                    Arc::clone(&self.replica),
                    self.options.max_payload_bytes,
                    self.options.journal_deferred_completion,
                )?)
            } else {
                None
            };
        let maintain_journal = standalone && self.has_background_journal;
        let worker_threads = self.options.worker_threads.max(1);
        let io_threads = self.io_threads.unwrap_or(worker_threads);
        let dispatch = super::connections::Dispatch::new(io_threads, 128);
        let mut handles = Vec::with_capacity(io_threads + 1);
        for index in 0..io_threads {
            let executors =
                worker_threads / io_threads + usize::from(index < worker_threads % io_threads);
            let dispatch = dispatch.clone();
            let listener = self.listener.try_clone()?;
            let replica = Arc::clone(&self.replica);
            let counters = Arc::clone(&self.counters);
            let stop = Arc::clone(&self.stop);
            let options = self.options.clone();
            let group_commit = group_commit.clone();
            #[cfg(feature = "chunk-consensus")]
            let consensus = self.consensus.clone();
            handles.push(std::thread::spawn(move || {
                accept_loop(
                    listener,
                    replica,
                    counters,
                    stop,
                    options,
                    executors,
                    dispatch,
                    group_commit,
                    #[cfg(feature = "chunk-consensus")]
                    consensus,
                );
            }));
        }
        if maintain_journal {
            let replica = Arc::clone(&self.replica);
            let stop = Arc::clone(&self.stop);
            handles.push(std::thread::spawn(move || {
                journal_maintenance::run(replica, stop)
            }));
        }
        #[cfg(feature = "meta-server")]
        if let Some(options) = self.heartbeat {
            let replica = Arc::clone(&self.replica);
            let health = Arc::clone(&self.health);
            let stop = Arc::clone(&self.stop);
            #[cfg(feature = "chunk-consensus")]
            let consensus = self.consensus.clone();
            handles.push(std::thread::spawn(move || {
                heartbeat::run(
                    options,
                    replica,
                    health,
                    stop,
                    #[cfg(feature = "chunk-consensus")]
                    consensus,
                )
            }));
        }
        for handle in handles {
            // A worker that panicked has already stopped serving; joining it is how that
            // becomes visible rather than a thread count quietly dropping to zero.
            let _ = handle.join();
        }
        #[cfg(feature = "chunk-consensus")]
        if self.consensus.as_ref().is_some_and(|c| c.is_failed()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "chunk consensus worker failed",
            ));
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
    executors: usize,
    dispatch: super::connections::Dispatch,
    group_commit: Option<Arc<star_group_commit::Committer>>,
    #[cfg(feature = "chunk-consensus")] consensus: Option<Arc<consensus::ChunkConsensus>>,
) {
    let responders = (0..executors)
        .map(|_| {
            |code, payload: Vec<u8>| {
                frame_response(
                    code,
                    payload,
                    &replica,
                    &counters,
                    &options,
                    group_commit.as_ref(),
                    #[cfg(feature = "chunk-consensus")]
                    consensus.as_deref(),
                )
            }
        })
        .collect();
    super::connections::accept_loop_with_owned_deferred_responders(
        listener,
        &counters.requests,
        &counters.failures,
        responders,
        stop,
        super::connections::Options {
            tls: options.tls.clone(),
            max_payload_bytes: options.max_payload_bytes,
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
            connections_per_worker: 128,
        },
        dispatch,
    );
}
fn frame_response(
    code: u8,
    payload: Vec<u8>,
    replica: &Arc<Mutex<NodeReplica>>,
    counters: &Arc<NodeCounters>,
    options: &ReplicaNodeOptions,
    group_commit: Option<&Arc<star_group_commit::Committer>>,
    #[cfg(feature = "chunk-consensus")] consensus: Option<&consensus::ChunkConsensus>,
) -> super::connections::Response {
    let wire_bytes = payload.len();
    let encode_failure = |status, message: &str| {
        crate::cluster::message::encode_bounded_failure(status, message, options.max_payload_bytes)
    };
    super::connections::Response::Immediate(match RequestCode::from_u8(code) {
        None => {
            counters.failures.fetch_add(1, Ordering::Relaxed);
            encode_failure(
                StatusCode::UnknownRequest,
                &format!("request code {code} is not one this replica implements"),
            )
        }
        Some(code) => match ReplicaRequest::decode_owned(code, payload) {
            Err(err) => {
                counters.failures.fetch_add(1, Ordering::Relaxed);
                encode_failure(StatusCode::for_error(&err), &err.to_string())
            }
            Ok(request) => {
                if options.journal_deferred_completion {
                    if let Some(committer) =
                        group_commit.filter(|_| star_group_commit::eligible(&request))
                    {
                        let committer = Arc::clone(committer);
                        let counters = Arc::clone(counters);
                        let max_payload_bytes = options.max_payload_bytes;
                        return super::connections::Response::Deferred(Box::new(
                            move |completion| {
                                committer.submit(request, wire_bytes, move |result| {
                                    completion.finish(encode_frame_result(
                                        result,
                                        &counters,
                                        max_payload_bytes,
                                    ));
                                });
                            },
                        ));
                    }
                }

                let result = if let Some(committer) =
                    group_commit.filter(|_| star_group_commit::eligible(&request))
                {
                    committer.request(request, wire_bytes)
                } else {
                    #[cfg(feature = "chunk-consensus")]
                    let result = match request {
                        ReplicaRequest::ConditionalWriteLimits => {
                            Ok(ReplicaReply::ConditionalWriteLimits {
                                max_writes: if consensus.is_some() { 8 } else { 32 },
                                max_payload_bytes: options
                                    .max_payload_bytes
                                    .min(4 * 1024 * 1024 + 4096)
                                    as u32,
                            })
                        }
                        ReplicaRequest::WideConditionalWriteLimits => {
                            Ok(ReplicaReply::ConditionalWriteLimits {
                                max_writes: if consensus.is_some() { 8 } else { 32 },
                                max_payload_bytes: options.max_payload_bytes.min(
                                    if consensus.is_some() {
                                        4 * 1024 * 1024 + 4096
                                    } else {
                                        MAX_PAYLOAD_BYTES
                                    },
                                ) as u32,
                            })
                        }
                        ReplicaRequest::ReadBatchLimits => Ok(ReplicaReply::ReadBatchLimits {
                            max_reads: if consensus.is_some() { 8 } else { 32 },
                            max_reply_payload_bytes: options.max_payload_bytes.min(
                                if consensus.is_some() {
                                    4 * 1024 * 1024
                                } else {
                                    MAX_PAYLOAD_BYTES
                                },
                            ) as u32,
                        }),
                        ReplicaRequest::WriteChunkBatchIfSequence { writes } => match consensus {
                            Some(c) => c.request_batch(writes),
                            None => apply_batch(replica, writes, options.max_payload_bytes),
                        },
                        ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } => {
                            match consensus {
                                Some(c) => c.request_batch_digests(writes),
                                None => {
                                    apply_batch_digests(replica, writes, options.max_payload_bytes)
                                }
                            }
                        }
                        request => match consensus {
                            Some(c) => c.request(request),
                            None => apply(replica, request, options.max_payload_bytes),
                        },
                    };
                    #[cfg(not(feature = "chunk-consensus"))]
                    let result = match request {
                        ReplicaRequest::ConditionalWriteLimits => {
                            Ok(ReplicaReply::ConditionalWriteLimits {
                                max_writes: 32,
                                max_payload_bytes: options
                                    .max_payload_bytes
                                    .min(4 * 1024 * 1024 + 4096)
                                    as u32,
                            })
                        }
                        ReplicaRequest::WideConditionalWriteLimits => {
                            Ok(ReplicaReply::ConditionalWriteLimits {
                                max_writes: 32,
                                max_payload_bytes: options.max_payload_bytes.min(MAX_PAYLOAD_BYTES)
                                    as u32,
                            })
                        }
                        ReplicaRequest::ReadBatchLimits => Ok(ReplicaReply::ReadBatchLimits {
                            max_reads: 32,
                            max_reply_payload_bytes: options
                                .max_payload_bytes
                                .min(MAX_PAYLOAD_BYTES)
                                as u32,
                        }),
                        ReplicaRequest::WriteChunkBatchIfSequence { writes } => {
                            apply_batch(replica, writes, options.max_payload_bytes)
                        }
                        ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } => {
                            apply_batch_digests(replica, writes, options.max_payload_bytes)
                        }
                        request => apply(replica, request, options.max_payload_bytes),
                    };
                    result
                };
                encode_frame_result(result, counters, options.max_payload_bytes)
            }
        },
    })
}

fn encode_frame_result(
    result: Result<ReplicaReply, ObjectError>,
    counters: &NodeCounters,
    max_payload_bytes: usize,
) -> Vec<u8> {
    match result.and_then(|reply| {
        crate::cluster::message::check_reply_size(reply.payload_len(), max_payload_bytes)?;
        Ok(reply)
    }) {
        Ok(reply) => encode_reply(&reply),
        Err(err) => {
            counters.failures.fetch_add(1, Ordering::Relaxed);
            crate::cluster::message::encode_bounded_failure(
                StatusCode::for_error(&err),
                &err.to_string(),
                max_payload_bytes,
            )
        }
    }
}

fn apply_batch(
    replica: &Arc<Mutex<NodeReplica>>,
    writes: Vec<super::message::ConditionalChunkWrite>,
    max_payload_bytes: usize,
) -> Result<ReplicaReply, ObjectError> {
    if writes.is_empty() {
        return Ok(ReplicaReply::Applied);
    }
    let mut guard = replica.lock().map_err(|_| {
        ObjectError::Corruption(
            "this replica's state was left locked by a request that panicked, so it can no longer be trusted to answer"
                .to_string(),
        )
    })?;
    let result = apply_batch_locked(&mut guard, writes, max_payload_bytes);
    finish_deferred_journal(&mut guard)?;
    result
}

fn apply_batch_locked(
    guard: &mut NodeReplica,
    writes: Vec<super::message::ConditionalChunkWrite>,
    max_payload_bytes: usize,
) -> Result<ReplicaReply, ObjectError> {
    for write in writes {
        if let Err(error) = apply_locked(
            guard,
            ReplicaRequest::WriteChunkIfSequence {
                page_id: write.page_id,
                offset: write.offset,
                data: write.data,
                expected_sequence: write.expected_sequence,
            },
            max_payload_bytes,
            false,
        ) {
            return Err(error);
        }
    }
    Ok(ReplicaReply::Applied)
}

fn apply_batch_digests(
    replica: &Arc<Mutex<NodeReplica>>,
    writes: Vec<super::message::ConditionalChunkWrite>,
    max_payload_bytes: usize,
) -> Result<ReplicaReply, ObjectError> {
    if writes.is_empty() {
        return Ok(ReplicaReply::Digests(Vec::new()));
    }
    let mut guard = replica.lock().map_err(|_| {
        ObjectError::Corruption(
            "this replica's state was left locked by a request that panicked, so it can no longer be trusted to answer"
                .to_string(),
        )
    })?;
    let result = apply_batch_digests_locked(&mut guard, writes, max_payload_bytes);
    finish_deferred_journal(&mut guard)?;
    result
}

fn apply_batch_digests_locked(
    guard: &mut NodeReplica,
    writes: Vec<super::message::ConditionalChunkWrite>,
    max_payload_bytes: usize,
) -> Result<ReplicaReply, ObjectError> {
    let mut digests = Vec::with_capacity(writes.len());
    for write in writes {
        let reply = match apply_locked(
            guard,
            ReplicaRequest::WriteChunkIfSequenceDigest {
                page_id: write.page_id,
                offset: write.offset,
                data: write.data,
                expected_sequence: write.expected_sequence,
            },
            max_payload_bytes,
            false,
        ) {
            Ok(reply) => reply,
            Err(error) => {
                return Err(error);
            }
        };
        match reply {
            ReplicaReply::Digest {
                write_sequence,
                length,
                sha256,
            } => digests.push((write_sequence, length, sha256)),
            other => {
                return Err(ObjectError::Corruption(format!(
                    "unexpected conditional write digest reply: {other:?}"
                )))
            }
        }
    }
    Ok(ReplicaReply::Digests(digests))
}

fn finish_deferred_journal(guard: &mut NodeReplica) -> Result<(), ObjectError> {
    if guard.health.failed() {
        return Err(ObjectError::Corruption(
            "replica failed before journal synchronization; restart required".into(),
        ));
    }
    let Some(journal) = guard.journal.as_mut() else {
        return Ok(());
    };
    if let Err(error) = journal.sync_deferred() {
        guard.health.fail();
        return Err(ObjectError::Corruption(format!(
            "replica outcome persistence failed: {error}"
        )));
    }
    if journal.checkpoint_due(guard.journal_checkpoint_after_bytes) {
        let NodeReplica {
            service, journal, ..
        } = guard;
        if let Err(error) = journal.as_mut().unwrap().checkpoint(service) {
            guard.health.fail();
            return Err(ObjectError::Corruption(format!(
                "replica outcome persistence failed: {error}"
            )));
        }
    }
    guard.trim_residency()?;
    Ok(())
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
    max_payload_bytes: usize,
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
    if guard.resident.is_some() {
        if let Some(captured) = cold_reads::capture(&mut guard, &request, max_payload_bytes)? {
            drop(guard);
            return captured.finish();
        }
    }
    let result = apply_locked(&mut guard, request, max_payload_bytes, true);
    if result.is_err() && guard.resident.is_some() && !guard.health.failed() {
        // A refused cold mutation may have admitted a durable working page.
        // Restore the residency bound before returning the refusal.
        finish_deferred_journal(&mut guard)?;
    }
    result
}

#[cfg(all(test, unix))]
#[path = "replica_append_tests.rs"]
mod append_outcome_tests;

#[cfg(all(test, unix))]
#[path = "log_op_tests.rs"]
mod log_op_tests;

#[cfg(all(test, unix))]
#[path = "log_op_group_tests.rs"]
mod log_op_group_tests;

fn prepare_rpc_append(
    service: &MatrixReplicaService,
    request: &ReplicaRequest,
) -> Result<Option<crate::outcome::PreparedPageAppend>, ObjectError> {
    let (page, offset, data, sequence, state_checksum) = match request {
        ReplicaRequest::WriteChunk {
            page_id,
            offset,
            data,
        } => (page_id, *offset, data, None, None),
        ReplicaRequest::WriteChunkIfSequence {
            page_id,
            offset,
            data,
            expected_sequence,
        } => (page_id, *offset, data, Some(*expected_sequence), None),
        ReplicaRequest::WriteChunkIfSequenceDigest {
            page_id,
            offset,
            data,
            expected_sequence,
        } if data.len() <= 1024 * 1024 => (page_id, *offset, data, Some(*expected_sequence), None),
        ReplicaRequest::WriteChunkIfState {
            page_id,
            offset,
            data,
            expected_sequence,
            expected_state_checksum,
        } => (
            page_id,
            *offset,
            data,
            Some(*expected_sequence),
            Some(expected_state_checksum),
        ),
        _ => return Ok(None),
    };
    if let Some(sequence) = sequence {
        if sequence == 0
            || sequence == u64::MAX
            || service.page_write_sequence(page) != Some(sequence)
        {
            return Ok(None);
        }
    }
    if let Some(expected) = state_checksum {
        if service.head_chunk(page)?.state_checksum != *expected {
            return Ok(None);
        }
    }
    service.prepare_journal_append(page, offset, data)
}

fn apply_locked(
    guard: &mut NodeReplica,
    request: ReplicaRequest,
    max_payload_bytes: usize,
    sync_journal: bool,
) -> Result<ReplicaReply, ObjectError> {
    if guard.health.failed() {
        return Err(ObjectError::Corruption(
            "replica journal failed; restart and recover before serving requests".to_string(),
        ));
    }
    if let Some(journal) = guard.journal.as_mut() {
        if let Err(error) = journal.check_health() {
            guard.health.fail();
            return Err(ObjectError::Corruption(error.to_string()));
        }
    }
    if let ReplicaRequest::JournalControl(command) = request {
        use super::replica_journal_control::{ReplicaJournalControl, ReplicaJournalStatus};
        command.validate()?;
        let NodeReplica {
            journal, service, ..
        } = guard;
        let journal = journal.as_mut().ok_or_else(|| {
            ObjectError::PreconditionFailed("replica has no standalone journal".into())
        })?;
        let status = match if command.version == 3 {
            journal.write_statistics_status()
        } else if command.version == 2 {
            journal.maintenance_status()
        } else {
            journal.status()
        } {
            Ok(status) => status,
            Err(error) => {
                guard.health.fail();
                return Err(ObjectError::Corruption(error.to_string()));
            }
        };
        if command.action == ReplicaJournalControl::CHECKPOINT && status.checkpoint_in_flight {
            return Err(ObjectError::PreconditionFailed(
                "replica checkpoint is already in flight".into(),
            ));
        }
        let result = (|| -> std::io::Result<ReplicaJournalStatus> {
            if command.action == ReplicaJournalControl::STATUS {
                return Ok(status);
            }
            journal.sync_deferred()?;
            journal.checkpoint_requested(service)?;
            journal.check_health()?;
            if command.version == 3 {
                journal.write_statistics_status()
            } else if command.version == 2 {
                journal.maintenance_status()
            } else {
                journal.status()
            }
        })();
        match result {
            Ok(status) => {
                status.validate()?;
                return Ok(ReplicaReply::JournalStatus(status));
            }
            Err(error) => {
                guard.health.fail();
                return Err(ObjectError::Corruption(error.to_string()));
            }
        }
    }
    if guard.staged.as_ref().is_some_and(|stage| stage.expired()) {
        guard.staged = None;
    }
    if guard.health.maintenance()
        && matches!(
            &request,
            ReplicaRequest::WriteChunk { .. }
                | ReplicaRequest::WriteChunkIfSequence { .. }
                | ReplicaRequest::WriteChunkIfState { .. }
                | ReplicaRequest::WriteChunkIfSequenceDigest { .. }
                | ReplicaRequest::DiscardChunk { .. }
                | ReplicaRequest::DiscardChunkIfSequence { .. }
        )
    {
        return Err(ObjectError::PreconditionFailed(
            "replica maintenance mode refuses ordinary mutations".into(),
        ));
    }
    if let ReplicaRequest::LogOp(op) = &request {
        if guard.health.maintenance() && op.is_mutation() {
            return Err(ObjectError::PreconditionFailed(
                "replica maintenance mode refuses ordinary mutations".into(),
            ));
        }
        let store = guard.log_chunks.as_mut().ok_or_else(|| {
            ObjectError::PreconditionFailed("log chunks require durable replica storage".into())
        })?;
        let reply = if sync_journal {
            store.apply(op)?
        } else {
            store.apply_deferred(op)?
        };
        crate::cluster::message::check_reply_size(
            prost::Message::encoded_len(&reply),
            max_payload_bytes,
        )?;
        return Ok(ReplicaReply::LogOp(reply));
    }
    if let ReplicaRequest::StateInstall(request) = request {
        return apply_state_install(guard, request);
    }
    if let ReplicaRequest::FenceNamespace(fence) = request {
        let outcome =
            crate::chunk_namespace_fence::NamespaceFenceOutcome::decide(&guard.service, fence)?;
        crate::cluster::message::check_reply_size(
            prost::Message::encoded_len(outcome.after.as_ref().unwrap()),
            max_payload_bytes,
        )?;
        if guard.journal.is_none() {
            return Err(ObjectError::PreconditionFailed(
                "namespace fencing requires durable replica storage".into(),
            ));
        }
        let contextual = guard.journal.as_ref().unwrap().requires_context();
        if contextual {
            if let Err(error) = outcome.apply(&mut guard.service) {
                guard.health.fail();
                return Err(error);
            }
        }
        let context = guard
            .journal
            .as_ref()
            .unwrap()
            .capture_context(&guard.service);
        let outcome = match guard.journal.as_mut().unwrap().append_namespace_owned(
            outcome,
            context,
            sync_journal,
        ) {
            Ok(outcome) => outcome,
            Err(error) => {
                guard.health.fail();
                return Err(ObjectError::Corruption(error.to_string()));
            }
        };
        if !contextual {
            if let Err(error) = outcome.apply(&mut guard.service) {
                guard.health.fail();
                return Err(error);
            }
        }
        if sync_journal
            && guard
                .journal
                .as_ref()
                .unwrap()
                .checkpoint_due(guard.journal_checkpoint_after_bytes)
        {
            let NodeReplica {
                service, journal, ..
            } = guard;
            if let Err(error) = journal.as_mut().unwrap().checkpoint(service) {
                guard.health.fail();
                return Err(ObjectError::Corruption(error.to_string()));
            }
        }
        return Ok(ReplicaReply::NamespaceFence(outcome.after.unwrap()));
    }
    if let ReplicaRequest::ReadChunkStateRange { length, .. } = &request {
        let size = usize::try_from(*length)
            .unwrap_or(usize::MAX)
            .saturating_add(crate::cluster::state_transfer::RANGE_OVERHEAD);
        crate::cluster::message::check_reply_size(size, max_payload_bytes)?;
    }
    if let ReplicaRequest::ReadChunk { length, .. } = &request {
        // Include sequence, checksum, and lengths before copying any page bytes.
        let size = usize::try_from(*length)
            .unwrap_or(usize::MAX)
            .saturating_add(40);
        crate::cluster::message::check_reply_size(size, max_payload_bytes)?;
    }
    if let ReplicaRequest::ReadChunkBatch { reads } = &request {
        let size = reads
            .iter()
            .try_fold(0usize, |sum, (_, _, length)| {
                usize::try_from(*length)
                    .ok()
                    .and_then(|length| sum.checked_add(length.saturating_add(64)))
            })
            .unwrap_or(usize::MAX);
        crate::cluster::message::check_reply_size(size, max_payload_bytes)?;
    }
    let page = if guard.journal.is_some() {
        match &request {
            ReplicaRequest::WriteChunk { page_id, .. }
            | ReplicaRequest::WriteChunkIfSequence { page_id, .. }
            | ReplicaRequest::WriteChunkIfState { page_id, .. }
            | ReplicaRequest::WriteChunkIfSequenceDigest { page_id, .. }
            | ReplicaRequest::DiscardChunkIfSequence { page_id, .. }
            | ReplicaRequest::DiscardChunk { page_id, .. } => Some(page_id.clone()),
            _ => None,
        }
    } else {
        None
    };
    if guard.resident.is_some() {
        if let Some(id) = &page {
            guard.load_resident_page(id)?;
            if let ReplicaRequest::WriteChunk { offset, data, .. }
            | ReplicaRequest::WriteChunkIfSequence { offset, data, .. }
            | ReplicaRequest::WriteChunkIfState { offset, data, .. }
            | ReplicaRequest::WriteChunkIfSequenceDigest { offset, data, .. } = &request
            {
                let existed = guard.service.page_write_sequence(id).is_some();
                if !existed && guard.global_page_count() >= guard.max_pages {
                    return Err(ObjectError::PreconditionFailed(
                        "replica page count limit exceeded".into(),
                    ));
                }
                let end =
                    crate::validate_pipeline_range("chunk write", *offset, data.len() as u64)?;
                let old = guard.service.page_physical_len(id);
                let used = guard
                    .used_bytes
                    .get()
                    .checked_sub(old)
                    .and_then(|n| n.checked_add(old.max(end)));
                if used.map_or(true, |n| n > guard.service.capacity_bytes()) {
                    return Err(guard.service.reject_global_capacity_write());
                }
            }
        }
    }
    if guard.resident.is_some() {
        match &request {
            ReplicaRequest::HeadChunk { page_id } => {
                return guard.page_head(page_id).map(ReplicaReply::Head)
            }
            ReplicaRequest::HeadChunkBatch { pages } => {
                let mut heads = Vec::with_capacity(pages.len());
                for id in pages {
                    heads.push(match guard.page_head(id) {
                        Ok(head) => Some(head),
                        Err(ObjectError::NotFound(_)) => None,
                        Err(error) => return Err(error),
                    });
                }
                return Ok(ReplicaReply::Heads(heads));
            }
            ReplicaRequest::Ping => {
                return Ok(ReplicaReply::Pong {
                    page_count: guard.global_page_count() as u64,
                })
            }
            ReplicaRequest::PageInventory { after, limit } => {
                crate::cluster::message::check_inventory_limit(*limit)?;
                let mut entries = guard.resident_inventory(after.as_ref(), *limit as usize + 1)?;
                let has_more = entries.len() > *limit as usize;
                entries.truncate(*limit as usize);
                return Ok(ReplicaReply::PageInventory(
                    crate::cluster::message::PageInventory { entries, has_more },
                ));
            }
            _ => {}
        }
    }
    let append = if guard.journal.is_some() {
        prepare_rpc_append(&guard.service, &request)?
    } else {
        None
    };
    let before = if let Some(page) = page.as_ref().filter(|_| append.is_none()) {
        match guard.service.read_chunk_state(page) {
            Ok(state) => Some(state),
            Err(ObjectError::NotFound(_)) => None,
            Err(err) => return Err(err),
        }
    } else {
        None
    };
    let counters = crate::outcome::PageOutcome::counters(&guard.service);
    let max_pages = guard.max_pages;
    let changed_page = match &request {
        ReplicaRequest::WriteChunk { page_id, .. }
        | ReplicaRequest::WriteChunkIfSequence { page_id, .. }
        | ReplicaRequest::WriteChunkIfState { page_id, .. }
        | ReplicaRequest::WriteChunkIfSequenceDigest { page_id, .. }
        | ReplicaRequest::DiscardChunkIfSequence { page_id, .. }
        | ReplicaRequest::DiscardChunk { page_id, .. } => Some(page_id.clone()),
        _ => None,
    };
    let previous_bytes = changed_page
        .as_ref()
        .map_or(0, |page| guard.service.page_physical_len(page));
    let existed = changed_page
        .as_ref()
        .is_some_and(|id| guard.service.page_write_sequence(id).is_some());
    let result = apply_to_replica(&mut guard.service, request, max_pages);
    if let Some(page) = changed_page.as_ref() {
        guard.track_resident_mutation(page, existed)?;
        let current_bytes = guard.service.page_physical_len(page);
        match guard
            .used_bytes
            .get()
            .checked_sub(previous_bytes)
            .and_then(|n| n.checked_add(current_bytes))
        {
            Some(bytes) => guard.used_bytes.set(bytes),
            None => {
                guard.health.fail();
                return Err(ObjectError::Corruption(
                    "replica usage accounting overflow".into(),
                ));
            }
        }
    }
    let reply = result?;
    if let Some(page) = page {
        let persist = (|| -> Result<(), ObjectError> {
            let outcome = if let Some(append) = append {
                guard.service.verify_journal_append(&append)?;
                append.outcome
            } else {
                let after = match guard.service.read_chunk_state(&page) {
                    Ok(state) => state,
                    Err(ObjectError::NotFound(_)) if before.is_none() => return Ok(()),
                    Err(err) => return Err(err),
                };
                if before.as_ref() == Some(&after)
                    && counters == crate::outcome::PageOutcome::counters(&guard.service)
                {
                    return Ok(());
                }
                crate::outcome::PageOutcome::between(&page, before.as_ref(), &after)?
            };
            let outcome = outcome.with_counters(
                counters,
                crate::outcome::PageOutcome::counters(&guard.service),
            );
            let context = guard
                .journal
                .as_ref()
                .unwrap()
                .capture_context(&guard.service);
            guard
                .journal
                .as_mut()
                .unwrap()
                .append_owned(outcome, context, sync_journal)
                .map_err(|err| ObjectError::Corruption(err.to_string()))
        })();
        let persist = persist.and_then(|()| {
            if !sync_journal {
                return Ok(());
            }
            if guard
                .journal
                .as_ref()
                .unwrap()
                .checkpoint_due(guard.journal_checkpoint_after_bytes)
            {
                let NodeReplica {
                    service, journal, ..
                } = &mut *guard;
                journal
                    .as_mut()
                    .unwrap()
                    .checkpoint(service)
                    .map_err(|error| ObjectError::Corruption(error.to_string()))?;
            }
            Ok(())
        });
        if let Err(err) = persist {
            guard.health.fail();
            return Err(ObjectError::Corruption(format!(
                "replica outcome persistence failed: {err}"
            )));
        }
    }
    if guard
        .resident
        .as_ref()
        .is_some_and(|cache| cache.over_budget())
        && !sync_journal
    {
        if !guard.try_trim_residency()? {
            finish_deferred_journal(guard)?;
        }
    } else if sync_journal {
        guard.trim_residency()?;
    }
    Ok(reply)
}

fn check_install_destination(
    node: &NodeReplica,
    page: &crate::PageId,
    expected: &str,
    source_sequence: u64,
) -> Result<(), ObjectError> {
    match node.page_head(page) {
        Ok(head) if head.state_checksum == expected && head.write_sequence <= source_sequence => {
            Ok(())
        }
        Err(ObjectError::NotFound(_)) if expected.is_empty() => Ok(()),
        Ok(_) | Err(ObjectError::NotFound(_)) => Err(ObjectError::PreconditionFailed(
            "install destination changed or is newer than source".into(),
        )),
        Err(error) => Err(error),
    }
}

fn apply_state_install(
    node: &mut NodeReplica,
    request: super::state_install::StateInstall,
) -> Result<ReplicaReply, ObjectError> {
    request.validate()?;
    match request.operation {
        1 => {
            if node.staged.is_some() {
                return Err(ObjectError::PreconditionFailed(
                    "one page install is already staged".into(),
                ));
            }
            #[cfg(not(unix))]
            if node.journal.is_some() {
                return Err(ObjectError::PreconditionFailed(
                    "durable page installation requires Unix checkpoint replacement".into(),
                ));
            }
            let page = request.page_id()?;
            let head = request.head.as_ref().unwrap();
            node.service.validate_namespace_install_shape(
                &page,
                head.logical_length,
                head.data_length,
                head.write_sequence,
                !head.discarded_ranges.is_empty(),
            )?;
            check_install_destination(
                node,
                &page,
                &request.expected_checksum,
                head.write_sequence,
            )?;
            if node.global_page_count() >= node.max_pages
                && matches!(node.page_head(&page), Err(ObjectError::NotFound(_)))
            {
                return Err(ObjectError::PreconditionFailed(
                    "install exceeds page limit".into(),
                ));
            }
            let used = node
                .used_bytes
                .get()
                .checked_sub(node.global_page_bytes(&page)?)
                .and_then(|n| n.checked_add(head.data_length));
            if used.map_or(true, |n| n > node.service.capacity_bytes()) {
                return Err(ObjectError::PreconditionFailed(
                    "install exceeds byte capacity".into(),
                ));
            }
            node.staged = Some(super::state_install::StagedState::new(request)?);
        }
        2 => {
            node.staged
                .as_mut()
                .ok_or_else(|| ObjectError::PreconditionFailed("no page install is staged".into()))?
                .append(&request)?;
        }
        3 | 4 => {
            let stage = node.staged.as_ref().ok_or_else(|| {
                ObjectError::PreconditionFailed("no page install is staged".into())
            })?;
            if stage.begin.token != request.token {
                return Err(ObjectError::PreconditionFailed(
                    "install token differs from staged operation".into(),
                ));
            }
            let stage = node.staged.take().unwrap();
            if request.operation == 4 {
                return Ok(ReplicaReply::Applied);
            }
            let (page, state, expected) = stage.finish()?;
            check_install_destination(node, &page, &expected, state.write_sequence)?;
            if node.global_page_count() >= node.max_pages
                && matches!(node.page_head(&page), Err(ObjectError::NotFound(_)))
            {
                return Err(ObjectError::PreconditionFailed(
                    "install exceeds page limit".into(),
                ));
            }
            let existed = node.page_head(&page).is_ok();
            let used = node
                .used_bytes
                .get()
                .checked_sub(node.global_page_bytes(&page)?)
                .and_then(|n| n.checked_add(state.data.len() as u64))
                .ok_or_else(|| {
                    ObjectError::Corruption("install usage accounting overflow".into())
                })?;
            if used > node.service.capacity_bytes() {
                return Err(ObjectError::PreconditionFailed(
                    "install exceeds byte capacity".into(),
                ));
            }
            let mut candidate = node.service.clone();
            candidate.install_chunk_state(page.clone(), state.clone())?;
            // A complete checkpoint can exceed the bounded normal outcome record.
            // Publish the candidate in memory only after durable replacement succeeds.
            if let Some(journal) = node.journal.as_mut() {
                if let Err(error) = journal.checkpoint_page_install(&candidate, &page, &state) {
                    node.health.fail();
                    return Err(ObjectError::Corruption(format!(
                        "install persistence failed; restart required: {error}"
                    )));
                }
            }
            node.used_bytes.set(used);
            node.service = candidate;
            node.track_resident_mutation(&page, existed)?;
            node.trim_residency()?;
        }
        _ => unreachable!(),
    }
    Ok(ReplicaReply::Applied)
}

pub(super) fn apply_to_replica(
    guard: &mut MatrixReplicaService,
    request: ReplicaRequest,
    max_pages: usize,
) -> Result<ReplicaReply, ObjectError> {
    match request {
        ReplicaRequest::JournalControl(_) => Err(ObjectError::PreconditionFailed(
            "journal control requires standalone replica storage".into(),
        )),
        ReplicaRequest::FenceNamespace(fence) => {
            let outcome =
                crate::chunk_namespace_fence::NamespaceFenceOutcome::decide(guard, fence)?;
            outcome.apply(guard)?;
            Ok(ReplicaReply::NamespaceFence(outcome.after.unwrap()))
        }
        ReplicaRequest::ReadNamespaceFence { namespace } => guard
            .namespace_fence(&namespace)
            .cloned()
            .map(ReplicaReply::NamespaceFence)
            .ok_or_else(|| ObjectError::NotFound("namespace fence is missing".into())),
        ReplicaRequest::StateInstall(_) => Err(ObjectError::PreconditionFailed(
            "state installation requires a standalone node".into(),
        )),
        ReplicaRequest::ReadChunkStateRange {
            page_id,
            expected_state_checksum,
            offset,
            length,
        } => super::state_transfer::read_range(
            guard,
            &page_id,
            &expected_state_checksum,
            offset,
            length,
        )
        .map(ReplicaReply::StateRange),
        ReplicaRequest::Membership(_) => Err(ObjectError::InvalidArgument(
            "membership requires an enabled consensus group".into(),
        )),
        ReplicaRequest::WriteChunkBatchIfSequence { .. }
        | ReplicaRequest::WriteChunkBatchIfSequenceDigest { .. } => Err(
            ObjectError::InvalidArgument("batch writes require node request dispatch".into()),
        ),
        ReplicaRequest::GroupIdentity => Err(ObjectError::PreconditionFailed(
            "standalone replica has no Raft group identity".into(),
        )),
        ReplicaRequest::LogOp(_) => Err(ObjectError::PreconditionFailed(
            "log chunks require node request dispatch".into(),
        )),
        ReplicaRequest::WriteChunk {
            page_id,
            offset,
            data,
        } => {
            if guard.page_count() >= max_pages && guard.page_write_sequence(&page_id).is_none() {
                return Err(ObjectError::PreconditionFailed(
                    "replica page count limit exceeded".to_string(),
                ));
            }
            guard.write_chunk(page_id, offset, &data)?;
            Ok(ReplicaReply::Applied)
        }
        ReplicaRequest::ReadChunk {
            page_id,
            offset,
            length,
        } => Ok(ReplicaReply::Read(
            guard.read_chunk(&page_id, offset, length)?,
        )),
        ReplicaRequest::ReadChunkBatch { reads } => {
            let reads = reads
                .into_iter()
                .map(|(page_id, offset, length)| guard.read_chunk(&page_id, offset, length))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ReplicaReply::Reads(reads))
        }
        ReplicaRequest::DigestChunk {
            page_id,
            offset,
            length,
        } => {
            if length > 1024 * 1024 {
                return Err(ObjectError::InvalidArgument(
                    "page digest range exceeds one MiB".into(),
                ));
            }
            let read = guard.read_chunk(&page_id, offset, length)?;
            let mut sha256 = [0u8; 32];
            sha256
                .copy_from_slice(ring::digest::digest(&ring::digest::SHA256, &read.data).as_ref());
            Ok(ReplicaReply::Digest {
                write_sequence: read.write_sequence,
                length: read.data.len() as u64,
                sha256,
            })
        }
        ReplicaRequest::DigestChunkBatch { reads } => {
            if reads.len() > 8 || reads.iter().any(|(_, _, length)| *length > 1024 * 1024) {
                return Err(ObjectError::InvalidArgument(
                    "digest batch exceeds range bounds".into(),
                ));
            }
            let mut digests = Vec::with_capacity(reads.len());
            for (page_id, offset, length) in reads {
                let read = guard.read_chunk(&page_id, offset, length)?;
                let mut sha256 = [0; 32];
                sha256.copy_from_slice(
                    ring::digest::digest(&ring::digest::SHA256, &read.data).as_ref(),
                );
                digests.push((read.write_sequence, read.data.len() as u64, sha256));
            }
            Ok(ReplicaReply::Digests(digests))
        }
        ReplicaRequest::HeadChunk { page_id } => {
            Ok(ReplicaReply::Head(guard.head_chunk(&page_id)?))
        }
        ReplicaRequest::HeadChunkBatch { pages } => {
            let mut heads = Vec::with_capacity(pages.len());
            for page_id in pages {
                match guard.head_chunk(&page_id) {
                    Ok(head) => heads.push(Some(head)),
                    Err(ObjectError::NotFound(_)) => heads.push(None),
                    Err(error) => return Err(error),
                }
            }
            Ok(ReplicaReply::Heads(heads))
        }
        ReplicaRequest::WriteChunkIfSequenceDigest {
            page_id,
            offset,
            data,
            expected_sequence,
        } => {
            if data.len() > 1024 * 1024 {
                return Err(ObjectError::InvalidArgument(
                    "write digest range exceeds one MiB".into(),
                ));
            }
            let length = data.len() as u64;
            apply_to_replica(
                guard,
                ReplicaRequest::WriteChunkIfSequence {
                    page_id: page_id.clone(),
                    offset,
                    data,
                    expected_sequence,
                },
                max_pages,
            )?;
            let read = guard.read_chunk(&page_id, offset, length)?;
            let mut sha256 = [0u8; 32];
            sha256
                .copy_from_slice(ring::digest::digest(&ring::digest::SHA256, &read.data).as_ref());
            Ok(ReplicaReply::Digest {
                write_sequence: read.write_sequence,
                length: read.data.len() as u64,
                sha256,
            })
        }
        ReplicaRequest::WriteChunkIfSequence {
            page_id,
            offset,
            data,
            expected_sequence,
        } => {
            let matches = match guard.head_chunk(&page_id) {
                Ok(head) => {
                    expected_sequence != 0
                        && expected_sequence != u64::MAX
                        && head.write_sequence == expected_sequence
                }
                Err(ObjectError::NotFound(_)) => expected_sequence == 0,
                Err(error) => return Err(error),
            };
            if !matches {
                return Err(ObjectError::PreconditionFailed(
                    "page sequence changed".into(),
                ));
            }
            apply_to_replica(
                guard,
                ReplicaRequest::WriteChunk {
                    page_id,
                    offset,
                    data,
                },
                max_pages,
            )
        }
        ReplicaRequest::WriteChunkIfState {
            page_id,
            offset,
            data,
            expected_sequence,
            expected_state_checksum,
        } => {
            let matches = match guard.head_chunk(&page_id) {
                Ok(head) => {
                    expected_sequence != 0
                        && expected_sequence != u64::MAX
                        && head.write_sequence == expected_sequence
                        && head.state_checksum == expected_state_checksum
                }
                Err(ObjectError::NotFound(_)) => {
                    expected_sequence == 0 && expected_state_checksum.is_empty()
                }
                Err(error) => return Err(error),
            };
            if !matches {
                return Err(ObjectError::PreconditionFailed("page state changed".into()));
            }
            apply_to_replica(
                guard,
                ReplicaRequest::WriteChunk {
                    page_id,
                    offset,
                    data,
                },
                max_pages,
            )
        }
        ReplicaRequest::DiscardChunkIfSequence {
            page_id,
            offset,
            length,
            expected_sequence,
        } => {
            if expected_sequence == 0
                || expected_sequence == u64::MAX
                || guard.head_chunk(&page_id)?.write_sequence != expected_sequence
            {
                return Err(ObjectError::PreconditionFailed(
                    "page sequence changed or exhausted".into(),
                ));
            }
            apply_to_replica(
                guard,
                ReplicaRequest::DiscardChunk {
                    page_id,
                    offset,
                    length,
                },
                max_pages,
            )
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
        ReplicaRequest::Capabilities => Ok(ReplicaReply::Capabilities {
            state_conditional_write: true,
        }),
        ReplicaRequest::ConditionalWriteLimits => Ok(ReplicaReply::ConditionalWriteLimits {
            max_writes: 8,
            max_payload_bytes: 4 * 1024 * 1024,
        }),
        ReplicaRequest::WideConditionalWriteLimits => Ok(ReplicaReply::ConditionalWriteLimits {
            max_writes: 8,
            max_payload_bytes: 4 * 1024 * 1024,
        }),
        ReplicaRequest::ReadBatchLimits => Ok(ReplicaReply::ReadBatchLimits {
            max_reads: 8,
            max_reply_payload_bytes: 4 * 1024 * 1024,
        }),
        ReplicaRequest::PageInventory { after, limit } => {
            crate::cluster::message::check_inventory_limit(limit)?;
            let mut entries = guard.page_write_sequences_after(after.as_ref(), limit as usize + 1);
            let has_more = entries.len() > limit as usize;
            entries.truncate(limit as usize);
            Ok(ReplicaReply::PageInventory(
                crate::cluster::message::PageInventory { entries, has_more },
            ))
        }
    }
}

#[cfg(test)]
mod journal_failure_tests {
    use super::*;
    use crate::{ChunkId, PageId};
    #[test]
    fn conditional_write_digest_matches_applied_bytes() {
        let mut service = MatrixReplicaService::new();
        let page_id = PageId {
            chunk_id: ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        };
        let reply = apply_to_replica(
            &mut service,
            ReplicaRequest::WriteChunkIfSequenceDigest {
                page_id: page_id.clone(),
                offset: 0,
                data: b"page".to_vec(),
                expected_sequence: 0,
            },
            8,
        )
        .unwrap();
        let mut expected = [0u8; 32];
        expected.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, b"page").as_ref());
        assert!(matches!(reply, ReplicaReply::Digest {
            write_sequence: 1, length: 4, sha256,
        } if sha256 == expected));
        assert_eq!(service.read_chunk(&page_id, 0, 4).unwrap().data, b"page");
    }

    #[test]
    fn state_conditional_write_checks_the_complete_prewrite_state() {
        let mut service = MatrixReplicaService::new();
        let page_id = PageId {
            chunk_id: ChunkId::new(1, 9).unwrap(),
            page_index: 0,
        };
        service
            .write_chunk(page_id.clone(), 0, b"original")
            .unwrap();
        let head = service.head_chunk(&page_id).unwrap();
        let changed_checksum = ReplicaRequest::WriteChunkIfState {
            page_id: page_id.clone(),
            offset: 0,
            data: b"changed!".to_vec(),
            expected_sequence: head.write_sequence,
            expected_state_checksum: format!("{}-changed", head.state_checksum),
        };
        assert!(matches!(
            apply_to_replica(&mut service, changed_checksum, 8),
            Err(ObjectError::PreconditionFailed(_))
        ));
        assert_eq!(
            service.read_chunk(&page_id, 0, 8).unwrap().data,
            b"original"
        );

        let matching = ReplicaRequest::WriteChunkIfState {
            page_id: page_id.clone(),
            offset: 0,
            data: b"changed!".to_vec(),
            expected_sequence: head.write_sequence,
            expected_state_checksum: head.state_checksum,
        };
        assert_eq!(
            apply_to_replica(&mut service, matching, 8).unwrap(),
            ReplicaReply::Applied
        );
        assert_eq!(
            service.read_chunk(&page_id, 0, 8).unwrap().data,
            b"changed!"
        );
    }
    #[test]
    fn conditional_mutations_refuse_exhausted_page_sequences() {
        let mut service = MatrixReplicaService::new();
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        };
        service.write_chunk(page.clone(), 0, b"original").unwrap();
        let mut state = service.read_chunk_state(&page).unwrap();
        state.write_sequence = u64::MAX;
        service.install_chunk_state(page.clone(), state).unwrap();
        let before = service.head_chunk(&page).unwrap();
        for request in [
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page.clone(),
                offset: 0,
                data: b"changed!".to_vec(),
                expected_sequence: u64::MAX,
            },
            ReplicaRequest::DiscardChunkIfSequence {
                page_id: page.clone(),
                offset: 0,
                length: 1,
                expected_sequence: u64::MAX,
            },
        ] {
            assert!(matches!(
                apply_to_replica(&mut service, request, 64),
                Err(ObjectError::PreconditionFailed(_))
            ));
            assert_eq!(service.head_chunk(&page).unwrap(), before);
        }
        assert_eq!(service.read_chunk(&page, 0, 8).unwrap().data, b"original");
    }

    #[test]
    fn standalone_star_batches_sync_once_and_recover_accepted_prefixes() {
        let directory = std::env::temp_dir().join(format!(
            "replica-star-batch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("replica.wal");
        let mut service = MatrixReplicaService::new();
        let journal = ReplicaJournal::open(&path, &mut service).unwrap();
        let (used_bytes, health) = replica_health::test_parts();
        let replica = Arc::new(Mutex::new(NodeReplica {
            used_bytes,
            service,
            resident: None,
            journal: Some(Box::new(journal)),
            journal_checkpoint_after_bytes: 0,
            staged: None,
            max_pages: 65_536,
            health,
            log_chunks: None,
        }));
        let make_page = |index| PageId {
            chunk_id: ChunkId::new(7, 9).unwrap(),
            page_index: index,
        };
        let write = |index, expected_sequence| super::super::message::ConditionalChunkWrite {
            page_id: make_page(index),
            offset: 0,
            data: vec![index as u8 + 1; 16],
            expected_sequence,
        };

        assert_eq!(
            apply_batch(
                &replica,
                (0..8).map(|index| write(index, 0)).collect(),
                MAX_PAYLOAD_BYTES,
            )
            .unwrap(),
            ReplicaReply::Applied
        );
        assert_eq!(
            replica
                .lock()
                .unwrap()
                .journal
                .as_ref()
                .unwrap()
                .sync_count(),
            1
        );

        assert!(matches!(
            apply_batch(&replica, vec![write(8, 0), write(9, 1)], MAX_PAYLOAD_BYTES,),
            Err(ObjectError::PreconditionFailed(_))
        ));
        assert_eq!(
            replica
                .lock()
                .unwrap()
                .journal
                .as_ref()
                .unwrap()
                .sync_count(),
            2
        );
        drop(replica);

        let mut recovered = MatrixReplicaService::new();
        drop(ReplicaJournal::open(&path, &mut recovered).unwrap());
        for index in 0..9 {
            assert_eq!(
                recovered.read_chunk(&make_page(index), 0, 16).unwrap().data,
                vec![index as u8 + 1; 16]
            );
        }
        assert!(matches!(
            recovered.head_chunk(&make_page(9)),
            Err(ObjectError::NotFound(_))
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn standalone_star_batch_syncs_repeated_page_sequence_once() {
        let directory = std::env::temp_dir().join(format!(
            "replica-star-repeated-page-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("replica.wal");
        let mut service = MatrixReplicaService::new();
        let journal = ReplicaJournal::open(&path, &mut service).unwrap();
        let (used_bytes, health) = replica_health::test_parts();
        let replica = Arc::new(Mutex::new(NodeReplica {
            used_bytes,
            service,
            resident: None,
            journal: Some(Box::new(journal)),
            journal_checkpoint_after_bytes: 0,
            staged: None,
            max_pages: 65_536,
            health,
            log_chunks: None,
        }));
        let page = PageId {
            chunk_id: ChunkId::new(7, 10).unwrap(),
            page_index: 11,
        };
        let writes = [b"abc".as_slice(), b"def", b"ghi"]
            .into_iter()
            .enumerate()
            .map(
                |(index, data)| super::super::message::ConditionalChunkWrite {
                    page_id: page.clone(),
                    offset: (index * 3) as u64,
                    data: data.to_vec(),
                    expected_sequence: index as u64,
                },
            )
            .collect();

        assert_eq!(
            apply_batch(&replica, writes, MAX_PAYLOAD_BYTES).unwrap(),
            ReplicaReply::Applied
        );
        let guard = replica.lock().unwrap();
        assert_eq!(guard.service.head_chunk(&page).unwrap().write_sequence, 3);
        assert_eq!(
            guard.service.read_chunk(&page, 0, 9).unwrap().data,
            b"abcdefghi"
        );
        assert_eq!(guard.journal.as_ref().unwrap().sync_count(), 1);
        drop(guard);
        drop(replica);

        let mut recovered = MatrixReplicaService::new();
        drop(ReplicaJournal::open(&path, &mut recovered).unwrap());
        assert_eq!(recovered.head_chunk(&page).unwrap().write_sequence, 3);
        assert_eq!(
            recovered.read_chunk(&page, 0, 9).unwrap().data,
            b"abcdefghi"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_persistence_blocks_acknowledgment_and_all_later_requests() {
        let path = std::env::temp_dir().join(format!(
            "replica-readonly-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, []).unwrap();
        let (used_bytes, health) = replica_health::test_parts();
        let replica = Arc::new(Mutex::new(NodeReplica {
            used_bytes,
            service: MatrixReplicaService::new(),
            resident: None,
            journal: Some(Box::new(ReplicaJournal::read_only_for_test(&path))),
            journal_checkpoint_after_bytes: 0,
            staged: None,
            max_pages: 65_536,
            health,
            log_chunks: None,
        }));
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 3,
        };
        let write = ReplicaRequest::WriteChunk {
            page_id: page.clone(),
            offset: 0,
            data: b"uncommitted".to_vec(),
        };
        assert!(apply(&replica, write.clone(), MAX_PAYLOAD_BYTES).is_err());
        assert!(replica.lock().unwrap().health.failed());
        for request in [
            write,
            ReplicaRequest::Ping,
            ReplicaRequest::HeadChunk { page_id: page },
        ] {
            assert!(apply(&replica, request, MAX_PAYLOAD_BYTES).is_err());
        }
        drop(replica);
        let mut recovered = MatrixReplicaService::new();
        drop(ReplicaJournal::open(&path, &mut recovered).unwrap());
        assert_eq!(recovered.page_count(), 0);
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(all(test, unix))]
#[path = "replica_resident_cache_tests.rs"]
mod resident_cache_tests;

#[cfg(test)]
mod io_configuration_tests {
    use super::*;

    #[test]
    fn io_configuration_preserves_default_and_rejects_invalid_counts() {
        let mut server = ReplicaNodeServer::bind(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                worker_threads: 16,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(server.io_threads, None);
        assert!(server.set_io_threads(0).is_err());
        assert!(server.set_io_threads(17).is_err());
        assert_eq!(server.io_threads, None);
        server.set_io_threads(4).unwrap();
        assert_eq!(server.io_threads, Some(4));
        assert!(server.set_io_threads(17).is_err());
        assert_eq!(server.io_threads, Some(4));
        assert_eq!(server.options.worker_threads, 16);
    }
}
