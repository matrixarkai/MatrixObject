// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A log object: bytes appended to a chain of log chunks, written by one leaseholder.
//!
//! The metadata plane is touched only when the object changes hands or moves on:
//!
//! * opening for append: one grant (the lease, and the first chunk if the object has none),
//! * every chunk boundary: ONE switch (the sealed chunk's length and footer, the next chunk, and the
//!   lease is renewed by it),
//! * half way through a lease: one renewal, which a writer holding many objects sends for all of
//!   them in one command ([`renew_leases`]).
//!
//! Between those, an append goes to the three chunk nodes of the open chunk and nowhere else: no
//! metadata command is sent for it. Everything here is behind the two cluster features that
//! nothing raises unless an operator does; without them the metadata plane refuses the grant.
//!
//! A reader ([`LogObjectReader`]) follows the manifest: a sealed chunk is read from its nodes up to
//! its recorded length, the open chunk (the live tail) only up to the length two of its nodes hold
//! durably.
use super::chunk_log::{ChunkLogError, ChunkLogReader, ChunkLogWriter, LogCallError, LOG_QUORUM};
use super::client::{RemoteReplicaClient, ReplicaClientOptions};
use super::log_chunk_commands::{LogChunkReply, LogChunkRequest, MAX_OBJECT_ENTRIES};
use super::log_chunk_store::LOG_CHUNK_LIMIT;
use crate::log_chunk_outcome::LeaseRef;
use crate::object_manifest::{ObjectExtent, ObjectManifest, EXTENT_FLAG_SEALED, EXTENT_KIND_LOG_CHUNK};
use crate::ObjectError;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// The chunk a capability probe asks about. A node answers a tail of a chunk it never held with
/// an empty chunk and creates nothing, so the probe leaves no trace on a node that speaks the verb.
const PROBE_VOLUME: u64 = 1;
const PROBE_CHUNK: u64 = 1;
/// Every log chunk of this series is written at generation 1.
const GENERATION: u32 = 1;

/// What a log object needs from the metadata plane. [`RemoteMetaClient`](super::RemoteMetaClient)
/// is the real one; a test may stand in for it.
pub trait LogMeta {
    fn grant(&mut self, bucket: &str, key: &str, holder: &str, ttl_ms: u64, group: &str) -> Result<LogChunkReply, ObjectError>;
    #[allow(clippy::too_many_arguments)]
    fn switch(
        &mut self,
        bucket: &str,
        key: &str,
        holder: &str,
        epoch: u64,
        group: &str,
        sealed_length: u64,
        footer_crc: u32,
        footer_sha256: Vec<u8>,
    ) -> Result<LogChunkReply, ObjectError>;
    fn renew(&mut self, holder: &str, ttl_ms: u64, renewals: Vec<LeaseRef>) -> Result<LogChunkReply, ObjectError>;
    fn manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError>;
    /// The API endpoints of the three nodes of a chunk group.
    fn group_endpoints(&mut self, group: &str) -> Result<Vec<String>, ObjectError>;
}

impl LogMeta for super::RemoteMetaClient {
    fn grant(&mut self, bucket: &str, key: &str, holder: &str, ttl_ms: u64, group: &str) -> Result<LogChunkReply, ObjectError> {
        self.grant_log_chunk(bucket, key, holder, ttl_ms, group)
    }
    fn switch(
        &mut self,
        bucket: &str,
        key: &str,
        holder: &str,
        epoch: u64,
        group: &str,
        sealed_length: u64,
        footer_crc: u32,
        footer_sha256: Vec<u8>,
    ) -> Result<LogChunkReply, ObjectError> {
        self.switch_log_chunk(bucket, key, holder, epoch, group, sealed_length, footer_crc, footer_sha256)
    }
    fn renew(&mut self, holder: &str, ttl_ms: u64, renewals: Vec<LeaseRef>) -> Result<LogChunkReply, ObjectError> {
        self.renew_log_leases(holder, ttl_ms, renewals)
    }
    fn manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        self.read_manifest(bucket, key)
    }
    fn group_endpoints(&mut self, group: &str) -> Result<Vec<String>, ObjectError> {
        Ok(self.read_chunk_group(group)?.api_endpoints)
    }
}

/// Why a log object operation did not complete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogObjectError {
    /// A node of the group does not speak log chunks. Nothing was granted.
    NotSpoken,
    /// The object is not open for append on this handle.
    NotOpen,
    /// Another writer holds the object's lease.
    LeasedElsewhere,
    /// This writer's lease ran out or another writer took the object: stop, re-open to continue.
    LostLease,
    /// The object holds the most entries it may. The open chunk may be filled; the object then ends.
    EntryCap,
    Chunk(ChunkLogError),
    Meta(ObjectError),
    Invalid(String),
}

impl std::fmt::Display for LogObjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSpoken => write!(f, "a node of the group does not speak log chunks"),
            Self::NotOpen => write!(f, "the log object is not open for append"),
            Self::LeasedElsewhere => write!(f, "the log object is leased to another writer"),
            Self::LostLease => write!(f, "the writer no longer holds the log object's lease"),
            Self::EntryCap => write!(f, "the log object holds the most chunks it may"),
            Self::Chunk(error) => write!(f, "{error}"),
            Self::Meta(error) => write!(f, "{error}"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for LogObjectError {}

fn from_meta(error: ObjectError) -> LogObjectError {
    if let ObjectError::PreconditionFailed(message) = &error {
        if message.contains("leased to another writer") {
            return LogObjectError::LeasedElsewhere;
        }
        if message.contains("does not hold the live lease") {
            return LogObjectError::LostLease;
        }
        if message.contains("most entries it may") {
            return LogObjectError::EntryCap;
        }
    }
    LogObjectError::Meta(error)
}

fn from_chunk(error: ChunkLogError) -> LogObjectError {
    match error {
        ChunkLogError::StaleEpoch { .. } => LogObjectError::LostLease,
        ChunkLogError::NotSpoken => LogObjectError::NotSpoken,
        other => LogObjectError::Chunk(other),
    }
}

fn corrupt(message: &str) -> LogObjectError {
    LogObjectError::Meta(ObjectError::Corruption(message.into()))
}

/// How a log object is written.
#[derive(Clone, Debug)]
pub struct LogObjectOptions {
    /// The writer's name: the lease is held by it.
    pub holder: String,
    /// The chunk group that holds the object's chunks.
    pub group: String,
    /// Bytes after which the object moves to its next chunk. At most the chunk limit (64 MiB), the
    /// default; a test uses less.
    pub chunk_bytes: u64,
    /// The term of a lease, renewed at half of it.
    pub ttl_ms: u64,
    /// Seal chunks with a SHA-256 as well as the CRC.
    pub with_sha256: bool,
    /// The most entries the object may hold. The metadata plane enforces the same bound.
    pub max_entries: usize,
    pub replica: ReplicaClientOptions,
}

impl LogObjectOptions {
    pub fn new(holder: &str, group: &str) -> Self {
        Self {
            holder: holder.into(),
            group: group.into(),
            chunk_bytes: LOG_CHUNK_LIMIT,
            ttl_ms: LogChunkRequest::SWITCH_TERM_MS,
            with_sha256: false,
            max_entries: MAX_OBJECT_ENTRIES,
            replica: ReplicaClientOptions::default(),
        }
    }
}

struct Writing {
    epoch: u64,
    /// When the lease runs out, as this writer sees it: from the moment it SENT the request that
    /// granted or renewed it, so it never believes in a lease longer than the metadata plane does.
    valid_until: Instant,
    ttl: Duration,
    endpoints: Vec<String>,
    volume: u64,
    chunk: u64,
    /// The object offset at which the open chunk starts.
    base: u64,
    /// Entries the manifest holds, the open chunk included.
    entries: usize,
    writer: ChunkLogWriter,
}

/// One log object, open for append by one writer.
pub struct LogObject {
    bucket: String,
    key: String,
    options: LogObjectOptions,
    writing: Option<Writing>,
}

/// Ask every node of the group whether it speaks log chunks. A node that does not stops the
/// writer before anything is granted: the grant is a metadata commit and cannot be taken back.
fn probe_group(endpoints: &[String], options: &ReplicaClientOptions) -> Result<(), LogObjectError> {
    let mut reachable = 0;
    for endpoint in endpoints {
        let Ok(mut client) = RemoteReplicaClient::connect(endpoint, options) else { continue };
        match client.log_tail(PROBE_VOLUME, PROBE_CHUNK, GENERATION) {
            Ok(_) => reachable += 1,
            Err(LogCallError::NotSpoken) => return Err(LogObjectError::NotSpoken),
            Err(LogCallError::Transport(_)) => {}
            Err(other) => return Err(from_chunk(other.into())),
        }
    }
    if reachable < LOG_QUORUM {
        return Err(LogObjectError::Chunk(ChunkLogError::NoQuorum { wanted: 1, durable: reachable as u64 }));
    }
    Ok(())
}

/// A switch that sealed the chunk on the nodes and then did not reach the metadata plane (the
/// writer died, or the answer was lost) leaves a sealed chunk that the manifest still names as the
/// open one. Finish it from what the nodes hold: their footer is the footer.
fn finish_interrupted_switch<M: LogMeta>(
    meta: &mut M,
    bucket: &str,
    key: &str,
    options: &LogObjectOptions,
    epoch: u64,
    endpoints: &[String],
    open: &ObjectExtent,
) -> Result<LogChunkReply, LogObjectError> {
    let mut footers: Vec<(u64, u32, Vec<u8>)> = Vec::new();
    for endpoint in endpoints {
        let Ok(mut client) = RemoteReplicaClient::connect(endpoint, &options.replica) else { continue };
        if let Ok(reply) = client.log_tail(open.volume_id, open.chunk_id, GENERATION) {
            if reply.sealed {
                footers.push((reply.length, reply.crc, reply.sha256));
            }
        }
    }
    if footers.len() < LOG_QUORUM {
        return Err(LogObjectError::Chunk(ChunkLogError::NoQuorum { wanted: 1, durable: footers.len() as u64 }));
    }
    if footers.iter().any(|footer| *footer != footers[0]) {
        return Err(corrupt("the nodes hold different footers for a sealed chunk"));
    }
    let (length, crc, sha256) = footers.swap_remove(0);
    meta.switch(bucket, key, &options.holder, epoch, &options.group, length, crc, sha256).map_err(from_meta)
}

fn extend(writing: &mut Writing, sent: Instant, ttl: Duration) {
    writing.valid_until = writing.valid_until.max(sent + ttl);
}

fn switch_chunk<M: LogMeta>(
    writing: &mut Writing,
    meta: &mut M,
    bucket: &str,
    key: &str,
    options: &LogObjectOptions,
) -> Result<(), LogObjectError> {
    // The cap is checked BEFORE anything is sealed: an object at its limit keeps an open chunk that
    // can still be filled, and refuses the append that does not fit.
    if writing.entries + 1 > options.max_entries {
        return Err(LogObjectError::EntryCap);
    }
    let footer = writing.writer.seal_footer(options.with_sha256).map_err(from_chunk)?;
    let sent = Instant::now();
    let reply = meta
        .switch(bucket, key, &options.holder, writing.epoch, &options.group, footer.length, footer.crc, footer.sha256.clone())
        .map_err(from_meta)?;
    let open = reply.open.ok_or_else(|| corrupt("a switch answered without the next chunk"))?;
    if open.object_offset != writing.base + footer.length {
        return Err(corrupt("the next chunk does not start where the sealed chunk ends"));
    }
    let writer = ChunkLogWriter::open(&writing.endpoints, open.volume_id, open.chunk_id, GENERATION, writing.epoch, options.replica.clone())
        .map_err(from_chunk)?;
    extend(writing, sent, Duration::from_millis(LogChunkRequest::SWITCH_TERM_MS));
    writing.volume = open.volume_id;
    writing.chunk = open.chunk_id;
    writing.base = open.object_offset;
    writing.entries += 1;
    writing.writer = writer;
    Ok(())
}

impl LogObject {
    pub fn new(bucket: &str, key: &str, options: LogObjectOptions) -> Self {
        Self { bucket: bucket.into(), key: key.into(), options, writing: None }
    }

    /// Take the object for writing: probe the group, grant the lease (creating the object as a log
    /// object if it is new), and open the chunk being written. Returns the object's length: where
    /// the next byte goes. After a writer died, this decides the tail from what the nodes hold.
    pub fn open_for_append<M: LogMeta>(&mut self, meta: &mut M) -> Result<u64, LogObjectError> {
        self.writing = None;
        let endpoints = meta.group_endpoints(&self.options.group).map_err(from_meta)?;
        if endpoints.len() != super::chunk_log::LOG_REPLICAS {
            return Err(LogObjectError::Invalid("a chunk group has three nodes".into()));
        }
        probe_group(&endpoints, &self.options.replica)?;
        let ttl = Duration::from_millis(self.options.ttl_ms);
        let sent = Instant::now();
        let reply = meta.grant(&self.bucket, &self.key, &self.options.holder, self.options.ttl_ms, &self.options.group).map_err(from_meta)?;
        let lease = reply.lease.ok_or_else(|| corrupt("a grant answered without the lease"))?;
        let mut open = reply.open.ok_or_else(|| corrupt("a grant answered without the open chunk"))?;
        let mut valid_until = sent + ttl;
        let writer = match ChunkLogWriter::open(&endpoints, open.volume_id, open.chunk_id, GENERATION, lease.epoch, self.options.replica.clone()) {
            Ok(writer) => writer,
            Err(ChunkLogError::Sealed) => {
                let sent = Instant::now();
                let finished = finish_interrupted_switch(meta, &self.bucket, &self.key, &self.options, lease.epoch, &endpoints, &open)?;
                open = finished.open.ok_or_else(|| corrupt("a switch answered without the next chunk"))?;
                valid_until = valid_until.max(sent + Duration::from_millis(LogChunkRequest::SWITCH_TERM_MS));
                ChunkLogWriter::open(&endpoints, open.volume_id, open.chunk_id, GENERATION, lease.epoch, self.options.replica.clone())
                    .map_err(from_chunk)?
            }
            Err(other) => return Err(from_chunk(other)),
        };
        let manifest = meta.manifest(&self.bucket, &self.key).map_err(from_meta)?;
        let length = open.object_offset + writer.length();
        self.writing = Some(Writing {
            epoch: lease.epoch,
            valid_until,
            ttl,
            endpoints,
            volume: open.volume_id,
            chunk: open.chunk_id,
            base: open.object_offset,
            entries: manifest.extents.len(),
            writer,
        });
        Ok(length)
    }

    /// Where the next byte goes: everything below it is held by two nodes of the open chunk.
    pub fn length(&self) -> Option<u64> {
        self.writing.as_ref().map(|w| w.base + w.writer.length())
    }

    /// The lease this handle holds on the object, for a batched renewal.
    pub fn lease_ref(&self) -> Option<LeaseRef> {
        self.writing.as_ref().map(|w| LeaseRef { bucket: self.bucket.clone(), key: self.key.clone(), epoch: w.epoch })
    }

    /// True once half of the lease's term is gone.
    pub fn renew_due(&self) -> bool {
        self.writing.as_ref().is_some_and(|w| Instant::now() + w.ttl / 2 >= w.valid_until)
    }

    /// The chunk being written: its identity and where it starts in the object.
    pub fn open_chunk(&self) -> Option<(u64, u64, u64)> {
        self.writing.as_ref().map(|w| (w.volume, w.chunk, w.base))
    }

    fn renew_self<M: LogMeta>(&mut self, meta: &mut M) -> Result<(), LogObjectError> {
        let epoch = self.writing.as_ref().ok_or(LogObjectError::NotOpen)?.epoch;
        let sent = Instant::now();
        let renewal = LeaseRef { bucket: self.bucket.clone(), key: self.key.clone(), epoch };
        let reply = meta.renew(&self.options.holder, self.options.ttl_ms, vec![renewal]);
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                let error = from_meta(error);
                if error == LogObjectError::LostLease {
                    self.writing = None;
                }
                return Err(error);
            }
        };
        if reply.lost.iter().any(|lost| lost.bucket == self.bucket && lost.key == self.key) {
            self.writing = None;
            return Err(LogObjectError::LostLease);
        }
        if let Some(writing) = self.writing.as_mut() {
            writing.ttl = Duration::from_millis(self.options.ttl_ms);
            extend(writing, sent, Duration::from_millis(self.options.ttl_ms));
        }
        Ok(())
    }

    /// Append `data`; returns the object length once two nodes hold every byte of it durably.
    /// The data is cut at chunk boundaries and the object moves to its next chunk as needed. On a
    /// `Chunk(NoQuorum)` error the bytes are kept and count toward the length: call [`settle`].
    ///
    /// [`settle`]: LogObject::settle
    pub fn append<M: LogMeta>(&mut self, meta: &mut M, data: &[u8]) -> Result<u64, LogObjectError> {
        let Some(writing) = self.writing.as_ref() else { return Err(LogObjectError::NotOpen) };
        if data.is_empty() {
            return Ok(writing.base + writing.writer.durable_length());
        }
        if Instant::now() >= writing.valid_until {
            // the lease may have run out and been taken: do not write on a belief
            self.writing = None;
            return Err(LogObjectError::LostLease);
        }
        if self.renew_due() {
            self.renew_self(meta)?;
        }
        let mut rest = data;
        while !rest.is_empty() {
            let room = match self.writing.as_ref() {
                Some(writing) => self.options.chunk_bytes.saturating_sub(writing.writer.length()),
                None => return Err(LogObjectError::NotOpen),
            };
            if room == 0 {
                let writing = self.writing.as_mut().ok_or(LogObjectError::NotOpen)?;
                if let Err(error) = switch_chunk(writing, meta, &self.bucket, &self.key, &self.options) {
                    // an object at its cap keeps its open chunk; any other failure after the nodes
                    // sealed it leaves the writer to re-open, which finishes the switch
                    if error != LogObjectError::EntryCap {
                        self.writing = None;
                    }
                    return Err(error);
                }
                continue;
            }
            let take = (room.min(rest.len() as u64)) as usize;
            let writing = self.writing.as_mut().ok_or(LogObjectError::NotOpen)?;
            if let Err(error) = writing.writer.append(&rest[..take]) {
                let error = from_chunk(error);
                if error == LogObjectError::LostLease {
                    self.writing = None;
                }
                return Err(error);
            }
            rest = &rest[take..];
        }
        let writing = self.writing.as_ref().ok_or(LogObjectError::NotOpen)?;
        Ok(writing.base + writing.writer.length())
    }

    /// Wait until two nodes hold every byte appended so far.
    pub fn settle(&mut self) -> Result<u64, LogObjectError> {
        let writing = self.writing.as_mut().ok_or(LogObjectError::NotOpen)?;
        match writing.writer.settle() {
            Ok(_) => Ok(writing.base + writing.writer.length()),
            Err(error) => {
                let error = from_chunk(error);
                if error == LogObjectError::LostLease {
                    self.writing = None;
                }
                Err(error)
            }
        }
    }
}

/// Renew the leases of every object in `objects` in ONE command, however many there are. Returns
/// the ones the metadata plane no longer holds for this writer: those handles are closed.
pub fn renew_leases<M: LogMeta>(
    meta: &mut M,
    holder: &str,
    ttl_ms: u64,
    objects: &mut [&mut LogObject],
) -> Result<Vec<LeaseRef>, LogObjectError> {
    let mut refs: Vec<LeaseRef> = objects.iter().filter_map(|object| object.lease_ref()).collect();
    if refs.is_empty() {
        return Ok(Vec::new());
    }
    refs.sort_by(|a, b| (&a.bucket, &a.key).cmp(&(&b.bucket, &b.key)));
    let sent = Instant::now();
    let reply = meta.renew(holder, ttl_ms, refs).map_err(from_meta)?;
    for object in objects.iter_mut() {
        let lost = reply.lost.iter().any(|lost| lost.bucket == object.bucket && lost.key == object.key);
        if lost {
            object.writing = None;
        } else if let Some(writing) = object.writing.as_mut() {
            writing.ttl = Duration::from_millis(ttl_ms);
            extend(writing, sent, Duration::from_millis(ttl_ms));
        }
    }
    Ok(reply.lost)
}

/// Reads a log object by byte range.
pub struct LogObjectReader {
    bucket: String,
    key: String,
    options: ReplicaClientOptions,
    manifest: Option<ObjectManifest>,
    endpoints: BTreeMap<String, Vec<String>>,
    readers: BTreeMap<(u64, u64), ChunkLogReader>,
}

struct Piece {
    group: String,
    volume: u64,
    chunk: u64,
    /// Where in the chunk, and how many bytes.
    at: u64,
    length: usize,
    sealed: bool,
}

impl LogObjectReader {
    pub fn new(bucket: &str, key: &str, options: ReplicaClientOptions) -> Self {
        Self { bucket: bucket.into(), key: key.into(), options, manifest: None, endpoints: BTreeMap::new(), readers: BTreeMap::new() }
    }

    /// Read the object's manifest again: chunks sealed and opened since are seen.
    pub fn refresh<M: LogMeta>(&mut self, meta: &mut M) -> Result<(), LogObjectError> {
        let manifest = meta.manifest(&self.bucket, &self.key).map_err(from_meta)?;
        self.manifest = Some(manifest);
        Ok(())
    }

    fn reader<M: LogMeta>(&mut self, meta: &mut M, group: &str, volume: u64, chunk: u64) -> Result<&mut ChunkLogReader, LogObjectError> {
        if !self.readers.contains_key(&(volume, chunk)) {
            if !self.endpoints.contains_key(group) {
                let endpoints = meta.group_endpoints(group).map_err(from_meta)?;
                self.endpoints.insert(group.to_string(), endpoints);
            }
            let endpoints = &self.endpoints[group];
            self.readers.insert((volume, chunk), ChunkLogReader::new(endpoints, volume, chunk, GENERATION, self.options.clone()));
        }
        Ok(self.readers.get_mut(&(volume, chunk)).expect("just inserted"))
    }

    /// Which bytes of which chunks make up `[offset, end)`, from the manifest as it stands.
    fn plan(&self, offset: u64, end: u64) -> Result<Vec<Piece>, LogObjectError> {
        let manifest = self.manifest.as_ref().ok_or(LogObjectError::NotOpen)?;
        let mut pieces = Vec::new();
        for extent in &manifest.extents {
            if extent.kind != EXTENT_KIND_LOG_CHUNK {
                return Err(LogObjectError::Invalid("the object is not a log object".into()));
            }
            let sealed = extent.flags & EXTENT_FLAG_SEALED != 0;
            let start = extent.object_offset;
            // the open chunk's length is not in the manifest: the nodes know it
            let stop = if sealed { start + extent.length } else { u64::MAX };
            let from = offset.max(start);
            let to = end.min(stop);
            if from < to {
                pieces.push(Piece {
                    group: extent.group.clone(),
                    volume: extent.volume_id,
                    chunk: extent.chunk_id,
                    at: from - start,
                    length: (to - from) as usize,
                    sealed,
                });
            }
        }
        Ok(pieces)
    }

    /// The bytes `[offset, offset + length)`, cut short at the end of the object: the end of the
    /// last sealed chunk plus what two nodes of the open chunk hold durably.
    pub fn read_range<M: LogMeta>(&mut self, meta: &mut M, offset: u64, length: usize) -> Result<Vec<u8>, LogObjectError> {
        let end = offset.checked_add(length as u64).ok_or_else(|| LogObjectError::Invalid("range overflow".into()))?;
        let mut fresh = false;
        if self.manifest.is_none() {
            self.refresh(meta)?;
            fresh = true;
        }
        loop {
            let pieces = self.plan(offset, end)?;
            let mut out = Vec::with_capacity(length.min(1 << 20));
            let mut short = false;
            for piece in &pieces {
                let reader = self.reader(meta, &piece.group, piece.volume, piece.chunk)?;
                let bytes = reader.read(piece.at, piece.length).map_err(from_chunk)?;
                if bytes.len() < piece.length {
                    if piece.sealed {
                        return Err(LogObjectError::Chunk(ChunkLogError::NoQuorum { wanted: piece.at + piece.length as u64, durable: piece.at + bytes.len() as u64 }));
                    }
                    short = true;
                }
                out.extend_from_slice(&bytes);
                if short {
                    break;
                }
            }
            // a short answer from the open chunk may only mean the manifest is old: the object may
            // have moved to a later chunk since it was read
            if (out.len() < length || pieces.is_empty()) && !fresh {
                self.refresh(meta)?;
                fresh = true;
                continue;
            }
            return Ok(out);
        }
    }

    /// The object's length: the end of the last sealed chunk plus the durable length of the open one.
    pub fn length<M: LogMeta>(&mut self, meta: &mut M) -> Result<u64, LogObjectError> {
        self.refresh(meta)?;
        let manifest = self.manifest.clone().ok_or(LogObjectError::NotOpen)?;
        let Some(last) = manifest.extents.last() else { return Ok(0) };
        if last.flags & EXTENT_FLAG_SEALED != 0 {
            return Ok(last.object_offset + last.length);
        }
        let (group, volume, chunk, base) = (last.group.clone(), last.volume_id, last.chunk_id, last.object_offset);
        let reader = self.reader(meta, &group, volume, chunk)?;
        Ok(base + reader.length().map_err(from_chunk)?)
    }
}

#[cfg(test)]
#[path = "log_object_tests.rs"]
mod tests;
