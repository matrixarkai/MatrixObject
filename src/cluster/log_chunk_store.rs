// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The log chunks a replica node holds: one file per chunk, opened when first used.
//!
//! Each file is a `LogChunk` (see `crate::log_chunk`): its length comes from replaying its own
//! records, so the node keeps no length table and a restart needs no recovery pass beyond opening
//! a chunk when something asks for it.
use super::log_op::{LogOpReply, LogOpRequest, FLAG_SEAL_SHA256, OP_APPEND, OP_READ, OP_SEAL, OP_TAIL, OP_TRUNCATE};
use crate::log_chunk::{LogChunk, LogError, RecordCache};
use crate::ObjectError;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

/// A chunk of one log: the size a writer fills before it switches to the next.
pub const LOG_CHUNK_LIMIT: u64 = 64 * 1024 * 1024;
/// Chunk files held open at once. A SEALED chunk past this is closed (least recently used first)
/// and reopened when something reads it again; nothing is refused for it.
pub const MAX_OPEN_LOG_CHUNKS: usize = 256;
/// Open tails at once: chunks that can still change keep all of their bytes in memory, so this is
/// what bounds the heap together with the record cache. A tail is never closed to make room.
pub const DEFAULT_TAIL_BUDGET: usize = 64;
/// Bytes of sealed-chunk records kept for the next read, for the whole node.
pub const DEFAULT_RECORD_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// The refusal for one more tail than the budget allows. It is retryable: a tail frees its slot
/// when it is sealed, and a writer that sees this asks again.
pub const TAIL_BUDGET_MESSAGE: &str = "too many log chunk tails are open on this replica; retry";

pub struct LogChunkStore {
    dir: PathBuf,
    chunks: BTreeMap<(u64, u64), LogChunk>,
    /// When each open chunk was last used, for closing the least recently used sealed one.
    used: BTreeMap<(u64, u64), u64>,
    tick: u64,
    max_sealed_open: usize,
    tail_budget: usize,
    /// Records of sealed chunks, shared by all of them.
    cache: RecordCache,
    /// Chunks with applied records that no sync has covered yet (deferred mode).
    dirty: BTreeSet<(u64, u64)>,
    /// Syncs completed, one per chunk per group.
    commits: u64,
    syncer: fn(&File) -> std::io::Result<()>,
}

/// What the committer fsyncs after it has released the replica lock.
pub struct PendingSync {
    key: (u64, u64),
    file: File,
    length: u64,
    syncer: fn(&File) -> std::io::Result<()>,
}

fn sync_data(file: &File) -> std::io::Result<()> {
    file.sync_data()
}

/// Make every pending handle durable. Takes no lock: the caller must not hold one.
pub fn sync_pending(pending: &[PendingSync]) -> Result<(), ObjectError> {
    for one in pending {
        (one.syncer)(&one.file).map_err(|error| ObjectError::Corruption(format!("log chunk sync failed: {error}")))?;
    }
    Ok(())
}

pub(super) fn to_object_error(error: LogError) -> ObjectError {
    match error {
        LogError::Fenced { current } => {
            ObjectError::PreconditionFailed(format!("log chunk writer epoch is stale; the chunk is at epoch {current}"))
        }
        LogError::Sealed => ObjectError::PreconditionFailed("log chunk is sealed".into()),
        LogError::Overlap => ObjectError::PreconditionFailed("log chunk bytes differ from bytes already held".into()),
        LogError::Window => ObjectError::PreconditionFailed("log chunk reorder window is full".into()),
        LogError::SealMismatch => ObjectError::PreconditionFailed("log chunk has records waiting; it cannot seal yet".into()),
        LogError::Bounds(what) => ObjectError::InvalidArgument(format!("log chunk bound: {what}")),
        LogError::Corrupt(message) => ObjectError::Corruption(message),
        LogError::Io(error) => ObjectError::Corruption(format!("log chunk storage failed: {error}")),
        LogError::Unavailable => ObjectError::Corruption("log chunk is unavailable after a failed write or sync; reopen to recover".into()),
    }
}

impl LogChunkStore {
    /// `dir` is created if it is missing.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        Self::with_limits(dir, MAX_OPEN_LOG_CHUNKS, DEFAULT_TAIL_BUDGET, DEFAULT_RECORD_CACHE_BYTES)
    }

    /// At most `max_sealed_open` sealed chunk files open at once, at most `tail_budget` open
    /// tails (each holds its bytes in memory), and `cache_bytes` of sealed-chunk records kept for
    /// reads. The two kinds of chunk are limited apart, so a read of a sealed chunk never waits for
    /// a tail to go.
    pub fn with_limits(dir: &Path, max_sealed_open: usize, tail_budget: usize, cache_bytes: usize) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            chunks: BTreeMap::new(),
            used: BTreeMap::new(),
            tick: 0,
            max_sealed_open: max_sealed_open.max(1),
            tail_budget,
            cache: RecordCache::new(cache_bytes),
            dirty: BTreeSet::new(),
            commits: 0,
            syncer: sync_data,
        })
    }

    /// Chunks open now.
    pub fn open_chunks(&self) -> usize {
        self.chunks.len()
    }
    /// Open chunks that can still change; each holds all of its bytes in memory.
    pub fn open_tails(&self) -> usize {
        self.chunks.values().filter(|chunk| chunk.sealed().is_none()).count()
    }
    /// Bytes of sealed-chunk records held for reads. Never above the cache bound.
    pub fn cache_bytes(&self) -> usize {
        self.cache.bytes()
    }
    #[cfg(test)]
    pub fn is_open(&self, volume: u64, chunk: u64) -> bool {
        self.chunks.contains_key(&(volume, chunk))
    }
    /// Open sealed chunks.
    pub fn open_sealed(&self) -> usize {
        self.chunks.values().filter(|chunk| chunk.sealed().is_some()).count()
    }
    /// Close the least recently used sealed chunk that has nothing waiting for a sync. An open
    /// tail, or any chunk with dirty state, is never closed. False when there was none to close.
    fn close_one_sealed(&mut self, keep: Option<(u64, u64)>) -> bool {
        let victim = self
            .used
            .iter()
              .filter(|(key, _)| Some(**key) != keep && !self.dirty.contains(key) && self.chunks.get(key).is_some_and(|chunk| chunk.sealed().is_some()))
            .min_by_key(|(_, used)| **used)
            .map(|(key, _)| *key);
        let Some(key) = victim else {
            return false;
        };
        self.chunks.remove(&key);
        self.used.remove(&key);
        true
    }
    /// Close sealed chunks, least recently used first, until no more than the limit are open (or
    /// the rest have a sync still to come).
    fn trim_sealed(&mut self, keep: Option<(u64, u64)>) {
        while self.open_sealed() > self.max_sealed_open {
            if !self.close_one_sealed(keep) {
                break;
            }
        }
    }

    fn path(&self, volume: u64, chunk: u64) -> PathBuf {
        self.dir.join(format!("{volume:016x}-{chunk:016x}.log"))
    }

    fn chunk(&mut self, request: &LogOpRequest, create: bool) -> Result<Option<&mut LogChunk>, ObjectError> {
        let key = (request.volume, request.chunk);
        if !self.chunks.contains_key(&key) {
            let path = self.path(request.volume, request.chunk);
            if !create && !path.exists() {
                return Ok(None);
            }
            let opened = LogChunk::open(&path, request.generation, LOG_CHUNK_LIMIT).map_err(to_object_error)?;
            let sealed = opened.sealed().is_some();
            if !sealed && self.open_tails() >= self.tail_budget {
                // a tail keeps all its bytes: the budget refuses, and nothing is closed for it
                return Err(ObjectError::PreconditionFailed(TAIL_BUDGET_MESSAGE.into()));
            }
            self.chunks.insert(key, opened);
            if sealed {
                self.tick += 1;
                self.used.insert(key, self.tick);
                self.trim_sealed(Some(key));
            }
        }
        self.tick += 1;
        self.used.insert(key, self.tick);
        Ok(self.chunks.get_mut(&key))
    }

    /// Replace the function that syncs a file (a test makes it slow or failing).
    #[cfg(test)]
    pub fn set_syncer(&mut self, syncer: fn(&File) -> std::io::Result<()>) {
        self.syncer = syncer;
    }

    pub fn has_dirty(&self) -> bool {
        !self.dirty.is_empty()
    }

    /// Syncs completed so far.
    #[cfg(test)]
    pub fn commits(&self) -> u64 {
        self.commits
    }

    /// Hand out one handle per dirty chunk and forget that they were dirty. The caller syncs them
    /// with `sync_pending` WITHOUT the replica lock and then calls `note_synced`.
    pub fn take_pending_syncs(&mut self) -> Result<Vec<PendingSync>, ObjectError> {
        let keys: Vec<(u64, u64)> = std::mem::take(&mut self.dirty).into_iter().collect();
        let mut pending = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(chunk) = self.chunks.get(&key) {
                let (file, length) = chunk
                    .sync_handle()
                    .map_err(|error| ObjectError::Corruption(format!("log chunk handle failed: {error}")))?;
                pending.push(PendingSync { key, file, length, syncer: self.syncer });
            }
        }
        Ok(pending)
    }

    pub fn note_synced(&mut self, done: &[PendingSync]) {
        for one in done {
            if let Some(chunk) = self.chunks.get_mut(&one.key) {
                chunk.note_durable(one.length);
                self.commits += 1;
            }
        }
    }

    /// Apply one operation. A mutation is durable when this returns (one sync per call).
    pub fn apply(&mut self, request: &LogOpRequest) -> Result<LogOpReply, ObjectError> {
        self.apply_with(request, true)
    }

    /// Apply one operation without syncing: the chunk is marked dirty, and the reply may be sent only
    /// after `take_pending_syncs`, `sync_pending` and `note_synced` have covered it.
    pub fn apply_deferred(&mut self, request: &LogOpRequest) -> Result<LogOpReply, ObjectError> {
        self.apply_with(request, false)
    }

    fn apply_with(&mut self, request: &LogOpRequest, commit: bool) -> Result<LogOpReply, ObjectError> {
        request.validate()?;
        let key = (request.volume, request.chunk);
        let mut dirtied = false;
        let reply = self.apply_inner(request, commit, &mut dirtied)?;
        if request.op == OP_SEAL {
            self.trim_sealed(None);
        }
        if dirtied {
            self.dirty.insert(key);
        } else if commit && request.is_mutation() {
            self.commits += 1;
        }
        Ok(reply)
    }

    fn apply_inner(&mut self, request: &LogOpRequest, commit: bool, dirtied: &mut bool) -> Result<LogOpReply, ObjectError> {
        let key = (request.volume, request.chunk);
        let Some(chunk) = self.chunk(request, request.op != OP_TAIL && request.op != OP_READ)? else {
            // a chunk this replica has never held: it is empty, and asking creates nothing
            return Ok(LogOpReply::default());
        };
        match request.op {
            OP_APPEND => {
                chunk.append(request.epoch, request.offset, &request.data).map_err(to_object_error)?;
                if commit {
                    chunk.commit().map_err(to_object_error)?;
                } else {
                    *dirtied = true;
                }
            }
            OP_SEAL => {
                chunk.seal(request.epoch, request.flags & FLAG_SEAL_SHA256 != 0).map_err(to_object_error)?;
            }
            OP_TRUNCATE => {
                chunk.truncate(request.epoch, request.offset).map_err(to_object_error)?;
                if commit {
                    chunk.commit().map_err(to_object_error)?;
                } else {
                    *dirtied = true;
                }
            }
            OP_TAIL | OP_READ => {}
            _ => return Err(ObjectError::InvalidArgument("unknown log chunk operation".into())),
        }
        let sealed = chunk.sealed();
        let durable = chunk.durable_length();
        let mut reply = LogOpReply {
            length: chunk.length(),
            // a deferred mutation is answered only after the group's sync, so its length is durable then
            durable_length: if *dirtied { chunk.length() } else { chunk.durable_length() },
            epoch: chunk.epoch(),
            sealed: sealed.is_some(),
            crc: chunk.footer_crc(),
            sha256: sealed.and_then(|s| s.sha256).map(|d| d.to_vec()).unwrap_or_default(),
            data: Vec::new(),
        };
        if request.op == OP_READ {
            // never above the durable length: an applied byte that no sync has covered is invisible
            let wanted = (request.length as u64).min(durable.saturating_sub(request.offset)) as usize;
            let mut data = Vec::new();
            let chunk = self.chunks.get(&key).expect("the chunk was opened above");
            chunk.read_into(request.offset, wanted, &mut self.cache, &mut data).map_err(to_object_error)?;
            reply.data = data;
        }
        Ok(reply)
    }
}

#[cfg(test)]
#[path = "log_chunk_store_limits_tests.rs"]
mod limits_tests;
