// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The log chunks a replica node holds: one file per chunk, opened when first used.
//!
//! Each file is a `LogChunk` (see `crate::log_chunk`): its length comes from replaying its own
//! records, so the node keeps no length table and a restart needs no recovery pass beyond opening
//! a chunk when something asks for it.
use super::log_op::{
    LogOpReply, LogOpRequest, FLAG_SEAL_SHA256, OP_APPEND, OP_IDENTITY, OP_READ, OP_SEAL, OP_TAIL, OP_TRUNCATE, OP_INITIALIZE, OP_FENCE, OP_STORAGE,
};
use crate::log_chunk::{LogChunk, LogError, RecordCache};
use crate::ObjectError;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    publication_writes: bool,
    native_authority_present: bool,
    native_authority: Option<super::native_storage_authority::NativeStorageAuthority>,
    publication_payload_limit: usize,
    // Release ownership after all cached chunk handles.
    directory_lock: Option<Arc<File>>,
}

/// What the committer fsyncs after it has released the replica lock.
pub struct PendingSync {
    key: (u64, u64),
    file: File,
    length: u64,
    syncer: fn(&File) -> std::io::Result<()>,
    // A dropped pending file must stop owning bytes before ownership is released.
    _directory_lock: Arc<File>,
}

fn sync_data(file: &File) -> std::io::Result<()> {
    file.sync_data()
}

fn create_log_directory(dir: &Path) -> std::io::Result<()> {
    let directory = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    #[cfg(unix)]
    let stop = {
        let mut existing = directory;
        while !existing.try_exists()? {
            existing = existing
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
        }
        existing
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(existing)
            .to_path_buf()
    };
    std::fs::create_dir_all(directory)?;
    // Persist each new directory entry, including the supplied directory when a caller
    // just created it. This is startup work, not an append or sealed-read operation.
    #[cfg(unix)]
    {
        let mut current = directory;
        loop {
            File::open(current)?.sync_all()?;
            if current == stop {
                break;
            }
            let parent = current
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            if parent == current {
                break;
            }
            current = parent;
        }
    }
    Ok(())
}
/// Make every pending handle durable. Takes no lock: the caller must not hold one.
pub fn sync_pending(pending: &[PendingSync]) -> Result<(), ObjectError> {
    for one in pending {
        (one.syncer)(&one.file)
            .map_err(|error| ObjectError::Corruption(format!("log chunk sync failed: {error}")))?;
    }
    Ok(())
}

/// What a node says when a writer's epoch is behind the chunk's: the client parses the number.
pub(super) const STALE_EPOCH_PREFIX: &str =
    "log chunk writer epoch is stale; the chunk is at epoch ";
pub(super) const SEALED_MESSAGE: &str = "log chunk is sealed";

pub(super) fn to_object_error(error: LogError) -> ObjectError {
    match error {
        LogError::Fenced { current } => {
            ObjectError::PreconditionFailed(format!("{STALE_EPOCH_PREFIX}{current}"))
        }
        LogError::Sealed => ObjectError::PreconditionFailed(SEALED_MESSAGE.into()),
        LogError::Overlap => {
            ObjectError::PreconditionFailed("log chunk bytes differ from bytes already held".into())
        }
        LogError::Window => {
            ObjectError::PreconditionFailed("log chunk reorder window is full".into())
        }
        LogError::SealMismatch => ObjectError::PreconditionFailed(
            "log chunk has records waiting; it cannot seal yet".into(),
        ),
        LogError::Bounds(what) => ObjectError::InvalidArgument(format!("log chunk bound: {what}")),
        LogError::Corrupt(message) => ObjectError::Corruption(message),
        LogError::Io(error) => {
            ObjectError::Corruption(format!("log chunk storage failed: {error}"))
        }
        LogError::Unavailable => ObjectError::Corruption(
            "log chunk is unavailable after a failed write or sync; reopen to recover".into(),
        ),
    }
}

impl LogChunkStore {
    /// Create a store without claiming its directory. The first operation that opens a chunk
    /// takes exclusive ownership; pending sync handles retain that ownership.
    /// Page-only replicas can therefore be constructed without reserving log storage.
    /// `dir` is created if it is missing.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        Self::with_limits(
            dir,
            MAX_OPEN_LOG_CHUNKS,
            DEFAULT_TAIL_BUDGET,
            DEFAULT_RECORD_CACHE_BYTES,
        )
    }

    /// At most `max_sealed_open` sealed chunk files open at once, at most `tail_budget` open
    /// tails (each holds its bytes in memory), and `cache_bytes` of sealed-chunk records kept for
    /// reads. The two kinds of chunk are limited apart, so a read of a sealed chunk never waits for
    /// a tail to go.
    pub fn with_limits(
        dir: &Path,
        max_sealed_open: usize,
        tail_budget: usize,
        cache_bytes: usize,
    ) -> std::io::Result<Self> {
        create_log_directory(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            directory_lock: None,
            chunks: BTreeMap::new(),
            used: BTreeMap::new(),
            tick: 0,
            max_sealed_open: max_sealed_open.max(1),
            tail_budget,
            cache: RecordCache::new(cache_bytes),
            dirty: BTreeSet::new(),
            commits: 0,
            syncer: sync_data,
            publication_writes: false,
            native_authority_present: super::native_storage_authority::NativeStorageAuthority::path(dir).try_exists()?,
            native_authority: None,
            publication_payload_limit: super::log_op::MAX_OP_DATA + 256,
        })
    }

    /// Explicit integration/deployment guard, disabled by default. This is not fresh-allocation,
    /// membership or metadata lease authorization; the upper protocol must provide those.
    pub(crate) fn set_publication_writes(&mut self, enabled: bool) { self.publication_writes = enabled; }
    pub(crate) fn set_publication_payload_limit(&mut self, limit: usize) { self.publication_payload_limit = limit; }

    fn native_authority(&mut self) -> Result<&mut super::native_storage_authority::NativeStorageAuthority,ObjectError> {
        self.claim_directory()?;
        if self.native_authority.is_none() {
            self.native_authority=Some(super::native_storage_authority::NativeStorageAuthority::open(&self.dir)
                .map_err(|e| ObjectError::Corruption(format!("native storage authority recovery failed: {e}")))?);
            self.native_authority_present=true;
        }
        Ok(self.native_authority.as_mut().unwrap())
    }
    /// Chunks open now.
    #[cfg(test)]
    pub fn open_chunks(&self) -> usize {
        self.chunks.len()
    }
    /// Open chunks that can still change; each holds all of its bytes in memory.
    pub fn open_tails(&self) -> usize {
        self.chunks
            .values()
            .filter(|chunk| chunk.sealed().is_none())
            .count()
    }
    /// Bytes of sealed-chunk records held for reads. Never above the cache bound.
    #[cfg(test)]
    pub fn cache_bytes(&self) -> usize {
        self.cache.bytes()
    }
    #[cfg(test)]
    pub fn is_open(&self, volume: u64, chunk: u64) -> bool {
        self.chunks.contains_key(&(volume, chunk))
    }
    /// Open sealed chunks.
    pub fn open_sealed(&self) -> usize {
        self.chunks
            .values()
            .filter(|chunk| chunk.sealed().is_some())
            .count()
    }
    /// Close the least recently used sealed chunk that has nothing waiting for a sync. An open
    /// tail, or any chunk with dirty state, is never closed. False when there was none to close.
    fn close_one_sealed(&mut self, keep: Option<(u64, u64)>) -> bool {
        let victim = self
            .used
            .iter()
            .filter(|(key, _)| {
                Some(**key) != keep
                    && !self.dirty.contains(key)
                    && self
                        .chunks
                        .get(key)
                        .is_some_and(|chunk| chunk.sealed().is_some())
            })
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

    fn claim_directory(&mut self) -> Result<(), ObjectError> {
        if self.directory_lock.is_some() {
            return Ok(());
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.dir.join(".writer.lock"))
            .map_err(|error| {
                ObjectError::Corruption(format!(
                    "log chunk directory lock cannot be opened: {error}"
                ))
            })?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
            {
                ObjectError::PreconditionFailed(
                    "log chunk directory already has an active owner".into(),
                )
            } else {
                ObjectError::Corruption(format!("log chunk directory lock failed: {error}"))
            }
        })?;
        self.directory_lock = Some(Arc::new(file));
        Ok(())
    }

    fn chunk(
        &mut self,
        request: &LogOpRequest,
        create: bool,
    ) -> Result<Option<&mut LogChunk>, ObjectError> {
        let key = (request.volume, request.chunk);
        if !self.chunks.contains_key(&key) {
            let path = self.path(request.volume, request.chunk);
            // A missing-chunk query neither caches a handle nor reserves storage.
            if !create
                && !path.try_exists().map_err(|error| {
                    ObjectError::Corruption(format!("log chunk lookup failed: {error}"))
                })?
            {
                return Ok(None);
            }
            self.claim_directory()?;
            if !create
                && !path.try_exists().map_err(|error| {
                    ObjectError::Corruption(format!("owned log chunk lookup failed: {error}"))
                })?
            {
                return Ok(None);
            }
            if request.version==5 && self.native_authority.as_ref().is_some_and(|a| a.is_ready(request.volume,request.chunk))
                && path.metadata().map_err(|e| ObjectError::Corruption(e.to_string()))?.len()==0 {
                return Err(ObjectError::Corruption("initialized native storage file is empty".into()));
            }
            let opened = LogChunk::open(&path, request.generation, LOG_CHUNK_LIMIT)
                .map_err(to_object_error)?;
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
        self.claim_directory()?;
        let chunk = self.chunks.get_mut(&key).expect("opened log chunk exists");
        if chunk.generation() != request.generation {
            return Err(ObjectError::PreconditionFailed(
                "log chunk request names another generation".into(),
            ));
        }
        Ok(Some(chunk))
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
                let (file, length) = chunk.sync_handle().map_err(|error| {
                    ObjectError::Corruption(format!("log chunk handle failed: {error}"))
                })?;
                let ownership = self.directory_lock.as_ref().ok_or_else(|| {
                    ObjectError::Corruption("dirty log chunk has no directory owner".into())
                })?;
                pending.push(PendingSync {
                    key,
                    file,
                    length,
                    syncer: self.syncer,
                    _directory_lock: Arc::clone(ownership),
                });
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

    fn apply_with(
        &mut self,
        request: &LogOpRequest,
        commit: bool,
    ) -> Result<LogOpReply, ObjectError> {
        request.validate()?;
        if request.version >= 4 && request.is_mutation() && !self.publication_writes {
            return Err(ObjectError::PreconditionFailed("publication wire writes are disabled".into()));
        }
        if self.native_authority_present { self.native_authority()?; }
        if request.version==5 && request.op==OP_STORAGE {
            let root=self.native_authority()?.incarnation().map_err(|e| ObjectError::Corruption(e.to_string()))?;
            return Ok(LogOpReply { publication_supported:true,publication_protocol:5,
                publication_writes_enabled:self.publication_writes,publication_payload_limit:self.publication_payload_limit as u64,
                storage_hi:u64::from_le_bytes(root[..8].try_into().unwrap()),storage_lo:u64::from_le_bytes(root[8..].try_into().unwrap()),..Default::default() });
        }
        if request.version!=5 && self.native_authority.as_ref().is_some_and(|a| a.contains(request.volume,request.chunk)) {
            return Err(ObjectError::PreconditionFailed("native storage requires a physical incarnation".into()));
        }
        if request.version==5 {
            let path=self.path(request.volume,request.chunk);
            let exists=if request.op==OP_INITIALIZE { path.try_exists().map_err(|e| ObjectError::Corruption(e.to_string()))? } else { false };
            let authority=self.native_authority()?;
            authority.verify_incarnation(request.storage_incarnation().unwrap()).map_err(|e| ObjectError::PreconditionFailed(e.to_string()))?;
            let initialization=request.initialization_tag().ok_or_else(|| ObjectError::InvalidArgument("native request lacks initialization".into()))?;
            if request.op==OP_INITIALIZE {
                authority.begin(super::native_storage_authority::StorageOwner { volume:request.volume,chunk:request.chunk,generation:request.generation,epoch:request.epoch,initialization:initialization.to_vec(),state:1 },exists)
                    .map_err(|e| ObjectError::PreconditionFailed(e.to_string()))?;
            } else {
                authority.authorize(request.volume,request.chunk,request.generation,initialization).map_err(|e| ObjectError::PreconditionFailed(e.to_string()))?;
            }
        }
        let key = (request.volume, request.chunk);
        let mut dirtied = false;
        let mut reply = self.apply_inner(request, commit, &mut dirtied)?;
        if request.version==5 {
            let authority=self.native_authority()?;
            if request.op==OP_INITIALIZE { authority.complete(request.volume,request.chunk).map_err(|e| ObjectError::Corruption(e.to_string()))?; }
            let root=authority.incarnation().map_err(|e| ObjectError::Corruption(e.to_string()))?;
            reply.storage_hi=u64::from_le_bytes(root[..8].try_into().unwrap());reply.storage_lo=u64::from_le_bytes(root[8..].try_into().unwrap());
        }
        if request.op == OP_SEAL {
            self.trim_sealed(None);
        }
        if dirtied {
            self.dirty.insert(key);
        } else if matches!(request.op, OP_INITIALIZE | OP_SEAL) || (commit && request.is_mutation()) {
            self.commits += 1;
        }
        Ok(reply)
    }

    fn apply_inner(
        &mut self,
        request: &LogOpRequest,
        commit: bool,
        dirtied: &mut bool,
    ) -> Result<LogOpReply, ObjectError> {
        let key = (request.volume, request.chunk);
        let writes_enabled = self.publication_writes;
        let payload_limit = self.publication_payload_limit;
        let completed_creation = request.version==5 && request.op==OP_INITIALIZE
            && self.native_authority.as_ref().is_some_and(|a| a.is_ready(request.volume,request.chunk));
        let create = request.is_mutation() && (request.version < 4 || request.op == OP_INITIALIZE);
        let Some(chunk) = self.chunk(request, create)?
        else {
            // a chunk this replica has never held: it is empty, and asking creates nothing
            if request.version == 5 || (request.version >= 4 && (request.is_mutation() || matches!(request.op,OP_READ|OP_IDENTITY))) {
                return Err(ObjectError::PreconditionFailed("publication member is absent; verified history transfer is required".into()));
            }
            return Ok(LogOpReply { publication_supported: request.version >= 3,
                publication_protocol: if request.version>=4 {request.version} else {0},
                publication_writes_enabled: request.version>=4 && writes_enabled,
                publication_payload_limit: if request.version>=4 {payload_limit as u64} else {0},
                ..LogOpReply::default() });
        };
        if completed_creation && chunk.publication_initialization()!=request.initialization_tag() {
            return Err(ObjectError::Corruption("completed native initialization is missing from storage".into()));
        }
        // Wire versions 1 and 2 cannot express object publication boundaries. Their reads
        // must refuse these files rather than exposing an unpublished physical prefix.
        if chunk.has_publication_records() && request.version < 3 {
            return Err(ObjectError::PreconditionFailed(
                "publication-aware log protocol is required".into(),
            ));
        }
        if request.version >= 4 && request.op != OP_INITIALIZE && (request.is_mutation() || matches!(request.op,OP_READ|OP_IDENTITY) || (request.version==5 && request.op==OP_TAIL)) {
            if chunk.publication_initialization() != request.initialization_tag() {
                return Err(ObjectError::PreconditionFailed("publication initialization identity differs".into()));
            }
            if request.is_mutation() && request.op != OP_FENCE && request.epoch != chunk.epoch() {
                return Err(ObjectError::PreconditionFailed("publication mutation requires the current fenced epoch".into()));
            }
        }
        match request.op {
            OP_INITIALIZE => chunk.initialize_publication(request.epoch, request.initialization_tag().expect("validated initialization request")).map_err(to_object_error)?,
            OP_FENCE => {
                chunk.fence_publication(request.epoch).map_err(to_object_error)?;
                if commit { chunk.commit().map_err(to_object_error)?; } else { *dirtied=true; }
            }
            OP_APPEND => {
                if request.version >= 4 {
                    chunk.append_published(request.epoch,request.offset,&request.data,request.attempt_tag().expect("validated identified append"),request.published_length)
                } else if let Some(tag) = request.attempt_tag() {
                    chunk.append_identified(request.epoch, request.offset, &request.data, tag)
                } else {
                    chunk.append(request.epoch, request.offset, &request.data)
                }.map_err(to_object_error)?;
                if commit {
                    chunk.commit().map_err(to_object_error)?;
                } else {
                    *dirtied = true;
                }
            }
            OP_SEAL => {
                if request.version >= 4 {
                    chunk.seal_publication(request.epoch, request.flags & FLAG_SEAL_SHA256 != 0)
                } else { chunk.seal(request.epoch, request.flags & FLAG_SEAL_SHA256 != 0) }.map_err(to_object_error)?;
            }
            OP_TRUNCATE => {
                chunk
                    .truncate(request.epoch, request.offset)
                    .map_err(to_object_error)?;
                if commit {
                    chunk.commit().map_err(to_object_error)?;
                } else {
                    *dirtied = true;
                }
            }
            OP_TAIL | OP_READ | OP_IDENTITY => {}
            _ => {
                return Err(ObjectError::InvalidArgument(
                    "unknown log chunk operation".into(),
                ))
            }
        }
        let sealed = chunk.sealed();
        let durable = chunk.durable_length();
        let mut reply = LogOpReply {
            length: chunk.length(),
            // a deferred mutation is answered only after the group's sync, so its length is durable then
            durable_length: if *dirtied {
                chunk.length()
            } else {
                chunk.durable_length()
            },
            epoch: chunk.epoch(),
            sealed: sealed.is_some(),
            crc: chunk.footer_crc(),
            sha256: sealed
                .and_then(|s| s.sha256)
                .map(|d| d.to_vec())
                .unwrap_or_default(),
            data: Vec::new(),
            published_length: if request.version >= 3 { if *dirtied {chunk.published_length()} else {chunk.durable_published_length().map_err(to_object_error)?} } else { 0 },
            publication_mode: request.version >= 3 && chunk.has_publication_records(),
            publication_supported: request.version >= 3,
            chunk_present: request.version >= 3,
            publication_protocol: if request.version>=4 {request.version} else {0},
            publication_writes_enabled: request.version>=4 && writes_enabled,
                publication_payload_limit: if request.version>=4 {payload_limit as u64} else {0},
            ..Default::default()
        };
        if request.version >= 4 {
            if let Some(tag) = chunk.publication_initialization() {
                reply.initialization_present=true;
                reply.initialization_hi=u64::from_le_bytes(tag[..8].try_into().unwrap());
                reply.initialization_lo=u64::from_le_bytes(tag[8..].try_into().unwrap());
            }
        }
        let record = if request.version >= 3 && request.op == OP_TAIL {
            chunk.durable_publication_record().map_err(to_object_error)?
        } else if request.op == OP_IDENTITY {
            chunk.durable_record_observation(request.offset).map_err(to_object_error)?
        } else { None };
        if let Some(record) = record {
            reply.record_present = true;
            reply.record_offset = record.identity.offset;
            reply.record_length = record.identity.length;
            if let Some(tag) = record.identity.attempt_tag {
                reply.record_identified = true;
                reply.attempt_hi = u64::from_le_bytes(tag[..8].try_into().unwrap());
                reply.attempt_lo = u64::from_le_bytes(tag[8..].try_into().unwrap());
            }
            if request.version >= 3 {
                reply.record_publication = record.published_length.is_some();
                reply.record_published_length = record.published_length.unwrap_or(0);
            }
        }
        if request.op == OP_READ {
            // never above the durable length: an applied byte that no sync has covered is invisible
            let wanted =
                (request.length as u64).min((if request.version >= 3 { reply.published_length } else { durable }).saturating_sub(request.offset)) as usize;
            let mut data = Vec::new();
            let chunk = self.chunks.get(&key).expect("the chunk was opened above");
            if wanted != 0 || request.version < 3 {
                chunk.read_into(request.offset, wanted, &mut self.cache, &mut data).map_err(to_object_error)?;
            }
            reply.data = data;
        }
        Ok(reply)
    }
}

#[cfg(test)]
#[path = "log_chunk_store_limits_tests.rs"]
mod limits_tests;

#[cfg(test)]
#[path = "log_chunk_generation_fencing_tests.rs"]
mod generation_fencing_tests;

#[cfg(test)]
#[path = "log_chunk_directory_ownership_tests.rs"]
mod directory_ownership_tests;

#[cfg(test)]
#[path = "log_chunk_publication_protocol_tests.rs"]
mod publication_protocol_tests;

#[cfg(test)]
#[path = "log_publication_query_tests.rs"]
mod publication_query_tests;

#[cfg(test)]
#[path = "guarded_publication_wire_tests.rs"]
mod guarded_wire_tests;
