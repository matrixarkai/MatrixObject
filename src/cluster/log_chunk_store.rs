// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The log chunks a replica node holds: one file per chunk, opened when first used.
//!
//! Each file is a `LogChunk` (see `crate::log_chunk`): its length comes from replaying its own
//! records, so the node keeps no length table and a restart needs no recovery pass beyond opening
//! a chunk when something asks for it.
use super::log_op::{LogOpReply, LogOpRequest, FLAG_SEAL_SHA256, OP_APPEND, OP_READ, OP_SEAL, OP_TAIL, OP_TRUNCATE};
use crate::log_chunk::{LogChunk, LogError};
use crate::ObjectError;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A chunk of one log: the size a writer fills before it switches to the next.
pub const LOG_CHUNK_LIMIT: u64 = 64 * 1024 * 1024;
/// Chunk files held open at once.
pub const MAX_OPEN_LOG_CHUNKS: usize = 256;

pub struct LogChunkStore {
    dir: PathBuf,
    chunks: BTreeMap<(u64, u64), LogChunk>,
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
    }
}

impl LogChunkStore {
    /// `dir` is created if it is missing.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self { dir: dir.to_path_buf(), chunks: BTreeMap::new() })
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
            if self.chunks.len() >= MAX_OPEN_LOG_CHUNKS {
                return Err(ObjectError::PreconditionFailed("too many log chunks are open on this replica".into()));
            }
            let opened = LogChunk::open(&path, request.generation, LOG_CHUNK_LIMIT).map_err(to_object_error)?;
            self.chunks.insert(key, opened);
        }
        Ok(self.chunks.get_mut(&key))
    }

    /// Apply one operation. A mutation is durable when this returns (one sync per call; the
    /// group commit of the next part shares that sync across calls).
    pub fn apply(&mut self, request: &LogOpRequest) -> Result<LogOpReply, ObjectError> {
        request.validate()?;
        let Some(chunk) = self.chunk(request, request.op != OP_TAIL && request.op != OP_READ)? else {
            // a chunk this replica has never held: it is empty, and asking creates nothing
            return Ok(LogOpReply::default());
        };
        match request.op {
            OP_APPEND => {
                chunk.append(request.epoch, request.offset, &request.data).map_err(to_object_error)?;
                chunk.commit().map_err(to_object_error)?;
            }
            OP_SEAL => {
                chunk.seal(request.epoch, request.flags & FLAG_SEAL_SHA256 != 0).map_err(to_object_error)?;
            }
            OP_TRUNCATE => {
                chunk.truncate(request.epoch, request.offset).map_err(to_object_error)?;
                chunk.commit().map_err(to_object_error)?;
            }
            OP_TAIL | OP_READ => {}
            _ => return Err(ObjectError::InvalidArgument("unknown log chunk operation".into())),
        }
        let sealed = chunk.sealed();
        Ok(LogOpReply {
            length: chunk.length(),
            durable_length: chunk.durable_length(),
            epoch: chunk.epoch(),
            sealed: sealed.is_some(),
            crc: chunk.footer_crc(),
            sha256: sealed.and_then(|s| s.sha256).map(|d| d.to_vec()).unwrap_or_default(),
            data: if request.op == OP_READ {
                chunk.read(request.offset, request.length as usize).to_vec()
            } else {
                Vec::new()
            },
        })
    }

    pub fn open_chunks(&self) -> usize {
        self.chunks.len()
    }
}
