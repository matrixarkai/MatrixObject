// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The one replica verb that carries log chunk operations: append, seal, truncate, tail, read.
//!
//! One verb with an operation field keeps the replica wire surface small: an older replica
//! answers "unknown verb" to the whole family, which is the probe a client uses before it
//! relies on log chunks.
use crate::{ChunkId, ObjectError};
use prost::Message;

pub const OP_APPEND: u32 = 1;
pub const OP_SEAL: u32 = 2;
pub const OP_TRUNCATE: u32 = 3;
pub const OP_TAIL: u32 = 4;
pub const OP_READ: u32 = 5;
/// Seal: also record the SHA-256 of the sealed bytes.
pub const FLAG_SEAL_SHA256: u32 = 1;
/// One record, and one read, carry at most this many bytes.
pub const MAX_OP_DATA: usize = crate::log_chunk::MAX_RECORD_DATA;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogOpRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub op: u32,
    #[prost(uint64, tag = "3")]
    pub volume: u64,
    #[prost(uint64, tag = "4")]
    pub chunk: u64,
    #[prost(uint32, tag = "5")]
    pub generation: u32,
    /// The writer epoch (append, seal, truncate). Zero for a tail or a read.
    #[prost(uint64, tag = "6")]
    pub epoch: u64,
    /// Append: where the data starts. Truncate: the new length. Read: where to start.
    #[prost(uint64, tag = "7")]
    pub offset: u64,
    /// Read: bytes wanted.
    #[prost(uint32, tag = "8")]
    pub length: u32,
    #[prost(uint32, tag = "9")]
    pub flags: u32,
    /// Append: the bytes.
    #[prost(bytes = "vec", tag = "10")]
    pub data: Vec<u8>,
}

/// The chunk as it stands after the operation.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogOpReply {
    /// Bytes applied: the acknowledgement. Everything below it is held.
    #[prost(uint64, tag = "1")]
    pub length: u64,
    /// Bytes made durable.
    #[prost(uint64, tag = "2")]
    pub durable_length: u64,
    #[prost(uint64, tag = "3")]
    pub epoch: u64,
    #[prost(bool, tag = "4")]
    pub sealed: bool,
    /// CRC-32C over the whole chunk (`length` bytes).
    #[prost(uint32, tag = "5")]
    pub crc: u32,
    /// Sealed with a digest: the SHA-256 of the sealed bytes.
    #[prost(bytes = "vec", tag = "6")]
    pub sha256: Vec<u8>,
    /// Read: the bytes.
    #[prost(bytes = "vec", tag = "7")]
    pub data: Vec<u8>,
}

impl LogOpRequest {
    pub fn is_mutation(&self) -> bool {
        matches!(self.op, OP_APPEND | OP_SEAL | OP_TRUNCATE)
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        ChunkId::new(self.volume, self.chunk)?;
        let shaped = self.version == 1
            && match self.op {
                OP_APPEND => {
                    self.epoch != 0
                        && !self.data.is_empty()
                        && self.data.len() <= MAX_OP_DATA
                        && self.length == 0
                        && self.flags == 0
                }
                OP_SEAL => {
                    self.epoch != 0
                        && self.offset == 0
                        && self.length == 0
                        && self.flags & !FLAG_SEAL_SHA256 == 0
                        && self.data.is_empty()
                }
                OP_TRUNCATE => self.epoch != 0 && self.length == 0 && self.flags == 0 && self.data.is_empty(),
                OP_TAIL => {
                    self.epoch == 0 && self.offset == 0 && self.length == 0 && self.flags == 0 && self.data.is_empty()
                }
                OP_READ => {
                    self.epoch == 0
                        && self.length as usize <= MAX_OP_DATA
                        && self.flags == 0
                        && self.data.is_empty()
                }
                _ => false,
            };
        if !shaped {
            return Err(ObjectError::InvalidArgument("invalid log chunk operation".into()));
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Err(ObjectError::InvalidArgument("log chunk operation exceeds its bound".into()));
        }
        let value = Self::decode(bytes).map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
        if value.encode_to_vec() != bytes {
            return Err(ObjectError::Corruption("noncanonical log chunk operation".into()));
        }
        value.validate().map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
        Ok(value)
    }
    pub fn append(volume: u64, chunk: u64, generation: u32, epoch: u64, offset: u64, data: Vec<u8>) -> Self {
        Self { version: 1, op: OP_APPEND, volume, chunk, generation, epoch, offset, data, ..Default::default() }
    }
    pub fn seal(volume: u64, chunk: u64, generation: u32, epoch: u64, with_sha256: bool) -> Self {
        Self {
            version: 1,
            op: OP_SEAL,
            volume,
            chunk,
            generation,
            epoch,
            flags: if with_sha256 { FLAG_SEAL_SHA256 } else { 0 },
            ..Default::default()
        }
    }
    pub fn truncate(volume: u64, chunk: u64, generation: u32, epoch: u64, length: u64) -> Self {
        Self { version: 1, op: OP_TRUNCATE, volume, chunk, generation, epoch, offset: length, ..Default::default() }
    }
    pub fn tail(volume: u64, chunk: u64, generation: u32) -> Self {
        Self { version: 1, op: OP_TAIL, volume, chunk, generation, ..Default::default() }
    }
    pub fn read(volume: u64, chunk: u64, generation: u32, offset: u64, length: u32) -> Self {
        Self { version: 1, op: OP_READ, volume, chunk, generation, offset, length, ..Default::default() }
    }
}

impl LogOpReply {
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Err(ObjectError::Corruption("log chunk reply exceeds its bound".into()));
        }
        let value = Self::decode(bytes).map_err(|_| ObjectError::Corruption("invalid log chunk reply".into()))?;
        if value.encode_to_vec() != bytes || !(value.sha256.is_empty() || value.sha256.len() == 32) {
            return Err(ObjectError::Corruption("noncanonical log chunk reply".into()));
        }
        Ok(value)
    }
}
