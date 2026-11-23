// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The one replica verb that carries log chunk operations: append, seal, truncate, tail, read.
//!
//! One verb with an operation field keeps the replica wire surface small: an older replica
//! answers "unknown verb" to the whole family, which is the probe a client uses before it
//! relies on log chunks.
use crate::{ChunkId, ObjectError};
use prost::Message;
#[path = "log_protobuf_decode.rs"]
mod log_protobuf_decode;

pub const OP_APPEND: u32 = 1;
pub const OP_SEAL: u32 = 2;
pub const OP_TRUNCATE: u32 = 3;
pub const OP_TAIL: u32 = 4;
pub const OP_READ: u32 = 5;
/// Inspect one complete durable record at an exact offset; absence is not a writer fence.
pub const OP_IDENTITY: u32 = 6;
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
    /// Opaque sixteen-byte tag, represented as two little-endian words without a heap buffer.
    #[prost(fixed64, tag = "11")]
    pub attempt_hi: u64,
    #[prost(fixed64, tag = "12")]
    pub attempt_lo: u64,
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
    #[prost(uint64, tag = "8")]
    pub record_offset: u64,
    #[prost(uint32, tag = "9")]
    pub record_length: u32,
    #[prost(fixed64, tag = "10")]
    pub attempt_hi: u64,
    #[prost(fixed64, tag = "11")]
    pub attempt_lo: u64,
    #[prost(bool, tag = "12")]
    pub record_present: bool,
    #[prost(bool, tag = "13")]
    pub record_identified: bool,
}

impl LogOpRequest {
    pub fn is_mutation(&self) -> bool {
        matches!(self.op, OP_APPEND | OP_SEAL | OP_TRUNCATE)
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        ChunkId::new(self.volume, self.chunk)?;
        let versioned = match self.version {
            1 => self.attempt_hi == 0 && self.attempt_lo == 0 && self.op != OP_IDENTITY,
            2 => matches!(self.op, OP_APPEND | OP_IDENTITY),
            _ => false,
        };
        let shaped = versioned
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
                OP_TRUNCATE => {
                    self.epoch != 0 && self.length == 0 && self.flags == 0 && self.data.is_empty()
                }
                OP_TAIL => {
                    self.epoch == 0
                        && self.offset == 0
                        && self.length == 0
                        && self.flags == 0
                        && self.data.is_empty()
                }
                OP_IDENTITY => {
                    self.epoch == 0
                        && self.length == 0
                        && self.flags == 0
                        && self.data.is_empty()
                        && self.attempt_hi == 0
                        && self.attempt_lo == 0
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
            return Err(ObjectError::InvalidArgument(
                "invalid log chunk operation".into(),
            ));
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Err(ObjectError::InvalidArgument(
                "log chunk operation exceeds its bound".into(),
            ));
        }
        let value = match log_protobuf_decode::request(bytes) {
            Some(value) => value,
            None => {
                // Keep malformed-message and noncanonical-message errors distinct.
                // Only the rejected path uses the general decoder.
                Self::decode(bytes)
                    .map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
                return Err(ObjectError::Corruption(
                    "noncanonical log chunk operation".into(),
                ));
            }
        };
        value
            .validate()
            .map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
        Ok(value)
    }
    /// Decode an owned RPC body, reusing its allocation for nonempty data.
    pub fn from_payload_owned(bytes: Vec<u8>) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Self::from_payload(&bytes);
        }
        let value = match log_protobuf_decode::request_owned(bytes) {
            Ok(value) => value,
            Err(bytes) => return Self::from_payload(&bytes),
        };
        value
            .validate()
            .map_err(|_| ObjectError::Corruption("invalid log chunk operation".into()))?;
        Ok(value)
    }
    pub fn append(
        volume: u64,
        chunk: u64,
        generation: u32,
        epoch: u64,
        offset: u64,
        data: Vec<u8>,
    ) -> Self {
        Self {
            version: 1,
            op: OP_APPEND,
            volume,
            chunk,
            generation,
            epoch,
            offset,
            data,
            ..Default::default()
        }
    }
    pub fn append_identified(
        volume: u64,
        chunk: u64,
        generation: u32,
        epoch: u64,
        offset: u64,
        data: Vec<u8>,
        tag: [u8; 16],
    ) -> Self {
        Self {
            version: 2,
            attempt_hi: u64::from_le_bytes(tag[..8].try_into().unwrap()),
            attempt_lo: u64::from_le_bytes(tag[8..].try_into().unwrap()),
            ..Self::append(volume, chunk, generation, epoch, offset, data)
        }
    }
    pub fn identity(volume: u64, chunk: u64, generation: u32, offset: u64) -> Self {
        Self {
            version: 2,
            op: OP_IDENTITY,
            volume,
            chunk,
            generation,
            offset,
            ..Default::default()
        }
    }
    pub fn attempt_tag(&self) -> Option<[u8; 16]> {
        (self.version == 2 && self.op == OP_APPEND)
            .then(|| tag_bytes(self.attempt_hi, self.attempt_lo))
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
        Self {
            version: 1,
            op: OP_TRUNCATE,
            volume,
            chunk,
            generation,
            epoch,
            offset: length,
            ..Default::default()
        }
    }
    pub fn tail(volume: u64, chunk: u64, generation: u32) -> Self {
        Self {
            version: 1,
            op: OP_TAIL,
            volume,
            chunk,
            generation,
            ..Default::default()
        }
    }
    pub fn read(volume: u64, chunk: u64, generation: u32, offset: u64, length: u32) -> Self {
        Self {
            version: 1,
            op: OP_READ,
            volume,
            chunk,
            generation,
            offset,
            length,
            ..Default::default()
        }
    }
}

fn tag_bytes(hi: u64, lo: u64) -> [u8; 16] {
    let mut tag = [0; 16];
    tag[..8].copy_from_slice(&hi.to_le_bytes());
    tag[8..].copy_from_slice(&lo.to_le_bytes());
    tag
}

impl LogOpReply {
    pub(super) fn identity_shape_valid(&self) -> bool {
        if !self.record_present {
            self.record_offset == 0
                && self.record_length == 0
                && !self.record_identified
                && self.attempt_hi == 0
                && self.attempt_lo == 0
        } else {
            self.record_length as usize <= MAX_OP_DATA
                && self
                    .record_offset
                    .checked_add(u64::from(self.record_length))
                    .is_some_and(|end| end <= self.durable_length && end <= self.length)
                && (self.record_identified || (self.attempt_hi == 0 && self.attempt_lo == 0))
        }
    }
    /// Storage observation only. The caller must fence pending writes and establish quorum.
    pub fn record_identity(&self) -> Option<crate::log_chunk::AppendRecordIdentity> {
        (self.record_present && self.identity_shape_valid()).then(|| {
            crate::log_chunk::AppendRecordIdentity {
                offset: self.record_offset,
                length: self.record_length,
                attempt_tag: self
                    .record_identified
                    .then(|| tag_bytes(self.attempt_hi, self.attempt_lo)),
            }
        })
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Err(ObjectError::Corruption(
                "log chunk reply exceeds its bound".into(),
            ));
        }
        let value = match log_protobuf_decode::reply(bytes) {
            Some(value) => value,
            None => {
                Self::decode(bytes)
                    .map_err(|_| ObjectError::Corruption("invalid log chunk reply".into()))?;
                return Err(ObjectError::Corruption(
                    "noncanonical log chunk reply".into(),
                ));
            }
        };
        if !(value.sha256.is_empty() || value.sha256.len() == 32) || !value.identity_shape_valid() {
            return Err(ObjectError::Corruption(
                "noncanonical log chunk reply".into(),
            ));
        }
        Ok(value)
    }
    /// Decode an owned RPC body, retaining its data buffer and copying a seal digest.
    pub fn from_payload_owned(bytes: Vec<u8>) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_OP_DATA + 256 {
            return Self::from_payload(&bytes);
        }
        let value = match log_protobuf_decode::reply_owned(bytes) {
            Ok(value) => value,
            Err(bytes) => return Self::from_payload(&bytes),
        };
        if !(value.sha256.is_empty() || value.sha256.len() == 32) || !value.identity_shape_valid() {
            return Err(ObjectError::Corruption(
                "noncanonical log chunk reply".into(),
            ));
        }
        Ok(value)
    }
}
