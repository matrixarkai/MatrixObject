// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Physical log transitions committed with their local page mutations.
use crate::{ChunkId, ObjectError};
use prost::Message;

pub const LOG_PAGE_OUTCOME_MAGIC: &[u8; 4] = b"MOLP";
pub const MAX_LOG_PAGE_OUTCOME_BYTES: usize = 2048;
pub const LOG_PAGE_INITIALIZE: u32 = 1;
pub const LOG_PAGE_APPEND: u32 = 2;
pub const LOG_PAGE_FENCE: u32 = 3;
pub const LOG_PAGE_SEAL: u32 = 4;

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn identity(value: &[u8]) -> bool {
    value.len() == 16 && value.iter().any(|byte| *byte != 0)
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogPageState {
    #[prost(uint32, tag = "1")]
    pub generation: u32,
    #[prost(uint64, tag = "2")]
    pub writer_epoch: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub initialization: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub storage_incarnation: Vec<u8>,
    #[prost(uint64, tag = "5")]
    pub length: u64,
    #[prost(uint64, tag = "6")]
    pub published_length: u64,
    #[prost(uint64, tag = "7")]
    pub limit: u64,
    #[prost(uint32, tag = "8")]
    pub page_bytes: u32,
    #[prost(fixed32, tag = "9")]
    pub crc32c: u32,
    #[prost(bool, tag = "10")]
    pub sealed: bool,
    #[prost(bytes = "vec", tag = "11")]
    pub seal_sha256: Vec<u8>,
}
impl LogPageState {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.generation == 0
            || self.writer_epoch == 0
            || !identity(&self.initialization)
            || !identity(&self.storage_incarnation)
            || self.limit == 0
            || self.limit > (1u64 << 40)
            || self.length > self.limit
            || self.published_length > self.length
            || !(4096..=1024 * 1024).contains(&self.page_bytes)
            || self.page_bytes % 4096 != 0
            || (self.length == 0 && self.crc32c != 0)
            || (!self.seal_sha256.is_empty() && (!self.sealed || self.seal_sha256.len() != 32))
            || (self.sealed && self.length != self.published_length)
        {
            return Err(invalid("invalid page-backed log state"));
        }
        Ok(())
    }
    fn same_storage(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.initialization == other.initialization
            && self.storage_incarnation == other.storage_incarnation
            && self.limit == other.limit
            && self.page_bytes == other.page_bytes
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogPageAppendIdentity {
    #[prost(bytes = "vec", tag = "1")]
    pub attempt: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub offset: u64,
    #[prost(uint32, tag = "3")]
    pub length: u32,
    #[prost(fixed32, tag = "4")]
    pub crc32c: u32,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogPageOutcome {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub volume_id: u64,
    #[prost(uint64, tag = "3")]
    pub chunk_id: u64,
    #[prost(uint32, tag = "4")]
    pub kind: u32,
    #[prost(message, optional, tag = "5")]
    pub before: Option<LogPageState>,
    #[prost(message, optional, tag = "6")]
    pub after: Option<LogPageState>,
    #[prost(message, optional, tag = "7")]
    pub append: Option<LogPageAppendIdentity>,
}
impl LogPageOutcome {
    pub fn validate(&self) -> Result<(), ObjectError> {
        ChunkId::new(self.volume_id, self.chunk_id)?;
        if self.version != 1 || self.encoded_len() + 4 > MAX_LOG_PAGE_OUTCOME_BYTES {
            return Err(invalid("unsupported page-backed log outcome"));
        }
        let after = self
            .after
            .as_ref()
            .ok_or_else(|| invalid("log outcome omits state"))?;
        after.validate()?;
        if self.kind == LOG_PAGE_INITIALIZE {
            if self.before.is_some() || self.append.is_some() || after.length != 0 || after.sealed {
                return Err(invalid("invalid page-backed log initialization"));
            }
            return Ok(());
        }
        let before = self
            .before
            .as_ref()
            .ok_or_else(|| invalid("log outcome omits prior state"))?;
        before.validate()?;
        if before.sealed || !before.same_storage(after) {
            return Err(invalid("log transition changes storage or a sealed chunk"));
        }
        match self.kind {
            LOG_PAGE_APPEND => {
                let append = self
                    .append
                    .as_ref()
                    .ok_or_else(|| invalid("log append omits identity"))?;
                if after.sealed
                    || after.writer_epoch != before.writer_epoch
                    || append.attempt.len() != 16
                    || append.length == 0
                    || append.length as usize > crate::log_chunk::MAX_RECORD_DATA
                    || append.offset != before.length
                    || append.offset.checked_add(u64::from(append.length)) != Some(after.length)
                    || ![before.published_length, after.length].contains(&after.published_length)
                {
                    return Err(invalid("invalid page-backed log append transition"));
                }
            }
            LOG_PAGE_FENCE => {
                if self.append.is_some()
                    || after.sealed
                    || after.writer_epoch < before.writer_epoch
                    || after.length != before.published_length
                    || after.published_length != before.published_length
                    || (after.length == before.length && after.crc32c != before.crc32c)
                {
                    return Err(invalid("invalid page-backed log fence transition"));
                }
            }
            LOG_PAGE_SEAL => {
                if self.append.is_some()
                    || !after.sealed
                    || after.writer_epoch != before.writer_epoch
                    || after.length != before.length
                    || after.crc32c != before.crc32c
                    || before.length != before.published_length
                {
                    return Err(invalid("invalid page-backed log seal transition"));
                }
            }
            _ => return Err(invalid("unsupported page-backed log transition kind")),
        }
        Ok(())
    }
    /// Validate the bytes already supplied by the affected page patches.
    pub fn check_append_parts<'a>(
        &self,
        parts: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<(), ObjectError> {
        self.validate()?;
        if self.kind != LOG_PAGE_APPEND {
            return Err(invalid("log data requires an append transition"));
        }
        let append = self.append.as_ref().unwrap();
        let mut crc = 0;
        let mut prefix_crc = self.before.as_ref().unwrap().crc32c;
        let mut length = 0u64;
        for part in parts {
            length = length
                .checked_add(part.len() as u64)
                .ok_or_else(|| invalid("log append data length overflow"))?;
            crc = crate::log_chunk::crc32c_update_stored(crc, part);
            prefix_crc = crate::log_chunk::crc32c_update_stored(prefix_crc, part);
        }
        if length != u64::from(append.length)
            || crc != append.crc32c
            || prefix_crc != self.after.as_ref().unwrap().crc32c
        {
            return Err(invalid(
                "log append page bytes differ from decided identity",
            ));
        }
        Ok(())
    }
    pub fn to_payload(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(4 + self.encoded_len());
        bytes.extend_from_slice(LOG_PAGE_OUTCOME_MAGIC);
        self.encode(&mut bytes)
            .map_err(|_| invalid("log outcome encoding failed"))?;
        Ok(bytes)
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_LOG_PAGE_OUTCOME_BYTES || !bytes.starts_with(LOG_PAGE_OUTCOME_MAGIC) {
            return Err(invalid("invalid page-backed log outcome envelope"));
        }
        let outcome =
            Self::decode(&bytes[4..]).map_err(|_| invalid("invalid log outcome protobuf"))?;
        outcome.validate()?;
        if outcome.to_payload()? != bytes {
            return Err(invalid("noncanonical page-backed log outcome"));
        }
        Ok(outcome)
    }
    /// The recovery owner must apply this beside its page outcomes, after checking
    /// the entire batch. This does not re-execute an append request.
    pub fn check_before(&self, state: Option<&LogPageState>) -> Result<(), ObjectError> {
        self.validate()?;
        if self.before.as_ref() != state {
            return Err(invalid("page-backed log prior state differs"));
        }
        Ok(())
    }
}
