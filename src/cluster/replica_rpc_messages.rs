// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One encoder and one decoder per message, shared by both ends.
//!
//! # WHY THE CLIENT AND THE SERVER DO NOT EACH HAVE THEIR OWN
//!
//! A wire format written twice is two formats that happen to agree today. The way that goes
//! wrong is quiet: a field is added to the writer, the reader is updated to match, and a
//! third call site that also writes the frame is missed -- and nothing fails, because the
//! reader is lenient about what it does not recognise. So the request is built here and
//! parsed here, the response is built here and parsed here, and the client and the server
//! are both callers. There is no second copy to keep in step.

use crate::cluster::wire::{
    put_bytes, put_str, put_u64, Cursor, RequestCode, StatusCode, MAX_PAYLOAD_BYTES,
    PROTOCOL_STAMP,
};
use crate::{ChunkId, DataRange, ObjectError, PageId, ReplicaHead, ReplicaReadResult};
use prost::Message;

/// Maximum page rows in one inventory response (32 bytes per row on the wire).
pub const MAX_INVENTORY_PAGES: u64 = 4096;

/// One bounded read of a replica's page index. Resume after the last entry when `has_more`.
/// Each call is consistent under the node lock; multiple calls are not a frozen snapshot.
/// Writers must be fenced before using a traversal to certify replica readmission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageInventory {
    pub entries: Vec<(PageId, u64)>,
    pub has_more: bool,
}

pub(crate) fn check_inventory_limit(limit: u64) -> Result<(), ObjectError> {
    if limit == 0 || limit > MAX_INVENTORY_PAGES {
        return Err(ObjectError::InvalidArgument(format!(
            "page inventory limit must be between 1 and {MAX_INVENTORY_PAGES}, got {limit}"
        )));
    }
    Ok(())
}

/// Immutable chunk-group configuration.
///
/// Versions 1 and 2 describe Raft voters. Version 3 describes fixed standalone
/// star members: `voters` supplies stable member numbers while peer endpoints,
/// bootstrap voters and the bootstrap ID stay empty. This identifies
/// configuration; it does not authenticate a server or establish readiness.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkGroupIdentity {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint64, tag = "2")]
    pub group_id: u64,
    #[prost(uint64, tag = "3")]
    pub node_id: u64,
    #[prost(uint64, tag = "4")]
    pub capacity_bytes: u64,
    #[prost(uint64, tag = "5")]
    pub max_pages: u64,
    #[prost(uint64, repeated, tag = "6")]
    pub voters: Vec<u64>,
    #[prost(string, repeated, tag = "7")]
    pub peer_endpoints: Vec<String>,
    /// Version 2: initial voters; voters/peer_endpoints describe the permitted inventory.
    #[prost(uint64, repeated, tag = "8")]
    pub bootstrap_voters: Vec<u64>,
    /// Version 2: effective group ID binding limits, bootstrap voters and endpoints.
    #[prost(uint64, tag = "9")]
    pub bootstrap_id: u64,
}
impl ChunkGroupIdentity {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        let mut sockets = std::collections::BTreeSet::new();
        let standalone_star = self.version == 3;
        if !matches!(self.version, 1 | 2 | 3) || self.group_id == 0 || self.node_id == 0
            || self.voters.is_empty() || self.voters.len() > if self.version == 2 {64} else {7}
            || (self.version == 1 && (!self.bootstrap_voters.is_empty() || self.bootstrap_id != 0))
            || (self.version == 2 && (self.bootstrap_id == 0 || self.bootstrap_voters.is_empty() || self.bootstrap_voters.len() > 7
                || self.bootstrap_voters.windows(2).any(|w| w[0] >= w[1])
                || self.bootstrap_voters.iter().any(|id| !self.voters.contains(id))))
            || (standalone_star && (self.capacity_bytes == 0 || self.max_pages == 0
                || !self.bootstrap_voters.is_empty() || self.bootstrap_id != 0
                || !self.peer_endpoints.is_empty()))
            || self.voters[0] == 0 || self.voters.windows(2).any(|w| w[0] >= w[1])
            || !self.voters.contains(&self.node_id)
            || (!standalone_star && self.peer_endpoints.len() != self.voters.len())
            || self.peer_endpoints.iter().any(|value| {
                let Ok(address) = value.parse::<std::net::SocketAddr>() else { return true; };
                value.len() > 128 || address.port() == 0 || address.ip().is_unspecified()
                    || address.ip().is_multicast() || address.to_string() != *value
                    || matches!(address.ip(), std::net::IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some())
                    || !sockets.insert(address)
            }) {
            return Err(ObjectError::Corruption("invalid chunk group identity".into()));
        }
        Ok(())
    }
    pub(crate) fn is_standalone_star(&self) -> bool {
        self.version == 3
    }
    pub(crate) fn same_group(&self, other: &Self) -> bool {
        let mut normalized = other.clone();
        normalized.node_id = self.node_id;
        *self == normalized
    }
}

/// A request as both ends understand it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalChunkWrite {
    pub page_id: PageId,
    pub offset: u64,
    pub data: Vec<u8>,
    pub expected_sequence: u64,
}

fn encoded_uint64_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        prost::encoding::key_len(tag) + prost::encoding::encoded_len_varint(value)
    }
}

fn encode_uint64(tag: u32, value: u64, out: &mut Vec<u8>) {
    if value != 0 {
        prost::encoding::uint64::encode(tag, &value, out);
    }
}

fn conditional_write_len(write: &ConditionalChunkWrite) -> usize {
    let mut len = encoded_uint64_len(1, 1)
        + encoded_uint64_len(2, write.page_id.chunk_id.volume_id())
        + encoded_uint64_len(3, write.page_id.chunk_id.id())
        + encoded_uint64_len(4, write.page_id.page_index)
        + encoded_uint64_len(5, write.offset)
        + encoded_uint64_len(6, write.expected_sequence);
    if !write.data.is_empty() {
        len += prost::encoding::key_len(7)
            + prost::encoding::encoded_len_varint(write.data.len() as u64)
            + write.data.len();
    }
    len
}

fn conditional_write_batch_len(writes: &[ConditionalChunkWrite]) -> usize {
    encoded_uint64_len(1, 1)
        + writes
            .iter()
            .map(|write| {
                let len = conditional_write_len(write);
                prost::encoding::key_len(2)
                    + prost::encoding::encoded_len_varint(len as u64)
                    + len
            })
            .sum::<usize>()
}

fn encode_conditional_write_batch(writes: &[ConditionalChunkWrite], out: &mut Vec<u8>) {
    use prost::encoding::{encode_key, encode_varint, WireType};

    encode_uint64(1, 1, out);
    for write in writes {
        encode_key(2, WireType::LengthDelimited, out);
        encode_varint(conditional_write_len(write) as u64, out);
        encode_uint64(1, 1, out);
        encode_uint64(2, write.page_id.chunk_id.volume_id(), out);
        encode_uint64(3, write.page_id.chunk_id.id(), out);
        encode_uint64(4, write.page_id.page_index, out);
        encode_uint64(5, write.offset, out);
        encode_uint64(6, write.expected_sequence, out);
        if !write.data.is_empty() {
            encode_key(7, WireType::LengthDelimited, out);
            encode_varint(write.data.len() as u64, out);
            out.extend_from_slice(&write.data);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaRequest {
    Membership(super::MembershipRequest),
    StateInstall(super::state_install::StateInstall),
    ReadChunkStateRange {
        page_id: PageId,
        expected_state_checksum: String,
        offset: u64,
        length: u64,
    },
    DiscardChunkIfSequence {
        page_id: PageId,
        offset: u64,
        length: u64,
        expected_sequence: u64,
    },
    WriteChunkIfSequence {
        page_id: PageId,
        offset: u64,
        data: Vec<u8>,
        expected_sequence: u64,
    },
    WriteChunkIfState {
        page_id: PageId,
        offset: u64,
        data: Vec<u8>,
        expected_sequence: u64,
        expected_state_checksum: String,
    },
    WriteChunkIfSequenceDigest {
        page_id: PageId,
        offset: u64,
        data: Vec<u8>,
        expected_sequence: u64,
    },
    WriteChunkBatchIfSequence {
        writes: Vec<ConditionalChunkWrite>,
    },
    WriteChunkBatchIfSequenceDigest {
        writes: Vec<ConditionalChunkWrite>,
    },
    WriteChunk {
        page_id: PageId,
        offset: u64,
        data: Vec<u8>,
    },
    ReadChunk {
        page_id: PageId,
        offset: u64,
        length: u64,
    },
    ReadChunkBatch {
        reads: Vec<(PageId, u64, u64)>,
    },
    DigestChunk {
        page_id: PageId,
        offset: u64,
        length: u64,
    },
    HeadChunk {
        page_id: PageId,
    },
    HeadChunkBatch {
        pages: Vec<PageId>,
    },
    DiscardChunk {
        page_id: PageId,
        offset: u64,
        length: u64,
    },
    Ping,
    Capabilities,
    ConditionalWriteLimits,
    WideConditionalWriteLimits,
    ReadBatchLimits,
    GroupIdentity,
    PageInventory {
        after: Option<PageId>,
        limit: u64,
    },
}

impl ReplicaRequest {
    pub fn code(&self) -> RequestCode {
        match self {
            ReplicaRequest::Membership(_) => RequestCode::Membership,
            ReplicaRequest::StateInstall(_) => RequestCode::StateInstall,
            ReplicaRequest::ReadChunkStateRange { .. } => RequestCode::ReadChunkStateRange,
            ReplicaRequest::DiscardChunkIfSequence { .. } => RequestCode::DiscardChunkIfSequence,
            ReplicaRequest::WriteChunk { .. } => RequestCode::WriteChunk,
            ReplicaRequest::WriteChunkIfSequence { .. } => RequestCode::WriteChunkIfSequence,
            ReplicaRequest::WriteChunkIfState { .. } => RequestCode::WriteChunkIfState,
            ReplicaRequest::WriteChunkIfSequenceDigest { .. } => {
                RequestCode::WriteChunkIfSequenceDigest
            }
            ReplicaRequest::WriteChunkBatchIfSequence { .. } => {
                RequestCode::WriteChunkBatchIfSequence
            }
            ReplicaRequest::WriteChunkBatchIfSequenceDigest { .. } => {
                RequestCode::WriteChunkBatchIfSequenceDigest
            }
            ReplicaRequest::ReadChunk { .. } => RequestCode::ReadChunk,
            ReplicaRequest::ReadChunkBatch { .. } => RequestCode::ReadChunkBatch,
            ReplicaRequest::DigestChunk { .. } => RequestCode::DigestChunk,
            ReplicaRequest::HeadChunk { .. } => RequestCode::HeadChunk,
            ReplicaRequest::HeadChunkBatch { .. } => RequestCode::HeadChunkBatch,
            ReplicaRequest::DiscardChunk { .. } => RequestCode::DiscardChunk,
            ReplicaRequest::Ping => RequestCode::Ping,
            ReplicaRequest::Capabilities => RequestCode::Capabilities,
            ReplicaRequest::ConditionalWriteLimits => RequestCode::ConditionalWriteLimits,
            ReplicaRequest::WideConditionalWriteLimits => RequestCode::WideConditionalWriteLimits,
            ReplicaRequest::ReadBatchLimits => RequestCode::ReadBatchLimits,
            ReplicaRequest::GroupIdentity => RequestCode::GroupIdentity,
            ReplicaRequest::PageInventory { .. } => RequestCode::PageInventory,
        }
    }

    /// The payload bytes, without the header.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            ReplicaRequest::Membership(value) => return value.encode_to_vec(),
            ReplicaRequest::StateInstall(value) => return value.encode_to_vec(),
            ReplicaRequest::ReadChunkStateRange {
                page_id,
                expected_state_checksum,
                offset,
                length,
            } => {
                return super::state_transfer::StateRangeRequest {
                    version: 1,
                    volume: page_id.chunk_id.volume_id(),
                    chunk: page_id.chunk_id.id(),
                    page: page_id.page_index,
                    state_checksum: expected_state_checksum.clone(),
                    offset: *offset,
                    length: *length,
                }
                .encode_to_vec();
            }
            ReplicaRequest::DiscardChunkIfSequence {
                page_id,
                offset,
                length,
                expected_sequence,
            } => {
                return super::conditional_requests::ChunkDiscard {
                    version: 1,
                    volume: page_id.chunk_id.volume_id(),
                    chunk: page_id.chunk_id.id(),
                    page: page_id.page_index,
                    offset: *offset,
                    length: *length,
                    expected_sequence: *expected_sequence,
                }
                .encode_to_vec();
            }
            ReplicaRequest::WriteChunkIfSequence {
                page_id,
                offset,
                data,
                expected_sequence,
            }
            | ReplicaRequest::WriteChunkIfSequenceDigest {
                page_id,
                offset,
                data,
                expected_sequence,
            } => {
                return super::conditional_requests::ChunkWrite {
                    version: 1,
                    volume: page_id.chunk_id.volume_id(),
                    chunk: page_id.chunk_id.id(),
                    page: page_id.page_index,
                    offset: *offset,
                    data: data.clone(),
                    expected_sequence: *expected_sequence,
                }
                .encode_to_vec();
            }
            ReplicaRequest::WriteChunkIfState {
                page_id,
                offset,
                data,
                expected_sequence,
                expected_state_checksum,
            } => {
                return super::conditional_requests::StateChunkWrite {
                    version: 1,
                    volume: page_id.chunk_id.volume_id(),
                    chunk: page_id.chunk_id.id(),
                    page: page_id.page_index,
                    offset: *offset,
                    expected_sequence: *expected_sequence,
                    data: data.clone(),
                    expected_state_checksum: expected_state_checksum.clone(),
                }
                .encode_to_vec();
            }
            ReplicaRequest::WriteChunkBatchIfSequence { writes }
            | ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } => {
                let mut payload = Vec::with_capacity(conditional_write_batch_len(writes));
                encode_conditional_write_batch(writes, &mut payload);
                return payload;
            }
            ReplicaRequest::ReadChunkBatch { reads } => {
                return super::conditional_requests::ChunkReadBatch {
                    version: 1,
                    reads: reads.iter().map(|(page_id, offset, length)| {
                        super::conditional_requests::ChunkRead {
                            version: 1,
                            volume: page_id.chunk_id.volume_id(),
                            chunk: page_id.chunk_id.id(),
                            page: page_id.page_index,
                            offset: *offset,
                            length: *length,
                        }
                    }).collect(),
                }.encode_to_vec();
            }
            ReplicaRequest::WriteChunk {
                page_id,
                offset,
                data,
            } => {
                put_page_id(&mut out, page_id);
                put_u64(&mut out, *offset);
                put_bytes(&mut out, data);
            }
            ReplicaRequest::ReadChunk {
                page_id,
                offset,
                length,
            }
            | ReplicaRequest::DigestChunk {
                page_id,
                offset,
                length,
            }
            | ReplicaRequest::DiscardChunk {
                page_id,
                offset,
                length,
            } => {
                put_page_id(&mut out, page_id);
                put_u64(&mut out, *offset);
                put_u64(&mut out, *length);
            }
            ReplicaRequest::HeadChunk { page_id } => put_page_id(&mut out, page_id),
            ReplicaRequest::HeadChunkBatch { pages } => {
                return super::conditional_requests::ChunkHeadBatch {
                    version: 1,
                    pages: pages
                        .iter()
                        .map(|page_id| super::conditional_requests::ChunkPage {
                            version: 1,
                            volume: page_id.chunk_id.volume_id(),
                            chunk: page_id.chunk_id.id(),
                            page: page_id.page_index,
                        })
                        .collect(),
                }
                .encode_to_vec();
            }
            ReplicaRequest::Ping
            | ReplicaRequest::Capabilities
            | ReplicaRequest::ConditionalWriteLimits
            | ReplicaRequest::WideConditionalWriteLimits
            | ReplicaRequest::ReadBatchLimits
            | ReplicaRequest::GroupIdentity => {}
            ReplicaRequest::PageInventory { after, limit } => {
                put_u64(&mut out, u64::from(after.is_some()));
                if let Some(id) = after {
                    put_page_id(&mut out, id);
                }
                put_u64(&mut out, *limit);
            }
        }
        out
    }

    /// Read a request from a payload, REFUSING a payload with bytes left over.
    ///
    /// The leftover check is not pedantry. Bytes to spare mean the sender laid the fields
    /// out differently from this reader, and every field this reader did take came out of
    /// the wrong place -- a page address read one field short still parses into a valid
    /// `PageId`, naming a page nobody asked about.
    pub fn decode(code: RequestCode, payload: &[u8]) -> Result<Self, ObjectError> {
        if code == RequestCode::Membership {
            if payload.len() > 128 {
                return Err(ObjectError::Corruption(
                    "membership request exceeds bound".into(),
                ));
            }
            let value: super::MembershipRequest = super::conditional_requests::decode(payload)?;
            value.validate()?;
            return Ok(Self::Membership(value));
        }
        if code == RequestCode::StateInstall {
            return super::state_install::StateInstall::decode_payload(payload)
                .map(Self::StateInstall);
        }
        if code == RequestCode::ReadChunkStateRange {
            if payload.len() > 512 {
                return Err(ObjectError::Corruption(
                    "page state range request exceeds bound".into(),
                ));
            }
            let value: super::state_transfer::StateRangeRequest =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 {
                return Err(ObjectError::Corruption(
                    "unsupported page state range version".into(),
                ));
            }
            super::state_transfer::validate_request(
                &value.state_checksum,
                value.offset,
                value.length,
            )?;
            return Ok(Self::ReadChunkStateRange {
                page_id: PageId {
                    chunk_id: ChunkId::new(value.volume, value.chunk)?,
                    page_index: value.page,
                },
                expected_state_checksum: value.state_checksum,
                offset: value.offset,
                length: value.length,
            });
        }
        if code == RequestCode::DiscardChunkIfSequence {
            if payload.len() > 96 {
                return Err(ObjectError::Corruption(
                    "conditional discard exceeds size bound".into(),
                ));
            }
            let value: super::conditional_requests::ChunkDiscard =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 || value.expected_sequence == 0 {
                return Err(ObjectError::Corruption(
                    "invalid conditional discard version or sequence".into(),
                ));
            }
            return Ok(Self::DiscardChunkIfSequence {
                page_id: PageId {
                    chunk_id: ChunkId::new(value.volume, value.chunk)?,
                    page_index: value.page,
                },
                offset: value.offset,
                length: value.length,
                expected_sequence: value.expected_sequence,
            });
        }

        if matches!(
            code,
            RequestCode::WriteChunkIfSequence | RequestCode::WriteChunkIfSequenceDigest
        ) {
            let value: super::conditional_requests::ChunkWrite =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 {
                return Err(ObjectError::Corruption(
                    "unsupported conditional write version".into(),
                ));
            }
            let page_id = PageId {
                chunk_id: ChunkId::new(value.volume, value.chunk)?,
                page_index: value.page,
            };
            return Ok(match code {
                RequestCode::WriteChunkIfSequenceDigest => Self::WriteChunkIfSequenceDigest {
                    page_id,
                    offset: value.offset,
                    data: value.data,
                    expected_sequence: value.expected_sequence,
                },
                _ => Self::WriteChunkIfSequence {
                    page_id,
                    offset: value.offset,
                    data: value.data,
                    expected_sequence: value.expected_sequence,
                },
            });
        }
        if code == RequestCode::WriteChunkIfState {
            if payload.len() > 1024 * 1024 + 512 {
                return Err(ObjectError::InvalidArgument(
                    "state-conditional write exceeds size bound".into(),
                ));
            }
            let value: super::conditional_requests::StateChunkWrite =
                super::conditional_requests::decode(payload)?;
            let absent = value.expected_sequence == 0 && value.expected_state_checksum.is_empty();
            let present = value.expected_sequence != 0
                && value.expected_sequence != u64::MAX
                && !value.expected_state_checksum.is_empty()
                && value.expected_state_checksum.len() <= 128;
            if value.version != 1 || value.data.len() > 1024 * 1024 || !(absent || present) {
                return Err(ObjectError::InvalidArgument(
                    "invalid state-conditional write".into(),
                ));
            }
            return Ok(Self::WriteChunkIfState {
                page_id: PageId {
                    chunk_id: ChunkId::new(value.volume, value.chunk)?,
                    page_index: value.page,
                },
                offset: value.offset,
                data: value.data,
                expected_sequence: value.expected_sequence,
                expected_state_checksum: value.expected_state_checksum,
            });
        }
        if code == RequestCode::ReadChunkBatch {
            if payload.len() > 1024 {
                return Err(ObjectError::InvalidArgument("read batch request exceeds size bound".into()));
            }
            let value: super::conditional_requests::ChunkReadBatch =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 || value.reads.len() > 32 || value.reads.iter().any(|read|
                read.version != 1 || read.length > 1024 * 1024)
            {
                return Err(ObjectError::InvalidArgument("invalid read batch".into()));
            }
            let total = value.reads.iter().try_fold(0u64, |sum, read|
                sum.checked_add(read.length).ok_or_else(|| ObjectError::InvalidArgument("read batch length overflow".into())))?;
            let framed = total
                .checked_add(value.reads.len() as u64 * 64)
                .ok_or_else(|| ObjectError::InvalidArgument("read batch length overflow".into()))?;
            if framed > MAX_PAYLOAD_BYTES as u64 {
                return Err(ObjectError::InvalidArgument("read batch exceeds byte bound".into()));
            }
            let reads = value.reads.into_iter().map(|read| Ok((
                PageId { chunk_id: ChunkId::new(read.volume, read.chunk)?, page_index: read.page },
                read.offset,
                read.length,
            ))).collect::<Result<Vec<_>, ObjectError>>()?;
            return Ok(Self::ReadChunkBatch { reads });
        }
        if code == RequestCode::HeadChunkBatch {
            if payload.len() > 2048 {
                return Err(ObjectError::InvalidArgument(
                    "page head batch request exceeds size bound".into(),
                ));
            }
            let value: super::conditional_requests::ChunkHeadBatch =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 || value.pages.is_empty() || value.pages.len() > 32 {
                return Err(ObjectError::InvalidArgument(
                    "invalid page head batch".into(),
                ));
            }
            let mut pages = Vec::with_capacity(value.pages.len());
            let mut distinct = std::collections::BTreeSet::new();
            for page in value.pages {
                if page.version != 1 {
                    return Err(ObjectError::InvalidArgument(
                        "invalid page head batch entry".into(),
                    ));
                }
                let page_id = PageId {
                    chunk_id: ChunkId::new(page.volume, page.chunk)?,
                    page_index: page.page,
                };
                if !distinct.insert(page_id.clone()) {
                    return Err(ObjectError::InvalidArgument(
                        "page head batch repeats a page".into(),
                    ));
                }
                pages.push(page_id);
            }
            return Ok(Self::HeadChunkBatch { pages });
        }
        if matches!(
            code,
            RequestCode::WriteChunkBatchIfSequence
                | RequestCode::WriteChunkBatchIfSequenceDigest
        ) {
            if payload.len() > MAX_PAYLOAD_BYTES {
                return Err(ObjectError::InvalidArgument(
                    "conditional write batch exceeds size bound".into(),
                ));
            }
            let value: super::conditional_requests::ChunkWriteBatch =
                super::conditional_requests::decode(payload)?;
            let max_writes = if code == RequestCode::WriteChunkBatchIfSequence {
                32
            } else {
                8
            };
            if value.version != 1
                || value.writes.len() > max_writes
                || value
                    .writes
                    .iter()
                    .any(|write| write.version != 1 || write.data.len() > 1024 * 1024)
                || value
                    .writes
                    .iter()
                    .try_fold(0usize, |total, write| total.checked_add(write.data.len()))
                    .is_none_or(|total| total > MAX_PAYLOAD_BYTES)
            {
                return Err(ObjectError::InvalidArgument(
                    "invalid conditional write batch".into(),
                ));
            }
            let writes = value
                .writes
                .into_iter()
                .map(|write| {
                    Ok(ConditionalChunkWrite {
                        page_id: PageId {
                            chunk_id: ChunkId::new(write.volume, write.chunk)?,
                            page_index: write.page,
                        },
                        offset: write.offset,
                        data: write.data,
                        expected_sequence: write.expected_sequence,
                    })
                })
                .collect::<Result<Vec<_>, ObjectError>>()?;
            return Ok(match code {
                RequestCode::WriteChunkBatchIfSequenceDigest => {
                    Self::WriteChunkBatchIfSequenceDigest { writes }
                }
                _ => Self::WriteChunkBatchIfSequence { writes },
            });
        }
        let mut cursor = Cursor::new(payload);
        let request = match code {
            RequestCode::Membership | RequestCode::StateInstall => unreachable!(),
            RequestCode::ReadChunkStateRange => unreachable!(),
            RequestCode::ReadChunkBatch => unreachable!(),
            RequestCode::HeadChunkBatch => unreachable!(),
            RequestCode::WriteChunkIfSequence
            | RequestCode::WriteChunkIfSequenceDigest
            | RequestCode::WriteChunkIfState
            | RequestCode::WriteChunkBatchIfSequence
            | RequestCode::WriteChunkBatchIfSequenceDigest
            | RequestCode::DiscardChunkIfSequence => {
                unreachable!()
            }
            RequestCode::WriteChunk => {
                let page_id = take_page_id(&mut cursor)?;
                let offset = cursor.take_u64()?;
                let data = cursor.take_bytes()?.to_vec();
                ReplicaRequest::WriteChunk {
                    page_id,
                    offset,
                    data,
                }
            }
            RequestCode::ReadChunk => {
                let page_id = take_page_id(&mut cursor)?;
                let offset = cursor.take_u64()?;
                let length = cursor.take_u64()?;
                ReplicaRequest::ReadChunk {
                    page_id,
                    offset,
                    length,
                }
            }
            RequestCode::DigestChunk => {
                let page_id = take_page_id(&mut cursor)?;
                let offset = cursor.take_u64()?;
                let length = cursor.take_u64()?;
                if length > 1024 * 1024 {
                    return Err(ObjectError::InvalidArgument(
                        "page digest range exceeds one MiB".into(),
                    ));
                }
                ReplicaRequest::DigestChunk {
                    page_id,
                    offset,
                    length,
                }
            }
            RequestCode::HeadChunk => ReplicaRequest::HeadChunk {
                page_id: take_page_id(&mut cursor)?,
            },
            RequestCode::DiscardChunk => {
                let page_id = take_page_id(&mut cursor)?;
                let offset = cursor.take_u64()?;
                let length = cursor.take_u64()?;
                ReplicaRequest::DiscardChunk {
                    page_id,
                    offset,
                    length,
                }
            }
            RequestCode::Ping => ReplicaRequest::Ping,
            RequestCode::Capabilities => ReplicaRequest::Capabilities,
            RequestCode::ConditionalWriteLimits => ReplicaRequest::ConditionalWriteLimits,
            RequestCode::WideConditionalWriteLimits => ReplicaRequest::WideConditionalWriteLimits,
            RequestCode::ReadBatchLimits => ReplicaRequest::ReadBatchLimits,
            RequestCode::GroupIdentity => ReplicaRequest::GroupIdentity,
            RequestCode::PageInventory => {
                let after = match cursor.take_u64()? {
                    0 => None,
                    1 => Some(take_page_id(&mut cursor)?),
                    _ => {
                        return Err(ObjectError::Corruption(
                            "invalid page inventory cursor tag".into(),
                        ))
                    }
                };
                let limit = cursor.take_u64()?;
                check_inventory_limit(limit)?;
                ReplicaRequest::PageInventory { after, limit }
            }
        };
        if cursor.remaining() != 0 {
            return Err(ObjectError::Corruption(format!(
                "a replica request of kind {code:?} left {} byte(s) unread, so the sender \
                     laid its fields out differently from this reader",
                cursor.remaining()
            )));
        }
        Ok(request)
    }
}

/// A decoded reply, including a typed pre-admission refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaReply {
    Membership(super::MembershipStatus),
    StateRange(super::state_transfer::ChunkStateRange),
    /// This voter did not admit the request; a catalog may try another voter.
    NotLeader,
    /// A write or a discard was applied. Nothing to carry back.
    Applied,
    Read(ReplicaReadResult),
    Reads(Vec<ReplicaReadResult>),
    Digest {
        write_sequence: u64,
        length: u64,
        sha256: [u8; 32],
    },
    Digests(Vec<(u64, u64, [u8; 32])>),
    Head(ReplicaHead),
    Heads(Vec<Option<ReplicaHead>>),
    /// The replica is answering, and holds this many pages.
    Pong {
        page_count: u64,
    },
    Capabilities {
        state_conditional_write: bool,
    },
    ConditionalWriteLimits {
        max_writes: u32,
        max_payload_bytes: u32,
    },
    ReadBatchLimits {
        max_reads: u32,
        max_reply_payload_bytes: u32,
    },
    PageInventory(PageInventory),
    GroupIdentity(ChunkGroupIdentity),
}

impl ReplicaReply {
    pub(crate) fn payload_len(&self) -> usize {
        match self {
            Self::StateRange(value) => value.encoded_len(),
            Self::Applied | Self::NotLeader => 0,
            Self::Capabilities { .. } => 8,
            Self::ConditionalWriteLimits {
                max_writes,
                max_payload_bytes,
            } => super::conditional_requests::ConditionalWriteLimits {
                version: 1,
                max_writes: *max_writes,
                max_payload_bytes: *max_payload_bytes,
            }
            .encoded_len(),
            Self::ReadBatchLimits {
                max_reads,
                max_reply_payload_bytes,
            } => super::conditional_requests::ReadBatchLimits {
                version: 1,
                max_reads: *max_reads,
                max_reply_payload_bytes: *max_reply_payload_bytes,
            }
            .encoded_len(),
            Self::Membership(value) => value.encoded_len(),
            Self::GroupIdentity(identity) => identity.encoded_len(),
            Self::Read(read) => 16usize
                .saturating_add(read.checksum.len())
                .saturating_add(read.data.len()),
            Self::Reads(reads) => read_batch_payload_len(reads),
            Self::Digest { .. } => 52,
            Self::Digests(digests) => super::conditional_requests::ChunkDigestBatch {
                version: 1,
                digests: digests
                    .iter()
                    .map(|(write_sequence, length, sha256)| {
                        super::conditional_requests::ChunkDigest {
                            version: 1,
                            write_sequence: *write_sequence,
                            length: *length,
                            sha256: sha256.to_vec(),
                        }
                    })
                    .collect(),
            }
            .encoded_len(),
            Self::Head(head) => 32usize
                .saturating_add(head.checksum.len())
                .saturating_add(head.state_checksum.len())
                .saturating_add(head.discarded_ranges.len().saturating_mul(16)),
            Self::Heads(heads) => encode_head_batch(heads).encoded_len(),
            Self::Pong { .. } => 8,
            Self::PageInventory(batch) => {
                16usize.saturating_add(batch.entries.len().saturating_mul(32))
            }
        }
    }

    pub fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            ReplicaReply::StateRange(value) => return value.encode_to_vec(),
            ReplicaReply::Applied | ReplicaReply::NotLeader => {}
            ReplicaReply::Membership(value) => return value.encode_to_vec(),
            ReplicaReply::GroupIdentity(identity) => return identity.encode_to_vec(),
            ReplicaReply::Read(read) => {
                put_u64(&mut out, read.write_sequence);
                put_str(&mut out, &read.checksum);
                put_bytes(&mut out, &read.data);
            }
            ReplicaReply::Reads(reads) => return encode_read_batch(reads),
            ReplicaReply::Digest {
                write_sequence,
                length,
                sha256,
            } => {
                put_u64(&mut out, *write_sequence);
                put_u64(&mut out, *length);
                put_bytes(&mut out, sha256);
            }
            ReplicaReply::Digests(digests) => {
                return super::conditional_requests::ChunkDigestBatch {
                    version: 1,
                    digests: digests
                        .iter()
                        .map(|(write_sequence, length, sha256)| {
                            super::conditional_requests::ChunkDigest {
                                version: 1,
                                write_sequence: *write_sequence,
                                length: *length,
                                sha256: sha256.to_vec(),
                            }
                        })
                        .collect(),
                }
                .encode_to_vec();
            }
            ReplicaReply::Head(head) => {
                put_u64(&mut out, head.logical_length);
                put_u64(&mut out, head.write_sequence);
                put_str(&mut out, &head.checksum);
                put_str(&mut out, &head.state_checksum);
                put_u64(&mut out, head.discarded_ranges.len() as u64);
                for range in &head.discarded_ranges {
                    put_u64(&mut out, range.offset);
                    put_u64(&mut out, range.length);
                }
            }
            ReplicaReply::Heads(heads) => return encode_head_batch(heads).encode_to_vec(),
            ReplicaReply::Pong { page_count } => put_u64(&mut out, *page_count),
            ReplicaReply::Capabilities {
                state_conditional_write,
            } => put_u64(&mut out, u64::from(*state_conditional_write)),
            ReplicaReply::ConditionalWriteLimits {
                max_writes,
                max_payload_bytes,
            } => {
                return super::conditional_requests::ConditionalWriteLimits {
                    version: 1,
                    max_writes: *max_writes,
                    max_payload_bytes: *max_payload_bytes,
                }
                .encode_to_vec();
            }
            ReplicaReply::ReadBatchLimits {
                max_reads,
                max_reply_payload_bytes,
            } => {
                return super::conditional_requests::ReadBatchLimits {
                    version: 1,
                    max_reads: *max_reads,
                    max_reply_payload_bytes: *max_reply_payload_bytes,
                }
                .encode_to_vec();
            }
            ReplicaReply::PageInventory(batch) => {
                put_u64(&mut out, u64::from(batch.has_more));
                put_u64(&mut out, batch.entries.len() as u64);
                for (id, sequence) in &batch.entries {
                    put_page_id(&mut out, id);
                    put_u64(&mut out, *sequence);
                }
            }
        }
        out
    }

    /// Read a reply, given the request it answers.
    ///
    /// The request kind is an argument rather than a byte on the response, because a reply
    /// whose shape is read from the reply itself can be read as the wrong shape. The caller
    /// knows what it asked.
    pub fn decode(code: RequestCode, payload: &[u8]) -> Result<Self, ObjectError> {
        if code == RequestCode::Membership {
            if payload.len() > 2048 {
                return Err(ObjectError::Corruption(
                    "membership reply exceeds bound".into(),
                ));
            }
            let value: super::MembershipStatus = super::conditional_requests::decode(payload)?;
            value.validate()?;
            return Ok(Self::Membership(value));
        }
        if code == RequestCode::StateInstall {
            if !payload.is_empty() {
                return Err(ObjectError::Corruption(
                    "install reply must be empty".into(),
                ));
            }
            return Ok(Self::Applied);
        }
        if code == RequestCode::ReadChunkStateRange {
            return super::state_transfer::ChunkStateRange::decode_payload(payload)
                .map(Self::StateRange);
        }
        if code == RequestCode::GroupIdentity {
            if payload.len() > 2048 {
                return Err(ObjectError::Corruption(
                    "chunk group identity exceeds size bound".into(),
                ));
            }
            let identity = ChunkGroupIdentity::decode(payload)
                .map_err(|_| ObjectError::Corruption("invalid chunk group protobuf".into()))?;
            identity.validate()?;
            if identity.encode_to_vec() != payload {
                return Err(ObjectError::Corruption(
                    "noncanonical chunk group protobuf".into(),
                ));
            }
            return Ok(ReplicaReply::GroupIdentity(identity));
        }
        if code == RequestCode::WriteChunkBatchIfSequenceDigest {
            if payload.len() > 1024 {
                return Err(ObjectError::Corruption(
                    "conditional write digest batch reply exceeds bound".into(),
                ));
            }
            let value: super::conditional_requests::ChunkDigestBatch =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 || value.digests.len() > 8 {
                return Err(ObjectError::Corruption(
                    "invalid conditional write digest batch reply".into(),
                ));
            }
            let mut digests = Vec::with_capacity(value.digests.len());
            for digest in value.digests {
                if digest.version != 1 || digest.sha256.len() != 32 {
                    return Err(ObjectError::Corruption(
                        "invalid conditional write digest entry".into(),
                    ));
                }
                let mut sha256 = [0u8; 32];
                sha256.copy_from_slice(&digest.sha256);
                digests.push((digest.write_sequence, digest.length, sha256));
            }
            return Ok(ReplicaReply::Digests(digests));
        }
        if code == RequestCode::ReadChunkBatch {
            if payload.len() > MAX_PAYLOAD_BYTES {
                return Err(ObjectError::Corruption("read batch reply exceeds bound".into()));
            }
            let value: super::conditional_requests::ChunkReadBatchResult =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 || value.reads.len() > 32 || value.reads.iter().any(|read|
                read.version != 1 || read.checksum.len() > 128 || read.data.len() > 1024 * 1024)
            {
                return Err(ObjectError::Corruption("invalid read batch reply".into()));
            }
            return Ok(Self::Reads(value.reads.into_iter().map(|read| ReplicaReadResult {
                write_sequence: read.write_sequence,
                checksum: read.checksum,
                data: read.data,
            }).collect()));
        }
        if code == RequestCode::HeadChunkBatch {
            let value: super::conditional_requests::ChunkHeadBatchResult =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 || value.heads.is_empty() || value.heads.len() > 32 {
                return Err(ObjectError::Corruption(
                    "invalid page head batch reply".into(),
                ));
            }
            let mut heads = Vec::with_capacity(value.heads.len());
            for head in value.heads {
                if head.version != 1
                    || head.checksum.len() > 128
                    || head.state_checksum.len() > 128
                {
                    return Err(ObjectError::Corruption(
                        "invalid page head batch entry".into(),
                    ));
                }
                if !head.present {
                    if head.logical_length != 0
                        || head.write_sequence != 0
                        || !head.checksum.is_empty()
                        || !head.state_checksum.is_empty()
                        || !head.discarded_ranges.is_empty()
                    {
                        return Err(ObjectError::Corruption(
                            "absent page head carries state".into(),
                        ));
                    }
                    heads.push(None);
                    continue;
                }
                heads.push(Some(ReplicaHead {
                    logical_length: head.logical_length,
                    write_sequence: head.write_sequence,
                    checksum: head.checksum,
                    state_checksum: head.state_checksum,
                    discarded_ranges: head
                        .discarded_ranges
                        .into_iter()
                        .map(|range| DataRange {
                            offset: range.offset,
                            length: range.length,
                        })
                        .collect(),
                }));
            }
            return Ok(Self::Heads(heads));
        }
        if matches!(
            code,
            RequestCode::ConditionalWriteLimits | RequestCode::WideConditionalWriteLimits
        ) {
            let value: super::conditional_requests::ConditionalWriteLimits =
                super::conditional_requests::decode(payload)?;
            let max_payload_bytes = if code == RequestCode::WideConditionalWriteLimits {
                MAX_PAYLOAD_BYTES as u32
            } else {
                (4 * 1024 * 1024 + 4096) as u32
            };
            if value.version != 1
                || !(1..=32).contains(&value.max_writes)
                || !(1..=max_payload_bytes).contains(&value.max_payload_bytes)
            {
                return Err(ObjectError::Corruption(
                    "invalid conditional write limits".into(),
                ));
            }
            return Ok(Self::ConditionalWriteLimits {
                max_writes: value.max_writes,
                max_payload_bytes: value.max_payload_bytes,
            });
        }
        if code == RequestCode::ReadBatchLimits {
            let value: super::conditional_requests::ReadBatchLimits =
                super::conditional_requests::decode(payload)?;
            if value.version != 1
                || !(1..=32).contains(&value.max_reads)
                || !(1024..=MAX_PAYLOAD_BYTES as u32)
                    .contains(&value.max_reply_payload_bytes)
            {
                return Err(ObjectError::Corruption("invalid read batch limits".into()));
            }
            return Ok(Self::ReadBatchLimits {
                max_reads: value.max_reads,
                max_reply_payload_bytes: value.max_reply_payload_bytes,
            });
        }
        let mut cursor = Cursor::new(payload);
        let reply = match code {
            RequestCode::Membership | RequestCode::StateInstall => unreachable!(),
            RequestCode::ReadChunkStateRange => unreachable!(),
            RequestCode::GroupIdentity => unreachable!(),
            RequestCode::WriteChunkBatchIfSequenceDigest => unreachable!(),
            RequestCode::ReadChunkBatch => unreachable!(),
            RequestCode::HeadChunkBatch => unreachable!(),
            RequestCode::ConditionalWriteLimits => unreachable!(),
            RequestCode::WideConditionalWriteLimits => unreachable!(),
            RequestCode::ReadBatchLimits => unreachable!(),
            RequestCode::WriteChunk
            | RequestCode::WriteChunkIfSequence
            | RequestCode::WriteChunkIfState
            | RequestCode::WriteChunkBatchIfSequence
            | RequestCode::DiscardChunk
            | RequestCode::DiscardChunkIfSequence => ReplicaReply::Applied,
            RequestCode::ReadChunk => {
                let write_sequence = cursor.take_u64()?;
                let checksum = cursor.take_str()?;
                let data = cursor.take_bytes()?.to_vec();
                ReplicaReply::Read(ReplicaReadResult {
                    data,
                    write_sequence,
                    checksum,
                })
            }
            RequestCode::DigestChunk | RequestCode::WriteChunkIfSequenceDigest => {
                let write_sequence = cursor.take_u64()?;
                let length = cursor.take_u64()?;
                let mut sha256 = [0u8; 32];
                let encoded = cursor.take_bytes()?;
                if encoded.len() != sha256.len() {
                    return Err(ObjectError::Corruption("invalid page digest length".into()));
                }
                sha256.copy_from_slice(encoded);
                ReplicaReply::Digest {
                    write_sequence,
                    length,
                    sha256,
                }
            }
            RequestCode::HeadChunk => {
                let logical_length = cursor.take_u64()?;
                let write_sequence = cursor.take_u64()?;
                let checksum = cursor.take_str()?;
                let state_checksum = cursor.take_str()?;
                let range_count = cursor.take_u64()?;
                // The count is bounded by what is left in the payload before anything is
                // reserved for it: sixteen bytes per range is the floor, so a count past
                // that is a frame this reader should refuse rather than a Vec it should
                // try to allocate.
                let floor = (range_count as usize).saturating_mul(16);
                if floor > cursor.remaining() {
                    return Err(ObjectError::Corruption(format!(
                        "a replica head claims {range_count} discarded range(s), which need \
                             {floor} byte(s), with only {} left in the frame",
                        cursor.remaining()
                    )));
                }
                let mut discarded_ranges = Vec::with_capacity(range_count as usize);
                for _ in 0..range_count {
                    let offset = cursor.take_u64()?;
                    let length = cursor.take_u64()?;
                    discarded_ranges.push(DataRange { offset, length });
                }
                ReplicaReply::Head(ReplicaHead {
                    logical_length,
                    write_sequence,
                    checksum,
                    state_checksum,
                    discarded_ranges,
                })
            }
            RequestCode::Ping => ReplicaReply::Pong {
                page_count: cursor.take_u64()?,
            },
            RequestCode::Capabilities => ReplicaReply::Capabilities {
                state_conditional_write: match cursor.take_u64()? {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(ObjectError::Corruption(
                            "invalid replica capability flag".into(),
                        ))
                    }
                },
            },
            RequestCode::PageInventory => {
                let has_more = match cursor.take_u64()? {
                    0 => false,
                    1 => true,
                    _ => {
                        return Err(ObjectError::Corruption(
                            "invalid page inventory continuation tag".into(),
                        ))
                    }
                };
                let count = cursor.take_u64()?;
                if count > MAX_INVENTORY_PAGES
                    || count > (cursor.remaining() / 32) as u64
                    || (has_more && count == 0)
                {
                    return Err(ObjectError::Corruption(
                        "invalid page inventory row count".into(),
                    ));
                }
                let mut entries = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let id = take_page_id(&mut cursor)?;
                    let sequence = cursor.take_u64()?;
                    if entries.last().is_some_and(|(last, _)| last >= &id) {
                        return Err(ObjectError::Corruption(
                            "page inventory rows are not strictly ordered".into(),
                        ));
                    }
                    entries.push((id, sequence));
                }
                ReplicaReply::PageInventory(PageInventory { entries, has_more })
            }
        };
        if cursor.remaining() != 0 {
            return Err(ObjectError::Corruption(format!(
                "a replica reply to {code:?} left {} byte(s) unread",
                cursor.remaining()
            )));
        }
        Ok(reply)
    }
}

fn encode_head_batch(
    heads: &[Option<ReplicaHead>],
) -> super::conditional_requests::ChunkHeadBatchResult {
    super::conditional_requests::ChunkHeadBatchResult {
        version: 1,
        heads: heads
            .iter()
            .map(|head| match head {
                Some(head) => super::conditional_requests::ChunkHead {
                    version: 1,
                    present: true,
                    logical_length: head.logical_length,
                    write_sequence: head.write_sequence,
                    checksum: head.checksum.clone(),
                    state_checksum: head.state_checksum.clone(),
                    discarded_ranges: head
                        .discarded_ranges
                        .iter()
                        .map(|range| super::conditional_requests::ChunkRange {
                            offset: range.offset,
                            length: range.length,
                        })
                        .collect(),
                },
                None => super::conditional_requests::ChunkHead {
                    version: 1,
                    present: false,
                    logical_length: 0,
                    write_sequence: 0,
                    checksum: String::new(),
                    state_checksum: String::new(),
                    discarded_ranges: Vec::new(),
                },
            })
            .collect(),
    }
}

fn protobuf_varint_len(value: u64) -> usize {
    prost::encoding::encoded_len_varint(value)
}

fn protobuf_field_len(bytes: &[u8]) -> usize {
    if bytes.is_empty() { 0 } else { 1 + protobuf_varint_len(bytes.len() as u64) + bytes.len() }
}

fn read_result_payload_len(read: &ReplicaReadResult) -> usize {
    2usize
        .saturating_add(if read.write_sequence == 0 { 0 } else { 1 + protobuf_varint_len(read.write_sequence) })
        .saturating_add(protobuf_field_len(read.checksum.as_bytes()))
        .saturating_add(protobuf_field_len(&read.data))
}

fn read_batch_payload_len(reads: &[ReplicaReadResult]) -> usize {
    reads.iter().fold(2usize, |total, read| {
        let entry = read_result_payload_len(read);
        total.saturating_add(1 + protobuf_varint_len(entry as u64) + entry)
    })
}

fn encode_read_batch(reads: &[ReplicaReadResult]) -> Vec<u8> {
    let mut out = Vec::with_capacity(read_batch_payload_len(reads));
    out.extend([0x08, 0x01]);
    for read in reads {
        let entry_len = read_result_payload_len(read);
        out.push(0x12);
        prost::encoding::encode_varint(entry_len as u64, &mut out);
        out.extend([0x08, 0x01]);
        if read.write_sequence != 0 {
            out.push(0x10);
            prost::encoding::encode_varint(read.write_sequence, &mut out);
        }
        for (tag, bytes) in [(0x1a, read.checksum.as_bytes()), (0x22, read.data.as_slice())] {
            if !bytes.is_empty() {
                out.push(tag);
                prost::encoding::encode_varint(bytes.len() as u64, &mut out);
                out.extend_from_slice(bytes);
            }
        }
    }
    out
}

/// Lay out a complete request frame: header then payload.
pub fn encode_request(request: &ReplicaRequest) -> Vec<u8> {
    if let ReplicaRequest::WriteChunkBatchIfSequence { writes }
    | ReplicaRequest::WriteChunkBatchIfSequenceDigest { writes } = request
    {
        let payload_len = conditional_write_batch_len(writes);
        let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload_len);
        crate::cluster::wire::put_header(&mut frame, request.code() as u8, payload_len);
        encode_conditional_write_batch(writes, &mut frame);
        debug_assert_eq!(frame.len(), crate::cluster::wire::HEADER_BYTES + payload_len);
        return frame;
    }
    let payload = request.encode_payload();
    let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload.len());
    crate::cluster::wire::put_header(&mut frame, request.code() as u8, payload.len());
    frame.extend_from_slice(&payload);
    frame
}

/// Lay out a complete reply frame.
pub fn encode_reply(reply: &ReplicaReply) -> Vec<u8> {
    let payload = reply.encode_payload();
    let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload.len());
    let status = if matches!(reply, ReplicaReply::NotLeader) {
        StatusCode::NotLeader
    } else {
        StatusCode::Ok
    };
    crate::cluster::wire::put_header(&mut frame, status as u8, payload.len());
    frame.extend_from_slice(&payload);
    frame
}

/// Lay out a complete failure frame, carrying the error's own text.
pub fn encode_failure(status: StatusCode, message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    put_str(&mut payload, message);
    let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload.len());
    crate::cluster::wire::put_header(&mut frame, status as u8, payload.len());
    frame.extend_from_slice(&payload);
    frame
}

/// Encode a failure without allowing its text to exceed the connection budget.
pub(crate) fn encode_bounded_failure(status: StatusCode, message: &str, limit: usize) -> Vec<u8> {
    let mut end = message.len().min(limit.saturating_sub(4));
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    encode_failure(status, &message[..end])
}

pub(crate) fn check_reply_size(size: usize, limit: usize) -> Result<(), ObjectError> {
    if size > limit || size > u32::MAX as usize {
        return Err(ObjectError::PreconditionFailed(
            "reply exceeds payload budget".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_payload_limit(limit: usize) -> std::io::Result<()> {
    if !(24..=u32::MAX as usize).contains(&limit) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "payload budget must fit a status reply and the frame length",
        ));
    }
    Ok(())
}

/// Read a failure frame's message.
pub fn decode_failure(payload: &[u8]) -> Result<String, ObjectError> {
    let mut cursor = Cursor::new(payload);
    let message = cursor.take_str()?;
    Ok(message)
}

/// A page address on the wire: volume, chunk, page index.
///
/// `ChunkId` narrows both of its numbers to thirty-two bits and refuses rather than
/// truncating, so this writes the accessors' `u64` and the reader puts them back through
/// `ChunkId::new`. A frame carrying a number this build cannot hold is refused at the
/// boundary with the same message a local caller would get.
fn put_page_id(out: &mut Vec<u8>, page_id: &PageId) {
    put_u64(out, page_id.chunk_id.volume_id());
    put_u64(out, page_id.chunk_id.id());
    put_u64(out, page_id.page_index);
}

fn take_page_id(cursor: &mut Cursor<'_>) -> Result<PageId, ObjectError> {
    let volume_id = cursor.take_u64()?;
    let id = cursor.take_u64()?;
    let page_index = cursor.take_u64()?;
    Ok(PageId {
        chunk_id: ChunkId::new(volume_id, id)?,
        page_index,
    })
}

/// The stamp a frame must carry to be read at all.
///
/// Exposed so the connection readers in [`crate::cluster::server`] and
/// [`crate::cluster::client`] check the same constant rather than each writing out the
/// comparison.
pub fn check_stamp(stamp: u8) -> Result<(), ObjectError> {
    if stamp != PROTOCOL_STAMP {
        return Err(ObjectError::PreconditionFailed(format!(
            "a replica frame carries protocol stamp {stamp}, and this build speaks \
             {PROTOCOL_STAMP}; the two ends are different versions"
        )));
    }
    Ok(())
}
