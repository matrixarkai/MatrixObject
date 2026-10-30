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
    put_bytes, put_str, put_u64, Cursor, RequestCode, StatusCode, PROTOCOL_STAMP,
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

/// Immutable voter configuration, available on leaders and followers.
/// This identifies configuration; it does not authenticate a server or establish readiness.
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
        if !matches!(self.version, 1 | 2) || self.group_id == 0 || self.node_id == 0
            || self.voters.is_empty() || self.voters.len() > if self.version == 1 {7} else {64}
            || (self.version == 1 && (!self.bootstrap_voters.is_empty() || self.bootstrap_id != 0))
            || (self.version == 2 && (self.bootstrap_id == 0 || self.bootstrap_voters.is_empty() || self.bootstrap_voters.len() > 7
                || self.bootstrap_voters.windows(2).any(|w| w[0] >= w[1])
                || self.bootstrap_voters.iter().any(|id| !self.voters.contains(id))))
            || self.voters[0] == 0 || self.voters.windows(2).any(|w| w[0] >= w[1])
            || !self.voters.contains(&self.node_id)
            || self.peer_endpoints.len() != self.voters.len()
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
    pub(crate) fn same_group(&self, other: &Self) -> bool {
        let mut normalized = other.clone();
        normalized.node_id = self.node_id;
        *self == normalized
    }
}

/// A request as both ends understand it.
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
    HeadChunk {
        page_id: PageId,
    },
    DiscardChunk {
        page_id: PageId,
        offset: u64,
        length: u64,
    },
    Ping,
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
            ReplicaRequest::ReadChunk { .. } => RequestCode::ReadChunk,
            ReplicaRequest::HeadChunk { .. } => RequestCode::HeadChunk,
            ReplicaRequest::DiscardChunk { .. } => RequestCode::DiscardChunk,
            ReplicaRequest::Ping => RequestCode::Ping,
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
            ReplicaRequest::Ping | ReplicaRequest::GroupIdentity => {}
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

        if code == RequestCode::WriteChunkIfSequence {
            let value: super::conditional_requests::ChunkWrite =
                super::conditional_requests::decode(payload)?;
            if value.version != 1 {
                return Err(ObjectError::Corruption(
                    "unsupported conditional write version".into(),
                ));
            }
            return Ok(Self::WriteChunkIfSequence {
                page_id: PageId {
                    chunk_id: ChunkId::new(value.volume, value.chunk)?,
                    page_index: value.page,
                },
                offset: value.offset,
                data: value.data,
                expected_sequence: value.expected_sequence,
            });
        }
        let mut cursor = Cursor::new(payload);
        let request = match code {
            RequestCode::Membership | RequestCode::StateInstall => unreachable!(),
            RequestCode::ReadChunkStateRange => unreachable!(),
            RequestCode::WriteChunkIfSequence | RequestCode::DiscardChunkIfSequence => {
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
    Head(ReplicaHead),
    /// The replica is answering, and holds this many pages.
    Pong {
        page_count: u64,
    },
    PageInventory(PageInventory),
    GroupIdentity(ChunkGroupIdentity),
}

impl ReplicaReply {
    pub(crate) fn payload_len(&self) -> usize {
        match self {
            Self::StateRange(value) => value.encoded_len(),
            Self::Applied | Self::NotLeader => 0,
            Self::Membership(value) => value.encoded_len(),
            Self::GroupIdentity(identity) => identity.encoded_len(),
            Self::Read(read) => 16usize
                .saturating_add(read.checksum.len())
                .saturating_add(read.data.len()),
            Self::Head(head) => 32usize
                .saturating_add(head.checksum.len())
                .saturating_add(head.state_checksum.len())
                .saturating_add(head.discarded_ranges.len().saturating_mul(16)),
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
            ReplicaReply::Pong { page_count } => put_u64(&mut out, *page_count),
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
        let mut cursor = Cursor::new(payload);
        let reply = match code {
            RequestCode::Membership | RequestCode::StateInstall => unreachable!(),
            RequestCode::ReadChunkStateRange => unreachable!(),
            RequestCode::GroupIdentity => unreachable!(),
            RequestCode::WriteChunk
            | RequestCode::WriteChunkIfSequence
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

/// Lay out a complete request frame: header then payload.
pub fn encode_request(request: &ReplicaRequest) -> Vec<u8> {
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
