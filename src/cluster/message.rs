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

/// A request as both ends understand it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaRequest {
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
}

impl ReplicaRequest {
    pub fn code(&self) -> RequestCode {
        match self {
            ReplicaRequest::WriteChunk { .. } => RequestCode::WriteChunk,
            ReplicaRequest::ReadChunk { .. } => RequestCode::ReadChunk,
            ReplicaRequest::HeadChunk { .. } => RequestCode::HeadChunk,
            ReplicaRequest::DiscardChunk { .. } => RequestCode::DiscardChunk,
            ReplicaRequest::Ping => RequestCode::Ping,
        }
    }

    /// The payload bytes, without the header.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
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
            ReplicaRequest::Ping => {}
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
        let mut cursor = Cursor::new(payload);
        let request = match code {
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

/// A successful reply's contents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaReply {
    /// A write or a discard was applied. Nothing to carry back.
    Applied,
    Read(ReplicaReadResult),
    Head(ReplicaHead),
    /// The replica is answering, and holds this many pages.
    Pong { page_count: u64 },
}

impl ReplicaReply {
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            ReplicaReply::Applied => {}
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
        }
        out
    }

    /// Read a reply, given the request it answers.
    ///
    /// The request kind is an argument rather than a byte on the response, because a reply
    /// whose shape is read from the reply itself can be read as the wrong shape. The caller
    /// knows what it asked.
    pub fn decode(code: RequestCode, payload: &[u8]) -> Result<Self, ObjectError> {
        let mut cursor = Cursor::new(payload);
        let reply = match code {
            RequestCode::WriteChunk | RequestCode::DiscardChunk => ReplicaReply::Applied,
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

/// Lay out a complete success frame.
pub fn encode_reply(reply: &ReplicaReply) -> Vec<u8> {
    let payload = reply.encode_payload();
    let mut frame = Vec::with_capacity(crate::cluster::wire::HEADER_BYTES + payload.len());
    crate::cluster::wire::put_header(&mut frame, StatusCode::Ok as u8, payload.len());
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
