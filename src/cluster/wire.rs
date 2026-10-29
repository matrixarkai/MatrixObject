// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The bytes one replica request and one replica response are made of.
//!
//! # WHY A FRAME AND NOT HTTP
//!
//! [`crate::net`] already speaks S3 over HTTP, and that is the right shape for a door the
//! world knocks on: every client is already written. This is the other direction -- one
//! process in this store talking to another process in the same store -- and the two have
//! different requirements. A replica call carries a page address and a byte range and
//! nothing else, it is made once per replica per write, and both ends ship from this
//! repository at the same version. Headers, verbs and percent-encoding buy nothing there.
//!
//! So this is a fixed six-byte header and a payload:
//!
//! ```text
//! [stamp: u8] [code: u8] [payload_len: u32 big-endian] [payload: payload_len bytes]
//! ```
//!
//! # THE STAMP IS NOT DECORATION
//!
//! Both ends of this protocol are built from one crate, so it is tempting to leave the
//! version out. The case where that hurts is the only case this protocol exists for: a
//! rolling restart, where some replicas are running the old binary and the coordinator is
//! running the new one. A reader that cannot tell "a frame I do not understand" from "a
//! frame whose length field I misread" will allocate whatever the next four bytes say.
//!
//! So [`PROTOCOL_STAMP`] travels on every frame in both directions, a frame carrying
//! anything else is refused by name rather than parsed, and [`MAX_PAYLOAD_BYTES`] bounds the
//! allocation a length field can ask for before any of it is read.
//!
//! # WHAT IS NOT HERE
//!
//! No framing for the 70-odd other things a replica can do -- snapshots, reclaim, trim flow,
//! cache control, mutation batches. See [`crate::cluster`] for the measured size of that
//! surface and why it is not in this file.

use crate::ObjectError;

/// The version of this frame format.
///
/// A frame carrying any other value is refused without being parsed. Bump it when the
/// meaning of a code or a payload layout changes, not when a code is added -- an added code
/// is already refused as unknown by an older peer, which is the right answer.
pub const PROTOCOL_STAMP: u8 = 1;

/// The largest payload either side will allocate for, in bytes.
///
/// A length field is read from the socket before the payload is, so without a bound the
/// first four bytes of a hostile or corrupt frame choose how much memory this process asks
/// for. Sized to hold one page (1 MiB) and the address and framing around it with room to
/// spare, because that is the largest thing a replica call carries.
pub const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

/// The fixed part of every frame: stamp, code, payload length.
pub const HEADER_BYTES: usize = 6;

/// What a request asks the replica to do.
///
/// The discriminants are the wire values and may not be renumbered without bumping
/// [`PROTOCOL_STAMP`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RequestCode {
    /// Write bytes at an offset in a page. Payload: page address, offset, data.
    WriteChunk = 1,
    /// Read a byte range of a page. Payload: page address, offset, length.
    ReadChunk = 2,
    /// Read a page's length, write sequence and checksums. Payload: page address.
    HeadChunk = 3,
    /// Discard a byte range of a page. Payload: page address, offset, length.
    DiscardChunk = 4,
    /// Ask whether this replica is answering, and how many pages it holds. Empty payload.
    Ping = 5,
    /// Read a bounded batch of page identities and write sequences, after an exclusive cursor.
    PageInventory = 6,
    /// Describe the immutable Raft voter configuration. Empty payload.
    GroupIdentity = 7,
    /// Write only when the page sequence matches a canonical protobuf condition.
    WriteChunkIfSequence = 8,
    DiscardChunkIfSequence = 9,
}

impl RequestCode {
    /// The code this byte names, or `None` for one this build does not know.
    ///
    /// `None` is a status on the wire, not a panic: an older replica reached by a newer
    /// coordinator must be able to say "I do not know that verb" rather than drop the
    /// connection and look like a dead process.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(RequestCode::WriteChunk),
            2 => Some(RequestCode::ReadChunk),
            3 => Some(RequestCode::HeadChunk),
            4 => Some(RequestCode::DiscardChunk),
            5 => Some(RequestCode::Ping),
            6 => Some(RequestCode::PageInventory),
            7 => Some(RequestCode::GroupIdentity),
            8 => Some(RequestCode::WriteChunkIfSequence),
            9 => Some(RequestCode::DiscardChunkIfSequence),
            _ => None,
        }
    }
}

/// How a request turned out.
///
/// The four error statuses are exactly [`ObjectError`]'s variants, so a remote failure
/// arrives at the caller as the same error a local call would have produced. That matters
/// for the read path: a quorum read has to tell "this replica does not have the page" from
/// "this replica refused the request", and collapsing both into one status would make a
/// missing key and a broken replica indistinguishable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0,
    NotFound = 1,
    InvalidArgument = 2,
    PreconditionFailed = 3,
    Corruption = 4,
    /// The replica does not know the request code. Distinct from the four above because it
    /// is a statement about the two builds, not about the store's contents.
    UnknownRequest = 5,
    /// Metadata voter refused the request before proposing it. Empty payload.
    NotLeader = 6,
}

impl StatusCode {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(StatusCode::Ok),
            1 => Some(StatusCode::NotFound),
            2 => Some(StatusCode::InvalidArgument),
            3 => Some(StatusCode::PreconditionFailed),
            4 => Some(StatusCode::Corruption),
            5 => Some(StatusCode::UnknownRequest),
            6 => Some(StatusCode::NotLeader),
            _ => None,
        }
    }

    /// The status that carries this error.
    pub fn for_error(error: &ObjectError) -> Self {
        match error {
            ObjectError::NotFound(_) => StatusCode::NotFound,
            ObjectError::InvalidArgument(_) => StatusCode::InvalidArgument,
            ObjectError::PreconditionFailed(_) => StatusCode::PreconditionFailed,
            ObjectError::Corruption(_) => StatusCode::Corruption,
        }
    }

    /// The error this status carries, given the message the replica sent with it.
    pub fn into_error(self, message: String) -> ObjectError {
        match self {
            StatusCode::NotLeader => ObjectError::PreconditionFailed(
                "metadata request was not admitted by a leader".into(),
            ),
            StatusCode::NotFound => ObjectError::NotFound(message),
            StatusCode::InvalidArgument => ObjectError::InvalidArgument(message),
            StatusCode::PreconditionFailed => ObjectError::PreconditionFailed(message),
            StatusCode::Corruption => ObjectError::Corruption(message),
            StatusCode::UnknownRequest => ObjectError::PreconditionFailed(format!(
                "the replica does not implement this request: {message}"
            )),
            // `Ok` is not an error, and a caller that asks for one anyway has a bug in its
            // own dispatch rather than a failure to report. Named rather than silently
            // mapped to something plausible.
            StatusCode::Ok => ObjectError::Corruption(format!(
                "a successful replica response was read as a failure: {message}"
            )),
        }
    }
}

/// Write the six-byte header for a frame of `payload_len` bytes.
pub fn put_header(out: &mut Vec<u8>, code: u8, payload_len: usize) {
    out.push(PROTOCOL_STAMP);
    out.push(code);
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Write a byte string as a `u32` length and then the bytes.
pub fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}

pub fn put_str(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

/// A read position in a payload, which refuses to read past the end.
///
/// Every `take` returns a `Result`. A truncated frame is the normal consequence of a replica
/// dying mid-write, so running off the end of a payload has to be an error the caller can
/// report and not an index panic in whichever thread happened to be decoding.
pub struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ObjectError> {
        let end = self.at.checked_add(count).ok_or_else(|| {
            ObjectError::Corruption("a replica frame's field length overflows".to_string())
        })?;
        if end > self.bytes.len() {
            return Err(ObjectError::Corruption(format!(
                "a replica frame ended early: {count} more byte(s) wanted at offset {}, {} available",
                self.at,
                self.bytes.len().saturating_sub(self.at)
            )));
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    pub fn take_u64(&mut self) -> Result<u64, ObjectError> {
        let bytes = self.take(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(buf))
    }

    pub fn take_u32(&mut self) -> Result<u32, ObjectError> {
        let bytes = self.take(4)?;
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(buf))
    }

    pub fn take_bytes(&mut self) -> Result<&'a [u8], ObjectError> {
        let length = self.take_u32()? as usize;
        self.take(length)
    }

    pub fn take_str(&mut self) -> Result<String, ObjectError> {
        let bytes = self.take_bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|err| {
            ObjectError::Corruption(format!("a replica frame's text is not UTF-8: {err}"))
        })
    }

    /// Bytes left unread.
    ///
    /// Checked after decoding every request, because a payload with bytes to spare means the
    /// two ends disagree about the layout even though both got a plausible answer out of it.
    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.at)
    }
}
