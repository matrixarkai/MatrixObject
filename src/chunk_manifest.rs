// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The chunk-granular object manifest: one extent names one run of a chunk, not one page.
//!
//! An extent says that bytes `[chunk_offset, chunk_offset + length)` of chunk `(volume_id,
//! chunk_id)` on `group` hold bytes `[object_offset, object_offset + length)` of the object. Two
//! storage kinds share the one extent type, and an object has exactly one of them for its whole
//! life:
//!
//! * `CHUNK_KIND_PAGED`: the chunk is an allocation of up to 4,096 pages of `page_bytes` each, as
//!   the chunk servers keep them today. An extent is a run of whole pages (the last may be short),
//!   written once and sealed from the moment it is published. Its `digest` is the SHA-256 of the
//!   page's bytes when the extent is one page, and otherwise the SHA-256 of a DIGEST PAGE, one extra
//!   page of the same allocation right after the data pages that lists the SHA-256 of each data
//!   page (see `chunk_integrity`). So a multi-page extent occupies its data pages plus one, at most
//!   4,096 in all, and no other extent of the object may use any of them. A small object is one
//!   extent of one page and has no digest page.
//! * `CHUNK_KIND_LOG`: an append-only chunk of up to 512 MiB written under a writer lease, which
//!   starts at chunk byte zero. Only the last extent may still be open.
//!
//! This module holds the types, the validation rules and the framed encoding. Nothing publishes or
//! reads this manifest yet.
use crate::object_manifest::{frame, unframe};
use crate::{ChunkId, ObjectError, ObjectId};
use prost::Message;

/// The only manifest version. A manifest of any other version is refused.
pub const CHUNK_MANIFEST_FORMAT: u32 = 1;
const MAGIC: &[u8; 4] = b"MOCM";
const MAX_CHUNK_MANIFEST: usize = 1024 * 1024;
/// The most extents one manifest may hold.
pub const MAX_CHUNK_EXTENTS: usize = 4096;
/// Pages of one allocation: a paged extent never reaches past the end of its allocation.
pub const PAGES_PER_ALLOCATION: u64 = 4096;
/// The largest page a paged object may use.
pub const MAX_PAGE_BYTES: u64 = 1 << 20;
const MAX_LOG_CHUNK_LENGTH: u64 = 512 * 1024 * 1024;

/// The extent is a run of pages of an allocation.
pub const CHUNK_KIND_PAGED: u32 = 1;
/// The extent is an append-only log chunk.
pub const CHUNK_KIND_LOG: u32 = 2;
/// The extent's length is final. Every paged extent carries it.
pub const CHUNK_FLAG_SEALED: u32 = 1;
/// A log chunk that is still being written: `length` is a lower bound.
pub const CHUNK_FLAG_TAIL: u32 = 2;
/// `footer_sha256` is recorded (a sealed log chunk).
pub const CHUNK_FLAG_FOOTER_SHA256: u32 = 4;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkExtent {
    #[prost(string, tag = "1")]
    pub group: String,
    #[prost(uint64, tag = "2")]
    pub volume_id: u64,
    #[prost(uint64, tag = "3")]
    pub chunk_id: u64,
    /// `CHUNK_KIND_PAGED` or `CHUNK_KIND_LOG`; the same as the manifest's.
    #[prost(uint32, tag = "4")]
    pub kind: u32,
    /// The byte of the object this extent starts at; equal to the sum of the lengths before it.
    #[prost(uint64, tag = "5")]
    pub object_offset: u64,
    /// The byte of the chunk this extent starts at. A paged extent starts on a page boundary; a
    /// log extent starts at zero.
    #[prost(uint64, tag = "6")]
    pub chunk_offset: u64,
    #[prost(uint64, tag = "7")]
    pub length: u64,
    /// `CHUNK_FLAG_*`.
    #[prost(uint32, tag = "8")]
    pub flags: u32,
    /// Paged only: SHA-256 over the concatenation of the SHA-256 of each page of the extent, in
    /// page order. For a one-page extent that is a hash of the page digest, not of the bytes.
    #[prost(bytes = "vec", tag = "9")]
    pub digest: Vec<u8>,
    /// Log only: CRC-32C over the sealed chunk bytes; zero unless sealed.
    #[prost(fixed32, tag = "10")]
    pub footer_crc: u32,
    /// Log only: SHA-256 over the sealed chunk bytes. Present exactly when
    /// `CHUNK_FLAG_FOOTER_SHA256` is set.
    #[prost(bytes = "vec", tag = "11")]
    pub footer_sha256: Vec<u8>,
}

impl ChunkExtent {
    /// The pages of the allocation this paged extent covers: the first page index and the count.
    pub fn pages(&self, page_bytes: u64) -> Result<(u64, u64), ObjectError> {
        if self.kind != CHUNK_KIND_PAGED || page_bytes == 0 || self.chunk_offset % page_bytes != 0 {
            return Err(invalid("an extent that is not a run of whole pages has no pages"));
        }
        let end = self
            .chunk_offset
            .checked_add(self.length)
            .ok_or_else(|| invalid("chunk extent overflow"))?;
        Ok((self.chunk_offset / page_bytes, end.div_ceil(page_bytes) - self.chunk_offset / page_bytes))
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkManifest {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub key: String,
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    #[prost(uint64, tag = "5")]
    pub length: u64,
    #[prost(message, repeated, tag = "6")]
    pub extents: Vec<ChunkExtent>,
    #[prost(bool, tag = "7")]
    pub deleted: bool,
    /// The kind of every extent of this object; zero for a deleted object.
    #[prost(uint32, tag = "8")]
    pub kind: u32,
    /// The size of a page of this object's paged extents; zero for a log object and for a
    /// deleted object. Recorded per object so that a change of the default never reinterprets
    /// an extent that already exists.
    #[prost(uint64, tag = "9")]
    pub page_bytes: u64,
}

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

fn group_ok(group: &str) -> bool {
    !group.is_empty() && group.len() <= 128 && group.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

/// The pages a paged extent occupies in its allocation: its data pages, and one more, the digest
/// page, when it has more than one (see `chunk_integrity`). `None` when the arithmetic overflows.
fn stored_pages(extent: &ChunkExtent, page_bytes: u64) -> Option<(u64, u64)> {
    let first = extent.chunk_offset / page_bytes;
    let data = extent.chunk_offset.checked_add(extent.length)?.div_ceil(page_bytes).checked_sub(first)?;
    let stored = if data > 1 { data.checked_add(1)? } else { data };
    Some((first, stored))
}

fn paged_extent_ok(extent: &ChunkExtent, page_bytes: u64) -> bool {
    let Some((first, stored)) = stored_pages(extent, page_bytes) else { return false };
    extent.length != 0
        && extent.flags == CHUNK_FLAG_SEALED
        && extent.footer_crc == 0
        && extent.footer_sha256.is_empty()
        && extent.digest.len() == 32
        && extent.chunk_offset % page_bytes == 0
        && first.checked_add(stored).is_some_and(|end| end <= PAGES_PER_ALLOCATION)
}

fn log_extent_ok(extent: &ChunkExtent, is_last: bool) -> bool {
    let sealed = extent.flags & CHUNK_FLAG_SEALED != 0;
    let tail = extent.flags & CHUNK_FLAG_TAIL != 0;
    extent.chunk_offset == 0
        && extent.digest.is_empty()
        && extent.length <= MAX_LOG_CHUNK_LENGTH
        && extent.flags & !(CHUNK_FLAG_SEALED | CHUNK_FLAG_TAIL | CHUNK_FLAG_FOOTER_SHA256) == 0
        && !(sealed && tail)
        // only the last chunk may still be open, and only an open chunk may be empty
        && (is_last || sealed)
        && (extent.length != 0 || tail)
        && (extent.footer_crc == 0 || sealed)
        && (extent.flags & CHUNK_FLAG_FOOTER_SHA256 != 0) == (extent.footer_sha256.len() == 32)
        && (extent.footer_sha256.is_empty() || sealed)
}

impl ChunkManifest {
    pub fn object_id(&self) -> ObjectId {
        ObjectId::new(self.bucket.clone(), self.key.clone())
    }

    pub fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.format_version != CHUNK_MANIFEST_FORMAT
            || self.generation == 0
            || self.extents.len() > MAX_CHUNK_EXTENTS
            || self.encoded_len() > MAX_CHUNK_MANIFEST
        {
            return Err(invalid("invalid chunk manifest version, generation or size"));
        }
        if self.deleted {
            if self.length != 0 || !self.extents.is_empty() || self.kind != 0 || self.page_bytes != 0 {
                return Err(invalid("deleted chunk manifest contains data"));
            }
            return Ok(());
        }
        let page_bytes = match self.kind {
            CHUNK_KIND_PAGED if (1..=MAX_PAGE_BYTES).contains(&self.page_bytes) => self.page_bytes,
            CHUNK_KIND_LOG if self.page_bytes == 0 => 1,
            _ => return Err(invalid("invalid chunk manifest kind or page size")),
        };
        let mut offset = 0u64;
        // (group, volume, chunk, first page, end page): the pages of one chunk may belong to
        // several extents of one object, but to no two of them at once
        let mut runs: Vec<(&str, u64, u64, u64, u64)> = Vec::with_capacity(self.extents.len());
        for (position, extent) in self.extents.iter().enumerate() {
            ChunkId::new(extent.volume_id, extent.chunk_id)?;
            let shape_ok = match self.kind {
                CHUNK_KIND_PAGED => paged_extent_ok(extent, page_bytes),
                _ => log_extent_ok(extent, position + 1 == self.extents.len()),
            };
            if !group_ok(&extent.group) || extent.kind != self.kind || extent.object_offset != offset || !shape_ok {
                return Err(invalid("invalid chunk extent ordering, identity or bounds"));
            }
            let (first, end) = if self.kind == CHUNK_KIND_PAGED {
                let (first, stored) = stored_pages(extent, page_bytes).expect("a paged extent that passed its shape check has pages");
                (first, first + stored)
            } else {
                (0, 1)
            };
            runs.push((extent.group.as_str(), extent.volume_id, extent.chunk_id, first, end));
            offset = offset.checked_add(extent.length).ok_or_else(|| invalid("object length overflow"))?;
        }
        if offset != self.length {
            return Err(invalid("chunk extents do not cover the object length"));
        }
        runs.sort_unstable();
        for pair in runs.windows(2) {
            let same_chunk = pair[0].0 == pair[1].0 && pair[0].1 == pair[1].1 && pair[0].2 == pair[1].2;
            if same_chunk && pair[1].3 < pair[0].4 {
                return Err(invalid("chunk extents overlap the same pages"));
            }
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        frame(MAGIC, self, MAX_CHUNK_MANIFEST)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = unframe(MAGIC, bytes, MAX_CHUNK_MANIFEST)?;
        value.validate()?;
        Ok(value)
    }
}

#[cfg(test)]
#[path = "chunk_manifest_tests.rs"]
mod tests;
