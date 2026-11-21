// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The geometry of a paged object stored as chunk extents, with no network in it.
//!
//! An object is cut into **stripes**: one extent per stripe, each at most `stripe_bytes` long and
//! never more than [`MAX_DATA_PAGES`] pages, so a stripe and its digest page fit the 4,096 pages of
//! one allocation. A stripe is stored as a run of pages in one chunk: its data pages, then (when it
//! has more than one) its digest page. [`crate::chunk_integrity`] makes the digests; this module
//! decides where each page goes, builds the manifest that names the runs, plans which pages a byte
//! range needs, and turns the pages that came back into the bytes asked for, verified.
//!
//! Everything here is a pure function of its arguments, so the whole of the write layout and the
//! read verification is tested without a cluster. The client that moves the pages over the network
//! is a thin layer over it.

use crate::chunk_integrity::{self, ExtentDigests, MAX_DATA_PAGES};
use crate::chunk_manifest::{
    ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_KIND_PAGED, CHUNK_MANIFEST_FORMAT, MAX_CHUNK_EXTENTS,
    MAX_PAGE_BYTES,
};
use crate::ObjectError;

/// The default stripe: 64 MiB, so an object under it is one extent on one group.
pub const DEFAULT_STRIPE_BYTES: u64 = 64 << 20;

fn invalid(message: &str) -> ObjectError {
    ObjectError::InvalidArgument(message.to_string())
}

/// One slice of an object that is stored as one extent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stripe {
    pub object_offset: u64,
    pub length: u64,
}

impl Stripe {
    pub fn range(&self) -> std::ops::Range<usize> {
        self.object_offset as usize..(self.object_offset + self.length) as usize
    }
}

/// The most data bytes one stripe may hold with pages of `page_bytes`.
pub fn stripe_capacity(page_bytes: u64, stripe_bytes: u64) -> Result<u64, ObjectError> {
    if page_bytes == 0 || page_bytes > MAX_PAGE_BYTES {
        return Err(invalid("the page size is zero or past the largest page"));
    }
    let pages = (stripe_bytes / page_bytes).min(MAX_DATA_PAGES);
    if pages == 0 {
        return Err(invalid("a stripe is smaller than one page"));
    }
    Ok(pages * page_bytes)
}

/// Cut `length` bytes into stripes. An empty object has none.
pub fn plan_stripes(length: u64, page_bytes: u64, stripe_bytes: u64) -> Result<Vec<Stripe>, ObjectError> {
    let capacity = stripe_capacity(page_bytes, stripe_bytes)?;
    let count = length.div_ceil(capacity);
    if count > MAX_CHUNK_EXTENTS as u64 {
        return Err(invalid("the object needs more extents than a manifest may name"));
    }
    Ok((0..count)
        .map(|index| {
            let object_offset = index * capacity;
            Stripe { object_offset, length: capacity.min(length - object_offset) }
        })
        .collect())
}

/// Where a stripe was placed: the group that stores it and the run of pages inside one chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    pub group: String,
    pub volume_id: u64,
    pub chunk_id: u64,
    /// The first page of the run: the first data page.
    pub first_page: u64,
}

/// The pages a stripe stores, in order: every data page, then the digest page when it has one.
/// Each entry is the page's index inside the stripe's run and its bytes.
pub fn pages_of<'a>(data: &'a [u8], page_bytes: usize, digests: &'a ExtentDigests) -> Vec<(u64, &'a [u8])> {
    let mut pages: Vec<(u64, &[u8])> = data.chunks(page_bytes).enumerate().map(|(index, page)| (index as u64, page)).collect();
    if let Some(digest_page) = &digests.digest_page {
        pages.push((pages.len() as u64, digest_page.as_slice()));
    }
    pages
}

/// The extent that names a placed stripe.
pub fn extent_for(placement: &Placement, stripe: &Stripe, digests: &ExtentDigests, page_bytes: u64) -> ChunkExtent {
    ChunkExtent {
        group: placement.group.clone(),
        volume_id: placement.volume_id,
        chunk_id: placement.chunk_id,
        kind: CHUNK_KIND_PAGED,
        object_offset: stripe.object_offset,
        chunk_offset: placement.first_page * page_bytes,
        length: stripe.length,
        flags: CHUNK_FLAG_SEALED,
        digest: digests.digest.to_vec(),
        footer_crc: 0,
        footer_sha256: Vec::new(),
    }
}

/// The manifest of a paged object, checked before anything is published.
pub fn build_manifest(
    bucket: &str,
    key: &str,
    generation: u64,
    page_bytes: u64,
    extents: Vec<ChunkExtent>,
) -> Result<ChunkManifest, ObjectError> {
    let manifest = ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: bucket.to_string(),
        key: key.to_string(),
        generation,
        length: extents.iter().map(|extent| extent.length).sum(),
        extents,
        deleted: false,
        kind: CHUNK_KIND_PAGED,
        page_bytes,
    };
    manifest.validate()?;
    Ok(manifest)
}

/// What one extent contributes to a read: which of its pages to fetch and how to cut the bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtentRead {
    /// The index of the extent in the manifest.
    pub extent: usize,
    /// The first data page of the extent this read touches, counted from the extent's own start.
    pub first_page: u64,
    /// How many data pages it touches.
    pub pages: u64,
    /// Whether the extent has a digest page, which a read fetches and verifies as well.
    pub digest_page: bool,
    /// Bytes to drop from the front of the first page fetched.
    pub skip: usize,
    /// Bytes this extent contributes to the answer.
    pub take: usize,
}

/// Which pages of which extents a read of `offset..offset + length` needs.
pub fn plan_read(manifest: &ChunkManifest, offset: u64, length: u64) -> Result<Vec<ExtentRead>, ObjectError> {
    if manifest.deleted || manifest.kind != CHUNK_KIND_PAGED {
        return Err(invalid("only a live paged object is read page by page"));
    }
    let end = offset.checked_add(length).ok_or_else(|| invalid("a read range overflows"))?;
    if end > manifest.length {
        return Err(invalid("a read reaches past the end of the object"));
    }
    if length == 0 {
        return Ok(Vec::new());
    }
    let page_bytes = manifest.page_bytes;
    let mut reads = Vec::new();
    for (index, extent) in manifest.extents.iter().enumerate() {
        let extent_end = extent.object_offset + extent.length;
        if extent_end <= offset || extent.object_offset >= end {
            continue;
        }
        let local_start = offset.max(extent.object_offset) - extent.object_offset;
        let local_end = end.min(extent_end) - extent.object_offset;
        let first_page = local_start / page_bytes;
        let last_page = (local_end - 1) / page_bytes;
        reads.push(ExtentRead {
            extent: index,
            first_page,
            pages: last_page - first_page + 1,
            digest_page: chunk_integrity::data_pages(extent.length, page_bytes) > 1,
            skip: (local_start - first_page * page_bytes) as usize,
            take: (local_end - local_start) as usize,
        });
    }
    Ok(reads)
}

/// The pages of the allocation a read of one extent fetches: `(chunk page index, is the digest
/// page)` for each data page it touches and then the digest page.
pub fn pages_to_fetch(extent: &ChunkExtent, read: &ExtentRead, page_bytes: u64) -> Vec<u64> {
    let base = extent.chunk_offset / page_bytes;
    let mut pages: Vec<u64> = (read.first_page..read.first_page + read.pages).map(|page| base + page).collect();
    if read.digest_page {
        pages.push(base + chunk_integrity::data_pages(extent.length, page_bytes));
    }
    pages
}

/// Verify what a read fetched and cut out the bytes it asked for.
///
/// `pages` are the data pages in order; `digest_page` is given exactly when the extent has one. The
/// digest page is verified against the manifest on every call, so a range read is as trustworthy as
/// a whole one. A tampered page the range does not touch does not fail the read.
pub fn verify_and_cut(
    extent: &ChunkExtent,
    page_bytes: u64,
    read: &ExtentRead,
    digest_page: Option<&[u8]>,
    pages: &[&[u8]],
) -> Result<Vec<u8>, ObjectError> {
    if pages.len() as u64 != read.pages {
        return Err(ObjectError::Corruption("an extent read returned the wrong number of pages".into()));
    }
    chunk_integrity::verify_range(&extent.digest, digest_page, extent.length, page_bytes, read.first_page, pages)
        .map_err(|error| error.into_object_error())?;
    let mut bytes = Vec::with_capacity(pages.iter().map(|page| page.len()).sum());
    for page in pages {
        bytes.extend_from_slice(page);
    }
    Ok(bytes[read.skip..read.skip + read.take].to_vec())
}

#[cfg(test)]
#[path = "chunk_object_tests.rs"]
mod tests;
