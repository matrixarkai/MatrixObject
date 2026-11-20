// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! End-to-end integrity of a paged chunk extent, held by the client and checked against the
//! manifest. Nothing here touches the chunk servers or any durable format.
//!
//! A paged extent is a run of pages written once. Its manifest `digest` is built so that a read of
//! the whole extent and a read of any range inside it can each be verified page by page:
//!
//! * An extent of ONE page (every small object): `digest` is the SHA-256 of the page's bytes, and
//!   there is no digest page. A small object costs the one page it costs today.
//! * An extent of MORE than one page: the writer appends one extra page to the run, the DIGEST
//!   PAGE, holding the 32-byte SHA-256 of each data page in order, and `digest` is the SHA-256 of
//!   the digest page's bytes. The digest page is at most 4,095 x 32 bytes (about 128 KiB), so it
//!   is one page, and a multi-page extent holds at most [`MAX_DATA_PAGES`] data pages so that the
//!   whole run, digest page included, fits the 4,096 pages of one allocation.
//!
//! A reader checks the digest page against the manifest's `digest`, and the pages it read against
//! the digest page. Whatever is missing or does not match is a typed [`IntegrityError`]; no
//! failure falls back to reading without verification.
//!
//! Crash safety is ordered writing, not a rule here: the writer puts the data pages, then the
//! digest page, and only then publishes the manifest that names the extent, so a reader (which
//! reaches pages only through a published manifest) never sees an extent without its digest page.
use crate::ObjectError;
use std::fmt;

/// Bytes of one SHA-256 digest.
pub const DIGEST_BYTES: usize = 32;
/// The most data pages a multi-page extent may hold: the allocation has 4,096 pages and the
/// digest page takes one of them.
pub const MAX_DATA_PAGES: u64 = 4095;

/// Why a read of a paged extent could not be verified. Each variant is a distinct way the stored
/// bytes failed to match what the manifest promised.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntegrityError {
    /// A multi-page extent was given no digest page (it could not be read).
    DigestPageMissing,
    /// A one-page extent was given a digest page: its digest is the page's own.
    UnexpectedDigestPage,
    /// The digest page is not 32 bytes per data page.
    DigestPageLength { expected: usize, found: usize },
    /// The digest page's SHA-256 is not the manifest's digest: a wrong page, or a changed byte.
    DigestPageMismatch,
    /// A data page's SHA-256 is not its entry in the (verified) digest page, or, for a one-page
    /// extent, not the manifest's digest.
    PageMismatch { page: u64 },
    /// A data page came back shorter or longer than the extent says it is.
    PageLength { page: u64, expected: usize, found: usize },
    /// The request itself does not fit the extent: no pages, a page past the end, a page size of
    /// zero, or a length that cannot be a paged extent.
    Geometry(&'static str),
}

impl fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DigestPageMissing => write!(f, "the digest page of a multi-page extent is missing"),
            Self::UnexpectedDigestPage => write!(f, "a one-page extent has no digest page"),
            Self::DigestPageLength { expected, found } => {
                write!(f, "the digest page holds {found} bytes where {expected} were expected")
            }
            Self::DigestPageMismatch => write!(f, "the digest page does not match the extent digest"),
            Self::PageMismatch { page } => write!(f, "page {page} does not match its digest"),
            Self::PageLength { page, expected, found } => {
                write!(f, "page {page} holds {found} bytes where {expected} were expected")
            }
            Self::Geometry(what) => write!(f, "invalid paged extent geometry: {what}"),
        }
    }
}

impl std::error::Error for IntegrityError {}

impl From<IntegrityError> for ObjectError {
    fn from(error: IntegrityError) -> Self {
        ObjectError::Corruption(format!("extent integrity: {error}"))
    }
}

/// The SHA-256 of `data`.
pub fn sha256(data: &[u8]) -> [u8; DIGEST_BYTES] {
    let mut out = [0u8; DIGEST_BYTES];
    out.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, data).as_ref());
    out
}

/// The data pages an extent of `length` bytes occupies: `length / page_bytes` rounded up.
pub fn data_pages(length: u64, page_bytes: u64) -> u64 {
    if page_bytes == 0 {
        return 0;
    }
    length.div_ceil(page_bytes)
}

/// The pages the extent occupies in its allocation, digest page included.
pub fn stored_pages(length: u64, page_bytes: u64) -> u64 {
    match data_pages(length, page_bytes) {
        0 => 0,
        1 => 1,
        pages => pages + 1,
    }
}

/// Whether an extent of `length` bytes can be written with pages of `page_bytes`: at least one
/// byte, and a multi-page extent no longer than [`MAX_DATA_PAGES`] pages.
pub fn fits_allocation(length: u64, page_bytes: u64) -> bool {
    let pages = data_pages(length, page_bytes);
    page_bytes != 0 && length != 0 && (pages == 1 || (2..=MAX_DATA_PAGES).contains(&pages))
}

/// What a writer stores for one paged extent: the manifest digest, and the digest page when the
/// extent has more than one page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtentDigests {
    pub digest: [u8; DIGEST_BYTES],
    pub digest_page: Option<Vec<u8>>,
}

/// The digests of the bytes of one paged extent, split into pages of `page_bytes`.
pub fn digest_extent(data: &[u8], page_bytes: usize) -> Result<ExtentDigests, IntegrityError> {
    if page_bytes == 0 || data.is_empty() {
        return Err(IntegrityError::Geometry("an extent has bytes and a page size"));
    }
    if !fits_allocation(data.len() as u64, page_bytes as u64) {
        return Err(IntegrityError::Geometry("an extent holds at most 4,095 pages when it has more than one"));
    }
    if data.len() <= page_bytes {
        return Ok(ExtentDigests { digest: sha256(data), digest_page: None });
    }
    let mut digest_page = Vec::with_capacity(data.chunks(page_bytes).count() * DIGEST_BYTES);
    for page in data.chunks(page_bytes) {
        digest_page.extend_from_slice(&sha256(page));
    }
    Ok(ExtentDigests { digest: sha256(&digest_page), digest_page: Some(digest_page) })
}

/// Verify pages `first_page..first_page + pages.len()` of an extent of `extent_length` bytes
/// against the manifest's `digest` and the extent's digest page.
///
/// `digest_page` is `None` exactly when the extent has one page. For a multi-page extent the
/// digest page is verified against `digest` on every call, whatever pages are asked for: a range
/// read is only as trustworthy as the page that lists the digests it is checked against.
pub fn verify_range(
    digest: &[u8],
    digest_page: Option<&[u8]>,
    extent_length: u64,
    page_bytes: u64,
    first_page: u64,
    pages: &[&[u8]],
) -> Result<(), IntegrityError> {
    if page_bytes == 0 || extent_length == 0 || digest.len() != DIGEST_BYTES || pages.is_empty() {
        return Err(IntegrityError::Geometry("a read names an extent, a page size and pages"));
    }
    let total = data_pages(extent_length, page_bytes);
    if total > MAX_DATA_PAGES {
        return Err(IntegrityError::Geometry("an extent holds at most 4,095 pages when it has more than one"));
    }
    let last = first_page
        .checked_add(pages.len() as u64)
        .ok_or(IntegrityError::Geometry("page range overflows"))?;
    if last > total {
        return Err(IntegrityError::Geometry("a page past the end of the extent"));
    }
    let expected_len = |page: u64| -> usize {
        if page + 1 == total {
            (extent_length - page * page_bytes) as usize
        } else {
            page_bytes as usize
        }
    };
    if total == 1 {
        if digest_page.is_some() {
            return Err(IntegrityError::UnexpectedDigestPage);
        }
        let page = pages[0];
        if page.len() != expected_len(0) {
            return Err(IntegrityError::PageLength { page: 0, expected: expected_len(0), found: page.len() });
        }
        if sha256(page).as_slice() != digest {
            return Err(IntegrityError::PageMismatch { page: 0 });
        }
        return Ok(());
    }
    let digest_page = digest_page.ok_or(IntegrityError::DigestPageMissing)?;
    let expected = total as usize * DIGEST_BYTES;
    if digest_page.len() != expected {
        return Err(IntegrityError::DigestPageLength { expected, found: digest_page.len() });
    }
    if sha256(digest_page).as_slice() != digest {
        return Err(IntegrityError::DigestPageMismatch);
    }
    for (offset, page) in pages.iter().enumerate() {
        let index = first_page + offset as u64;
        if page.len() != expected_len(index) {
            return Err(IntegrityError::PageLength { page: index, expected: expected_len(index), found: page.len() });
        }
        let at = index as usize * DIGEST_BYTES;
        if sha256(page).as_slice() != &digest_page[at..at + DIGEST_BYTES] {
            return Err(IntegrityError::PageMismatch { page: index });
        }
    }
    Ok(())
}

/// Verify the whole extent: `data` is every byte of it, in order.
pub fn verify_whole(
    digest: &[u8],
    digest_page: Option<&[u8]>,
    page_bytes: u64,
    data: &[u8],
) -> Result<(), IntegrityError> {
    if page_bytes == 0 || data.is_empty() {
        return Err(IntegrityError::Geometry("an extent has bytes and a page size"));
    }
    let pages: Vec<&[u8]> = data.chunks(page_bytes as usize).collect();
    verify_range(digest, digest_page, data.len() as u64, page_bytes, 0, &pages)
}

#[cfg(test)]
#[path = "chunk_integrity_tests.rs"]
mod tests;
