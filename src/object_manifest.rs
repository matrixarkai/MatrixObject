// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Binary object extents and conditional publication state. Node wiring is separate.
use crate::{ChunkId, ObjectError, ObjectId};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
const MAX_MANIFEST: usize = 1024 * 1024;

/// A multiply-and-rotate hasher for the page-uniqueness sets in manifest validation. The keys
/// are small and bounded (at most 4096 per manifest), nothing is stored or sent from it, and
/// the default hasher's resistance to crafted collisions is not worth its cost here: a
/// pathological set costs at most 4096 squared comparisons.
#[derive(Default)]
struct PageHasher(u64);
impl std::hash::Hasher for PageHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = 0u64;
            for (index, byte) in chunk.iter().enumerate() {
                word |= u64::from(*byte) << (8 * index);
            }
            self.write_u64(word ^ ((chunk.len() as u64) << 56));
        }
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = (self.0.rotate_left(5) ^ value).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}
type PageSet<K> = std::collections::HashSet<K, std::hash::BuildHasherDefault<PageHasher>>;
const MAX_SNAPSHOT: usize = 64 * 1024 * 1024;
const MAX_OBJECTS: usize = 100_000;

/// A committed namespace for immutable object pages.
/// Allocation IDs are unique within one metadata history, including recovery.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ObjectPageAllocation {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(uint64, tag = "2")]
    pub allocation_id: u64,
    #[prost(uint32, tag = "3")]
    pub page_count: u32,
}
impl ObjectPageAllocation {
    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.format_version != 1
            || self.allocation_id == 0
            || !(1..=4096).contains(&self.page_count)
        {
            return Err(ObjectError::InvalidArgument(
                "invalid object page allocation".into(),
            ));
        }
        Ok(())
    }
    pub fn page(&self, index: u32) -> Result<crate::PageId, ObjectError> {
        self.validate()?;
        if index >= self.page_count {
            return Err(ObjectError::InvalidArgument(
                "object page index exceeds allocation".into(),
            ));
        }
        Ok(crate::PageId {
            chunk_id: ChunkId::new(self.allocation_id >> 32, self.allocation_id & 0xffff_ffff)?,
            page_index: u64::from(index),
        })
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ObjectExtent {
    #[prost(string, tag = "1")]
    pub group: String,
    #[prost(uint64, tag = "2")]
    pub volume_id: u64,
    #[prost(uint64, tag = "3")]
    pub chunk_id: u64,
    #[prost(uint64, tag = "4")]
    pub page_index: u64,
    #[prost(uint64, tag = "5")]
    pub object_offset: u64,
    #[prost(uint64, tag = "6")]
    pub page_offset: u64,
    #[prost(uint64, tag = "7")]
    pub length: u64,
    #[prost(uint64, tag = "8")]
    pub write_sequence: u64,
    #[prost(fixed64, tag = "9")]
    pub data_checksum: u64,
    /// Empty on manifests written before authenticated extent reads.
    #[prost(bytes = "vec", tag = "10")]
    pub data_sha256: Vec<u8>,
    /// `EXTENT_KIND_PAGE` (0, absent on the wire) or `EXTENT_KIND_LOG_CHUNK`. Only a format 3
    /// manifest may hold a log-chunk extent; earlier readers reject the field as unknown.
    #[prost(uint32, tag = "11")]
    pub kind: u32,
    /// `EXTENT_FLAG_SEALED` / `EXTENT_FLAG_TAIL`; zero for a page extent.
    #[prost(uint32, tag = "12")]
    pub flags: u32,
    /// CRC-32C over the sealed chunk bytes; zero unless the extent is sealed.
    #[prost(fixed32, tag = "13")]
    pub footer_crc: u32,
    /// SHA-256 over the sealed chunk bytes, computed once at seal. Present exactly when
    /// `EXTENT_FLAG_FOOTER_SHA256` is set (it can be switched off by configuration).
    #[prost(bytes = "vec", tag = "14")]
    pub footer_sha256: Vec<u8>,
}
/// A manifest whose extents may name log chunks (a byte range of one chunk) as well as pages.
pub const MANIFEST_FORMAT_CHUNKED: u32 = 3;
pub const EXTENT_KIND_PAGE: u32 = 0;
/// One entry covers the object bytes held by a whole log chunk: `page_index` and `page_offset`
/// are zero and `length` is the chunk's length when it was last recorded.
pub const EXTENT_KIND_LOG_CHUNK: u32 = 1;
/// The chunk's length is final.
pub const EXTENT_FLAG_SEALED: u32 = 1;
/// The chunk is still being written: `length` is a lower bound. Only the last entry may carry it.
pub const EXTENT_FLAG_TAIL: u32 = 2;
/// `footer_sha256` is recorded (requires `EXTENT_FLAG_SEALED`).
pub const EXTENT_FLAG_FOOTER_SHA256: u32 = 4;
const MAX_PAGE_EXTENT_END: u64 = 256 * 1024 * 1024;
const MAX_LOG_CHUNK_LENGTH: u64 = 512 * 1024 * 1024;
/// Whether `format_version` is one this build reads.
fn known_format(version: u32) -> bool {
    (1..=MANIFEST_FORMAT_CHUNKED).contains(&version)
}
/// The per-extent shape rules that depend only on the extent and the manifest version.
fn extent_shape_ok(version: u32, extent: &ObjectExtent) -> bool {
    match (version, extent.kind) {
        (1 | 2 | 3, EXTENT_KIND_PAGE) => {
            extent.flags == 0
                && extent.footer_crc == 0
                && extent.footer_sha256.is_empty()
                && match version {
                    1 => extent.data_sha256.is_empty(),
                    _ => extent.data_sha256.len() == 32,
                }
                && extent
                    .page_offset
                    .checked_add(extent.length)
                    .is_some_and(|n| n <= MAX_PAGE_EXTENT_END)
        }
        (MANIFEST_FORMAT_CHUNKED, EXTENT_KIND_LOG_CHUNK) => {
            extent.page_index == 0
                && extent.page_offset == 0
                && extent.data_checksum == 0
                && extent.data_sha256.is_empty()
                && extent.length <= MAX_LOG_CHUNK_LENGTH
                && extent.flags & !(EXTENT_FLAG_SEALED | EXTENT_FLAG_TAIL | EXTENT_FLAG_FOOTER_SHA256) == 0
                && extent.flags & (EXTENT_FLAG_SEALED | EXTENT_FLAG_TAIL) != EXTENT_FLAG_SEALED | EXTENT_FLAG_TAIL
                && (extent.footer_crc == 0 || extent.flags & EXTENT_FLAG_SEALED != 0)
                && (extent.flags & EXTENT_FLAG_FOOTER_SHA256 != 0) == (extent.footer_sha256.len() == 32)
                && (extent.footer_sha256.is_empty() || extent.flags & EXTENT_FLAG_SEALED != 0)
        }
        _ => false,
    }
}
/// Whether this build may PUBLISH a format 3 manifest. Readers accept it first; the writers are
/// enabled in a later change, once every node reads it.
const EMIT_CHUNKED_MANIFESTS: bool = false;
fn refuse_unemitted_format(manifest: &ObjectManifest) -> Result<(), ObjectError> {
    if manifest.format_version == MANIFEST_FORMAT_CHUNKED && !EMIT_CHUNKED_MANIFESTS {
        return Err(ObjectError::InvalidArgument(
            "chunk-granular manifests are not published by this build".into(),
        ));
    }
    Ok(())
}
/// The ordering rules between extents: page extents come first, then log chunks, and only the
/// last extent may be unsealed. `after_chunk` is whether a log chunk precedes this extent.
fn extent_order_ok(extent: &ObjectExtent, after_chunk: bool, is_last: bool) -> bool {
    match extent.kind {
        EXTENT_KIND_PAGE => !after_chunk,
        _ => is_last || extent.flags & EXTENT_FLAG_SEALED != 0,
    }
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ObjectManifest {
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
    pub extents: Vec<ObjectExtent>,
    #[prost(bool, tag = "7")]
    pub deleted: bool,
}
fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
#[cfg(test)]
thread_local! {
    static VALIDATE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
/// Full manifest validations on this thread since the last reset (tests only).
#[cfg(all(test, feature = "metadata-consensus"))]
pub(crate) fn validate_calls() -> usize {
    VALIDATE_CALLS.with(|calls| calls.get())
}
#[cfg(all(test, feature = "metadata-consensus"))]
pub(crate) fn reset_validate_calls() {
    VALIDATE_CALLS.with(|calls| calls.set(0));
}
impl ObjectManifest {
    pub fn object_id(&self) -> ObjectId {
        ObjectId::new(self.bucket.clone(), self.key.clone())
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        #[cfg(test)]
        VALIDATE_CALLS.with(|calls| calls.set(calls.get() + 1));
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if !known_format(self.format_version)
            || self.generation == 0
            || self.extents.len() > 4096
            || self.encoded_len() > MAX_MANIFEST
        {
            return Err(invalid(
                "invalid object manifest version, generation or size",
            ));
        }
        if self.deleted && (self.length != 0 || !self.extents.is_empty()) {
            return Err(invalid("deleted object manifest contains data"));
        }
        let mut offset = 0u64;
        let mut pages = PageSet::with_capacity_and_hasher(self.extents.len(), Default::default());
        let mut after_chunk = false;
        for (position, extent) in self.extents.iter().enumerate() {
            if !extent_order_ok(extent, after_chunk, position + 1 == self.extents.len()) {
                return Err(invalid("invalid object extent ordering, identity or bounds"));
            }
            after_chunk |= extent.kind == EXTENT_KIND_LOG_CHUNK;
            ChunkId::new(extent.volume_id, extent.chunk_id)?;
            if extent.group.is_empty()
                || extent.group.len() > 128
                || !extent
                    .group
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || extent.length == 0
                || extent.write_sequence == 0
                || !extent_shape_ok(self.format_version, extent)
                || extent.object_offset != offset
                || !pages.insert((
                    &extent.group,
                    extent.volume_id,
                    extent.chunk_id,
                    extent.page_index,
                ))
            {
                return Err(invalid(
                    "invalid object extent ordering, identity or bounds",
                ));
            }
            offset = offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("object length overflow"))?;
        }
        if offset != self.length {
            return Err(invalid("object extents do not cover its length"));
        }
        Ok(())
    }
    /// Validate `self` where the first `prefix.extents.len()` extents are exactly those of
    /// `prefix`, which the caller has already validated. Only what the append added is checked
    /// in full: the header, the new extents, their continuity with the prefix, and that none
    /// of their pages is already named by the prefix. Equivalent to `validate` for such a pair.
    pub(crate) fn validate_extension(&self, prefix: &ObjectManifest) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        let kept = prefix.extents.len();
        if !known_format(self.format_version)
            || self.format_version != prefix.format_version
            || self.generation == 0
            || self.extents.len() > 4096
            || self.extents.len() < kept
            || self.encoded_len() > MAX_MANIFEST
        {
            return Err(invalid(
                "invalid object manifest version, generation or size",
            ));
        }
        if self.deleted && (self.length != 0 || !self.extents.is_empty()) {
            return Err(invalid("deleted object manifest contains data"));
        }
        let suffix = &self.extents[kept..];
        let mut offset = prefix.length;
        // The prefix's last log chunk becomes a middle entry when anything follows it, so it
        // must already be sealed; and a page extent may not follow a log chunk.
        let mut after_chunk = prefix.extents.last().is_some_and(|last| last.kind == EXTENT_KIND_LOG_CHUNK);
        if after_chunk && !suffix.is_empty() && prefix.extents.last().is_some_and(|last| last.flags & EXTENT_FLAG_SEALED == 0) {
            return Err(invalid("invalid object extent ordering, identity or bounds"));
        }
        for (position, extent) in suffix.iter().enumerate() {
            if !extent_order_ok(extent, after_chunk, position + 1 == suffix.len()) {
                return Err(invalid("invalid object extent ordering, identity or bounds"));
            }
            after_chunk |= extent.kind == EXTENT_KIND_LOG_CHUNK;
            ChunkId::new(extent.volume_id, extent.chunk_id)?;
            if extent.group.is_empty()
                || extent.group.len() > 128
                || !extent
                    .group
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || extent.length == 0
                || extent.write_sequence == 0
                || !extent_shape_ok(self.format_version, extent)
                || extent.object_offset != offset
            {
                return Err(invalid(
                    "invalid object extent ordering, identity or bounds",
                ));
            }
            offset = offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("object length overflow"))?;
        }
        if offset != self.length {
            return Err(invalid("object extents do not cover its length"));
        }
        // A page may be named once: among the new extents, and against the prefix.
        let same_page = |a: &ObjectExtent, b: &ObjectExtent| {
            a.chunk_id == b.chunk_id
                && a.page_index == b.page_index
                && a.volume_id == b.volume_id
                && a.group == b.group
        };
        if suffix.len() <= 16 {
            for (index, extent) in suffix.iter().enumerate() {
                if suffix[..index].iter().any(|other| same_page(extent, other))
                    || prefix.extents.iter().any(|other| same_page(extent, other))
                {
                    return Err(invalid(
                        "invalid object extent ordering, identity or bounds",
                    ));
                }
            }
        } else {
            let mut pages = PageSet::with_capacity_and_hasher(self.extents.len(), Default::default());
            for extent in &self.extents {
                if !pages.insert((&extent.group, extent.volume_id, extent.chunk_id, extent.page_index)) {
                    return Err(invalid(
                        "invalid object extent ordering, identity or bounds",
                    ));
                }
            }
        }
        Ok(())
    }
    /// Validate `self` when its extents are exactly the first extents of `after`, which the
    /// caller has validated. Every per-extent rule then already holds for `self` (it is a prefix
    /// of a valid extent list), so only the header and the covered length are left to check.
    /// Equivalent to `validate` for such a pair.
    pub(crate) fn validate_as_prefix_of(&self, after: &ObjectManifest) -> Result<(), ObjectError> {
        self.validate_as_prefix_of_sized(after, true)
    }
    /// `validate_as_prefix_of`; with `check_size` false the encoded size is not measured, for a
    /// caller that has bounded the size of `after` (which holds every extent of `self`).
    fn validate_as_prefix_of_sized(&self, after: &ObjectManifest, check_size: bool) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        let kept = self.extents.len();
        if !known_format(self.format_version)
            || self.generation == 0
            || kept > 4096
            || kept > after.extents.len()
        {
            return Err(invalid(
                "invalid object manifest version, generation or size",
            ));
        }
        if self.deleted && (self.length != 0 || !self.extents.is_empty()) {
            return Err(invalid("deleted object manifest contains data"));
        }
        let covered = if kept == after.extents.len() {
            after.length
        } else {
            after.extents[kept].object_offset
        };
        if covered != self.length {
            return Err(invalid("object extents do not cover its length"));
        }
        if check_size && self.encoded_len() > MAX_MANIFEST {
            return Err(invalid(
                "invalid object manifest version, generation or size",
            ));
        }
        Ok(())
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        frame(b"MOMF", self, MAX_MANIFEST)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = unframe(b"MOMF", bytes, MAX_MANIFEST)?;
        value.validate()?;
        Ok(value)
    }
}
/// A decided publication, including the complete expected prior value.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestOutcome {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    #[prost(message, optional, tag = "2")]
    pub before: Option<ObjectManifest>,
    #[prost(message, optional, tag = "3")]
    pub after: Option<ObjectManifest>,
}
impl ManifestOutcome {
    pub fn decide(
        before: Option<ObjectManifest>,
        after: ObjectManifest,
    ) -> Result<Self, ObjectError> {
        refuse_unemitted_format(&after)?;
        let value = Self {
            format_version: 1,
            before,
            after: Some(after),
        };
        value.validate()?;
        Ok(value)
    }
    /// The decision of an append for a compact publication. Only what the append adds is
    /// validated in full (`validate_extension`); the prior manifest is bound by its digest, which
    /// the metaserver checks against the manifest it holds before it applies the append, so a
    /// prior that was not valid is refused there. Cost: the new extents, plus the cheap
    /// comparisons of the prefix, instead of a pass of hashing over every extent.
    pub(crate) fn decide_append(before: ObjectManifest, after: ObjectManifest) -> Result<Self, ObjectError> {
        refuse_unemitted_format(&after)?;
        let value = Self {
            format_version: 1,
            before: Some(before),
            after: Some(after),
        };
        let (before, after) = (value.before.as_ref().unwrap(), value.after.as_ref().unwrap());
        if before.format_version != after.format_version || !after.extents.starts_with(&before.extents) {
            return Err(invalid("append changes immutable prefix"));
        }
        // `validate_extension` bounds the encoded size of `after`, which holds every extent of `before`
        after.validate_extension(before)?;
        before.validate_as_prefix_of_sized(after, false)?;
        if before.object_id() != after.object_id() {
            return Err(invalid("manifest outcome changes object identity"));
        }
        let generation = before
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("manifest generation exhausted"))?;
        if after.generation != generation {
            return Err(invalid("manifest outcome generation is not consecutive"));
        }
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.format_version != 1 {
            return Err(invalid("manifest outcome version mismatch"));
        }
        let after = self
            .after
            .as_ref()
            .ok_or_else(|| invalid("manifest outcome has no resulting state"))?;
        after.validate()?;
        let generation = if let Some(before) = &self.before {
            if before.format_version == after.format_version && after.extents.starts_with(&before.extents) {
                before.validate_as_prefix_of(after)?;
            } else {
                before.validate()?;
            }
            if before.object_id() != after.object_id() {
                return Err(invalid("manifest outcome changes object identity"));
            }
            before
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("manifest generation exhausted"))?
        } else {
            1
        };
        if after.generation != generation {
            return Err(invalid("manifest outcome generation is not consecutive"));
        }
        Ok(())
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        frame(b"MOMO", self, 2 * MAX_MANIFEST + 128)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = unframe(b"MOMO", bytes, 2 * MAX_MANIFEST + 128)?;
        value.validate()?;
        Ok(value)
    }
}
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct ManifestCatalog {
    objects: BTreeMap<ObjectId, Arc<ManifestEntry>>,
    body_bytes: usize,
    retained: BTreeMap<u64, Arc<crate::manifest_retention::RetainedManifest>>,
    retention_bytes: usize,
    staged: BTreeMap<u64, crate::staged_allocation::StagedAllocation>,
    staged_bytes: usize,
    publication_fence: u64,
    // Tombstones retain every generation advance. Rebuild this sum on restore;
    // 100,000 retained u64 generations fit without overflow in u128.
    revision: u128,
}
/// Immutable catalog value shared by snapshots and speculative undo.
#[derive(Debug)]
pub(crate) struct ManifestEntry {
    manifest: crate::extent_list::StoredManifest,
    encoded_bytes: usize,
    /// The hash chain over the manifest's extents and the manifest digest built from it,
    /// computed on first use or carried forward from the previous entry by an append. The
    /// entry is immutable and every change to an object builds a new entry, so the digest
    /// cannot outlive the value it describes. Not part of an entry's identity.
    digest: std::sync::OnceLock<StoredDigest>,
}
/// The chain over a stored manifest's extents and the digest of the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDigest {
    pub(crate) chain: crate::manifest_chain::ChainState,
    pub(crate) digest: Vec<u8>,
}
impl StoredDigest {
    pub(crate) fn of(manifest: &ObjectManifest) -> Self {
        let chain = crate::manifest_chain::ChainState::of(&manifest.extents);
        let digest = chain.digest(manifest);
        Self { chain, digest }
    }
    pub(crate) fn of_stored(manifest: &crate::extent_list::StoredManifest) -> Self {
        let chain = crate::manifest_chain::ChainState::of_stored(&manifest.extents);
        let digest = chain.digest_stored(manifest);
        Self { chain, digest }
    }
}
impl PartialEq for ManifestEntry {
    fn eq(&self, other: &Self) -> bool {
        self.manifest == other.manifest && self.encoded_bytes == other.encoded_bytes
    }
}
impl Eq for ManifestEntry {}
impl ManifestEntry {
    fn new(manifest: ObjectManifest) -> Self {
        let manifest = crate::extent_list::StoredManifest::from_manifest(manifest);
        let encoded_bytes = framed_size(manifest.encoded_len());
        Self { manifest, encoded_bytes, digest: std::sync::OnceLock::new() }
    }
}
impl std::ops::Deref for ManifestEntry {
    type Target = crate::extent_list::StoredManifest;
    fn deref(&self) -> &crate::extent_list::StoredManifest { &self.manifest }
}
impl crate::extent_list::StoredManifest {
    /// `ObjectManifest::validate_extension` for a stored manifest: `self` is `prefix` plus new
    /// extents, and only the header and the new extents are checked (against each other and
    /// against the stored ones).
    pub(crate) fn validate_extension_of(&self, prefix: &crate::extent_list::StoredManifest) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        let kept = prefix.extents.len();
        if !(1..=2).contains(&self.format_version)
            || self.format_version != prefix.format_version
            || self.generation == 0
            || self.extents.len() > 4096
            || self.extents.len() < kept
            || self.encoded_len() > MAX_MANIFEST
        {
            return Err(invalid(
                "invalid object manifest version, generation or size",
            ));
        }
        if self.deleted && (self.length != 0 || !self.extents.is_empty()) {
            return Err(invalid("deleted object manifest contains data"));
        }
        let suffix: Vec<&ObjectExtent> = self.extents.iter_from(kept).collect();
        let mut offset = prefix.length;
        for extent in &suffix {
            ChunkId::new(extent.volume_id, extent.chunk_id)?;
            if extent.group.is_empty()
                || extent.group.len() > 128
                || !extent
                    .group
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || extent.length == 0
                || extent.write_sequence == 0
                || !extent_shape_ok(self.format_version, extent)
                || extent.object_offset != offset
            {
                return Err(invalid(
                    "invalid object extent ordering, identity or bounds",
                ));
            }
            offset = offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("object length overflow"))?;
        }
        if offset != self.length {
            return Err(invalid("object extents do not cover its length"));
        }
        // A page may be named once: among the new extents, and against the stored ones.
        let same_page = |a: &ObjectExtent, b: &ObjectExtent| {
            a.chunk_id == b.chunk_id
                && a.page_index == b.page_index
                && a.volume_id == b.volume_id
                && a.group == b.group
        };
        if suffix.len() <= 16 {
            for (index, extent) in suffix.iter().enumerate() {
                if suffix[..index].iter().any(|other| same_page(extent, other))
                    || prefix.extents.iter().any(|other| same_page(extent, other))
                {
                    return Err(invalid(
                        "invalid object extent ordering, identity or bounds",
                    ));
                }
            }
        } else {
            let mut pages = PageSet::with_capacity_and_hasher(self.extents.len(), Default::default());
            for extent in self.extents.iter() {
                if !pages.insert((&extent.group, extent.volume_id, extent.chunk_id, extent.page_index)) {
                    return Err(invalid(
                        "invalid object extent ordering, identity or bounds",
                    ));
                }
            }
        }
        Ok(())
    }
}
fn entry_size(value: &impl Message) -> usize {
    framed_size(value.encoded_len())
}
/// The bytes a message of `size` takes as one length-prefixed record of the catalog image.
fn framed_size(size: usize) -> usize {
    let mut remaining = size;
    let mut delimiter = 1;
    while remaining >= 128 {
        delimiter += 1;
        remaining >>= 7;
    }
    1 + delimiter + size
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestListRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(string, tag = "3")]
    pub after: String,
    #[prost(uint32, tag = "4")]
    pub limit: u32,
    #[prost(bytes = "vec", tag = "5")]
    pub expected_revision: Vec<u8>,
    #[prost(string, tag = "6")]
    pub after_bucket: String,
}
impl ManifestListRequest {
    pub(crate) fn validate(&self) -> Result<(), ObjectError> {
        if self.version == 3 {
            if !self.bucket.is_empty() || self.after_bucket.is_empty() != self.after.is_empty() {
                return Err(invalid("invalid global manifest cursor"));
            }
            if !self.after_bucket.is_empty() { crate::validate_bucket(&self.after_bucket)?; }
        } else {
            crate::validate_bucket(&self.bucket)?;
            if !self.after_bucket.is_empty() { return Err(invalid("unexpected manifest cursor bucket")); }
        }
        if !self.after.is_empty() { crate::validate_key(&self.after)?; }
        if !(1..=3).contains(&self.version) || !(1..=32).contains(&self.limit)
            || (self.version == 1 && !self.expected_revision.is_empty())
            || (!self.expected_revision.is_empty() && self.expected_revision.len() != 16) {
            return Err(invalid("invalid manifest listing bounds"));
        }
        Ok(())
    }
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 65536 { return Err(invalid("manifest listing request too large")); }
        let value = Self::decode(bytes).map_err(|_| invalid("invalid manifest listing protobuf"))?;
        value.validate()?;
        if value.encode_to_vec() != bytes { return Err(invalid("noncanonical manifest listing")); }
        Ok(value)
    }
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestSummary {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(uint64, tag = "3")]
    pub length: u64,
    #[prost(bool, tag = "4")]
    pub deleted: bool,
    #[prost(string, tag = "5")]
    pub bucket: String,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ManifestListPage {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub bucket: String,
    #[prost(message, repeated, tag = "3")]
    pub entries: Vec<ManifestSummary>,
    #[prost(bool, tag = "4")]
    pub has_more: bool,
    #[prost(bytes = "vec", tag = "5")]
    pub revision: Vec<u8>,
}
impl ManifestListPage {
    pub(crate) fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > 262144 { return Err(invalid("manifest listing reply too large")); }
        let value = Self::decode(bytes).map_err(|_| invalid("invalid manifest listing reply"))?;
        if value.version == 3 {
            if !value.bucket.is_empty() { return Err(invalid("unexpected global page bucket")); }
        } else { crate::validate_bucket(&value.bucket)?; }
        if !(1..=3).contains(&value.version) || value.entries.len() > 32
            || (value.version == 1 && !value.revision.is_empty())
            || (value.version >= 2 && value.revision.len() != 16)
            || (value.has_more && value.entries.is_empty())
            || value.entries.windows(2).any(|pair| if value.version == 3 {
                (&pair[0].bucket, &pair[0].key) >= (&pair[1].bucket, &pair[1].key)
            } else { pair[0].key >= pair[1].key })
        { return Err(invalid("invalid manifest listing reply bounds or order")); }
        for entry in &value.entries {
            if value.version == 3 { crate::validate_bucket(&entry.bucket)?; }
            else if !entry.bucket.is_empty() { return Err(invalid("unexpected manifest summary bucket")); }
            crate::validate_key(&entry.key)?;
            if entry.generation == 0 || (entry.deleted && entry.length != 0) {
                return Err(invalid("invalid manifest summary"));
            }
        }
        if value.encode_to_vec() != bytes { return Err(invalid("noncanonical manifest listing reply")); }
        Ok(value)
    }
}
#[derive(Clone, PartialEq, Message)]
struct CatalogImage {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(message, repeated, tag = "2")]
    objects: Vec<ObjectManifest>,
    #[prost(message, repeated, tag = "3")]
    retained: Vec<crate::manifest_retention::RetainedManifest>,
    #[prost(message, repeated, tag = "4")]
    staged: Vec<crate::staged_allocation::StagedAllocation>,
    #[prost(uint64, tag = "5")]
    publication_fence: u64,
}
impl ManifestCatalog {
    pub fn publication_fence(&self) -> u64 { self.publication_fence }
    fn header_size(&self) -> usize {
        if self.publication_fence == 0 { 2 } else {
            3 + prost::encoding::encoded_len_varint(self.publication_fence)
        }
    }
    pub(crate) fn check_publication_fence(&self, before: u64, after: u64) -> Result<(), ObjectError> {
        if before != 0 || self.publication_fence != before || after == 0 {
            return Err(invalid("publication fence must advance once from disabled"));
        }
        let header = 3 + prost::encoding::encoded_len_varint(after);
        if self.body_bytes + self.retention_bytes + self.staged_bytes + header > MAX_SNAPSHOT {
            return Err(ObjectError::PreconditionFailed("publication fence exceeds checkpoint bound".into()));
        }
        Ok(())
    }
    pub(crate) fn restore_publication_fence(&mut self, id: u64) { self.publication_fence = id; }
    pub fn staged_allocation(&self, id: u64) -> Option<&crate::staged_allocation::StagedAllocation> {
        self.staged.get(&id)
    }
    #[cfg(feature = "meta-server")]
    pub(crate) fn expired_staged(&self, now: u64) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.staged.values().filter(move |record| record.expires_at_ms != 0 && record.expires_at_ms <= now)
            .map(|record| (record.allocation_id, record.expires_at_ms))
    }
    pub(crate) fn staged_ids(&self, after: u64, limit: usize) -> (Vec<u64>, bool) {
        let mut rows = self.staged.range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded));
        let ids = rows.by_ref().take(limit).map(|(id, _)| *id).collect();
        (ids, rows.next().is_some())
    }
    pub(crate) fn staged_count(&self) -> usize { self.staged.len() }
    pub(crate) fn max_staged_id(&self) -> u64 { self.staged.last_key_value().map_or(0, |(id, _)| *id) }
    pub(crate) fn check_staging(&self, change: &crate::staged_allocation::StagedAllocationChange) -> Result<(), ObjectError> {
        let record = match (&change.before, &change.after) {
            (None, Some(after)) => after,
            (Some(before), None) => before,
            (Some(before), Some(after)) if after.renews(before) => {
                before.validate()?;
                after
            }
            _ => return Err(invalid("staging must create, renew or remove one allocation")),
        };
        record.validate()?;
        if self.staged.get(&record.allocation_id) != change.before.as_ref() {
            return Err(ObjectError::PreconditionFailed("staging prior state mismatch".into()));
        }
        if change.after.is_some() {
            if record.version >= 2 && self.publication_fence == 0 {
                return Err(ObjectError::PreconditionFailed("leased staging requires publication fencing".into()));
            }
            let released = change.before.as_ref().map_or(0, entry_size);
            if (change.before.is_none() && self.staged.len() >= crate::staged_allocation::MAX_STAGED_ALLOCATIONS)
                || self.body_bytes + self.retention_bytes + self.staged_bytes - released + entry_size(record) > MAX_SNAPSHOT - self.header_size() {
                return Err(ObjectError::PreconditionFailed("staged allocation capacity exceeded".into()));
            }
            let current = self.get(&ObjectId::new(record.bucket.clone(), record.key.clone()));
            if current.map_or(0, |manifest| manifest.generation) != record.expected_generation
                || (record.append && current.is_none_or(|manifest| manifest.deleted || manifest.format_version != 2)) {
                return Err(ObjectError::PreconditionFailed("object changed before staging".into()));
            }
        }
        Ok(())
    }
    /// Check a reservation made by the same commit that publishes `after` and removes
    /// `consumed`: the reserved allocation is for the next append to the object just published.
    /// `body_bytes` is the catalog's size once `after` is stored.
    pub(crate) fn check_reservation(
        &self,
        reserved: &crate::staged_allocation::StagedAllocation,
        after: &ObjectManifest,
        body_bytes: usize,
        consumed: &crate::staged_allocation::StagedAllocation,
    ) -> Result<(), ObjectError> {
        self.check_reservation_for(
            reserved, &after.bucket, &after.key, after.generation, after.deleted, after.format_version,
            body_bytes, consumed,
        )
    }
    /// `check_reservation` against the header of the published manifest.
    pub(crate) fn check_reservation_for(
        &self,
        reserved: &crate::staged_allocation::StagedAllocation,
        bucket: &str,
        key: &str,
        generation: u64,
        deleted: bool,
        format_version: u32,
        body_bytes: usize,
        consumed: &crate::staged_allocation::StagedAllocation,
    ) -> Result<(), ObjectError> {
        reserved.validate()?;
        if reserved.version != 2 || !reserved.append || reserved.bucket != bucket
            || reserved.key != key || reserved.expected_generation != generation
            || deleted || format_version != 2 {
            return Err(invalid("invalid reservation for the published manifest"));
        }
        if self.publication_fence == 0 {
            return Err(ObjectError::PreconditionFailed("leased staging requires publication fencing".into()));
        }
        if self.staged.get(&consumed.allocation_id) != Some(consumed) || self.staged.contains_key(&reserved.allocation_id) {
            return Err(ObjectError::PreconditionFailed("reservation prior state mismatch".into()));
        }
        // One allocation is removed and one created, so the count is unchanged; the byte
        // budget is checked with the new manifest stored and the consumed record released.
        if body_bytes + self.retention_bytes + self.staged_bytes - entry_size(consumed) + entry_size(reserved)
            > MAX_SNAPSHOT - self.header_size() {
            return Err(ObjectError::PreconditionFailed("staged allocation capacity exceeded".into()));
        }
        Ok(())
    }
    pub(crate) fn apply_staging(&mut self, change: &crate::staged_allocation::StagedAllocationChange) -> Result<(), ObjectError> {
        self.check_staging(change)?;
        let id = change.before.as_ref().or(change.after.as_ref()).unwrap().allocation_id;
        self.restore_staging(id, change.after.clone());
        Ok(())
    }
    pub(crate) fn restore_staging(&mut self, id: u64, value: Option<crate::staged_allocation::StagedAllocation>) {
        if let Some(before) = self.staged.remove(&id) { self.staged_bytes -= entry_size(&before); }
        if let Some(value) = value {
            self.staged_bytes += entry_size(&value);
            self.staged.insert(id, value);
        }
    }
    pub fn retained_manifest(&self, id: u64) -> Option<&crate::manifest_retention::RetainedManifest> {
        self.retained.get(&id).map(Arc::as_ref)
    }
    pub(crate) fn max_retention_id(&self) -> u64 {
        self.retained.last_key_value().map_or(0, |(id, _)| *id)
    }
    pub(crate) fn retention_ids(&self, after: u64, limit: usize) -> (Vec<u64>, bool) {
        let mut ids = self.retained.range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded));
        let page = ids.by_ref().take(limit).map(|(id, _)| *id).collect();
        (page, ids.next().is_some())
    }
    pub(crate) fn check_retention(&self, change: &crate::manifest_retention::ManifestRetentionChange) -> Result<(), ObjectError> {
        let record = match (&change.before, &change.after) {
            (None, Some(after)) => after,
            (Some(before), None) => before,
            _ => return Err(invalid("retention must create or remove one handle")),
        };
        record.validate()?;
        if self.retained_manifest(record.retention_id) != change.before.as_ref() {
            return Err(ObjectError::PreconditionFailed("retention prior state mismatch".into()));
        }
        if change.after.is_some() {
            if self.retained.len() >= crate::manifest_retention::MAX_RETAINED_MANIFESTS
                || self.body_bytes + self.retention_bytes + self.staged_bytes + entry_size(record) > MAX_SNAPSHOT - self.header_size() {
                return Err(ObjectError::PreconditionFailed("retention catalog capacity exceeded".into()));
            }
            let manifest = record.manifest.as_ref().unwrap();
            if !self.matches_expected(&manifest.object_id(), Some(manifest)) {
                return Err(ObjectError::PreconditionFailed("manifest changed before retention".into()));
            }
        }
        Ok(())
    }
    pub(crate) fn apply_retention(&mut self, change: &crate::manifest_retention::ManifestRetentionChange) -> Result<(), ObjectError> {
        self.check_retention(change)?;
        let id = change.before.as_ref().or(change.after.as_ref()).unwrap().retention_id;
        self.restore_retention(id, change.after.clone());
        Ok(())
    }
    pub(crate) fn restore_retention(&mut self, id: u64, value: Option<crate::manifest_retention::RetainedManifest>) {
        self.restore_retention_arc(id, value.map(Arc::new));
    }
    #[cfg(feature = "metadata-consensus")]
    pub(crate) fn retained_arc(&self, id: u64) -> Option<Arc<crate::manifest_retention::RetainedManifest>> {
        self.retained.get(&id).cloned()
    }
    pub(crate) fn restore_retention_arc(&mut self, id: u64, value: Option<Arc<crate::manifest_retention::RetainedManifest>>) {
        if let Some(previous) = self.retained.remove(&id) {
            self.retention_bytes -= entry_size(previous.as_ref());
        }
        if let Some(value) = value {
            self.retention_bytes += entry_size(value.as_ref());
            self.retained.insert(id, value);
        }
    }
    pub(crate) fn expired_retention(&self, now: u64) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.retained.values().filter(move |record| record.expires_at_ms != 0 && record.expires_at_ms <= now)
            .map(|record| (record.retention_id, record.expires_at_ms))
    }
    #[cfg(feature = "metadata-consensus")]
    pub(crate) fn restore_retention_deadline(&mut self, id: u64, deadline: u64) {
        if let Some(record) = self.retained.get_mut(&id) {
            self.retention_bytes -= entry_size(record.as_ref());
            Arc::make_mut(record).expires_at_ms = deadline;
            self.retention_bytes += entry_size(record.as_ref());
        }
    }
    pub(crate) fn check_retention_deadline(&self, change: &crate::manifest_retention::RetentionDeadlineChange)
        -> Result<usize, ObjectError> {
        change.validate()?;
        let record = self.retained.get(&change.retention_id)
            .filter(|record| record.expires_at_ms == change.before)
            .ok_or_else(|| ObjectError::PreconditionFailed("read session deadline changed".into()))?;
        let old_len = record.encoded_len();
        let new_len = old_len - prost::encoding::encoded_len_varint(change.before)
            + prost::encoding::encoded_len_varint(change.after);
        let new_entry = 1 + prost::encoding::encoded_len_varint(new_len as u64) + new_len;
        let bytes = self.retention_bytes - entry_size(record.as_ref()) + new_entry;
        if self.body_bytes + bytes + self.staged_bytes > MAX_SNAPSHOT - self.header_size() {
            return Err(ObjectError::PreconditionFailed("retention catalog capacity exceeded".into()));
        }
        Ok(bytes)
    }
    pub(crate) fn apply_retention_deadline(&mut self, change: &crate::manifest_retention::RetentionDeadlineChange)
        -> Result<(), ObjectError> {
        let bytes = self.check_retention_deadline(change)?;
        Arc::make_mut(self.retained.get_mut(&change.retention_id).unwrap()).expires_at_ms = change.after;
        self.retention_bytes = bytes;
        Ok(())
    }
    pub(crate) fn check_read_session_open(&self, open: &crate::read_session_outcome::ReadSessionOpen)
        -> Result<(ObjectManifest, usize), ObjectError> {
        open.validate()?;
        // The stored manifest was validated when it was stored, and its digest is the one
        // computed once for the stored value: neither is derived again from every extent here.
        let (stored, cached) = self.get_with_digest(&ObjectId::new(open.bucket.clone(), open.key.clone()))?
            .filter(|(manifest, _)| !manifest.deleted && manifest.generation == open.generation)
            .ok_or_else(|| ObjectError::PreconditionFailed("read session generation changed".into()))?;
        if cached.digest != open.manifest_sha256 {
            return Err(ObjectError::PreconditionFailed("read session manifest digest changed".into()));
        }
        let manifest_len = stored.encoded_len();
        let manifest = stored.to_manifest();
        let record_len = 1 + prost::encoding::encoded_len_varint(open.retention_id)
            + 1 + prost::encoding::encoded_len_varint(manifest_len as u64) + manifest_len
            + 1 + prost::encoding::encoded_len_varint(open.expires_at_ms);
        let entry = 1 + prost::encoding::encoded_len_varint(record_len as u64) + record_len;
        let bytes = self.retention_bytes + entry;
        if self.retained.contains_key(&open.retention_id) || self.staged.contains_key(&open.retention_id)
            || self.retained.len() >= crate::manifest_retention::MAX_RETAINED_MANIFESTS
            || self.body_bytes + bytes + self.staged_bytes > MAX_SNAPSHOT - self.header_size() {
            return Err(ObjectError::PreconditionFailed("read session catalog capacity or identity conflict".into()));
        }
        Ok((manifest, bytes))
    }
    pub(crate) fn commit_checked_read_session(&mut self, record: crate::manifest_retention::RetainedManifest, bytes: usize) {
        self.retained.insert(record.retention_id, Arc::new(record));
        self.retention_bytes = bytes;
    }
    pub(crate) fn check_read_session_removal(&self, removal: &crate::read_session_outcome::ReadSessionRemoval)
        -> Result<usize, ObjectError> {
        removal.validate()?;
        let record = self.retained.get(&removal.retention_id)
            .filter(|record| record.expires_at_ms == removal.expected_expiry_ms)
            .ok_or_else(|| ObjectError::PreconditionFailed("read session removal deadline changed".into()))?;
        Ok(self.retention_bytes - entry_size(record.as_ref()))
    }
    pub(crate) fn commit_checked_read_session_removal(&mut self, id: u64, bytes: usize) {
        let removed = self.retained.remove(&id);
        debug_assert!(removed.is_some());
        self.retention_bytes = bytes;
    }
    pub(crate) fn list_page(&self, request: &ManifestListRequest) -> Result<ManifestListPage, ObjectError> {
        request.validate()?;
        if request.version >= 2 && !request.expected_revision.is_empty()
            && request.expected_revision != self.revision.to_be_bytes()
        {
            return Err(ObjectError::PreconditionFailed("manifest catalog changed during listing".into()));
        }
        let start = ObjectId::new(if request.version == 3 { request.after_bucket.clone() }
            else { request.bucket.clone() }, request.after.clone());
        let mut values = self.objects.range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
            .take_while(|(id, _)| request.version == 3 || id.bucket.as_str() == request.bucket);
        let entries = values.by_ref().take(request.limit as usize).map(|(_, manifest)| ManifestSummary {
            key: manifest.key.clone(), generation: manifest.generation,
            length: manifest.length, deleted: manifest.deleted,
            bucket: if request.version == 3 { manifest.bucket.clone() } else { String::new() },
        }).collect();
        Ok(ManifestListPage { version: request.version, bucket: request.bucket.clone(), entries,
            has_more: values.next().is_some(),
            revision: if request.version >= 2 { self.revision.to_be_bytes().to_vec() } else { Vec::new() } })
    }
    #[cfg(feature = "metadata-consensus")]
    pub(crate) fn restore_entry_arc(&mut self, id: ObjectId, previous: Option<Arc<ManifestEntry>>) {
        let previous_bytes = previous.as_ref().map_or(0, |entry| entry.encoded_bytes);
        let previous_generation = previous.as_ref().map_or(0, |entry| u128::from(entry.generation));
        let current = if let Some(previous) = previous {
            self.objects.insert(id, previous)
        } else { self.objects.remove(&id) };
        if let Some(current) = current {
            self.revision -= u128::from(current.generation);
            self.body_bytes -= current.encoded_bytes;
        }
        self.revision += previous_generation;
        self.body_bytes += previous_bytes;
    }
    pub(crate) fn encoded_size(&self) -> usize {
        self.body_bytes + self.retention_bytes + self.staged_bytes + self.header_size() + 12
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.staged.is_empty() && self.publication_fence == 0
    }
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Includes tombstones so a recreation must name the deleted generation.
    pub(crate) fn get(&self, id: &ObjectId) -> Option<&crate::extent_list::StoredManifest> {
        self.objects.get(id).map(|entry| &entry.manifest)
    }
    /// Whether the catalog holds exactly `expected` (or nothing, when `expected` is `None`).
    pub(crate) fn matches_expected(&self, id: &ObjectId, expected: Option<&ObjectManifest>) -> bool {
        match (self.get(id), expected) {
            (Some(stored), Some(expected)) => stored.matches(expected),
            (None, None) => true,
            _ => false,
        }
    }
    /// A copy of the stored manifest as the protobuf message: O(extents).
    pub fn get_manifest(&self, id: &ObjectId) -> Option<ObjectManifest> {
        self.objects.get(id).map(|entry| entry.manifest.to_manifest())
    }
    /// The stored manifest, which was validated when it was stored, and its chain and digest,
    /// computed once per stored value (an append carries them forward from the entry it
    /// replaces).
    pub(crate) fn get_with_digest(&self, id: &ObjectId) -> Result<Option<(&crate::extent_list::StoredManifest, &StoredDigest)>, ObjectError> {
        let Some(entry) = self.objects.get(id) else { return Ok(None) };
        let digest = entry.digest.get_or_init(|| StoredDigest::of_stored(&entry.manifest));
        Ok(Some((&entry.manifest, digest)))
    }
    #[cfg(feature = "metadata-consensus")]
    pub(crate) fn entry_arc(&self, id: &ObjectId) -> Option<Arc<ManifestEntry>> {
        self.objects.get(id).cloned()
    }
    /// Compare before touching state. An identical already-applied outcome is a no-op.
    pub(crate) fn check_outcome(
        &self,
        outcome: &ManifestOutcome,
    ) -> Result<Option<usize>, ObjectError> {
        self.check_outcome_with_admission(outcome, None, None)
    }
    pub(crate) fn check_outcome_with_admission(
        &self, outcome: &ManifestOutcome, allocation_id: Option<u64>, source: Option<&crate::manifest_copy::LiveCopySource>,
    ) -> Result<Option<usize>, ObjectError> {
        outcome.validate()?;
        self.check_validated_outcome_with_admission(outcome, allocation_id, source)
    }
    /// `check_outcome_with_admission` for an outcome the caller has validated in full.
    pub(crate) fn check_validated_outcome_with_admission(
        &self, outcome: &ManifestOutcome, allocation_id: Option<u64>, source: Option<&crate::manifest_copy::LiveCopySource>,
    ) -> Result<Option<usize>, ObjectError> {
        let after = outcome.after.as_ref().unwrap();
        let id = after.object_id();
        let current = self.get(&id);
        if current.is_some_and(|stored| stored.matches(after)) {
            return Ok(None);
        }
        if !self.matches_expected(&id, outcome.before.as_ref()) {
            return Err(ObjectError::PreconditionFailed(
                "object manifest changed before publication".into(),
            ));
        }
        if let Some(source) = source {
            source.validate()?;
            let value = self.get(&ObjectId::new(source.bucket.clone(), source.key.clone()))
                .ok_or_else(|| ObjectError::PreconditionFailed("copy source is missing".into()))?;
            if value.deleted || value.generation != source.generation
                || after.format_version != value.format_version || after.extents.len() != value.extents.len()
                || !after.extents.iter().eq(value.extents.iter()) || after.length != value.length {
                return Err(ObjectError::PreconditionFailed("copy source changed before publication".into()));
            }
        }
        if self.publication_fence != 0 && allocation_id.is_none() && source.is_none()
            && !after.extents.is_empty()
            && outcome.before.as_ref().is_none_or(|before| before.extents != after.extents) {
            return Err(ObjectError::PreconditionFailed("fresh extents require owned publication".into()));
        }
        if current.is_none() && self.objects.len() >= MAX_OBJECTS {
            return Err(invalid("manifest catalog exceeds object bound"));
        }
        let body_bytes = self.body_bytes - self.objects.get(&id).map_or(0, |entry| entry.encoded_bytes) + entry_size(after);
        if !self.staged.is_empty() {
            for extent in after.extents.iter() {
                let id = extent.volume_id.checked_shl(32).filter(|_| extent.volume_id <= u32::MAX as u64)
                    .and_then(|volume| if extent.chunk_id <= u32::MAX as u64 { Some(volume | extent.chunk_id) } else { None });
                if id.is_some_and(|id| self.staged.contains_key(&id) && Some(id) != allocation_id) {
                    return Err(ObjectError::PreconditionFailed("owned pages require staged publication".into()));
                }
            }
        }
        let released = allocation_id.and_then(|id| self.staged.get(&id)).map_or(0, entry_size);
        if body_bytes + self.retention_bytes + self.staged_bytes - released > MAX_SNAPSHOT - self.header_size() {
            return Err(invalid(
                "manifest publication exceeds checkpoint size bound",
            ));
        }
        Ok(Some(body_bytes))
    }
    pub fn apply(&mut self, outcome: &ManifestOutcome) -> Result<bool, ObjectError> {
        let Some(body_bytes) = self.check_outcome(outcome)? else {
            return Ok(false);
        };
        self.commit_checked_manifest(outcome, body_bytes);
        Ok(true)
    }
    /// The checks of `check_validated_outcome_with_admission` for the publication of a compact
    /// append: `after` is `before`, the stored manifest, plus new extents (see
    /// `ManifestAppend::expand_shared`), so only the header of the stored manifest is compared
    /// and only the new extents are checked against the staged pages. Returns the catalog size
    /// once `after` is stored.
    pub(crate) fn check_append_publication(
        &self,
        before: &crate::extent_list::StoredManifest,
        after: &crate::extent_list::StoredManifest,
        allocation_id: Option<u64>,
    ) -> Result<usize, ObjectError> {
        let id = after.object_id();
        let unchanged = self.get(&id).is_some_and(|stored| {
            stored.bucket == before.bucket
                && stored.key == before.key
                && stored.generation == before.generation
                && stored.length == before.length
                && stored.deleted == before.deleted
                && stored.format_version == before.format_version
                && stored.extents.len() == before.extents.len()
        });
        if !unchanged {
            return Err(ObjectError::PreconditionFailed(
                "object manifest changed before publication".into(),
            ));
        }
        let body_bytes = self.body_bytes - self.objects.get(&id).map_or(0, |entry| entry.encoded_bytes)
            + framed_size(after.encoded_len());
        if !self.staged.is_empty() {
            for extent in after.extents.iter_from(before.extents.len()) {
                let id = extent.volume_id.checked_shl(32).filter(|_| extent.volume_id <= u32::MAX as u64)
                    .and_then(|volume| if extent.chunk_id <= u32::MAX as u64 { Some(volume | extent.chunk_id) } else { None });
                if id.is_some_and(|id| self.staged.contains_key(&id) && Some(id) != allocation_id) {
                    return Err(ObjectError::PreconditionFailed("owned pages require staged publication".into()));
                }
            }
        }
        let released = allocation_id.and_then(|id| self.staged.get(&id)).map_or(0, entry_size);
        if body_bytes + self.retention_bytes + self.staged_bytes - released > MAX_SNAPSHOT - self.header_size() {
            return Err(invalid(
                "manifest publication exceeds checkpoint size bound",
            ));
        }
        Ok(body_bytes)
    }
    /// Store `after`, which extends the stored manifest (`check_append_publication` approved it).
    /// The stored chunks are shared with the entry it replaces, and its digest is that entry's
    /// chain extended by the new extents.
    pub(crate) fn commit_append(&mut self, after: crate::extent_list::StoredManifest, body_bytes: usize) {
        let id = after.object_id();
        let old_bytes = self.objects.get(&id).map_or(0, |entry| entry.encoded_bytes);
        let encoded_bytes = body_bytes - (self.body_bytes - old_bytes);
        let digest = std::sync::OnceLock::new();
        if let Some(previous) = self.objects.get(&id) {
            if let Some(stored) = previous.digest.get() {
                let kept = previous.manifest.extents.len();
                let chain = stored.chain.extended_by(after.extents.iter_from(kept));
                let seeded = StoredDigest { digest: chain.digest_stored(&after), chain };
                #[cfg(test)]
                crate::manifest_chain::uncounted(|| {
                    assert_eq!(seeded, StoredDigest::of_stored(&after), "a carried-forward digest differs from a fresh one")
                });
                let _ = digest.set(seeded);
            }
        }
        self.objects.insert(id, Arc::new(ManifestEntry { manifest: after, encoded_bytes, digest }));
        self.revision += 1;
        self.body_bytes = body_bytes;
    }
    pub(crate) fn commit_checked_manifest(&mut self, outcome: &ManifestOutcome, body_bytes: usize) {
        let after = outcome.after.as_ref().unwrap();
        let id = after.object_id();
        let old_bytes = self.objects.get(&id).map_or(0, |entry| entry.encoded_bytes);
        let encoded_bytes = body_bytes - (self.body_bytes - old_bytes);
        // An append keeps every extent of the entry it replaces: its chain and digest are
        // the old chain extended by the new extents, not a pass over all of them.
        let digest = std::sync::OnceLock::new();
        if let Some(previous) = self.objects.get(&id) {
            let kept = previous.manifest.extents.len();
            if let Some(stored) = previous.digest.get() {
                if after.extents.len() >= kept && previous.manifest.extents.iter().eq(after.extents[..kept].iter()) {
                    let chain = stored.chain.extended(&after.extents[kept..]);
                    let seeded = StoredDigest { digest: chain.digest(after), chain };
                    #[cfg(test)]
                    crate::manifest_chain::uncounted(|| {
                        assert_eq!(seeded, StoredDigest::of(after), "a carried-forward digest differs from a fresh one")
                    });
                    let _ = digest.set(seeded);
                }
            }
        }
        self.objects.insert(id, Arc::new(ManifestEntry {
            manifest: crate::extent_list::StoredManifest::from_ref(after),
            encoded_bytes,
            digest,
        }));
        self.revision += 1;
        self.body_bytes = body_bytes;
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        let image = CatalogImage {
            format_version: if self.publication_fence != 0 { 4 } else if !self.staged.is_empty() { 3 } else if self.retained.is_empty() { 1 } else { 2 },
            objects: self.objects.values().map(|entry| entry.manifest.to_manifest()).collect(),
            retained: self.retained.values().map(|record| record.as_ref().clone()).collect(),
            staged: self.staged.values().cloned().collect(),
            publication_fence: self.publication_fence,
        };
        frame(b"MOMC", &image, MAX_SNAPSHOT)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let image: CatalogImage = unframe(b"MOMC", bytes, MAX_SNAPSHOT)?;
        if !(1..=4).contains(&image.format_version) || image.objects.len() > MAX_OBJECTS
            || (image.format_version < 4 && image.publication_fence != 0)
            || (image.format_version == 4 && image.publication_fence == 0)
            || (image.format_version == 1 && (!image.retained.is_empty() || !image.staged.is_empty()))
            || (image.format_version == 2 && (image.retained.is_empty() || !image.staged.is_empty()))
            || (image.format_version == 3 && image.staged.is_empty())
            || image.retained.len() > crate::manifest_retention::MAX_RETAINED_MANIFESTS
            || image.staged.len() > crate::staged_allocation::MAX_STAGED_ALLOCATIONS {
            return Err(invalid("manifest catalog version or count mismatch"));
        }
        let mut result = Self::default();
        result.publication_fence = image.publication_fence;
        let mut previous = None;
        for value in image.objects {
            value.validate()?;
            let id = value.object_id();
            if previous.as_ref().is_some_and(|p| p >= &id) {
                return Err(invalid("manifest catalog is not strictly ordered"));
            }
            previous = Some(id.clone());
            let entry = ManifestEntry::new(value);
            result.body_bytes += entry.encoded_bytes;
            result.revision += u128::from(entry.generation);
            result.objects.insert(id, Arc::new(entry));
        }
        let mut previous = 0;
        for retained in image.retained {
            retained.validate()?;
            if retained.retention_id <= previous { return Err(invalid("retention handles are not ordered")); }
            let manifest = retained.manifest.as_ref().unwrap();
            let current = result.get(&manifest.object_id())
                .ok_or_else(|| invalid("retained object is missing from catalog"))?;
            if manifest.generation > current.generation
                || (manifest.generation == current.generation && !current.matches(manifest)) {
                return Err(invalid("retained generation exceeds or conflicts with current state"));
            }
            previous = retained.retention_id;
            result.restore_retention(retained.retention_id, Some(retained));
        }
        let mut previous = 0;
        for record in image.staged {
            record.validate()?;
            if record.version >= 2 && result.publication_fence == 0 {
                return Err(invalid("leased staging lacks publication fencing"));
            }
            if record.allocation_id <= previous { return Err(invalid("staged allocations are not ordered")); }
            if result.retained.contains_key(&record.allocation_id) {
                return Err(invalid("protection identities overlap"));
            }
            if record.expected_generation > 0 && result.get(&ObjectId::new(record.bucket.clone(), record.key.clone()))
                .is_none_or(|manifest| manifest.generation < record.expected_generation) {
                return Err(invalid("staged target generation exceeds current history"));
            }
            previous = record.allocation_id;
            result.restore_staging(record.allocation_id, Some(record));
        }
        Ok(result)
    }
}
fn digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    })
}
fn frame<T: Message>(magic: &[u8; 4], value: &T, limit: usize) -> Result<Vec<u8>, ObjectError> {
    if value.encoded_len() > limit {
        return Err(invalid("manifest encoding exceeds size bound"));
    }
    let mut bytes = magic.to_vec();
    value
        .encode(&mut bytes)
        .map_err(|_| invalid("manifest encode failed"))?;
    bytes.extend_from_slice(&digest(&bytes).to_le_bytes());
    Ok(bytes)
}
fn unframe<T: Message + Default>(
    magic: &[u8; 4],
    bytes: &[u8],
    limit: usize,
) -> Result<T, ObjectError> {
    if bytes.len() < 12 || bytes.len() > limit + 12 || &bytes[..4] != magic {
        return Err(invalid("manifest envelope size or magic mismatch"));
    }
    let end = bytes.len() - 8;
    if digest(&bytes[..end]).to_le_bytes() != bytes[end..] {
        return Err(invalid("manifest checksum mismatch"));
    }
    let value =
        T::decode(&bytes[4..end]).map_err(|_| invalid("manifest protobuf decode failed"))?;
    if value.encode_to_vec() != bytes[4..end] {
        return Err(invalid("manifest protobuf is not canonical"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    include!("manifest_catalog_sharing_tests.rs");
    fn sized_manifest(count: usize) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 1,
            length: count as u64,
            extents: (0..count)
                .map(|index| ObjectExtent {
                    group: "group".into(),
                    chunk_id: index as u64 + 1,
                    object_offset: index as u64,
                    length: 1,
                    write_sequence: 1,
                    data_sha256: vec![1; 32],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    /// What `ManifestOutcome::validate` decided before prior manifests were validated as
    /// prefixes: both manifests in full.
    fn full_outcome_validation(outcome: &ManifestOutcome) -> bool {
        if outcome.format_version != 1 {
            return false;
        }
        let Some(after) = &outcome.after else { return false };
        if after.validate().is_err() {
            return false;
        }
        let generation = if let Some(before) = &outcome.before {
            if before.validate().is_err() || before.object_id() != after.object_id() {
                return false;
            }
            match before.generation.checked_add(1) {
                Some(value) => value,
                None => return false,
            }
        } else {
            1
        };
        after.generation == generation
    }
    #[test]
    fn a_stored_manifest_digest_always_describes_the_stored_manifest() {
        let digest = |m: &ObjectManifest| crate::read_session_outcome::manifest_sha256(m).unwrap();
        let mut catalog = ManifestCatalog::default();
        let first = sized_manifest(3);
        catalog.apply(&ManifestOutcome::decide(None, first.clone()).unwrap()).unwrap();
        let id = first.object_id();
        let check = |catalog: &ManifestCatalog, expected: &ObjectManifest| {
            for _ in 0..2 {
                let (stored, cached) = catalog.get_with_digest(&id).unwrap().unwrap();
                assert!(stored.matches(expected));
                assert_eq!(cached.digest, digest(expected));
                assert_eq!(cached.chain, crate::manifest_chain::ChainState::of(&expected.extents));
            }
        };
        check(&catalog, &first);
        // an append, a replacement with the same extent count, a tombstone, a recreation
        let mut appended = sized_manifest(4);
        appended.generation = 2;
        catalog.apply(&ManifestOutcome::decide(Some(first.clone()), appended.clone()).unwrap()).unwrap();
        check(&catalog, &appended);
        assert_ne!(digest(&first), digest(&appended));
        let mut replaced = sized_manifest(4);
        replaced.generation = 3;
        replaced.extents[3].data_sha256 = vec![77; 32];
        catalog.apply(&ManifestOutcome::decide(Some(appended.clone()), replaced.clone()).unwrap()).unwrap();
        check(&catalog, &replaced);
        assert_ne!(digest(&appended), digest(&replaced));
        let tombstone = ObjectManifest { generation: 4, deleted: true, ..sized_manifest(0) };
        catalog.apply(&ManifestOutcome::decide(Some(replaced), tombstone.clone()).unwrap()).unwrap();
        check(&catalog, &tombstone);
        let mut recreated = sized_manifest(2);
        recreated.generation = 5;
        catalog.apply(&ManifestOutcome::decide(Some(tombstone), recreated.clone()).unwrap()).unwrap();
        check(&catalog, &recreated);
        // a restored catalog computes its own
        let restored = ManifestCatalog::from_bytes(&catalog.to_bytes().unwrap()).unwrap();
        let (_, cached) = restored.get_with_digest(&id).unwrap().unwrap();
        assert_eq!(cached.digest, digest(&recreated));
        assert_eq!(*cached, StoredDigest::of(&recreated));
        // a missing object has none
        assert!(catalog.get_with_digest(&ObjectManifest { key: "absent".into(), ..sized_manifest(0) }.object_id()).unwrap().is_none());
    }
    /// The session open trusts the digest the catalog caches for the stored manifest, so the
    /// cache must follow every change of the stored value: after an append (the cache is
    /// carried forward), after a rewrite or a truncation (it must not be), and on a restored
    /// catalog (no cache: it is computed from the extents, to the same value). At each step
    /// the open succeeds with the current digest and is refused with the previous one, and the
    /// cached digest equals one recomputed from the extents.
    #[test]
    fn a_session_opens_only_against_the_current_digest_at_every_size() {
        let digest = |m: &ObjectManifest| crate::read_session_outcome::manifest_sha256(m).unwrap();
        for count in [0usize, 1, 63, 64, 65, 1000] {
            let first = sized_manifest(count);
            let id = first.object_id();
            let mut catalog = ManifestCatalog::default();
            catalog.apply(&ManifestOutcome::decide(None, first.clone()).unwrap()).unwrap();
            let open_with = |catalog: &ManifestCatalog, manifest: &ObjectManifest, sha: Vec<u8>| {
                catalog.check_read_session_open(&crate::read_session_outcome::ReadSessionOpen {
                    retention_id: 9,
                    bucket: manifest.bucket.clone(),
                    key: manifest.key.clone(),
                    generation: manifest.generation,
                    expires_at_ms: 1000,
                    manifest_sha256: sha,
                })
            };
            let check = |catalog: &ManifestCatalog, current: &ObjectManifest, previous: &ObjectManifest, step: &str| {
                let (_, cached) = catalog.get_with_digest(&id).unwrap().unwrap();
                assert_eq!(cached.digest, digest(current), "{step} at {count}: cache differs from the extents");
                assert_eq!(open_with(catalog, current, digest(current)).unwrap().0, *current, "{step} at {count}");
                assert_ne!(digest(previous), digest(current), "{step} at {count}: the step changed nothing");
                assert!(open_with(catalog, current, digest(previous)).is_err(), "{step} at {count}: the previous digest opened");
                assert!(open_with(catalog, previous, digest(current)).is_err(), "{step} at {count}: the previous generation opened");
            };
            // the cache is populated by the first open, then an append must replace it
            let _ = open_with(&catalog, &first, digest(&first)).unwrap();
            let mut appended = sized_manifest(count + 1);
            appended.generation = 2;
            appended.extents[count].chunk_id += 10_000;
            catalog.apply(&ManifestOutcome::decide(Some(first.clone()), appended.clone()).unwrap()).unwrap();
            check(&catalog, &appended, &first, "append");
            // a rewrite that keeps the count, then one that truncates and changes the head
            let mut rewritten = appended.clone();
            rewritten.generation = 3;
            rewritten.extents[count].data_sha256 = vec![42; 32];
            catalog.apply(&ManifestOutcome::decide(Some(appended.clone()), rewritten.clone()).unwrap()).unwrap();
            check(&catalog, &rewritten, &appended, "rewrite");
            let mut truncated = sized_manifest(count / 2);
            truncated.generation = 4;
            if let Some(extent) = truncated.extents.first_mut() {
                extent.data_sha256 = vec![43; 32];
            }
            catalog.apply(&ManifestOutcome::decide(Some(rewritten.clone()), truncated.clone()).unwrap()).unwrap();
            check(&catalog, &truncated, &rewritten, "truncate");
            // a restored catalog has no cache and computes the same digest
            let restored = ManifestCatalog::from_bytes(&catalog.to_bytes().unwrap()).unwrap();
            let (_, cached) = restored.get_with_digest(&id).unwrap().unwrap();
            assert_eq!(*cached, StoredDigest::of(&truncated), "restore at {count}");
            check(&restored, &truncated, &rewritten, "restore");
        }
    }
    #[test]
    fn replaying_appends_reproduces_the_chain_the_running_catalog_carried_forward() {
        let id = sized_manifest(1).object_id();
        let mut live = ManifestCatalog::default();
        let mut replayed = ManifestCatalog::default();
        let mut outcomes = Vec::new();
        let mut current = sized_manifest(1);
        outcomes.push(ManifestOutcome::decide(None, current.clone()).unwrap());
        for step in 0..150usize {
            let mut next = current.clone();
            next.generation += 1;
            next.extents.push(ObjectExtent {
                group: "group".into(),
                chunk_id: 1_000 + step as u64,
                object_offset: next.length,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![(step % 251) as u8; 32],
                ..Default::default()
            });
            next.length += 1;
            outcomes.push(ManifestOutcome::decide(Some(current), next.clone()).unwrap());
            current = next;
        }
        for outcome in &outcomes {
            live.apply(outcome).unwrap();
            // materialise the digest after every step so the next append carries it forward
            let (stored, digest) = live.get_with_digest(&id).unwrap().unwrap();
            assert_eq!(digest.chain.count(), stored.extents.len());
        }
        // a replica replaying the same outcomes without reading digests in between
        for outcome in &outcomes {
            replayed.apply(outcome).unwrap();
        }
        let (final_live, carried) = live.get_with_digest(&id).unwrap().unwrap();
        let (final_replayed, recomputed) = replayed.get_with_digest(&id).unwrap().unwrap();
        assert_eq!(final_live, final_replayed);
        assert_eq!(carried, recomputed);
        assert_eq!(*carried, StoredDigest::of_stored(final_live));
        // and a catalog restored from a snapshot computes the same
        let restored = ManifestCatalog::from_bytes(&live.to_bytes().unwrap()).unwrap();
        assert_eq!(restored.get_with_digest(&id).unwrap().unwrap().1, carried);
    }
    #[test]
    fn outcome_validation_agrees_with_validating_both_manifests_in_full() {
        let mut checked = 0;
        for count in [0usize, 1, 2, 40] {
            for added in [1usize, 3] {
                let before = sized_manifest(count);
                let mut after = sized_manifest(count + added);
                after.generation = 2;
                for extent in &mut after.extents[count..] {
                    extent.chunk_id += 10_000;
                }
                let changes: Vec<(&str, Box<dyn Fn(&mut ObjectManifest, &mut ObjectManifest)>)> = vec![
                    ("none", Box::new(|_, _| {})),
                    ("before length", Box::new(|b, _| b.length += 1)),
                    ("before length down", Box::new(|b, _| b.length = b.length.saturating_sub(1))),
                    ("before generation zero", Box::new(|b, _| b.generation = 0)),
                    ("before generation max", Box::new(|b, _| b.generation = u64::MAX)),
                    ("before deleted", Box::new(|b, _| b.deleted = true)),
                    ("before bucket", Box::new(|b, _| b.bucket = "other".into())),
                    ("before key", Box::new(|b, _| b.key = "bad key ".into())),
                    ("before version", Box::new(|b, _| b.format_version = 3)),
                    ("before version one", Box::new(|b, _| b.format_version = 1)),
                    ("before extent", Box::new(|b, _| if let Some(e) = b.extents.last_mut() { e.length += 1 })),
                    ("before drops an extent", Box::new(|b, _| { b.extents.pop(); })),
                    ("after generation", Box::new(|_, a| a.generation = 5)),
                    ("after length", Box::new(|_, a| a.length += 1)),
                    ("after prefix extent", Box::new(|_, a| if let Some(e) = a.extents.first_mut() { e.data_sha256[0] ^= 1 })),
                    ("after duplicate page", Box::new(move |_, a| {
                        let last = a.extents.len() - 1;
                        a.extents[last].chunk_id = a.extents[0].chunk_id;
                    })),
                ];
                for (name, change) in changes {
                    let (mut b, mut a) = (before.clone(), after.clone());
                    change(&mut b, &mut a);
                    let outcome = ManifestOutcome { format_version: 1, before: Some(b), after: Some(a) };
                    assert_eq!(
                        outcome.validate().is_ok(),
                        full_outcome_validation(&outcome),
                        "{name} ({count}+{added})"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 100);
    }
    #[test]
    fn the_page_hasher_spreads_sequential_pages_and_validation_still_refuses_repeats() {
        use std::hash::BuildHasher;
        let build = std::hash::BuildHasherDefault::<PageHasher>::default();
        let hashes: std::collections::HashSet<u64> = (0..4096u64)
            .map(|index| build.hash_one(("group-a", 0u64, index + 1, 0u64)))
            .collect();
        assert!(hashes.len() >= 4090, "only {} distinct hashes for 4096 pages", hashes.len());
        let largest = sized_manifest(4096);
        assert!(largest.validate().is_ok());
        for position in [1usize, 17, 2047, 4095] {
            let mut repeated = sized_manifest(4096);
            let first = repeated.extents[0].clone();
            repeated.extents[position].chunk_id = first.chunk_id;
            repeated.extents[position].page_index = first.page_index;
            assert!(repeated.validate().is_err(), "repeat at {position}");
        }
    }
    #[test]
    fn extension_validation_agrees_with_full_validation() {
        for count in [0usize, 1, 17, 300, 4000] {
            let before = sized_manifest(count);
            assert!(before.validate().is_ok());
            for added in [1usize, 2, 16, 17, 60] {
                if count + added > 4096 {
                    continue;
                }
                let mut after = sized_manifest(count + added);
                after.generation = 2;
                for extent in &mut after.extents[count..] {
                    extent.chunk_id += 10_000;
                }
                assert!(after.validate().is_ok());
                assert!(after.validate_extension(&before).is_ok(), "{count}+{added}");
                let damage: Vec<(&str, Box<dyn Fn(&mut ObjectManifest)>)> = vec![
                    ("offset", Box::new(move |m| m.extents[count].object_offset += 1)),
                    ("zero length", Box::new(move |m| m.extents[count].length = 0)),
                    ("digest length", Box::new(move |m| { m.extents[count].data_sha256.pop(); })),
                    ("sequence", Box::new(move |m| m.extents[count].write_sequence = 0)),
                    ("total length", Box::new(|m| m.length += 1)),
                    ("generation", Box::new(|m| m.generation = 0)),
                    ("group name", Box::new(move |m| m.extents[count].group = "bad group".into())),
                    ("duplicate inside the append", Box::new(move |m| {
                        let first = m.extents[count].clone();
                        let last = m.extents.len() - 1;
                        m.extents[last].chunk_id = first.chunk_id;
                        m.extents[last].page_index = first.page_index;
                    })),
                    ("duplicate of a prefix page", Box::new(move |m| {
                        if count > 0 {
                            m.extents[count].chunk_id = m.extents[0].chunk_id;
                        } else {
                            m.extents[count].length = 0;
                        }
                    })),
                ];
                for (name, change) in damage {
                    let mut wrong = after.clone();
                    change(&mut wrong);
                    if added == 1 && name == "duplicate inside the append" {
                        continue; // one new extent cannot repeat itself
                    }
                    let full = wrong.validate().is_err();
                    let incremental = wrong.validate_extension(&before).is_err();
                    assert!(full, "{name} must be refused by full validation ({count}+{added})");
                    assert_eq!(full, incremental, "{name} ({count}+{added})");
                }
            }
        }
    }
    /// An append trusted against its prefix is still refused when its new extents overlap the
    /// bytes the prefix covers or leave a hole after them, whatever length the result claims.
    #[test]
    fn an_append_that_overlaps_or_leaves_a_hole_is_refused() {
        for count in [1usize, 20, 300] {
            let before = sized_manifest(count);
            for (name, shift) in [("hole", 1i64), ("overlap", -1), ("far hole", 50), ("deep overlap", -(count as i64))] {
                let mut after = sized_manifest(count + 1);
                after.generation = 2;
                after.extents[count].chunk_id += 10_000;
                after.extents[count].object_offset = (count as i64 + shift) as u64;
                // the result's length is made to agree with the shifted extent, so only the
                // continuity with the prefix is wrong
                after.length = after.extents[count].object_offset + after.extents[count].length;
                assert!(
                    ManifestOutcome::decide_append(before.clone(), after.clone()).is_err(),
                    "{name} at {count} was accepted"
                );
                assert!(ManifestOutcome::decide(Some(before.clone()), after).is_err(), "{name} at {count}");
            }
            // and the honest append is accepted by both
            let mut after = sized_manifest(count + 1);
            after.generation = 2;
            after.extents[count].chunk_id += 10_000;
            assert!(ManifestOutcome::decide_append(before.clone(), after.clone()).is_ok());
            assert!(ManifestOutcome::decide(Some(before), after).is_ok());
        }
    }
    /// A read session opens against the digest the catalog holds for the stored manifest, which
    /// is the digest of the stored manifest's encoding: the same one a client derives from
    /// the manifest it read, so the two agree at every size, after appends too.
    #[test]
    fn a_read_session_opens_against_the_digest_the_catalog_holds() {
        for count in [0usize, 1, 63, 64, 65, 300] {
            let stored = sized_manifest(count);
            let mut catalog = ManifestCatalog::default();
            catalog.apply(&ManifestOutcome::decide(None, stored.clone()).unwrap()).unwrap();
            let mut latest = stored;
            // one more generation by append, so the digest is the carried-forward one
            let mut next = sized_manifest(count + 1);
            next.generation = 2;
            next.extents[count].chunk_id += 10_000;
            catalog.apply(&ManifestOutcome::decide(Some(latest.clone()), next.clone()).unwrap()).unwrap();
            latest = next;
            let digest = crate::read_session_outcome::manifest_sha256(&latest).unwrap();
            let (_, cached) = catalog.get_with_digest(&latest.object_id()).unwrap().unwrap();
            assert_eq!(cached.digest, digest, "{count}");
            let open = crate::read_session_outcome::ReadSessionOpen {
                retention_id: 7,
                bucket: latest.bucket.clone(),
                key: latest.key.clone(),
                generation: latest.generation,
                expires_at_ms: 1000,
                manifest_sha256: digest,
            };
            let (manifest, bytes) = catalog.check_read_session_open(&open).unwrap();
            assert_eq!(manifest, latest, "{count}");
            assert!(bytes > latest.encoded_len(), "{count}");
            let mut wrong = open.clone();
            wrong.manifest_sha256[0] ^= 1;
            assert!(catalog.check_read_session_open(&wrong).is_err(), "{count}");
            let mut stale = open;
            stale.generation = 1;
            assert!(catalog.check_read_session_open(&stale).is_err(), "{count}");
        }
    }
    /// `decide_append` decides what `decide` decides for an append onto a valid manifest, and
    /// refuses what is not an append: only the validation of a prior that was never valid is
    /// left to the metaserver's digest check.
    #[test]
    fn deciding_an_append_agrees_with_deciding_it_in_full() {
        for count in [0usize, 1, 17, 300, 4000] {
            let before = sized_manifest(count);
            assert!(before.validate().is_ok());
            for added in [1usize, 2, 17, 60] {
                if count + added > 4096 {
                    continue;
                }
                let mut after = sized_manifest(count + added);
                after.generation = 2;
                for extent in &mut after.extents[count..] {
                    extent.chunk_id += 10_000;
                }
                let damage: Vec<(&str, Box<dyn Fn(&mut ObjectManifest)>)> = vec![
                    ("none", Box::new(|_| {})),
                    ("offset", Box::new(move |m| m.extents[count].object_offset += 1)),
                    ("zero length", Box::new(move |m| m.extents[count].length = 0)),
                    ("digest length", Box::new(move |m| { m.extents[count].data_sha256.pop(); })),
                    ("sequence", Box::new(move |m| m.extents[count].write_sequence = 0)),
                    ("total length", Box::new(|m| m.length += 1)),
                    ("generation", Box::new(|m| m.generation = 5)),
                    ("same generation", Box::new(|m| m.generation = 1)),
                    ("group name", Box::new(move |m| m.extents[count].group = "bad group".into())),
                    ("other bucket", Box::new(|m| m.bucket = "other".into())),
                    ("other key", Box::new(|m| m.key = "other".into())),
                    ("duplicate inside the append", Box::new(move |m| {
                        let first = m.extents[count].clone();
                        let last = m.extents.len() - 1;
                        m.extents[last].chunk_id = first.chunk_id;
                        m.extents[last].page_index = first.page_index;
                    })),
                    ("duplicate of a prefix page", Box::new(move |m| {
                        if count > 0 {
                            m.extents[count].chunk_id = m.extents[0].chunk_id;
                        } else {
                            m.extents[count].length = 0;
                        }
                    })),
                ];
                for (name, change) in damage {
                    if added == 1 && name == "duplicate inside the append" {
                        continue;
                    }
                    let mut wrong = after.clone();
                    change(&mut wrong);
                    let full = ManifestOutcome::decide(Some(before.clone()), wrong.clone());
                    let append = ManifestOutcome::decide_append(before.clone(), wrong);
                    assert_eq!(full.is_ok(), append.is_ok(), "{name} ({count}+{added})");
                    if let (Ok(full), Ok(append)) = (full, append) {
                        assert_eq!(full, append, "{name} ({count}+{added})");
                    }
                }
            }
        }
        // what is not an append is refused: the prior's extents are not the first of the result
        let before = sized_manifest(20);
        let mut replaced = sized_manifest(21);
        replaced.generation = 2;
        replaced.extents[3].data_sha256[0] ^= 1;
        assert!(ManifestOutcome::decide_append(before.clone(), replaced).is_err());
        let mut other_format = sized_manifest(21);
        other_format.generation = 2;
        other_format.format_version = 1;
        for extent in &mut other_format.extents {
            extent.data_sha256.clear();
        }
        assert!(ManifestOutcome::decide_append(before, other_format).is_err());
    }
    #[test]
    fn manifest_listing_is_bounded_ordered_and_bucket_scoped() {
        let mut catalog = ManifestCatalog::default();
        for (bucket, key, deleted) in [("a", "z", false), ("b", "a", false),
            ("b", "b", true), ("b", "c", false), ("c", "a", false)] {
            let value = ObjectManifest { format_version: 2, bucket: bucket.into(), key: key.into(),
                generation: 1, length: 0, extents: Vec::new(), deleted };
            catalog.apply(&ManifestOutcome::decide(None, value).unwrap()).unwrap();
        }
        let request = ManifestListRequest { version: 1, bucket: "b".into(), after: String::new(), limit: 2, expected_revision: Vec::new(), after_bucket: String::new() };
        let first = catalog.list_page(&request).unwrap();
        assert_eq!(first.entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        assert!(first.entries[1].deleted);
        assert!(first.has_more);
        assert_eq!(ManifestListPage::from_payload(&first.encode_to_vec()).unwrap(), first);
        let second = catalog.list_page(&ManifestListRequest { after: "b".into(), ..request.clone() }).unwrap();
        assert_eq!(second.entries[0].key, "c");
        assert!(!second.has_more);
        for limit in [0, 33] {
            assert!(ManifestListRequest::from_payload(&ManifestListRequest { limit, ..request.clone() }.encode_to_vec()).is_err());
        }
        let mut invalid = first.clone();
        invalid.entries.reverse();
        assert!(ManifestListPage::from_payload(&invalid.encode_to_vec()).is_err());
        let long = ManifestListPage { version: 1, bucket: "b".into(), has_more: false,
            revision: Vec::new(),
            entries: (0..32).map(|index| ManifestSummary { key: format!("{index:02}{}", "x".repeat(4094)),
                generation: 1, length: 0, deleted: false, bucket: String::new() }).collect() };
        assert!(ManifestListPage::from_payload(&long.encode_to_vec()).is_ok());
        let global_request = ManifestListRequest { version: 3, bucket: String::new(), ..request.clone() };
        let global = catalog.list_page(&global_request).unwrap();
        assert_eq!(global.entries.iter().map(|entry| (entry.bucket.as_str(), entry.key.as_str()))
            .collect::<Vec<_>>(), [("a", "z"), ("b", "a")]);
        assert_eq!(ManifestListPage::from_payload(&global.encode_to_vec()).unwrap(), global);
        let next_request = ManifestListRequest { after_bucket: "b".into(), after: "a".into(),
            expected_revision: global.revision.clone(), ..global_request.clone() };
        let next = catalog.list_page(&next_request).unwrap();
        assert!(next.entries[0].deleted);
        assert_eq!(next.entries[1].key, "c");
        let last = catalog.list_page(&ManifestListRequest { after: "c".into(), ..next_request }).unwrap();
        assert_eq!(last.entries[0].bucket, "c");
        assert!(!last.has_more);
        assert!(ManifestListRequest { after_bucket: "b".into(), ..global_request.clone() }.validate().is_err());
        assert!(ManifestListRequest { after: "a".into(), ..global_request }.validate().is_err());
        let mut long_global = long;
        long_global.version = 3;
        long_global.bucket.clear();
        long_global.revision = vec![0; 16];
        for entry in &mut long_global.entries { entry.bucket = "b".repeat(255); }
        assert!(ManifestListPage::from_payload(&long_global.encode_to_vec()).is_ok());
    }
    fn manifest(generation: u64) -> ObjectManifest {
        ObjectManifest {
            format_version: 1,
            bucket: "bucket".into(),
            key: "object".into(),
            generation,
            length: 4,
            deleted: false,
            extents: vec![ObjectExtent {
                group: "group-a".into(),
                volume_id: 1,
                chunk_id: generation,
                page_index: 0,
                object_offset: 0,
                page_offset: 0,
                length: 4,
                write_sequence: 1,
                data_checksum: 42,
                data_sha256: Vec::new(), ..Default::default()
            }],
        }
    }
    #[test]
    fn manifest_listing_revision_tracks_publications_and_survives_restore() {
        let mut catalog = ManifestCatalog::default();
        let request = ManifestListRequest { version: 2, bucket: "bucket".into(),
            after: String::new(), limit: 2, expected_revision: Vec::new(), after_bucket: String::new() };
        let empty = catalog.list_page(&request).unwrap();
        assert_eq!(empty.revision.len(), 16);
        let created = manifest(1);
        let outcome = ManifestOutcome::decide(None, created.clone()).unwrap();
        catalog.apply(&outcome).unwrap();
        assert!(catalog.list_page(&ManifestListRequest {
            expected_revision: empty.revision, ..request.clone()
        }).is_err());
        let first = catalog.list_page(&request).unwrap();
        assert_eq!(ManifestListPage::from_payload(&first.encode_to_vec()).unwrap(), first);
        assert!(!catalog.apply(&outcome).unwrap());
        assert_eq!(catalog.list_page(&request).unwrap().revision, first.revision);
        let restored = ManifestCatalog::from_bytes(&catalog.to_bytes().unwrap()).unwrap();
        assert_eq!(restored.list_page(&request).unwrap(), first);
        let mut after = created.clone();
        after.generation = 2;
        catalog.apply(&ManifestOutcome::decide(Some(created.clone()), after).unwrap()).unwrap();
        assert!(catalog.list_page(&ManifestListRequest {
            expected_revision: first.revision.clone(), ..request.clone()
        }).is_err());
        #[cfg(feature = "metadata-consensus")]
        {
            catalog.restore_entry_arc(created.object_id(), Some(Arc::new(ManifestEntry::new(created))));
            assert_eq!(catalog.list_page(&request).unwrap().revision, first.revision);
        }
        assert!(ManifestListRequest { expected_revision: vec![1], ..request }.validate().is_err());
    }
    #[test]
    fn binary_manifest_roundtrip_checks_layout_and_corruption() {
        let original = manifest(1);
        let bytes = original.to_bytes().unwrap();
        assert_eq!(ObjectManifest::from_bytes(&bytes).unwrap(), original);
        let mut authenticated = original.clone();
        authenticated.format_version = 2;
        authenticated.extents[0].data_sha256 = vec![7; 32];
        assert_eq!(
            ObjectManifest::from_bytes(&authenticated.to_bytes().unwrap()).unwrap(),
            authenticated
        );
        authenticated.extents[0].data_sha256.pop();
        assert!(authenticated.validate().is_err());
        authenticated.extents[0].data_sha256.clear();
        assert!(authenticated.validate().is_err());
        authenticated.format_version = 1;
        authenticated.extents[0].data_sha256 = vec![7; 32];
        assert!(authenticated.validate().is_err());
        for end in 0..bytes.len() {
            assert!(ObjectManifest::from_bytes(&bytes[..end]).is_err());
        }
        for index in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[index] ^= 1;
            assert!(ObjectManifest::from_bytes(&bad).is_err());
        }
        let mut bad = original.clone();
        bad.extents[0].object_offset = 1;
        assert!(bad.validate().is_err());
        bad = original.clone();
        bad.extents[0].page_offset = u64::MAX;
        assert!(bad.validate().is_err());
        bad = original.clone();
        bad.extents[0].write_sequence = 0;
        assert!(bad.validate().is_err());
        bad = original.clone();
        let mut duplicate = bad.extents[0].clone();
        duplicate.object_offset = 4;
        bad.extents.push(duplicate);
        bad.length = 8;
        assert!(bad.validate().is_err());
        bad = original.clone();
        bad.deleted = true;
        assert!(bad.validate().is_err());
        bad = original;
        bad.extents.clear();
        bad.length = 0;
        bad.validate().unwrap();
    }
    #[test]
    fn manifest_publication_fences_stale_writers_and_deleted_generations() {
        let one = manifest(1);
        let two = manifest(2);
        let create = ManifestOutcome::decide(None, one.clone()).unwrap();
        let mut catalog = ManifestCatalog::default();
        assert!(catalog.apply(&create).unwrap());
        assert!(!catalog
            .apply(&ManifestOutcome::from_bytes(&create.to_bytes().unwrap()).unwrap())
            .unwrap());
        let replace = ManifestOutcome::decide(Some(one.clone()), two.clone()).unwrap();
        catalog.apply(&replace).unwrap();
        let saved = catalog.clone();
        assert!(catalog.apply(&create).is_err());
        assert_eq!(catalog, saved);
        let mut deleted = two.clone();
        deleted.generation = 3;
        deleted.deleted = true;
        deleted.length = 0;
        deleted.extents.clear();
        catalog
            .apply(&ManifestOutcome::decide(Some(two.clone()), deleted.clone()).unwrap())
            .unwrap();
        assert!(catalog.apply(&replace).is_err());
        let recreated = manifest(4);
        catalog
            .apply(&ManifestOutcome::decide(Some(deleted), recreated.clone()).unwrap())
            .unwrap();
        assert_eq!(catalog.get_manifest(&one.object_id()), Some(recreated.clone()));
        assert!(ManifestOutcome::decide(None, two).is_err());
        let mut wrong = manifest(5);
        wrong.key = "other".into();
        assert!(ManifestOutcome::decide(Some(recreated), wrong).is_err());
    }
    #[test]
    fn manifest_catalog_snapshot_and_outcome_suffix_restore_identical_state() {
        let mut catalog = ManifestCatalog::default();
        let one = manifest(1);
        catalog
            .apply(&ManifestOutcome::decide(None, one.clone()).unwrap())
            .unwrap();
        let snapshot = catalog.to_bytes().unwrap();
        let next = ManifestOutcome::decide(Some(one), manifest(2)).unwrap();
        let journal = next.to_bytes().unwrap();
        catalog.apply(&next).unwrap();
        let mut reopened = ManifestCatalog::from_bytes(&snapshot).unwrap();
        reopened
            .apply(&ManifestOutcome::from_bytes(&journal).unwrap())
            .unwrap();
        assert_eq!(reopened, catalog);
        let before = reopened.clone();
        let mut malformed = next;
        malformed.after.as_mut().unwrap().generation = 99;
        assert!(reopened.apply(&malformed).is_err());
        assert_eq!(reopened, before);
        let duplicate = CatalogImage {
            format_version: 1,
            objects: vec![manifest(1), manifest(1)],
            retained: Vec::new(),
            staged: Vec::new(),
            publication_fence: 0,
        };
        assert!(
            ManifestCatalog::from_bytes(&frame(b"MOMC", &duplicate, MAX_SNAPSHOT).unwrap())
                .is_err()
        );
    }
    #[test]
    fn manifest_codec_rejects_unknown_fields_and_enforces_version_and_bounds() {
        let original = manifest(1);
        let mut encoded = original.encode_to_vec();
        encoded.extend_from_slice(&[0xa0, 0x06, 0x01]); // unknown field 100
        let mut bytes = b"MOMF".to_vec();
        bytes.extend(encoded);
        bytes.extend_from_slice(&digest(&bytes).to_le_bytes());
        assert!(ObjectManifest::from_bytes(&bytes).is_err());
        let mut bad = original.clone();
        bad.format_version = 2;
        assert!(bad.to_bytes().is_err());
        bad = original.clone();
        bad.extents[0].length = 256 * 1024 * 1024 + 1;
        bad.length = bad.extents[0].length;
        assert!(bad.validate().is_err());
        bad = original.clone();
        bad.extents = vec![bad.extents[0].clone(); 4097];
        assert!(bad.validate().is_err());
        bad = original;
        bad.generation = u64::MAX;
        assert!(ManifestOutcome::decide(Some(bad.clone()), bad).is_err());
        assert!(ManifestCatalog::from_bytes(&vec![0; MAX_SNAPSHOT + 13]).is_err());
    }
    #[test]
    fn manifest_publication_keeps_the_catalog_checkpointable_at_capacity() {
        let mut catalog = ManifestCatalog::default();
        let padding = "x".repeat(4000);
        let mut refused = false;
        for i in 0..20_000 {
            let mut value = manifest(1);
            value.key = format!("{i:05}-{padding}");
            value.length = 0;
            value.extents.clear();
            let outcome = ManifestOutcome::decide(None, value.clone()).unwrap();
            let count = catalog.objects.len();
            let size = catalog.body_bytes;
            if catalog.apply(&outcome).is_err() {
                assert_eq!(catalog.objects.len(), count);
                assert_eq!(catalog.body_bytes, size);
                assert!(catalog.get(&value.object_id()).is_none());
                refused = true;
                break;
            }
        }
        assert!(refused);
        let snapshot = catalog.to_bytes().unwrap();
        assert_eq!(snapshot.len(), catalog.body_bytes + 2 + 12);
        assert!(snapshot.len() <= MAX_SNAPSHOT + 12);
        // Restoring the largest accepted state must remain possible.
        assert_eq!(ManifestCatalog::from_bytes(&snapshot).unwrap(), catalog);
    }
    #[test]
    fn full_manifest_catalog_replays_and_recovers_without_losing_capacity_fences() {
        use crate::placement_outcome::PlacementOutcome;
        use crate::{MatrixMetaService, MetaSnapshot};
        fn value(index: usize, generation: u64, deleted: bool) -> ObjectManifest {
            ObjectManifest {
                format_version: 1,
                bucket: "capacity".into(),
                key: format!("object-{index:06}"),
                generation,
                length: 0,
                extents: vec![],
                deleted,
            }
        }
        fn publish(
            left: &mut MatrixMetaService,
            right: &mut MatrixMetaService,
            outcome: ManifestOutcome,
        ) {
            let decided = PlacementOutcome::publish_manifest(left, outcome, 0).unwrap();
            let bytes = decided.to_payload().unwrap();
            decided.apply(left).unwrap();
            PlacementOutcome::from_payload(&bytes)
                .unwrap()
                .apply(right)
                .unwrap();
        }
        let began = std::time::Instant::now();
        let mut service = MatrixMetaService::new();
        let mut replayed = MatrixMetaService::new();
        for index in 0..MAX_OBJECTS {
            publish(
                &mut service,
                &mut replayed,
                ManifestOutcome::decide(None, value(index, 1, false)).unwrap(),
            );
        }
        assert_eq!(service.object_manifests.len(), MAX_OBJECTS);
        assert_eq!(service.placements.len(), MAX_OBJECTS);
        let extra = value(MAX_OBJECTS, 1, false);
        let overflow = ManifestOutcome::decide(None, extra.clone()).unwrap();
        assert!(PlacementOutcome::publish_manifest(&service, overflow.clone(), 0).is_err());
        // A structurally valid decoded outcome must enforce the same limit during
        // replay, before changing either the catalog or its paired placement.
        let mut unchecked =
            PlacementOutcome::publish_manifest(&MatrixMetaService::new(), overflow.clone(), 0)
                .unwrap();
        unchecked.before_index = service.placement_commit_index;
        unchecked.after_index = unchecked.before_index + 1;
        let unchecked = PlacementOutcome::from_payload(&unchecked.to_payload().unwrap()).unwrap();
        let before = service.export_snapshot();
        assert!(unchecked.apply(&mut service).is_err());
        // Keep failures bounded instead of printing 100,000 complete entries.
        assert!(
            service.export_snapshot() == before,
            "rejected replay changed metadata"
        );
        drop(before);
        assert!(
            service.export_snapshot() == replayed.export_snapshot(),
            "binary replay diverged at capacity"
        );
        assert!(service.object_manifests.get(&extra.object_id()).is_none());
        assert!(!service.placements.contains_key(&extra.object_id()));
        for index in [0, MAX_OBJECTS / 2, MAX_OBJECTS - 1] {
            publish(
                &mut service,
                &mut replayed,
                ManifestOutcome::decide(Some(value(index, 1, false)), value(index, 2, true))
                    .unwrap(),
            );
        }
        assert_eq!(service.object_manifests.len(), MAX_OBJECTS);
        assert!(PlacementOutcome::publish_manifest(&service, overflow.clone(), 0).is_err());
        let snapshot = service.export_snapshot();
        assert!(
            snapshot == replayed.export_snapshot(),
            "tombstone replay diverged at capacity"
        );
        let bytes = snapshot.to_bytes().unwrap();
        let streamed = MetaSnapshot::read_from(&mut bytes.as_slice(), bytes.len() as u64).unwrap();
        assert!(streamed == snapshot, "streamed snapshot changed metadata");
        assert!(
            MetaSnapshot::from_bytes(&bytes).unwrap() == snapshot,
            "decoded snapshot changed metadata"
        );
        let mut restored = MatrixMetaService::new();
        restored.install_placement_roster(&streamed).unwrap();
        restored.install_snapshot(streamed).unwrap();
        assert!(
            restored.export_snapshot() == snapshot,
            "installed snapshot changed metadata"
        );
        assert!(PlacementOutcome::publish_manifest(&restored, overflow, 0).is_err());
        for index in [0, MAX_OBJECTS / 2, MAX_OBJECTS - 1] {
            // Deletion keeps the slot and generation fence even across recovery.
            assert!(PlacementOutcome::publish_manifest(
                &restored,
                ManifestOutcome::decide(None, value(index, 1, false)).unwrap(),
                0
            )
            .is_err());
            publish(
                &mut restored,
                &mut replayed,
                ManifestOutcome::decide(Some(value(index, 2, true)), value(index, 3, false))
                    .unwrap(),
            );
        }
        assert_eq!(restored.object_manifests.len(), MAX_OBJECTS);
        assert_eq!(restored.placements.len(), MAX_OBJECTS);
        assert!(
            restored.export_snapshot() == replayed.export_snapshot(),
            "recreation after recovery diverged"
        );
        println!(
            "MANIFEST_CAPACITY objects={} snapshot_bytes={} elapsed_ms={}",
            MAX_OBJECTS,
            bytes.len(),
            began.elapsed().as_millis()
        );
    }

    #[test]
    fn metadata_manifest_snapshot_preserves_tombstones_and_invalidates_cached_exports() {
        use crate::{MatrixMetaService, MetaSnapshot};
        let mut service = MatrixMetaService::new();
        service.next_commit_index = 2;
        let empty = service.export_snapshot();
        let old = empty.to_bytes().unwrap();
        assert_eq!(&old[8..19], b"MOSRSMETA18");
        assert_eq!(MetaSnapshot::from_bytes(&old).unwrap(), empty);
        let cached = service.export_snapshot_chunk(0, 32).unwrap();
        assert!(!cached.eof);
        let fingerprint = service.metadata_state_fingerprint();
        let create = ManifestOutcome::decide(None, manifest(1)).unwrap();
        service.object_manifests.apply(&create).unwrap();
        assert_ne!(service.metadata_state_fingerprint(), fingerprint);
        let next = service.export_snapshot_chunk(0, 32).unwrap();
        assert_ne!(next.full_checksum, cached.full_checksum);
        let live = service.export_snapshot();
        let live_bytes = live.to_bytes().unwrap();
        assert_eq!(MetaSnapshot::from_bytes(&live_bytes).unwrap(), live);
        assert_eq!(
            MetaSnapshot::read_from(&mut live_bytes.as_slice(), live_bytes.len() as u64).unwrap(),
            live
        );
        let mut stale_checksum = empty;
        stale_checksum.object_manifests = live.object_manifests.clone();
        assert!(stale_checksum.validate().is_err());
        let mut deleted = manifest(2);
        deleted.deleted = true;
        deleted.length = 0;
        deleted.extents.clear();
        service
            .object_manifests
            .apply(&ManifestOutcome::decide(Some(manifest(1)), deleted.clone()).unwrap())
            .unwrap();
        let snapshot = service.export_snapshot();
        let bytes = snapshot.to_bytes().unwrap();
        assert_eq!(&bytes[8..19], b"MOSRSMETA19");
        assert_eq!(MetaSnapshot::from_bytes(&bytes).unwrap(), snapshot);
        assert_eq!(
            MetaSnapshot::read_from(&mut bytes.as_slice(), bytes.len() as u64).unwrap(),
            snapshot
        );
        for cut in [0, 18, bytes.len() / 2, bytes.len() - 1] {
            assert!(MetaSnapshot::from_bytes(&bytes[..cut]).is_err());
            assert!(MetaSnapshot::read_from(&mut &bytes[..cut], bytes.len() as u64).is_err());
        }
        let mut damaged = bytes.clone();
        let offset = damaged.windows(4).position(|w| w == b"MOMC").unwrap();
        let mut oversized = bytes.clone();
        oversized[offset - 8..offset].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(MetaSnapshot::from_bytes(&oversized).is_err());
        assert!(
            MetaSnapshot::read_from(&mut oversized.as_slice(), oversized.len() as u64).is_err()
        );
        damaged[offset + 4] ^= 1;
        assert!(MetaSnapshot::from_bytes(&damaged).is_err());
        assert!(MetaSnapshot::read_from(&mut damaged.as_slice(), damaged.len() as u64).is_err());
        let mut restored = MatrixMetaService::new();
        restored.install_snapshot(snapshot).unwrap();
        assert_eq!(restored.object_manifests, service.object_manifests);
        assert!(restored.object_manifests.apply(&create).is_err());
        restored
            .object_manifests
            .apply(&ManifestOutcome::decide(Some(deleted), manifest(3)).unwrap())
            .unwrap();
        assert_eq!(
            restored.object_manifests.get_manifest(&manifest(3).object_id()),
            Some(manifest(3).clone())
        );
    }
}
