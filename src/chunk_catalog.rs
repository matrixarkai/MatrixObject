// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The catalog of chunk manifests: one [`ChunkManifest`] per object, published by generation.
//!
//! This is the state a metadata voter keeps for the chunk-granular manifests, and the rules its
//! publications follow, as a library: nothing here knows about the metadata service, the raft log or
//! the snapshot. A publication is a [`ChunkOutcome`], the complete expected prior manifest and the
//! complete new one, decided by the client and re-checked by every voter:
//!
//! * the new generation is the prior one plus one, or one for an object the catalog has never held
//!   (a deleted object stays in the catalog as a tombstone, so a create over it takes the next
//!   generation and a stale writer of an older generation is refused);
//! * the stored manifest must equal the outcome's `before`: a publication decided against anything
//!   else is refused, so two writers of one generation cannot both win;
//! * an outcome whose `after` is already what the catalog holds changes nothing (a replay);
//! * a NEW object is admitted only below a limit that the leader alone applies when it decides, and
//!   never when a voter applies a committed outcome (a voter with a lower limit would otherwise
//!   refuse what the group had committed and diverge);
//! * the catalog image, which is what a snapshot carries, stays within a fixed bound.
use crate::chunk_manifest::ChunkManifest;
use crate::object_manifest::{catalog_full, catalog_image_too_large, frame, unframe, MAX_OBJECTS};
use crate::{ObjectError, ObjectId};
use prost::Message;
use std::sync::Arc;

/// The most objects a catalog may hold: the same ceiling as the page-mode catalog.
pub const MAX_CHUNK_OBJECTS: usize = MAX_OBJECTS;
/// The most bytes a catalog image may take.
const MAX_IMAGE: usize = 64 * 1024 * 1024;
/// Room kept for the image's own header when the bound is checked.
const IMAGE_HEADER_ALLOWANCE: usize = 4096;
/// The largest framed outcome: two manifests of at most 1 MiB each and a little more.
const MAX_OUTCOME: usize = 2 * 1024 * 1024 + 256;
const OUTCOME_FORMAT: u32 = 1;
const IMAGE_FORMAT: u32 = 1;

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}

fn entry_size(body_bytes: usize) -> usize {
    if body_bytes == 0 { return 0; }
    1 + prost::encoding::encoded_len_varint(body_bytes as u64) + body_bytes
}

/// A publication: the manifest the writer saw and the manifest it publishes.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkOutcome {
    #[prost(uint32, tag = "1")]
    pub format_version: u32,
    /// The manifest this was decided against: absent for an object the catalog never held.
    #[prost(message, optional, tag = "2")]
    pub before: Option<ChunkManifest>,
    #[prost(message, optional, tag = "3")]
    pub after: Option<ChunkManifest>,
}

impl ChunkOutcome {
    /// Decide a publication of `after` against `before`. The object, the generation and both
    /// manifests are checked, so an outcome that exists is one a catalog could apply.
    pub fn decide(before: Option<ChunkManifest>, after: ChunkManifest) -> Result<Self, ObjectError> {
        let outcome = Self { format_version: OUTCOME_FORMAT, before, after: Some(after) };
        outcome.validate()?;
        Ok(outcome)
    }

    pub fn after(&self) -> Result<&ChunkManifest, ObjectError> {
        self.after.as_ref().ok_or_else(|| invalid("a chunk outcome has no new manifest"))
    }

    pub fn object_id(&self) -> Result<ObjectId, ObjectError> {
        Ok(self.after()?.object_id())
    }

    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.format_version != OUTCOME_FORMAT {
            return Err(invalid("unsupported chunk outcome version"));
        }
        let after = self.after()?;
        after.validate()?;
        let expected = match &self.before {
            Some(before) => {
                before.validate()?;
                if before.object_id() != after.object_id() {
                    return Err(invalid("a chunk outcome changes the object it names"));
                }
                before.generation.checked_add(1).ok_or_else(|| invalid("generation overflow"))?
            }
            None => 1,
        };
        if after.generation != expected {
            return Err(invalid("a chunk outcome does not take the next generation"));
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        self.validate()?;
        frame(b"MOCO", self, MAX_OUTCOME)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let value: Self = unframe(b"MOCO", bytes, MAX_OUTCOME)?;
        value.validate()?;
        Ok(value)
    }
}

/// The image a snapshot carries: every manifest, in object order.
#[derive(Clone, PartialEq, Eq, Message)]
struct ChunkCatalogImage {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(message, repeated, tag = "2")]
    manifests: Vec<ChunkManifest>,
}

/// One manifest per object, with the bytes they take and a revision that moves on every change.
#[derive(Clone, Debug, Default)]
pub struct ChunkCatalog {
    objects: imbl::OrdMap<ObjectId, Arc<ChunkManifest>>,
    body_bytes: usize,
    /// Protobuf field framing plus bodies, maintained without walking the catalog.
    framed_bytes: usize,
    revision: u64,
    /// The image bound; the default unless a test lowers it.
    image_limit: Option<usize>,
}

/// Two catalogs are equal when they hold the same manifests. The revision is bookkeeping (a decoded
/// image restarts it at the number of objects) and the image bound is a test switch, so neither is
/// part of what a snapshot says.
impl PartialEq for ChunkCatalog {
    fn eq(&self, other: &Self) -> bool {
        self.objects == other.objects
    }
}

impl Eq for ChunkCatalog {}

impl ChunkCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// A catalog with a smaller image bound, so a test can reach it.
    #[cfg(test)]
    pub(crate) fn with_image_limit(limit: usize) -> Self {
        Self { image_limit: Some(limit), ..Self::default() }
    }

    fn image_limit(&self) -> usize {
        self.image_limit.unwrap_or(MAX_IMAGE)
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Moves by one on every applied change and on no replay, so two voters that applied the same
    /// outcomes hold the same number.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn body_bytes(&self) -> usize {
        self.body_bytes
    }

    pub fn get(&self, id: &ObjectId) -> Option<&Arc<ChunkManifest>> {
        self.objects.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ObjectId, &Arc<ChunkManifest>)> {
        self.objects.iter()
    }

    /// Whether `outcome` would change the catalog, and why not when it cannot be applied.
    ///
    /// `admission` is the leader's limit on NEW objects, `None` where a voter applies a committed
    /// outcome. `Ok(false)` is a replay: the catalog already holds the new manifest.
    pub fn check(&self, outcome: &ChunkOutcome, admission: Option<usize>) -> Result<bool, ObjectError> {
        outcome.validate()?;
        let after = outcome.after()?;
        let id = after.object_id();
        let stored = self.objects.get(&id);
        if stored.is_some_and(|stored| **stored == *after) {
            return Ok(false);
        }
        if stored.map(|stored| stored.as_ref()) != outcome.before.as_ref() {
            return Err(ObjectError::PreconditionFailed("chunk manifest prior state mismatch".into()));
        }
        if stored.is_none() {
            if self.objects.len() >= MAX_CHUNK_OBJECTS {
                return Err(catalog_full(MAX_CHUNK_OBJECTS));
            }
            if let Some(limit) = admission {
                if self.objects.len() >= limit.min(MAX_CHUNK_OBJECTS) {
                    return Err(catalog_full(limit.min(MAX_CHUNK_OBJECTS)));
                }
            }
        }
        let replaced = stored.map_or(0, |stored| stored.encoded_len());
        let grown = self.body_bytes - replaced + after.encoded_len();
        if grown + IMAGE_HEADER_ALLOWANCE > self.image_limit() {
            return Err(catalog_image_too_large());
        }
        Ok(true)
    }

    /// Apply a committed outcome. Returns whether the catalog changed.
    pub fn apply(&mut self, outcome: &ChunkOutcome) -> Result<bool, ObjectError> {
        if !self.check(outcome, None)? {
            return Ok(false);
        }
        let after = outcome.after()?.clone();
        let id = after.object_id();
        let replaced = self.objects.get(&id).map_or(0, |stored| stored.encoded_len());
        let encoded = after.encoded_len();
        self.body_bytes = self.body_bytes - replaced + encoded;
        self.framed_bytes = self.framed_bytes - entry_size(replaced) + entry_size(encoded);
        self.objects.insert(id, Arc::new(after));
        self.revision += 1;
        Ok(true)
    }

    /// What the catalog holds for `id`, to put back with [`Self::restore_entry`]: a leader applies
    /// each outcome of a batch to the live catalog to prepare the next against it, and undoes
    /// them before anything is committed.
    pub fn entry(&self, id: &ObjectId) -> Option<Arc<ChunkManifest>> {
        self.objects.get(id).cloned()
    }

    /// Put `entry` back for `id` (or remove it), with the byte count adjusted and the revision
    /// set to the value `revision()` had when the entry was captured.
    pub fn restore_entry(&mut self, id: &ObjectId, entry: Option<Arc<ChunkManifest>>, revision: u64) {
        let current = self.objects.get(id).map_or(0, |stored| stored.encoded_len());
        let restored = entry.as_ref().map_or(0, |stored| stored.encoded_len());
        self.body_bytes = self.body_bytes - current + restored;
        self.framed_bytes = self.framed_bytes - entry_size(current) + entry_size(restored);
        match entry {
            Some(entry) => {
                self.objects.insert(id.clone(), entry);
            }
            None => {
                self.objects.remove(id);
            }
        }
        self.revision = revision;
    }

    /// One page of the catalog, in the shape the object listing already answers with: the objects of
    /// one bucket after a key, or (cursor version 3) of every bucket after a bucket and key. The
    /// revision stands in for a listing that must not straddle a change.
    pub fn list_page(&self, request: &crate::object_manifest::ManifestListRequest) -> Result<crate::object_manifest::ManifestListPage, ObjectError> {
        use crate::object_manifest::{ManifestListPage, ManifestSummary};
        request.validate()?;
        let revision = u128::from(self.revision).to_be_bytes();
        if request.version >= 2 && !request.expected_revision.is_empty() && request.expected_revision != revision {
            return Err(ObjectError::PreconditionFailed("chunk catalog changed during listing".into()));
        }
        let start = ObjectId::new(
            if request.version == 3 { request.after_bucket.clone() } else { request.bucket.clone() },
            request.after.clone(),
        );
        let mut values = self
            .objects
            .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
            .take_while(|(id, _)| request.version == 3 || id.bucket.as_str() == request.bucket);
        let entries = values
            .by_ref()
            .take(request.limit as usize)
            .map(|(_, manifest)| ManifestSummary {
                key: manifest.key.clone(),
                generation: manifest.generation,
                length: manifest.length,
                deleted: manifest.deleted,
                bucket: if request.version == 3 { manifest.bucket.clone() } else { String::new() },
            })
            .collect();
        Ok(ManifestListPage {
            version: request.version,
            bucket: request.bucket.clone(),
            entries,
            has_more: values.next().is_some(),
            revision: if request.version >= 2 { revision.to_vec() } else { Vec::new() },
        })
    }

    /// Exact frame size, without traversing manifests or allocating a catalog image.
    pub(crate) fn encoded_size(&self) -> Result<usize, ObjectError> {
        let body_len = self.framed_bytes.checked_add(2)
            .ok_or_else(|| invalid("manifest encoding exceeds size bound"))?;
        if body_len > self.image_limit() {
            return Err(invalid("manifest encoding exceeds size bound"));
        }
        body_len.checked_add(12).ok_or_else(|| invalid("manifest encoding exceeds size bound"))
    }

    /// Emit canonical image bytes using scratch bounded by the largest manifest.
    /// Callers check `encoded_size` before emitting; callback errors stop output immediately.
    pub(crate) fn visit_encoded<E>(
        &self,
        emit: &mut impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut checksum = 0xcbf29ce484222325u64;
        let mut body = |bytes: &[u8]| {
            checksum = bytes.iter().fold(checksum, |h, byte| {
                (h ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            });
            emit(bytes)
        };
        body(b"MOCC")?;
        body(&[8, IMAGE_FORMAT as u8])?;
        let mut scratch = Vec::new();
        for manifest in self.objects.values() {
            scratch.clear();
            scratch.reserve(prost::encoding::message::encoded_len(2, manifest.as_ref()));
            prost::encoding::message::encode(2, manifest.as_ref(), &mut scratch);
            body(&scratch)?;
        }
        drop(body);
        emit(&checksum.to_le_bytes())
    }

    /// The image a snapshot carries.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        let size = self.encoded_size()?;
        let header = ChunkCatalogImage { format_version: IMAGE_FORMAT, manifests: Vec::new() };
        // Encode immutable entries directly. A checkpoint needs one output buffer,
        // without cloning every stored extent and digest into an intermediate image.
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(b"MOCC");
        header.encode_raw(&mut bytes);
        for manifest in self.objects.values() {
            prost::encoding::message::encode(2, manifest.as_ref(), &mut bytes);
        }
        debug_assert_eq!(bytes.len(), size - 8);
        bytes.extend_from_slice(&crate::fnv1a64(&bytes).to_le_bytes());
        Ok(bytes)
    }

    /// Rebuild a catalog from an image, checking everything a voter relies on: the version, every
    /// manifest, the order and uniqueness of the objects, their number and the image bound. The
    /// byte count is recomputed and the revision starts at the number of objects.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        Self::from_bytes_with_limit(bytes, None)
    }

    fn from_bytes_with_limit(bytes: &[u8], limit: Option<usize>) -> Result<Self, ObjectError> {
        let bound = limit.unwrap_or(MAX_IMAGE);
        let image: ChunkCatalogImage = unframe(b"MOCC", bytes, bound)?;
        if image.format_version != IMAGE_FORMAT {
            return Err(invalid("unsupported chunk catalog image version"));
        }
        if image.manifests.len() > MAX_CHUNK_OBJECTS {
            return Err(invalid("a chunk catalog image holds more objects than a catalog may"));
        }
        let mut catalog = Self { image_limit: limit, ..Self::default() };
        let mut previous: Option<ObjectId> = None;
        for manifest in image.manifests {
            manifest.validate()?;
            let id = manifest.object_id();
            if previous.as_ref().is_some_and(|previous| *previous >= id) {
                return Err(invalid("chunk catalog objects are not in order"));
            }
            let encoded = manifest.encoded_len();
            catalog.body_bytes += encoded;
            catalog.framed_bytes += entry_size(encoded);
            previous = Some(id.clone());
            catalog.objects.insert(id, Arc::new(manifest));
        }
        catalog.revision = catalog.objects.len() as u64;
        if catalog.body_bytes + IMAGE_HEADER_ALLOWANCE > bound {
            return Err(catalog_image_too_large());
        }
        Ok(catalog)
    }

    /// `from_bytes` with a smaller image bound, for a test.
    #[cfg(test)]
    pub(crate) fn from_bytes_with_image_limit(bytes: &[u8], limit: usize) -> Result<Self, ObjectError> {
        Self::from_bytes_with_limit(bytes, Some(limit))
    }
}

#[cfg(test)]
#[path = "chunk_catalog_tests.rs"]
mod tests;
