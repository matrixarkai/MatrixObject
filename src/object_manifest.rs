// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Binary object extents and conditional publication state. Node wiring is separate.
use crate::{ChunkId, ObjectError, ObjectId};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
const MAX_MANIFEST: usize = 1024 * 1024;
const MAX_SNAPSHOT: usize = 64 * 1024 * 1024;
const MAX_OBJECTS: usize = 100_000;

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
impl ObjectManifest {
    pub fn object_id(&self) -> ObjectId {
        ObjectId::new(self.bucket.clone(), self.key.clone())
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        crate::validate_bucket(&self.bucket)?;
        crate::validate_key(&self.key)?;
        if self.format_version != 1
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
        let mut pages = BTreeSet::new();
        for extent in &self.extents {
            ChunkId::new(extent.volume_id, extent.chunk_id)?;
            if extent.group.is_empty()
                || extent.group.len() > 128
                || !extent
                    .group
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                || extent.length == 0
                || extent.write_sequence == 0
                || extent.object_offset != offset
                || extent
                    .page_offset
                    .checked_add(extent.length)
                    .map_or(true, |n| n > 256 * 1024 * 1024)
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
        let value = Self {
            format_version: 1,
            before,
            after: Some(after),
        };
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), ObjectError> {
        if self.format_version != 1 {
            return Err(invalid("manifest outcome version mismatch"));
        }
        let after = self
            .after
            .as_ref()
            .ok_or_else(|| invalid("manifest outcome has no resulting state"))?;
        after.validate()?;
        let generation = if let Some(before) = &self.before {
            before.validate()?;
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
    objects: BTreeMap<ObjectId, ObjectManifest>,
    body_bytes: usize,
}
fn entry_size(value: &ObjectManifest) -> usize {
    let size = value.encoded_len();
    let mut remaining = size;
    let mut delimiter = 1;
    while remaining >= 128 {
        delimiter += 1;
        remaining >>= 7;
    }
    1 + delimiter + size
}
#[derive(Clone, PartialEq, Message)]
struct CatalogImage {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(message, repeated, tag = "2")]
    objects: Vec<ObjectManifest>,
}
impl ManifestCatalog {
    /// Includes tombstones so a recreation must name the deleted generation.
    pub fn get(&self, id: &ObjectId) -> Option<&ObjectManifest> {
        self.objects.get(id)
    }
    /// Compare before touching state. An identical already-applied outcome is a no-op.
    pub fn apply(&mut self, outcome: &ManifestOutcome) -> Result<bool, ObjectError> {
        outcome.validate()?;
        let after = outcome.after.as_ref().unwrap();
        let id = after.object_id();
        let current = self.objects.get(&id);
        if current == Some(after) {
            return Ok(false);
        }
        if current != outcome.before.as_ref() {
            return Err(ObjectError::PreconditionFailed(
                "object manifest changed before publication".into(),
            ));
        }
        if current.is_none() && self.objects.len() >= MAX_OBJECTS {
            return Err(invalid("manifest catalog exceeds object bound"));
        }
        let body_bytes = self.body_bytes - current.map_or(0, entry_size) + entry_size(after);
        if body_bytes > MAX_SNAPSHOT - 2 {
            return Err(invalid(
                "manifest publication exceeds checkpoint size bound",
            ));
        }
        self.objects.insert(id, after.clone());
        self.body_bytes = body_bytes;
        Ok(true)
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, ObjectError> {
        let image = CatalogImage {
            format_version: 1,
            objects: self.objects.values().cloned().collect(),
        };
        frame(b"MOMC", &image, MAX_SNAPSHOT)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ObjectError> {
        let image: CatalogImage = unframe(b"MOMC", bytes, MAX_SNAPSHOT)?;
        if image.format_version != 1 || image.objects.len() > MAX_OBJECTS {
            return Err(invalid("manifest catalog version or count mismatch"));
        }
        let mut result = Self::default();
        let mut previous = None;
        for value in image.objects {
            value.validate()?;
            let id = value.object_id();
            if previous.as_ref().is_some_and(|p| p >= &id) {
                return Err(invalid("manifest catalog is not strictly ordered"));
            }
            previous = Some(id.clone());
            result.body_bytes += entry_size(&value);
            result.objects.insert(id, value);
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
            }],
        }
    }
    #[test]
    fn binary_manifest_roundtrip_checks_layout_and_corruption() {
        let original = manifest(1);
        let bytes = original.to_bytes().unwrap();
        assert_eq!(ObjectManifest::from_bytes(&bytes).unwrap(), original);
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
        assert_eq!(catalog.get(&one.object_id()), Some(&recreated));
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
}
