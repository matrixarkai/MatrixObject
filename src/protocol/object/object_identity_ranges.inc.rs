// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

/// An object's metadata, ordered and identified BY THE IDENTITY IT ALREADY HOLDS, so the
/// collection does not keep a second copy of it.
///
/// # EQUALITY IS THE IDENTITY ALONE, AND THAT IS NOT A PREFERENCE
///
/// `BTreeSet` requires `Ord` and `Eq` to agree: if `a == b` then `a.cmp(&b)` must be `Equal`,
/// or the set's ordering invariant is unspecified rather than merely wrong. The ordering has to
/// be the identity, because **that order is the listing contract** -- it is what `list_objects`
/// serves a prefix and a marker from. So equality is the identity too.
///
/// This type therefore says NOTHING about whether two objects have the same metadata. Ask the
/// metadata, through [`KeyedObject::metadata`]. The field is private so nothing outside the
/// collection can compare two of them by accident.
///
/// # AND FILING USES `replace`, NOT `insert`
///
/// `BTreeSet::insert` KEEPS the element already there and returns `false`; it does not
/// overwrite. Filing a new version of an object with `insert` would leave the old version in
/// place and report success.
#[derive(Clone, Debug)]
pub struct KeyedObject(ObjectMetadata);

impl KeyedObject {
    pub fn new(metadata: ObjectMetadata) -> Self {
        Self(metadata)
    }

    pub fn metadata(&self) -> &ObjectMetadata {
        &self.0
    }

    pub fn into_metadata(self) -> ObjectMetadata {
        self.0
    }

    pub fn id(&self) -> &ObjectId {
        &self.0.object_id
    }
}

impl Ord for KeyedObject {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.object_id.cmp(&other.0.object_id)
    }
}

impl PartialOrd for KeyedObject {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for KeyedObject {}

impl PartialEq for KeyedObject {
    fn eq(&self, other: &Self) -> bool {
        self.0.object_id == other.0.object_id
    }
}

/// So the collection can be searched by an identity without building a metadata value.
impl std::borrow::Borrow<ObjectId> for KeyedObject {
    fn borrow(&self) -> &ObjectId {
        &self.0.object_id
    }
}

impl From<ObjectMetadata> for KeyedObject {
    fn from(metadata: ObjectMetadata) -> Self {
        Self(metadata)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObjectId {
    pub bucket: SharedText,
    pub key: SharedText,
}

impl ObjectId {
    pub fn new(bucket: impl Into<SharedText>, key: impl Into<SharedText>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.to_string());
        fields.insert(format!("{field_prefix}key"), self.key.to_string());
        fields
    }
}

/// Which chunk, and which volume holds it.
///
/// # WHY THIRTY-TWO IS ENOUGH, AND WHY THE FIELDS ARE PRIVATE
///
/// `id` counts the chunks of a store and a chunk is `chunk_size` bytes, 64 MiB by default.
/// Four billion of them is over two hundred exabytes in one store, so a 32-bit count is not
/// the thing that runs out. `volume_id` is `(id - 1) % volume_count`, so it is bounded by
/// `volume_count`, which a store refuses above `u32::MAX` rather than letting a chunk id
/// silently wrap.
///
/// THE WIRE AND THE ORDER ARE BOTH UNCHANGED, which is what makes this a representation
/// change and not a migration. `append_chunk_id` still writes two `u64`, and the derived
/// `Ord` is still lexicographic on `(volume_id, id)` -- a 32-bit comparison orders the same
/// values the same way a 64-bit one does. Every `BTreeMap` keyed by a chunk or a page
/// therefore iterates in the order it iterated before, so the snapshot material built from
/// those walks and the checksums over it are byte for byte what they were. Same shape as
/// the extent offset and the storage class before it.
///
/// The fields are private for the reason the extent's are: a retype alone would leave
/// `chunk_id.id + 1` compiling as `u32 + u32` and wrapping where it used to be safe.
/// `volume_id()` and `id()` return `u64`, so arithmetic stays in 64 bits and every call
/// site had to be revisited by the compiler.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ChunkId {
    volume_id: u32,
    id: u32,
}

impl ChunkId {
    /// The chunk `id` in `volume_id`.
    ///
    /// Refused rather than truncated when either number does not fit: a wrapped chunk id
    /// names a different chunk, which would hand back another object's bytes.
    pub fn new(volume_id: u64, id: u64) -> Result<Self, ObjectError> {
        let narrow = |value: u64, what: &str| -> Result<u32, ObjectError> {
            u32::try_from(value).map_err(|_| {
                ObjectError::InvalidArgument(format!(
                    "a chunk's {what} is {value}, past what thirty-two bits can hold"
                ))
            })
        };
        Ok(Self {
            volume_id: narrow(volume_id, "volume id")?,
            id: narrow(id, "id")?,
        })
    }

    /// Which volume holds the chunk.
    pub fn volume_id(&self) -> u64 {
        u64::from(self.volume_id)
    }

    /// Which chunk, counting from one.
    pub fn id(&self) -> u64 {
        u64::from(self.id)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.volume_id().to_string(),
        );
        fields.insert(format!("{field_prefix}segment_id"), self.id().to_string());
        fields
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataRange {
    pub offset: u64,
    pub length: u64,
}

impl DataRange {
    pub fn end(&self) -> Result<u64, ObjectError> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| ObjectError::InvalidArgument("range end overflows u64".to_string()))
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}offset"), self.offset.to_string());
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields
    }
}

pub(crate) fn validate_pipeline_range(
    operation: &str,
    offset: u64,
    length: u64,
) -> Result<u64, ObjectError> {
    if offset > PIPELINE_OFFSET_MAX {
        return Err(ObjectError::InvalidArgument(format!(
            "{operation} offset exceeds pipeline offset limit"
        )));
    }
    let end = offset.checked_add(length).ok_or_else(|| {
        ObjectError::InvalidArgument(format!("{operation} range offset plus length overflows"))
    })?;
    if length > 0 && end - 1 > PIPELINE_OFFSET_MAX {
        return Err(ObjectError::InvalidArgument(format!(
            "{operation} range end exceeds pipeline offset limit"
        )));
    }
    Ok(end)
}

pub(crate) fn validate_pipeline_data_range(
    operation: &str,
    range: &DataRange,
) -> Result<u64, ObjectError> {
    validate_pipeline_range(operation, range.offset, range.length)
}

