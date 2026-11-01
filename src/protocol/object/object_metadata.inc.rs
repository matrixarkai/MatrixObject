// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectExtent {
    pub chunk_id: ChunkId,
    /// Where in the chunk, in 32 bits.
    ///
    /// # WHY THIRTY-TWO IS ENOUGH, AND WHY THE FIELDS ARE PRIVATE
    ///
    /// Both numbers are bounded by `chunk_size`: the offset is a position inside one chunk,
    /// and `max_extent_bytes` is validated as `1..=chunk_size` when a store is built. So a
    /// chunk below 4 GiB cannot produce either number outside 32 bits, and
    /// `StoreOptions` refuses a `chunk_size` at or above that bound rather than letting an
    /// extent silently wrap. The default chunk is 64 MiB, sixty-four times under it.
    ///
    /// The SNAPSHOT STILL WRITES TWO `u64`, so this is a representation change and not a
    /// migration -- the same shape as the projection and the etag digest before it.
    ///
    /// They are private because a retype alone would be dangerous: `extent.offset +
    /// extent.length` would go on compiling as `u32 + u32` and overflow where it used to be
    /// safe. Reading through `offset()` and `length()` returns `u64`, so every piece of
    /// arithmetic stays in 64 bits and every one of the call sites had to be revisited by
    /// the compiler.
    offset: u32,
    length: u32,
}

impl ObjectExtent {
    /// An extent at `offset` of `length` bytes in `chunk_id`.
    ///
    /// Refused rather than truncated when either number does not fit: a wrapped offset is an
    /// object pointing at the wrong bytes, which is the one failure this must not have.
    pub fn new(chunk_id: ChunkId, offset: u64, length: u64) -> Result<Self, ObjectError> {
        let narrow = |value: u64, what: &str| -> Result<u32, ObjectError> {
            u32::try_from(value).map_err(|_| {
                ObjectError::InvalidArgument(format!(
                    "an extent's {what} is {value}, past what a chunk under 4 GiB can hold"
                ))
            })
        };
        Ok(Self {
            chunk_id,
            offset: narrow(offset, "offset")?,
            length: narrow(length, "length")?,
        })
    }

    /// Where in the chunk this extent starts.
    pub fn offset(&self) -> u64 {
        u64::from(self.offset)
    }

    /// How many bytes it covers.
    pub fn length(&self) -> u64 {
        u64::from(self.length)
    }

    /// One past its last byte, in 64 bits so the sum cannot wrap.
    pub fn end(&self) -> u64 {
        self.offset().saturating_add(self.length())
    }
}

/// An object's extents, with the single one nearly every object has held inline.
///
/// # WHY ONE IS INLINE
///
/// `Box<[ObjectExtent]>` costs sixteen bytes inline AND a heap allocation of sixteen more
/// for the one extent every object under `max_extent_bytes` has. Holding that one inline
/// costs twenty-four inline and nothing on the heap: eight bytes an object, and ONE FEWER
/// ALLOCATION PER OBJECT, which the byte figure does not show and the allocator does.
///
/// It DEREFS TO A SLICE, so every reader that indexed, counted or iterated the old field
/// still does, and `From<Vec<ObjectExtent>>` is how it is built -- which is how the old
/// field was built too, so no construction site moved. The wire is untouched: the snapshot
/// writes a count and then each extent, read off the slice either way.
///
/// # WHY THE MANY ARE SHARED
///
/// `Many` holds an `Arc<[ObjectExtent]>` and not a `Box<[ObjectExtent]>`, so **cloning
/// this is an atomic increment rather than a copy of the list**. That matters because
/// `ObjectMetadata` is cloned on paths that only READ it: `head_object` and
/// `get_object_range` both return it by value, and the append's apply takes a copy of the
/// previous object. An append-only object grows two extents per append, so a clone of its
/// metadata was O(extents) and every one of those paths paid it -- 7.00% of a 6,400-entry
/// shared-store sweep, inclusive, across four separately-inlined copies of
/// `ObjectMetadata::clone`.
///
/// IT COSTS NOTHING INLINE. `One(ObjectExtent)` is sixteen bytes and leaves eight spare,
/// which is where a discriminant goes; `Box<[T]>`, `Arc<[T]>` and `Vec<T>` all leave this
/// enum at twenty-four bytes. The size to compare is the ENUM's, never the variant's --
/// comparing `size_of::<Box<[T]>>()` against `size_of::<Vec<T>>()` says 16 < 24 and
/// answers a question nobody asked, because what an object stores is this enum.
///
/// IT CARRIES NO SPARE CAPACITY, so `an_extent_list_carries_no_spare_capacity` still
/// holds: an `Arc<[T]>` is exact, exactly as a boxed slice is.
///
/// NOTHING MUTATES IT IN PLACE. Every site that changes an object's extents assigns a
/// whole new list (`metadata.extents = new_extents.into()`), so no caller needs
/// `Arc::make_mut` and sharing can never be observed. A site that wants to push one extent
/// onto a live object would have to go through `make_mut` and would pay the copy there,
/// which is the only place it could be correct.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectExtents {
    One(ObjectExtent),
    Many(std::sync::Arc<[ObjectExtent]>),
}

impl ObjectExtents {
    /// Every extent, in order.
    pub fn as_slice(&self) -> &[ObjectExtent] {
        match self {
            Self::One(extent) => std::slice::from_ref(extent),
            Self::Many(extents) => &extents[..],
        }
    }

    /// How many extents sit on the HEAP. The audit needs this and not `len()`: a single
    /// extent is inline and owns nothing, so charging it heap bytes would report a saving
    /// that did not happen.
    pub fn heap_len(&self) -> usize {
        match self {
            Self::One(_) => 0,
            Self::Many(extents) => extents.len(),
        }
    }
}

impl std::ops::Deref for ObjectExtents {
    type Target = [ObjectExtent];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Vec<ObjectExtent>> for ObjectExtents {
    fn from(mut extents: Vec<ObjectExtent>) -> Self {
        if extents.len() == 1 {
            // `pop` rather than `remove(0)`: the same element without the shift.
            Self::One(
                extents
                    .pop()
                    .expect("a vector of length one has an element to pop"),
            )
        } else {
            Self::Many(std::sync::Arc::from(extents))
        }
    }
}

impl From<Box<[ObjectExtent]>> for ObjectExtents {
    fn from(extents: Box<[ObjectExtent]>) -> Self {
        Self::from(extents.into_vec())
    }
}

/// Which storage class an object carries.
///
/// # WHY THIS IS NOT TEXT
///
/// The set is CLOSED: `validate_storage_class` accepts exactly these four names and refuses
/// everything else, so an object's class is always one of four things. Holding it as a
/// `SharedText` spent sixteen bytes -- a fat pointer -- to say which. One byte says the same
/// thing, and it lands in padding both structures already had:
///
/// | | was | now |
/// |---|---:|---:|
/// | `ObjectMetadata` | 144 | **128** |
/// | `ObjectProjection` | 88 | **72** |
///
/// THE WIRE IS UNCHANGED. `as_str()` renders the same four names and the snapshot still
/// writes a string, so this is a representation change and not a migration -- the same shape
/// as the etag digest and the extent offset before it.
///
/// `the_storage_class_variants_are_the_valid_names` pins the variants to the validation, so
/// one cannot gain a name the other does not.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StorageClass {
    #[default]
    Standard,
    Archive,
    Glacier,
    DeepArchive,
}

impl StorageClass {
    /// Every class there is, in the order they are declared.
    pub fn all() -> [Self; 4] {
        [Self::Standard, Self::Archive, Self::Glacier, Self::DeepArchive]
    }

    /// The name this class has always had on the wire and in a response.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::Archive => "ARCHIVE",
            Self::Glacier => "GLACIER",
            Self::DeepArchive => "DEEP_ARCHIVE",
        }
    }

    /// The class that name spells.
    ///
    /// Refused rather than defaulted: a name outside the set is one
    /// `validate_storage_class` already rejects, so accepting it here by falling back to
    /// `STANDARD` would silently change an object's class instead of reporting the problem.
    pub fn parse(text: &str) -> Result<Self, ObjectError> {
        Self::all()
            .into_iter()
            .find(|class| class.as_str() == text)
            .ok_or_else(|| {
                ObjectError::InvalidArgument(format!(
                    "storage class must be STANDARD, ARCHIVE, GLACIER, or DEEP_ARCHIVE, \
                      and {text:?} is none of them"
                ))
            })
    }

    /// Whether reading an object of this class is a restore rather than a read.
    pub fn is_cold(&self) -> bool {
        matches!(self, Self::Archive | Self::Glacier | Self::DeepArchive)
    }
}

impl std::fmt::Display for StorageClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PartialEq<str> for StorageClass {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for StorageClass {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for StorageClass {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<StorageClass> for str {
    fn eq(&self, other: &StorageClass) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<StorageClass> for &str {
    fn eq(&self, other: &StorageClass) -> bool {
        *self == other.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    /// The DIGEST, not its spelling. `etag()` formats this same number as
    /// "fnv1a64:" followed by 16 hex digits, which is the only form an etag has
    /// ever taken, so holding the number costs the same 16 inline bytes a
    /// `Box<str>` cost and drops the 24-byte buffer behind it -- 24 bytes an
    /// object, 8.5% of what `objects` costs and 3.6% of the metadata slope.
    ///
    /// # WHY THIS IS NOT AN `Option<u64>`
    ///
    /// It was, and that cost eight bytes an object for one bit. A `u64` uses every
    /// one of its bit patterns, so `Option<u64>` has no niche to put the tag in and
    /// pays a whole extra word. Split, the flag lands in padding this struct already
    /// had beside `deleted` and costs nothing: 152 bytes an object became 144.
    ///
    /// Read it through `etag_digest()`, which gives back the `Option`.
    pub etag_digest_bits: u64,
    /// Whether there is an etag at all.
    ///
    /// ABSENT IS THE EMPTY ETAG, a real state rather than a missing one: a delete
    /// marker and a bucket membership record both carry it, and a digest of 0 spells
    /// "fnv1a64:0000000000000000", which is not "". So this cannot be inferred from
    /// the bits being zero, which is exactly why a sentinel was not used.
    pub has_etag: bool,
    pub content_type: SharedText,
    pub user_metadata: ObjectAttributes,
    pub storage_class: StorageClass,
    pub tags: ObjectAttributes,
    pub lock: ObjectLock,
    pub update_time_ms: u64,
    pub extents: ObjectExtents,
    pub deleted: bool,
}

impl ObjectMetadata {
    /// The etag as it has always been spelled, and as it still goes on the
    /// wire, into proof material and out of the HTTP front door.
    /// The digest if there is one, as the `Option` this used to store.
    ///
    /// See `etag_digest_fields` for the other direction.
    pub fn etag_digest(&self) -> Option<u64> {
        if self.has_etag {
            Some(self.etag_digest_bits)
        } else {
            None
        }
    }

    pub fn etag(&self) -> String {
        match self.etag_digest() {
            None => String::new(),
            Some(digest) => {
                let mut text = String::with_capacity(24);
                append_etag_of_digest(&mut text, digest);
                text
            }
        }
    }

    pub fn metadata_to_flat_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_flat_fields(&self.user_metadata, field_prefix)
    }

    pub fn metadata_to_indexed_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_indexed_fields(&self.user_metadata, field_prefix)
    }

    pub fn tags_to_flat_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_flat_fields(&self.tags, field_prefix)
    }

    pub fn tags_to_indexed_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_indexed_fields(&self.tags, field_prefix)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}bucket"),
            self.object_id.bucket.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.object_id.key.to_string());
        fields.insert(format!("{field_prefix}etag"), self.etag().clone().into());
        fields.insert(
            format!("{field_prefix}content_type"),
            self.content_type.clone().into(),
        );
        fields.insert(format!("{field_prefix}size"), self.length.to_string().into());
        fields.insert(format!("{field_prefix}version"), self.version.to_string().into());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}delete_marker"),
            self.deleted.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}legal_hold"),
            self.lock.legal_hold.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}legal_hold_update_time_ms"),
            self.lock.legal_hold_update_time_ms.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}retention_until_ms"),
            self.lock.retention_until_ms.to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}storage_class"),
            // NO EMPTY CASE ANY MORE: this branch spelled an empty text as "STANDARD",
            // and an empty text is no longer a storage class. The default variant is
            // Standard, so the value it produced is the value it still produces.
            self.storage_class.as_str().to_string().into(),
        );
        fields.insert(
            format!("{field_prefix}restore_until_ms"),
            self.lock.restore_until_ms.to_string().into(),
        );
        fields.extend(self.metadata_to_indexed_fields(&format!("{field_prefix}metadata.")));
        fields.extend(self.tags_to_indexed_fields(&format!("{field_prefix}tag.")));
        fields.insert(
            format!("{field_prefix}extent_count"),
            self.extents.len().to_string().into(),
        );
        for (index, extent) in self.extents.iter().enumerate() {
            let prefix = format!("{field_prefix}extent.{index}.");
            fields.insert(
                format!("{prefix}segment_volume_id"),
                extent.chunk_id.volume_id().to_string().into(),
            );
            fields.insert(
                format!("{prefix}segment_id"),
                extent.chunk_id.id().to_string().into(),
            );
            fields.insert(format!("{prefix}offset"), extent.offset.to_string().into());
            fields.insert(format!("{prefix}length"), extent.length.to_string().into());
        }
        fields
    }
}

/// The two fields an optional digest becomes.
///
/// The inverse of `etag_digest()`, for the construction sites that have an `Option<u64>` in
/// hand -- a decoded etag, a parsed one, a computed one. Absent is `(0, false)`: the bits
/// are not consulted when the flag is clear, and ZERO IS A LEGITIMATE DIGEST, which is why
/// the flag exists rather than a sentinel.
pub(crate) fn etag_digest_fields(digest: Option<u64>) -> (u64, bool) {
    match digest {
        None => (0, false),
        Some(bits) => (bits, true),
    }
}
