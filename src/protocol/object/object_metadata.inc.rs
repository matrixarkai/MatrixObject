// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectExtent {
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub length: u64,
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
    pub storage_class: SharedText,
    pub tags: ObjectAttributes,
    pub lock: ObjectLock,
    pub update_time_ms: u64,
    pub extents: Box<[ObjectExtent]>,
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
            if self.storage_class.is_empty() {
                "STANDARD".to_string().into()
            } else {
                self.storage_class.clone().into()
            },
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
                extent.chunk_id.volume_id.to_string().into(),
            );
            fields.insert(
                format!("{prefix}segment_id"),
                extent.chunk_id.id.to_string().into(),
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
