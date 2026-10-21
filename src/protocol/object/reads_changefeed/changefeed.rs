// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectChangeRecord {
    pub sequence: u64,
    pub previous_sequence: u64,
    /// One of a closed set of SEVENTEEN operation names -- the doc comment here
    /// said twelve, and the call sites say otherwise: ten reach
    /// `append_object_change` as literals and seven more arrive through the put
    /// paths. `interned_change_operation` holds the list.
    ///
    /// A `SharedText` rather than a `Box<str>`: the set is closed and small, so
    /// every record naming the same operation names it through the same
    /// allocation. As a `Box<str>` each record carried its own copy, which
    /// measured 10,240 bytes across the 1,024 records the feed retains at its
    /// bound -- for seventeen distinct words.
    ///
    /// An enum would be smaller still -- one byte against sixteen -- but the
    /// snapshot stores this as text, so an enum would have to decide what a
    /// decode does with a name it does not know. That is a compatibility
    /// decision rather than a representation one, and it is in the issue
    /// rather than here. Interning needs no such decision: an unknown name
    /// still works, it just gets its own allocation.
    pub operation: SharedText,
    pub object_id: ObjectId,
    pub version: u64,
    pub length: u64,
    /// The DIGEST, not its spelling. `etag` formats this same number as
    /// "fnv1a64:" followed by 16 hex digits, so holding the number costs the
    /// same 16 inline bytes a `Box<str>` cost and drops the 24-byte buffer
    /// behind it -- 24,576 bytes across the 1,024 records the feed retains at
    /// its bound, at no per-object cost. `store/core.rs` already holds payload
    /// digests this way.
    ///
    /// `None` is the EMPTY etag, which is a real state rather than a missing
    /// one: a delete marker and a bucket membership record both carry it, and
    /// a delete marker is exactly what a change record records on a delete.
    /// A digest of 0 formats as "fnv1a64:0000000000000000", which is not "",
    /// so the two cannot share a representation.
    /// The digest and whether there is one, for the reason `ObjectMetadata` gives.
    ///
    /// This one is charged at the feed's CAPACITY rather than its length -- the feed is
    /// bounded at 1,024 records and the audit counts the slots -- so the eight bytes come
    /// off the store's FIXED term, 1,024 times, rather than off its per-object slope.
    pub etag_digest_bits: u64,
    pub has_etag: bool,
    pub deleted: bool,
    pub event_time_ms: u64,
}

impl ObjectChangeRecord {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}sequence"), self.sequence.to_string());
        fields.insert(
            format!("{field_prefix}previous_sequence"),
            self.previous_sequence.to_string(),
        );
        fields.insert(format!("{field_prefix}record_id"), String::new());
        fields.insert(format!("{field_prefix}record_crc64"), String::new());
        fields.insert(
            format!("{field_prefix}operation"),
            self.operation.to_string(),
        );
        fields.insert(
            format!("{field_prefix}bucket"),
            self.object_id.bucket.to_string(),
        );
        fields.insert(format!("{field_prefix}key"), self.object_id.key.to_string());
        fields.insert(format!("{field_prefix}version"), self.version.to_string());
        fields.insert(
            format!("{field_prefix}metadata_commit_index"),
            self.sequence.to_string(),
        );
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(format!("{field_prefix}etag"), self.etag());
        fields.insert(format!("{field_prefix}deleted"), self.deleted.to_string());
        fields.insert(
            format!("{field_prefix}event_time_ms"),
            self.event_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}json"), String::new());
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectChangeCursor {
    pub consumer_id: String,
    pub sequence: u64,
    pub update_time_ms: u64,
}

impl ObjectChangeCursor {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}consumer_id"),
            self.consumer_id.clone(),
        );
        fields.insert(format!("{field_prefix}sequence"), self.sequence.to_string());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectChangePage {
    pub records: Vec<ObjectChangeRecord>,
    pub next_sequence: u64,
    pub retained_floor_sequence: u64,
    pub cursor_compacted: bool,
    pub resync_required: bool,
    pub resync_from_sequence: u64,
    pub truncated: bool,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub budget_limited: bool,
    pub read_proof: ObjectReadProof,
    pub object_change_page_proof_format: String,
    pub object_change_page_proof_checksum: String,
    pub expected_object_change_page_proof_checksum: String,
}

impl ObjectChangePage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}next_sequence"),
            self.next_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}retained_floor_sequence"),
            self.retained_floor_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}cursor_compacted"),
            self.cursor_compacted.to_string(),
        );
        fields.insert(
            format!("{field_prefix}resync_required"),
            self.resync_required.to_string(),
        );
        fields.insert(
            format!("{field_prefix}resync_from_sequence"),
            self.resync_from_sequence.to_string(),
        );
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_payload_bytes"),
            self.max_payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}record_count"),
            self.records.len().to_string(),
        );
        fields.extend(self.read_proof.to_control_fields(field_prefix));
        fields.insert(
            format!("{field_prefix}object_change_page_proof_format"),
            self.object_change_page_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}object_change_page_proof_checksum"),
            self.object_change_page_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_object_change_page_proof_checksum"),
            self.expected_object_change_page_proof_checksum.clone(),
        );
        for (index, record) in self.records.iter().enumerate() {
            fields.extend(record.to_control_fields(&format!("{field_prefix}record.{index}.")));
        }
        fields
    }
}

/// Every operation name a change record may carry, one allocation each.
///
/// The set is closed by the call sites rather than by a validator, so
/// `the_interned_change_operations_cover_every_operation_recorded` pins this
/// list against what a run actually produces. A name outside the set still
/// works and simply gets its own allocation -- this is only about which
/// allocation the characters live in.
pub(crate) fn interned_change_operation(operation: &str) -> SharedText {
    static OPERATIONS: std::sync::OnceLock<[SharedText; 17]> = std::sync::OnceLock::new();
    OPERATIONS
        .get_or_init(|| {
            [
                SharedText::from("append_object"),
                SharedText::from("batch_put_object"),
                SharedText::from("compact_segment_put"),
                SharedText::from("complete_multipart_put"),
                SharedText::from("copy_object"),
                SharedText::from("delete_object"),
                SharedText::from("delete_object_version"),
                SharedText::from("put_object"),
                SharedText::from("put_object_with_root_placement"),
                SharedText::from("refresh_object_root_placement"),
                SharedText::from("restore_object"),
                SharedText::from("restore_object_version"),
                SharedText::from("set_object_legal_hold"),
                SharedText::from("set_object_retention"),
                SharedText::from("set_object_tags"),
                SharedText::from("transition_object"),
                SharedText::from("transition_object_with_root_placement"),
            ]
        })
        .iter()
        .find(|known| known.as_str() == operation)
        .cloned()
        .unwrap_or_else(|| SharedText::from(operation))
}

impl ObjectChangeRecord {
    /// The digest if there is one, as the `Option` this used to store.
    pub fn etag_digest(&self) -> Option<u64> {
        if self.has_etag {
            Some(self.etag_digest_bits)
        } else {
            None
        }
    }

    /// Replace the digest, from a site that has an `Option<u64>`.
    pub fn set_etag_digest(&mut self, digest: Option<u64>) {
        let (bits, present) = etag_digest_fields(digest);
        self.etag_digest_bits = bits;
        self.has_etag = present;
    }

    /// The etag as it was stored and as it still goes on the wire.
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
}

/// The inverse: the digest behind an etag's spelling.
///
/// An etag is either empty or `etag()`'s output, because those are the only
/// two things that ever produce one. Anything else reaching here came from a
/// snapshot that does not hold what this format says it holds, so it is
/// reported rather than silently reshaped.
pub(crate) fn change_etag_digest(text: &str) -> Result<Option<u64>, ObjectError> {
    const PREFIX: &str = "fnv1a64:";
    if text.is_empty() {
        return Ok(None);
    }
    let Some(hex) = text.strip_prefix(PREFIX) else {
        return Err(ObjectError::Corruption(format!(
            "change feed etag is neither empty nor a {PREFIX} digest: {text}"
        )));
    };
    u64::from_str_radix(hex, 16).map(Some).map_err(|_| {
        ObjectError::Corruption(format!("change feed etag digest is not hex: {text}"))
    })
}
