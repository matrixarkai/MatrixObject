// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn root_node_usage(
    node: &RootNode,
    min_free_bytes: u64,
    min_reclaimable_bytes: u64,
    registered: bool,
    object_count: u64,
    referenced_bytes: u64,
) -> RootNodeUsage {
    let free_bytes = node.capacity_bytes.saturating_sub(node.committed_bytes);
    let reported_free_bytes = MatrixMetaService::node_effective_free_bytes(node);
    let committed_vs_reported_delta_bytes = node.committed_bytes.abs_diff(node.used_bytes);
    let reclaimable_bytes = node.used_bytes.saturating_sub(node.committed_bytes);
    let overcommitted_bytes = node.committed_bytes.saturating_sub(node.used_bytes);
    let data_service = MatrixMetaService::is_data_service_node(node);
    let chunk_reclaim_pressure = node.chunk_health.reclaimable_bytes > 0
        || node.chunk_health.trailing_trim_bytes > 0
        || node.chunk_health.discarded_range_count > 0
        || node.chunk_health.fragmented_chunk_count > 0
        || node.chunk_health.fragmented_discarded_bytes > 0;
    let placement_eligible = MatrixMetaService::node_accepts_new_placement(node)
        && node.committed_bytes <= MatrixMetaService::node_effective_capacity_bytes(node)
        && (min_free_bytes == 0 || reported_free_bytes >= min_free_bytes);
    let effective_reclaimable_bytes = reclaimable_bytes.max(node.chunk_health.reclaimable_bytes);
    let reclaim_pressure = data_service
        && (chunk_reclaim_pressure || effective_reclaimable_bytes > 0)
        && (min_reclaimable_bytes == 0 || effective_reclaimable_bytes >= min_reclaimable_bytes);
    RootNodeUsage {
        node: node.clone(),
        registered,
        object_count,
        referenced_bytes,
        free_bytes,
        reported_free_bytes,
        committed_vs_reported_delta_bytes,
        reclaimable_bytes: effective_reclaimable_bytes,
        overcommitted_bytes,
        capacity_pressure: min_free_bytes > 0 && reported_free_bytes < min_free_bytes,
        reclaim_pressure,
        placement_eligible,
    }
}

pub(crate) fn unregistered_root_node_usage(
    node_id: &str,
    object_count: u64,
    referenced_bytes: u64,
) -> RootNodeUsage {
    RootNodeUsage {
        node: RootNode {
            node_id: node_id.to_string(),
            endpoint: String::new(),
            service_kind: "unregistered".to_string(),
            failure_domain: String::new(),
            metadata_eligible: false,
            data_eligible: false,
            gateway_eligible: false,
            metadata_value_bytes: 0,
            capacity_bytes: 0,
            used_bytes: 0,
            committed_bytes: referenced_bytes,
            draining: true,
            online: false,
            last_heartbeat_ms: 0,
            page_allocator_shard_index: 0,
            page_allocator_shard_count: 0,
            meta_health: RootMetaHealth::default(),
            page_health: RootPageHealth::default(),
            chunk_health: RootChunkHealth::default(),
        },
        registered: false,
        object_count,
        referenced_bytes,
        free_bytes: 0,
        reported_free_bytes: 0,
        committed_vs_reported_delta_bytes: referenced_bytes,
        reclaimable_bytes: 0,
        overcommitted_bytes: referenced_bytes,
        capacity_pressure: referenced_bytes > 0,
        reclaim_pressure: false,
        placement_eligible: false,
    }
}

pub(crate) fn k_max_multipart_part_number() -> u32 {
    10_000
}

pub(crate) fn meta_bucket_key(bucket: &str) -> String {
    format!("bucket/{bucket}")
}

pub(crate) fn meta_lifecycle_apply_key() -> &'static str {
    "lifecycle/apply"
}

pub(crate) fn meta_lifecycle_transition_key() -> &'static str {
    "lifecycle/transition"
}

pub(crate) fn meta_object_key(object_id: &ObjectId) -> String {
    if object_id.key.is_empty() {
        meta_bucket_key(&object_id.bucket)
    } else {
        format!("object/{}/{}", object_id.bucket, object_id.key)
    }
}

pub(crate) fn metadata_projection_value(metadata: &ObjectMetadata) -> String {
    format!(
        "version={} length={} etag={} content_type={} metadata_count={} storage_class={} restore_until_ms={} deleted={} extent_count={} tag_count={} legal_hold={} retention_until_ms={}",
        metadata.version,
        metadata.length,
        metadata.etag(),
        metadata.content_type,
        metadata.user_metadata.len(),
        metadata.storage_class,
        metadata.lock.restore_until_ms,
        metadata.deleted,
        metadata.extents.len(),
        metadata.tags.len(),
        metadata.lock.legal_hold,
        metadata.lock.retention_until_ms
    )
}

pub(crate) fn object_inventory_record_payload_bytes(record: &ObjectInventoryRecord) -> u64 {
    let mut bytes = metadata_projection_value(&record.metadata).len() as u64;
    bytes = bytes
        .saturating_add(record.metadata.object_id.bucket.len() as u64)
        .saturating_add(record.metadata.object_id.key.len() as u64)
        .saturating_add(record.metadata.etag().len() as u64)
        .saturating_add(record.metadata.content_type.len() as u64)
        .saturating_add(record.metadata.storage_class.as_str().len() as u64)
        .saturating_add((record.metadata.extents.len() as u64).saturating_mul(32));
    for (key, value) in &record.metadata.tags {
        bytes = bytes
            .saturating_add(key.len() as u64)
            .saturating_add(value.len() as u64);
    }
    if let Some(placement) = &record.placement {
        bytes = bytes
            .saturating_add(placement.object_id.bucket.len() as u64)
            .saturating_add(placement.object_id.key.len() as u64)
            .saturating_add((placement.node_ids.len() as u64).saturating_mul(16));
        for node_id in &placement.node_ids {
            bytes = bytes.saturating_add(node_id.len() as u64);
        }
    }
    bytes
}

pub(crate) fn retention_active(
    bucket_retention: &BTreeMap<String, u64>,
    metadata: &ObjectMetadata,
    now_ms: u64,
) -> bool {
    if metadata.lock.retention_until_ms > now_ms {
        return true;
    }
    bucket_retention
        .get(metadata.object_id.bucket.as_str())
        .map_or(false, |retain_for_ms| {
            *retain_for_ms > 0 && metadata.update_time_ms.saturating_add(*retain_for_ms) > now_ms
        })
}

/// The twelve values `metadata_projection_value` formats, held as values.
///
/// The projection is the largest single metadata leg at 355 bytes an object,
/// and 227 of that is this text. Every field below is already in
/// `ObjectMetadata`, and four of them are counts of collections it already
/// holds, so the text is a restatement rather than a record of anything.
///
/// Three proofs hash that text -- the snapshot export fingerprint and the
/// record handoff proof through `etag()`, and the snapshot validation checksum
/// verbatim -- so its FORMAT cannot change. Holding the values and formatting
/// on demand produces the same bytes, which is what makes this a
/// representation change rather than a migration.
///
/// It owns NO heap: the etag is its digest, and the content type and storage
/// class are `SharedText` handles on allocations the object already holds and
/// shares across every object carrying the same one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectProjection {
    pub version: u64,
    pub length: u64,
    /// The digest and whether there is one, for the reason `ObjectMetadata` gives:
    /// `Option<u64>` has no niche and costs a whole word for one bit, while this flag
    /// sits in padding beside `deleted` and `legal_hold`. 96 bytes became 88.
    pub etag_digest_bits: u64,
    pub has_etag: bool,
    pub content_type: SharedText,
    pub metadata_count: u32,
    pub storage_class: StorageClass,
    pub deleted: bool,
    pub extent_count: u32,
    pub tag_count: u32,
    /// A restore deadline, a retention deadline and a legal hold -- behind one pointer,
    /// because most objects are under none of the three and paid sixteen bytes to say so.
    ///
    /// `None` and an all-zero box would spell the SAME TEXT, and a derived `PartialEq`
    /// would call them different values. So `from_metadata` never builds the second, and
    /// `a_projection_with_no_holds_is_the_same_value_as_one_with_empty_holds` asserts it.
    holds: Option<Box<ProjectedHolds>>,
}

/// The three deadlines and the hold, for an object that has any of them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProjectedHolds {
    pub restore_until_ms: u64,
    pub retention_until_ms: u64,
    pub legal_hold: bool,
}

impl ObjectProjection {
    pub fn from_metadata(metadata: &ObjectMetadata) -> Self {
        Self {
            version: metadata.version,
            length: metadata.length,
            etag_digest_bits: metadata.etag_digest_bits,
            has_etag: metadata.has_etag,
            content_type: metadata.content_type.clone(),
            metadata_count: metadata.user_metadata.len() as u32,
            storage_class: metadata.storage_class.clone(),
            deleted: metadata.deleted,
            extent_count: metadata.extents.len() as u32,
            tag_count: metadata.tags.len() as u32,
            // ONLY WHEN THERE IS SOMETHING TO HOLD. An all-zero box spells the same text
            // as no box, and a derived `PartialEq` would call the two different values.
            holds: (metadata.lock.restore_until_ms != 0
                || metadata.lock.retention_until_ms != 0
                || metadata.lock.legal_hold)
                .then(|| {
                    Box::new(ProjectedHolds {
                        restore_until_ms: metadata.lock.restore_until_ms,
                        retention_until_ms: metadata.lock.retention_until_ms,
                        legal_hold: metadata.lock.legal_hold,
                    })
                }),
        }
    }

    /// When a restore expires, or zero for an object under none.
    pub fn restore_until_ms(&self) -> u64 {
        self.holds.as_deref().map_or(0, |holds| holds.restore_until_ms)
    }

    /// When a retention expires, or zero for an object under none.
    pub fn retention_until_ms(&self) -> u64 {
        self.holds
            .as_deref()
            .map_or(0, |holds| holds.retention_until_ms)
    }

    /// Whether a legal hold is in force.
    pub fn legal_hold(&self) -> bool {
        self.holds.as_deref().is_some_and(|holds| holds.legal_hold)
    }

    /// The same bytes `metadata_projection_value` produces, which is the whole
    /// requirement: `the_packed_projection_spells_the_same_text` asserts it
    /// against a seeded store rather than against a reading of the format.
    pub fn to_text(&self) -> String {
        let mut etag = String::new();
        if self.has_etag {
            let digest = self.etag_digest_bits;
            append_etag_of_digest(&mut etag, digest);
        }
        format!(
            "version={} length={} etag={} content_type={} metadata_count={} storage_class={} restore_until_ms={} deleted={} extent_count={} tag_count={} legal_hold={} retention_until_ms={}",
            self.version,
            self.length,
            etag,
            self.content_type,
            self.metadata_count,
            self.storage_class,
            self.restore_until_ms(),
            self.deleted,
            self.extent_count,
            self.tag_count,
            self.legal_hold(),
            self.retention_until_ms()
        )
    }
}

/// What a metadata record holds as its value.
///
/// A record at an object key holds a DIGEST of the object's rendered metadata;
/// a record at a bucket, lifecycle or multipart key holds the text itself, and
/// so does any record a metadata transaction wrote directly.
///
/// # WHY AN OBJECT'S VALUE CAN BE A DIGEST AND A BUCKET'S CANNOT
///
/// The characters at an object key are a RENDERING of the object, and the
/// object is the authority: anything that needs them can ask it. The characters
/// at a bucket key are the only copy there is, so a digest would destroy them.
/// That asymmetry is the whole design, and it is why this is an enum rather
/// than one representation.
///
/// Sixteen bytes inline either way. A `Box<str>` is a non-null fat pointer, so
/// the niche absorbs both the discriminant and the digest's own eight bytes --
/// measured, not assumed, by `measure_the_route_to_eighty_bytes_a_metadata_record`,
/// which is also what ruled out a hand-rolled tagged pointer: it is the same
/// sixteen and needs `unsafe` to read back.
#[derive(Clone, Debug)]
pub enum RecordValue {
    Text(Box<str>),
    Digest(u64),
}

/// Equality is on the CHARACTERS, not on the variant.
///
/// # WHY THIS IS NOT DERIVED
///
/// A derived `PartialEq` makes `Projection(p)` and `Text(p.to_text())` different values. They
/// are not: the characters are the whole of what a record means -- they are what goes into
/// every proof, every snapshot and every comparison a caller makes -- so two values that spell
/// the same thing are the same value.
///
/// It was derived at first, and the listing audit immediately flagged a correctly written
/// object as drifted: it compares a record against a freshly rendered projection, which is a
/// `Text`, against a stored `Projection`. That is the representation becoming observable, which
/// is the one thing a representation change must not do. Fixing it at the call site would have
/// left the next comparison to find the same trap.
///
/// The same-variant cases are compared without rendering anything, so the only comparison that
/// builds a string is the mixed one -- which is the one that was silently wrong before.
impl PartialEq for RecordValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RecordValue::Text(left), RecordValue::Text(right)) => left == right,
            (RecordValue::Digest(left), RecordValue::Digest(right)) => left == right,
            // A DIGEST EQUALS THE TEXT IT IS A DIGEST OF. Not the digest's own
            // spelling -- comparing `as_text` to `as_text` would make a stored
            // digest differ from the freshly rendered projection it was taken
            // from, which is the exact drift this impl exists to prevent.
            (RecordValue::Digest(digest), RecordValue::Text(text))
            | (RecordValue::Text(text), RecordValue::Digest(digest)) => {
                *digest == fnv1a64(text.as_bytes())
            }
        }
    }
}

impl Eq for RecordValue {}

impl RecordValue {
    /// The value a delete or a tombstone carries, which is no value.
    ///
    /// Named rather than written as `String::new().into()` at each site: a boxed empty string
    /// allocates nothing, and spelling it out at eight call sites invited one of them to use
    /// `String::with_capacity` or a `format!` instead and quietly allocate.
    pub fn empty() -> Self {
        RecordValue::Text(Box::from(""))
    }

    /// The digest of an object's rendered metadata.
    ///
    /// `ObjectProjection` is still the renderer -- it is what defines the
    /// characters, and `the_packed_projection_spells_the_same_text` is what
    /// keeps that definition honest. It is no longer what gets STORED.
    pub(crate) fn of_object(metadata: &ObjectMetadata) -> Self {
        RecordValue::Digest(Self::digest_of_object(metadata))
    }

    pub(crate) fn digest_of_object(metadata: &ObjectMetadata) -> u64 {
        fnv1a64(ObjectProjection::from_metadata(metadata).to_text().as_bytes())
    }

    /// Which shape the value is, for a durable encoding that must not guess.
    ///
    /// The key cannot be used to tell them apart: `meta_object_key` returns a
    /// BUCKET key for an object whose key is empty, so an object-keyed test
    /// would misread those. The encoding states the shape instead.
    pub(crate) fn kind_tag(&self) -> u64 {
        match self {
            RecordValue::Text(_) => 0,
            RecordValue::Digest(_) => 1,
        }
    }

    /// Rebuild a value from what a snapshot or journal stated.
    pub(crate) fn from_kind_tag(tag: u64, text: &str) -> Result<Self, ObjectError> {
        match tag {
            0 => Ok(RecordValue::Text(text.into())),
            1 => parse_etag_digest(text)
                .map(RecordValue::Digest)
                .ok_or_else(|| {
                    ObjectError::Corruption(format!(
                        "metadata record value is tagged as a digest and spells {text}"
                    ))
                }),
            other => Err(ObjectError::Corruption(format!(
                "metadata record value kind {other} is not a shape this build knows"
            ))),
        }
    }

    /// The text the record has always carried. Borrowed when it is stored and
    /// built when it is derived, so a reader that only needs to look pays
    /// nothing for the ones that are stored.
    pub fn as_text(&self) -> std::borrow::Cow<'_, str> {
        match self {
            RecordValue::Text(text) => std::borrow::Cow::Borrowed(text),
            RecordValue::Digest(digest) => {
                let mut spelled = String::with_capacity(24);
                append_etag_of_digest(&mut spelled, *digest);
                std::borrow::Cow::Owned(spelled)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RecordValue::Text(text) => text.len(),
            RecordValue::Digest(_) => self.as_text().len(),
        }
    }

    /// What the value owns on the heap. A text's is its characters, with no
    /// spare capacity; a digest owns NOTHING -- it is eight bytes inline.
    pub fn heap_bytes(&self) -> u64 {
        match self {
            RecordValue::Text(text) => text.len() as u64,
            RecordValue::Digest(_) => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            RecordValue::Text(text) => text.is_empty(),
            // A digest always spells twenty-four characters, so it is never empty.
            RecordValue::Digest(_) => false,
        }
    }
}

impl From<String> for RecordValue {
    fn from(text: String) -> Self {
        RecordValue::Text(text.into())
    }
}

impl From<&String> for RecordValue {
    fn from(text: &String) -> Self {
        RecordValue::Text(text.as_str().into())
    }
}

impl From<&str> for RecordValue {
    fn from(text: &str) -> Self {
        RecordValue::Text(text.into())
    }
}
