// SPDX-License-Identifier: Apache-2.0
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
        .saturating_add(record.metadata.storage_class.len() as u64)
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
    pub storage_class: SharedText,
    pub restore_until_ms: u64,
    pub deleted: bool,
    pub extent_count: u32,
    pub tag_count: u32,
    pub legal_hold: bool,
    pub retention_until_ms: u64,
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
            restore_until_ms: metadata.lock.restore_until_ms,
            deleted: metadata.deleted,
            extent_count: metadata.extents.len() as u32,
            tag_count: metadata.tags.len() as u32,
            legal_hold: metadata.lock.legal_hold,
            retention_until_ms: metadata.lock.retention_until_ms,
        }
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
            self.restore_until_ms,
            self.deleted,
            self.extent_count,
            self.tag_count,
            self.legal_hold,
            self.retention_until_ms
        )
    }
}

/// What a metadata record holds as its value.
///
/// A record at an object key holds a projection of that object; a record at a
/// bucket, lifecycle or multipart key holds arbitrary text, and so does any
/// record a metadata transaction wrote directly. Both shapes are BOXED, which
/// is what keeps this sixteen bytes inline -- the same as the `Box<str>` it
/// replaces, because a `Box` is non-null and the niche absorbs the
/// discriminant. So a record that still holds text costs exactly what it cost
/// before, and one holding a projection costs 112 bytes instead of 243.
///
/// An inline projection would be 96 bytes inline and look better: 96 against
/// 243 for a packed record. It would also be 323 for a text one, so a store
/// that had just loaded a snapshot would be worse off until every record was
/// rewritten. `measure_what_each_record_value_representation_would_cost`
/// prints all three.
#[derive(Clone, Debug)]
pub enum RecordValue {
    Text(Box<str>),
    Projection(Box<ObjectProjection>),
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
            (RecordValue::Projection(left), RecordValue::Projection(right)) => left == right,
            _ => self.as_text() == other.as_text(),
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

    pub(crate) fn of_object(metadata: &ObjectMetadata) -> Self {
        RecordValue::Projection(Box::new(ObjectProjection::from_metadata(metadata)))
    }

    /// The text the record has always carried. Borrowed when it is stored and
    /// built when it is derived, so a reader that only needs to look pays
    /// nothing for the ones that are stored.
    pub fn as_text(&self) -> std::borrow::Cow<'_, str> {
        match self {
            RecordValue::Text(text) => std::borrow::Cow::Borrowed(text),
            RecordValue::Projection(projection) => {
                std::borrow::Cow::Owned(projection.to_text())
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RecordValue::Text(text) => text.len(),
            RecordValue::Projection(projection) => projection.to_text().len(),
        }
    }

    /// What the value owns on the heap. A projection's box is its own size; a
    /// text's is its characters. Neither has spare capacity.
    pub fn heap_bytes(&self) -> u64 {
        match self {
            RecordValue::Text(text) => text.len() as u64,
            RecordValue::Projection(_) => std::mem::size_of::<ObjectProjection>() as u64,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            RecordValue::Text(text) => text.is_empty(),
            // A projection always formats twelve fields, so it is never empty.
            RecordValue::Projection(_) => false,
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
