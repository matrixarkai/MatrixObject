// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkInfo {
    pub chunk_id: ChunkId,
    pub logical_bytes: u64,
    pub live_bytes: u64,
    pub stale_bytes: u64,
    pub discarded_bytes: u64,
    pub next_offset: u64,
    pub stale_ranges: Vec<DataRange>,
    pub discarded_ranges: Vec<DataRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PageId {
    pub chunk_id: ChunkId,
    pub page_index: u64,
}

impl PageId {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}segment."));
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.chunk_id.volume_id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_id"),
            self.chunk_id.id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_index"),
            self.page_index.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageState {
    pub logical_length: u64,
    pub write_sequence: u64,
    /// The DIGEST of this page's bytes, not its spelling. `checksum()` formats
    /// this same number as "fnv1a64:" followed by 16 hex digits, which is the
    /// only form it has ever had -- `etag(&chunk.data)` produces it and
    /// `etag(&[])` produces the empty page's.
    ///
    /// A `String` cost 24 bytes inline and a 24-byte buffer behind it, per page
    /// PER REPLICA, which is the first footprint term here that scales with
    /// data rather than object count: 1 TiB at a 1 MiB page and three replicas
    /// is a million pages, so 40 bytes each is about 125 MB.
    pub checksum_fnv1a64: u64,
    pub data: std::sync::Arc<Vec<u8>>,
    /// The redundancy computed from this page's own bytes, kept on the same
    /// replica, and EMPTY whenever the page has peers -- which is every page in
    /// a three-replica store.
    ///
    /// `None` rather than an empty `Arc<Vec<u8>>`, because that handle
    /// allocates whether or not the Vec holds anything: two counts and the
    /// Vec's own three words, 40 bytes, per page per replica. `Arc` is non-null
    /// so the `Option` costs nothing inline.
    pub ec_parity_fragment: Option<std::sync::Arc<Vec<u8>>>,
    pub discarded_ranges: Vec<DataRange>,
}

impl PageState {
    /// This page's redundancy fragment, or nothing when it has peers and needs
    /// none. Borrowed, so a reader that only looks allocates nothing.
    pub fn fragment_bytes(&self) -> &[u8] {
        match &self.ec_parity_fragment {
            None => &[],
            Some(fragment) => fragment.as_slice(),
        }
    }

    /// The checksum as it has always been spelled, and as it still goes into
    /// proof material and onto the wire.
    pub fn checksum(&self) -> String {
        let mut text = String::with_capacity(24);
        append_etag_of_digest(&mut text, self.checksum_fnv1a64);
        text
    }

    /// A chunk that exists and holds nothing.
    ///
    /// Both write paths built this same literal inline. Moving the state behind
    /// a shared handle made that construction a second thing to keep in step,
    /// so it is one thing now.
    pub fn empty() -> Self {
        Self {
            logical_length: 0,
            write_sequence: 0,
            checksum_fnv1a64: fnv1a64(&[]),
            data: std::sync::Arc::new(Vec::new()),
            ec_parity_fragment: None,
            discarded_ranges: Vec::new(),
        }
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        let ec_data_fragments = chunk_ec_data_fragments(&self.data);
        let ec_parity_fragment = self.fragment_bytes();
        for (name, value) in [
            ("logical_length", self.logical_length),
            ("write_sequence", self.write_sequence),
            ("payload_bytes", self.data.len() as u64),
            ("ec_data_shard_count", ec_data_fragments.len() as u64),
            ("ec_parity_bytes", ec_parity_fragment.len() as u64),
            ("discard_range_count", self.discarded_ranges.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}checksum"), self.checksum());
        fields.insert(format!("{field_prefix}payload_checksum"), etag(&self.data));
        for (index, shard) in ec_data_fragments.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}ec_data_shard.{index}.bytes"),
                shard.len().to_string(),
            );
            fields.insert(
                format!("{field_prefix}ec_data_shard.{index}.checksum"),
                etag(shard),
            );
        }
        if !ec_parity_fragment.is_empty() {
            fields.insert(
                format!("{field_prefix}ec_parity_checksum"),
                etag(ec_parity_fragment),
            );
        }
        for (index, range) in self.discarded_ranges.iter().enumerate() {
            fields
                .extend(range.to_control_fields(&format!("{field_prefix}discard_range.{index}.")));
        }
        fields
    }
}

/// The digest behind a page checksum's spelling.
///
/// Unlike an object's etag there is no empty case: every page has a checksum,
/// and an empty page's is `etag(&[])`. So an empty one came from a snapshot
/// that does not hold what this format says it holds.
pub(crate) fn page_checksum_digest(text: &str) -> Result<u64, ObjectError> {
    change_etag_digest(text)?.ok_or_else(|| {
        ObjectError::Corruption("a page checksum is a digest and cannot be empty".to_string())
    })
}
