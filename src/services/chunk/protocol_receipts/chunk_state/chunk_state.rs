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
    /// One digest per data fragment, kept beside the sidecar and under the same policy.
    ///
    /// # WHY THIS IS STORED AND NOT DERIVED
    ///
    /// Everything else about the fragments is derived from the payload wherever it is
    /// needed, because keeping it cost a second copy of every byte. A digest cannot be:
    /// the whole point of it is to be read when the payload is damaged, and a digest
    /// recomputed from damaged bytes agrees with them.
    ///
    /// # WHAT IT BUYS
    ///
    /// Without it the recovery does not know WHICH fragment is damaged, so it assumes each
    /// in turn, rebuilds the whole page on that assumption and tests the page checksum --
    /// `k` rebuilds and `k` page checksums at worst, and no way to tell a damaged payload
    /// from a damaged sidecar. With it the damaged fragment is a lookup, and a page whose
    /// fragments all verify says the sidecar is the damaged part.
    ///
    /// `k` is 2..16, so this is 16 to 128 bytes against a sidecar of `len / k` -- 128 bytes
    /// beside 16 KiB on a 256 KiB page.
    ///
    /// It is NOT in any proof material and NOT on the wire, and both are deliberate.
    /// Neither `chunk_state_handoff_checksum` nor `chunk_local_state_checksum` has ever
    /// hashed the sidecar, so neither hashes this. And a snapshot does not carry it
    /// because a snapshot cannot carry a damaged page -- `to_bytes` validates every one
    /// and refuses a checksum mismatch -- so a table derived at decode is byte for byte
    /// the table that was stored. `a_snapshot_cannot_carry_a_damaged_page` pins that; if
    /// it stops holding, the derivation becomes wrong and this has to start travelling.
    pub ec_fragment_digests: Option<std::sync::Arc<Vec<u64>>>,
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

    /// The per-fragment digests, or nothing when this page keeps no sidecar.
    pub fn fragment_digests(&self) -> &[u64] {
        match &self.ec_fragment_digests {
            None => &[],
            Some(digests) => digests.as_slice(),
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
            ec_fragment_digests: None,
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
