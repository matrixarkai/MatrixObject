#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlabInfo {
    pub slab_id: SlabId,
    pub logical_bytes: u64,
    pub live_bytes: u64,
    pub stale_bytes: u64,
    pub discarded_bytes: u64,
    pub next_offset: u64,
    pub stale_ranges: Vec<DataRange>,
    pub discarded_ranges: Vec<DataRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ChunkId {
    pub slab_id: SlabId,
    pub chunk_index: u64,
}

impl ChunkId {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .slab_id
            .to_control_fields(&format!("{field_prefix}segment."));
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.slab_id.volume_id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_id"),
            self.slab_id.id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}chunk_index"),
            self.chunk_index.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkState {
    pub logical_length: u64,
    pub write_sequence: u64,
    pub checksum: String,
    pub data: std::sync::Arc<Vec<u8>>,
    pub ec_parity_fragment: std::sync::Arc<Vec<u8>>,
    pub discarded_ranges: Vec<DataRange>,
}

impl ChunkState {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        let ec_data_fragments = chunk_ec_data_fragments(&self.data);
        let ec_parity_fragment = &self.ec_parity_fragment;
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
        fields.insert(format!("{field_prefix}checksum"), self.checksum.clone());
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
                etag(&ec_parity_fragment),
            );
        }
        for (index, range) in self.discarded_ranges.iter().enumerate() {
            fields
                .extend(range.to_control_fields(&format!("{field_prefix}discard_range.{index}.")));
        }
        fields
    }
}
