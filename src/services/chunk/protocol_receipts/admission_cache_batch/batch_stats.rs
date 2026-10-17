#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkDiscardBatchStats {
    pub input_ranges: u64,
    pub input_bytes: u64,
    pub coalesced_ranges: u64,
    pub coalesced_bytes: u64,
    pub physical_discard_ranges: u64,
    pub physical_discard_bytes: u64,
    pub invalidated_cache_entries: u64,
}

impl ChunkDiscardBatchStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("input_ranges", self.input_ranges),
            ("input_bytes", self.input_bytes),
            ("coalesced_ranges", self.coalesced_ranges),
            ("coalesced_bytes", self.coalesced_bytes),
            ("physical_discard_ranges", self.physical_discard_ranges),
            ("physical_discard_bytes", self.physical_discard_bytes),
            ("invalidated_cache_entries", self.invalidated_cache_entries),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkWriteBatchStats {
    pub input_ranges: u64,
    pub input_bytes: u64,
    pub coalesced_ranges: u64,
    pub coalesced_bytes: u64,
    pub physical_write_count: u64,
    pub physical_write_bytes: u64,
    pub invalidated_cache_entries: u64,
}

impl ChunkWriteBatchStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("input_ranges", self.input_ranges),
            ("input_bytes", self.input_bytes),
            ("coalesced_ranges", self.coalesced_ranges),
            ("coalesced_bytes", self.coalesced_bytes),
            ("physical_write_count", self.physical_write_count),
            ("physical_write_bytes", self.physical_write_bytes),
            ("invalidated_cache_entries", self.invalidated_cache_entries),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkReadBatchStats {
    pub input_ranges: u64,
    pub input_bytes: u64,
    pub coalesced_ranges: u64,
    pub coalesced_bytes: u64,
    pub payload_bytes: u64,
    pub cache_hits: u64,
    pub physical_reads: u64,
    pub budget_limited: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkReadAdmission {
    pub can_accept: bool,
    pub stats: ChunkReadBatchStats,
    pub max_payload_bytes: u64,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub missing_chunk: bool,
    pub range_invalid: bool,
    pub discarded_range_conflict: bool,
    pub partial_status: String,
}

impl ChunkReadBatchStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("input_ranges", self.input_ranges),
            ("input_bytes", self.input_bytes),
            ("coalesced_ranges", self.coalesced_ranges),
            ("coalesced_bytes", self.coalesced_bytes),
            ("payload_bytes", self.payload_bytes),
            ("cache_hits", self.cache_hits),
            ("physical_reads", self.physical_reads),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields
    }
}

impl ChunkReadAdmission {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        for (name, value) in [
            ("max_payload_bytes", self.max_payload_bytes),
            ("min_read_index", self.min_read_index),
            ("min_commit_index", self.min_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("can_accept", self.can_accept),
            ("missing_chunk", self.missing_chunk),
            ("range_invalid", self.range_invalid),
            ("discarded_range_conflict", self.discarded_range_conflict),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}partial_status"),
            self.partial_status.clone(),
        );
        fields
    }
}

