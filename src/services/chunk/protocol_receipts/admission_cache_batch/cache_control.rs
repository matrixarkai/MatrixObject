pub(crate) type ReplicaReadCacheKey = (PageId, u64, u64);
pub(crate) type ReplicaReadCacheRangeKey = (u64, u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaReadCacheEntry {
    pub(crate) result: ReplicaReadResult,
    range_checksum: String,
    last_access_sequence: u64,
}

impl ReplicaReadCacheEntry {
    /// The checksum string beside each cached range. Private to this module,
    /// so the footprint audit asks for it rather than reaching in.
    pub(crate) fn range_checksum_capacity(&self) -> u64 {
        self.range_checksum.capacity() as u64
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheControlStats {
    pub requested_ranges: u64,
    pub requested_bytes: u64,
    pub selected_ranges: u64,
    pub selected_bytes: u64,
    pub skipped_bytes: u64,
    pub contacted_chunk_endpoints: u64,
    pub failed_chunk_endpoints: u64,
    pub applied_chunk_ranges: u64,
    pub failed_chunk_ranges: u64,
    pub cache_hits: u64,
    pub physical_reads: u64,
    pub invalidated_entries: u64,
    pub budget_limited: bool,
}

impl CacheControlStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("requested_ranges", self.requested_ranges),
            ("requested_bytes", self.requested_bytes),
            ("selected_ranges", self.selected_ranges),
            ("selected_bytes", self.selected_bytes),
            ("skipped_bytes", self.skipped_bytes),
            ("contacted_chunk_endpoints", self.contacted_chunk_endpoints),
            ("failed_chunk_endpoints", self.failed_chunk_endpoints),
            ("applied_chunk_ranges", self.applied_chunk_ranges),
            ("failed_chunk_ranges", self.failed_chunk_ranges),
            ("cache_hits", self.cache_hits),
            ("physical_reads", self.physical_reads),
            ("invalidated_entries", self.invalidated_entries),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReadCacheRange {
    pub page_id: PageId,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaReadCacheControlReport {
    pub stats: CacheControlStats,
    pub previous_read_cache_bytes: u64,
    pub previous_read_cache_entries: u64,
    pub previous_read_cache_max_bytes: u64,
    pub read_cache_bytes: u64,
    pub read_cache_entries: u64,
    pub read_cache_max_bytes: u64,
    pub cleared_read_cache_bytes: u64,
    pub cleared_read_cache_entries: u64,
    pub remote_chunk_endpoint_count: u64,
    pub configured_chunk_endpoint_count: u64,
    pub cleared_chunk_endpoint_count: u64,
    pub invalidated_chunk_endpoint_count: u64,
    pub selected_chunk_endpoint: Option<String>,
}

impl ReplicaReadCacheControlReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        for (name, value) in [
            ("previous_read_cache_bytes", self.previous_read_cache_bytes),
            (
                "previous_read_cache_entries",
                self.previous_read_cache_entries,
            ),
            (
                "previous_read_cache_max_bytes",
                self.previous_read_cache_max_bytes,
            ),
            ("read_cache_bytes", self.read_cache_bytes),
            ("read_cache_entries", self.read_cache_entries),
            ("read_cache_max_bytes", self.read_cache_max_bytes),
            ("cleared_read_cache_bytes", self.cleared_read_cache_bytes),
            (
                "cleared_read_cache_entries",
                self.cleared_read_cache_entries,
            ),
            (
                "remote_chunk_endpoint_count",
                self.remote_chunk_endpoint_count,
            ),
            (
                "configured_chunk_endpoint_count",
                self.configured_chunk_endpoint_count,
            ),
            (
                "cleared_chunk_endpoint_count",
                self.cleared_chunk_endpoint_count,
            ),
            (
                "invalidated_chunk_endpoint_count",
                self.invalidated_chunk_endpoint_count,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "selected_chunk_endpoint",
            &self.selected_chunk_endpoint,
        );
        fields
    }
}

