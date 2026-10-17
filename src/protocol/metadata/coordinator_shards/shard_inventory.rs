#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardRecordStats {
    pub live_records: u64,
    pub deleted_records: u64,
    pub live_value_bytes: u64,
    pub deleted_value_bytes: u64,
    pub total_payload_bytes: u64,
}

impl MetadataShardRecordStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("live_records", self.live_records),
            ("deleted_records", self.deleted_records),
            ("live_value_bytes", self.live_value_bytes),
            ("deleted_value_bytes", self.deleted_value_bytes),
            ("total_payload_bytes", self.total_payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataShardInventoryItem {
    pub shard: MetadataShard,
    pub record_stats: Option<MetadataShardRecordStats>,
}

impl MetadataShardInventoryItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.shard.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}record_stats_present"),
            self.record_stats.is_some().to_string(),
        );
        if let Some(stats) = &self.record_stats {
            fields.extend(stats.to_control_fields(&format!("{field_prefix}record_stats.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataShardInventoryPage {
    pub items: Vec<MetadataShardInventoryItem>,
    pub owner_node_id: Option<String>,
    pub next_shard_marker: Option<String>,
    pub scanned_shards: u64,
    pub skipped_shards: u64,
    pub payload_bytes: u64,
    pub budget_limited: bool,
    pub has_more: bool,
}

impl MetadataShardInventoryPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "owner_node_id",
            &self.owner_node_id,
        );
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "next_shard_marker",
            &self.next_shard_marker,
        );
        for (name, value) in [
            ("item_count", self.items.len() as u64),
            ("scanned_shards", self.scanned_shards),
            ("skipped_shards", self.skipped_shards),
            ("payload_bytes", self.payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        fields
    }
}
