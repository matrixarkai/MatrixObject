// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCacheRangeRequest {
    pub bucket: String,
    pub key: String,
    pub offset: u64,
    pub length: u64,
    pub version: Option<u64>,
}

impl ObjectCacheRangeRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(format!("{field_prefix}offset"), self.offset.to_string());
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(
            format!("{field_prefix}version_present"),
            self.version.is_some().to_string(),
        );
        if let Some(version) = self.version {
            fields.insert(format!("{field_prefix}version"), version.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCacheRangeItem {
    pub request: ObjectCacheRangeRequest,
    pub metadata: Option<ObjectMetadata>,
    pub stats: CacheControlStats,
    pub skipped: bool,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ObjectCacheRangeItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.metadata.is_some(),
            field_prefix,
        );
        fields.extend(
            self.request
                .to_control_fields(&format!("{field_prefix}request.")),
        );
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        fields.insert(format!("{field_prefix}skipped"), self.skipped.to_string());
        if let Some(metadata) = &self.metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}object.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectBatchCacheControlReport {
    pub items: Vec<ObjectCacheRangeItem>,
    pub requested_objects: u64,
    pub selected_objects: u64,
    pub skipped_objects: u64,
    pub failed_objects: u64,
    pub requested_bytes: u64,
    pub selected_bytes: u64,
    pub skipped_bytes: u64,
    pub stats: CacheControlStats,
    pub budget_limited: bool,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub cache_batch_proof_format: String,
    pub cache_batch_proof_checksum: String,
    pub expected_cache_batch_proof_checksum: String,
}

impl ObjectBatchCacheControlReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("item_count", self.items.len() as u64),
            ("requested_objects", self.requested_objects),
            ("selected_objects", self.selected_objects),
            ("skipped_objects", self.skipped_objects),
            ("failed_objects", self.failed_objects),
            ("requested_bytes", self.requested_bytes),
            ("selected_bytes", self.selected_bytes),
            ("skipped_bytes", self.skipped_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        fields.extend(
            self.read_proof
                .to_control_fields(&format!("{field_prefix}read_proof.")),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}cache_batch_proof_format"),
            self.cache_batch_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}cache_batch_proof_checksum"),
            self.cache_batch_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_cache_batch_proof_checksum"),
            self.expected_cache_batch_proof_checksum.clone(),
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        fields
    }
}

