#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectDeleteRequest {
    pub bucket: String,
    pub key: String,
    pub if_match_etag: Option<String>,
    pub version: Option<u64>,
}

impl ObjectDeleteRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        if let Some(if_match_etag) = &self.if_match_etag {
            fields.insert(
                format!("{field_prefix}if_match_etag"),
                if_match_etag.clone(),
            );
        }
        if let Some(version) = self.version {
            fields.insert(format!("{field_prefix}version"), version.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectDeleteReport {
    pub request: ObjectDeleteRequest,
    pub metadata: ObjectMetadata,
    pub deleted_bytes: u64,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectDeleteReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .request
            .to_control_fields(&format!("{field_prefix}request."));
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields.insert(
            format!("{field_prefix}deleted_bytes"),
            self.deleted_bytes.to_string(),
        );
        append_route_fenced_read_control_fields(
            &self.read_proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectDeleteItem {
    pub request: ObjectDeleteRequest,
    pub metadata: Option<ObjectMetadata>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ObjectDeleteItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.metadata.is_some(),
            field_prefix,
        );
        fields.extend(self.request.to_control_fields(field_prefix));
        if let Some(metadata) = &self.metadata {
            fields.insert(
                format!("{field_prefix}version"),
                metadata.version.to_string(),
            );
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}metadata.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectBatchDeleteReport {
    pub items: Vec<ObjectDeleteItem>,
    pub requested_objects: u64,
    pub unique_objects: u64,
    pub duplicate_objects: u64,
    pub deleted_objects: u64,
    pub failed_objects: u64,
    pub deleted_bytes: u64,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub delete_batch_proof_format: String,
    pub delete_batch_proof_checksum: String,
    pub expected_delete_batch_proof_checksum: String,
}

impl ObjectBatchDeleteReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("result_count", self.items.len() as u64),
            ("requested_objects", self.requested_objects),
            ("processed_count", self.unique_objects),
            ("unique_objects", self.unique_objects),
            ("duplicate_objects", self.duplicate_objects),
            ("deleted_count", self.deleted_objects),
            ("deleted_objects", self.deleted_objects),
            ("failed_count", self.failed_objects),
            ("failed_objects", self.failed_objects),
            ("deleted_bytes", self.deleted_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        append_route_fenced_read_control_fields(
            &self.read_proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        append_delete_batch_proof_fields(
            &self.delete_batch_proof_format,
            &self.delete_batch_proof_checksum,
            &self.expected_delete_batch_proof_checksum,
            field_prefix,
            &mut fields,
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}result.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPrefixDeleteReport {
    pub prefix: String,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub listed_objects: u64,
    pub has_more: bool,
    pub batch: ObjectBatchDeleteReport,
    pub read_proof: ObjectReadProof,
}

impl ObjectPrefixDeleteReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        if let Some(marker) = &self.marker {
            fields.insert(format!("{field_prefix}marker"), marker.clone());
        }
        if let Some(next_marker) = &self.next_marker {
            fields.insert(format!("{field_prefix}next_marker"), next_marker.clone());
        }
        fields.insert(
            format!("{field_prefix}listed_objects"),
            self.listed_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.extend(self.read_proof.to_control_fields(field_prefix));
        fields.extend(self.batch.to_control_fields(field_prefix));
        fields
    }
}

fn append_delete_batch_proof_fields(
    delete_batch_proof_format: &str,
    delete_batch_proof_checksum: &str,
    expected_delete_batch_proof_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}delete_batch_proof_format"),
        delete_batch_proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}delete_batch_proof_checksum"),
        delete_batch_proof_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_delete_batch_proof_checksum"),
        expected_delete_batch_proof_checksum.to_string(),
    );
}

