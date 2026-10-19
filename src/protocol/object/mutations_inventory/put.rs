// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPutRequest {
    pub bucket: String,
    pub key: String,
    pub data: Vec<u8>,
    pub content_type: String,
    pub options: PutOptions,
}

impl ObjectPutRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.data.len().to_string(),
        );
        fields.insert(format!("{field_prefix}payload_checksum"), etag(&self.data));
        fields.insert(
            format!("{field_prefix}content_type"),
            self.content_type.clone(),
        );
        fields.extend(
            self.options
                .metadata_to_indexed_fields(&format!("{field_prefix}metadata.")),
        );
        fields.extend(
            self.options
                .tags_to_indexed_fields(&format!("{field_prefix}tag.")),
        );
        if let Some(storage_class) = &self.options.storage_class {
            fields.insert(
                format!("{field_prefix}storage_class"),
                storage_class.clone(),
            );
        }
        fields.insert(
            format!("{field_prefix}if_none_match"),
            self.options.if_none_match.to_string(),
        );
        if let Some(if_match_etag) = &self.options.if_match_etag {
            fields.insert(
                format!("{field_prefix}if_match_etag"),
                if_match_etag.clone(),
            );
        }
        if let Some(expected_checksum) = &self.options.expected_checksum {
            fields.insert(
                format!("{field_prefix}expected_checksum"),
                expected_checksum.clone(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPutItem {
    pub bucket: String,
    pub key: String,
    pub metadata: Option<ObjectMetadata>,
    pub payload_bytes: u64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ObjectPutItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.metadata.is_some(),
            field_prefix,
        );
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        if let Some(metadata) = &self.metadata {
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}info.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectBatchPutReport {
    pub items: Vec<ObjectPutItem>,
    pub requested_objects: u64,
    pub put_objects: u64,
    pub failed_objects: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub put_batch_proof_format: String,
    pub put_batch_proof_checksum: String,
    pub expected_put_batch_proof_checksum: String,
}

impl ObjectBatchPutReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("result_count", self.items.len() as u64),
            ("requested_objects", self.requested_objects),
            ("processed_count", self.put_objects),
            ("put_count", self.put_objects),
            ("put_objects", self.put_objects),
            ("failed_count", self.failed_objects),
            ("failed_objects", self.failed_objects),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
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
        append_put_batch_proof_fields(
            &self.put_batch_proof_format,
            &self.put_batch_proof_checksum,
            &self.expected_put_batch_proof_checksum,
            field_prefix,
            &mut fields,
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}result.{index}.")));
        }
        fields
    }
}

