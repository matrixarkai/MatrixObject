// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCopyRequest {
    pub source_bucket: String,
    pub source_key: String,
    pub source_version: Option<u64>,
    pub destination_bucket: String,
    pub destination_key: String,
    pub content_type: Option<String>,
    pub options: PutOptions,
}

impl ObjectCopyRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}source_bucket"),
            self.source_bucket.clone(),
        );
        fields.insert(format!("{field_prefix}source_key"), self.source_key.clone());
        if let Some(source_version) = self.source_version {
            fields.insert(
                format!("{field_prefix}source_version"),
                source_version.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}destination_bucket"),
            self.destination_bucket.clone(),
        );
        fields.insert(
            format!("{field_prefix}destination_key"),
            self.destination_key.clone(),
        );
        if let Some(content_type) = &self.content_type {
            fields.insert(format!("{field_prefix}content_type"), content_type.clone());
        }
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
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCopyItem {
    pub request: ObjectCopyRequest,
    pub metadata: Option<ObjectMetadata>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ObjectCopyItem {
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
                format!("{field_prefix}copied_bytes"),
                metadata.length.to_string(),
            );
            fields.insert(
                format!("{field_prefix}selected_copy_bytes"),
                metadata.length.to_string(),
            );
            fields.extend(metadata.to_control_fields(&format!("{field_prefix}info.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectBatchCopyReport {
    pub items: Vec<ObjectCopyItem>,
    pub requested_objects: u64,
    pub copied_objects: u64,
    pub failed_objects: u64,
    pub copied_bytes: u64,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub copy_batch_proof_format: String,
    pub copy_batch_proof_checksum: String,
    pub expected_copy_batch_proof_checksum: String,
}

impl ObjectBatchCopyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("result_count", self.items.len() as u64),
            ("requested_objects", self.requested_objects),
            ("processed_count", self.copied_objects),
            ("copied_count", self.copied_objects),
            ("copied_objects", self.copied_objects),
            ("failed_count", self.failed_objects),
            ("failed_objects", self.failed_objects),
            ("copied_bytes", self.copied_bytes),
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
        append_copy_batch_proof_fields(
            &self.copy_batch_proof_format,
            &self.copy_batch_proof_checksum,
            &self.expected_copy_batch_proof_checksum,
            field_prefix,
            &mut fields,
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}result.{index}.")));
        }
        fields
    }
}

fn append_put_batch_proof_fields(
    put_batch_proof_format: &str,
    put_batch_proof_checksum: &str,
    expected_put_batch_proof_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}put_batch_proof_format"),
        put_batch_proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}put_batch_proof_checksum"),
        put_batch_proof_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_put_batch_proof_checksum"),
        expected_put_batch_proof_checksum.to_string(),
    );
}

fn append_copy_batch_proof_fields(
    copy_batch_proof_format: &str,
    copy_batch_proof_checksum: &str,
    expected_copy_batch_proof_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}copy_batch_proof_format"),
        copy_batch_proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}copy_batch_proof_checksum"),
        copy_batch_proof_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_copy_batch_proof_checksum"),
        expected_copy_batch_proof_checksum.to_string(),
    );
}

