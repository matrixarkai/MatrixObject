// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartPart {
    pub part_number: u32,
    pub length: u64,
    pub etag: String,
    pub update_time_ms: u64,
    pub idempotency_key: Option<String>,
}

impl MultipartPart {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}part_number"),
            self.part_number.to_string(),
        );
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(format!("{field_prefix}etag"), self.etag.clone());
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        if let Some(idempotency_key) = &self.idempotency_key {
            fields.insert(
                format!("{field_prefix}idempotency_key"),
                idempotency_key.clone(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: String,
    pub create_idempotency_key: Option<String>,
    pub options: PutOptions,
    pub create_time_ms: u64,
    pub update_time_ms: u64,
    pub next_part_number: u32,
    pub total_bytes: u64,
    pub parts: BTreeMap<u32, MultipartPart>,
    pub part_payloads: BTreeMap<u32, Vec<u8>>,
}

impl MultipartUpload {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}upload_id"), self.upload_id.clone());
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        fields.insert(
            format!("{field_prefix}content_type"),
            self.content_type.clone(),
        );
        if let Some(idempotency_key) = &self.create_idempotency_key {
            fields.insert(
                format!("{field_prefix}create_idempotency_key"),
                idempotency_key.clone(),
            );
        }
        fields.insert(
            format!("{field_prefix}create_time_ms"),
            self.create_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_part_number"),
            self.next_part_number.to_string(),
        );
        fields.insert(
            format!("{field_prefix}total_bytes"),
            self.total_bytes.to_string(),
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
            format!("{field_prefix}part_count"),
            self.parts.len().to_string(),
        );
        for (index, part) in self.parts.values().enumerate() {
            fields.extend(part.to_control_fields(&format!("{field_prefix}part.{index}.")));
        }
        fields
    }
}
