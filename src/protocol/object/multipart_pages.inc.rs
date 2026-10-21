// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MultipartUploadPage {
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub next_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub uploads: Vec<MultipartUpload>,
    pub common_prefixes: Vec<String>,
    pub scanned_uploads: u64,
    pub emitted_uploads: u64,
    pub emitted_common_prefixes: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
}

impl MultipartUploadPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "bucket", &self.bucket);
        append_optional_string_field(&mut fields, field_prefix, "prefix", &self.prefix);
        append_optional_string_field(&mut fields, field_prefix, "delimiter", &self.delimiter);
        append_optional_string_field(&mut fields, field_prefix, "marker", &self.marker);
        append_optional_string_field(&mut fields, field_prefix, "key_marker", &self.key_marker);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "upload_id_marker",
            &self.upload_id_marker,
        );
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "next_key_marker",
            &self.next_key_marker,
        );
        for (name, value) in [
            ("upload_count", self.uploads.len() as u64),
            ("common_prefix_count", self.common_prefixes.len() as u64),
            ("scanned_uploads", self.scanned_uploads),
            ("emitted_uploads", self.emitted_uploads),
            ("emitted_common_prefixes", self.emitted_common_prefixes),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.extend(self.read_proof.to_control_fields(field_prefix));
        for (index, upload) in self.uploads.iter().enumerate() {
            fields.extend(upload.to_control_fields(&format!("{field_prefix}upload.{index}.")));
        }
        for (index, common_prefix) in self.common_prefixes.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}common_prefix.{index}"),
                common_prefix.clone(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MultipartPartPage {
    pub upload_id: String,
    pub marker: u32,
    pub next_part_marker: Option<u32>,
    pub parts: Vec<MultipartPart>,
    pub scanned_parts: u64,
    pub emitted_parts: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
}

impl MultipartPartPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}upload_id"), self.upload_id.clone());
        fields.insert(format!("{field_prefix}marker"), self.marker.to_string());
        if let Some(next_part_marker) = self.next_part_marker {
            fields.insert(
                format!("{field_prefix}next_marker"),
                next_part_marker.to_string(),
            );
            fields.insert(
                format!("{field_prefix}next_part_marker"),
                next_part_marker.to_string(),
            );
        }
        for (name, value) in [
            ("part_count", self.parts.len() as u64),
            ("scanned_parts", self.scanned_parts),
            ("emitted_parts", self.emitted_parts),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.extend(self.read_proof.to_control_fields(field_prefix));
        for (index, part) in self.parts.iter().enumerate() {
            fields.extend(part.to_control_fields(&format!("{field_prefix}part.{index}.")));
            fields.insert(
                format!("{field_prefix}part.{index}.checksum"),
                part.etag.clone(),
            );
        }
        fields
    }
}

pub(crate) fn append_optional_string_field(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    field_name: &str,
    value: &Option<String>,
) {
    if let Some(value) = value {
        fields.insert(format!("{field_prefix}{field_name}"), value.clone());
    }
}
