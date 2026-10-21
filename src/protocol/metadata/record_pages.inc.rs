// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataRecordPage {
    pub records: Vec<MetadataRecord>,
    pub next_key_marker: Option<String>,
    pub scanned_records: u64,
    pub payload_bytes: u64,
    pub has_more: bool,
}

impl MetadataRecordPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "next_key_marker",
            &self.next_key_marker,
        );
        fields.insert(
            format!("{field_prefix}record_count"),
            self.records.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}scanned_records"),
            self.scanned_records.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        for (index, record) in self.records.iter().enumerate() {
            fields.extend(record.to_control_fields(&format!("{field_prefix}record.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataRecordBatchRead {
    pub records: Vec<MetadataRecord>,
    pub requested_records: u64,
    pub found_records: u64,
    pub missing_records: u64,
    pub payload_bytes: u64,
}
