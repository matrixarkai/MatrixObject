// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataTransaction {
    pub transaction_id: String,
    pub owner: String,
    pub expire_time_ms: u64,
    pub state: String,
    pub mutations: Vec<MetadataMutation>,
    pub read_versions: BTreeMap<String, u64>,
    pub begin_time_ms: u64,
    pub update_time_ms: u64,
}

impl MetadataTransaction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}transaction_id"),
            self.transaction_id.clone(),
        );
        fields.insert(format!("{field_prefix}owner"), self.owner.clone());
        fields.insert(
            format!("{field_prefix}expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}state"), self.state.clone());
        fields.insert(
            format!("{field_prefix}begin_time_ms"),
            self.begin_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}update_time_ms"),
            self.update_time_ms.to_string(),
        );
        fields.insert(
            format!("{field_prefix}mutation_count"),
            self.mutations.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}read_version_count"),
            self.read_versions.len().to_string(),
        );
        for (index, mutation) in self.mutations.iter().enumerate() {
            fields.extend(mutation.to_control_fields(&format!("{field_prefix}mutation.{index}.")));
        }
        for (index, (key, version)) in self.read_versions.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}read_version.{index}.key"),
                key.clone(),
            );
            fields.insert(
                format!("{field_prefix}read_version.{index}.version"),
                version.to_string(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetadataTransactionSummary {
    pub transaction_id: String,
    pub owner: String,
    pub expire_time_ms: u64,
    pub state: String,
    pub begin_time_ms: u64,
    pub update_time_ms: u64,
    pub mutation_count: u64,
    pub read_version_count: u64,
    pub mutation_value_bytes: u64,
    pub read_key_bytes: u64,
}

impl MetadataTransactionSummary {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}transaction_id"),
            self.transaction_id.clone(),
        );
        fields.insert(format!("{field_prefix}owner"), self.owner.clone());
        fields.insert(
            format!("{field_prefix}expire_time_ms"),
            self.expire_time_ms.to_string(),
        );
        fields.insert(format!("{field_prefix}state"), self.state.clone());
        for (name, value) in [
            ("begin_time_ms", self.begin_time_ms),
            ("update_time_ms", self.update_time_ms),
            ("mutation_count", self.mutation_count),
            ("read_version_count", self.read_version_count),
            ("mutation_value_bytes", self.mutation_value_bytes),
            ("read_key_bytes", self.read_key_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

