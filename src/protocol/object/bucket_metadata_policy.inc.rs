// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketQuota {
    pub max_live_bytes: u64,
    pub max_live_objects: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketInventoryPage {
    pub buckets: Vec<String>,
    pub marker: Option<String>,
    pub limit: u64,
    pub scanned_count: u64,
    pub returned_count: u64,
    pub next_marker: Option<String>,
    pub has_more: bool,
    pub readiness: MetadataRaftReadiness,
}

impl BucketInventoryPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}marker"),
            self.marker.clone().unwrap_or_default(),
        );
        fields.insert(format!("{field_prefix}limit"), self.limit.to_string());
        fields.insert(
            format!("{field_prefix}scanned_count"),
            self.scanned_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}returned_count"),
            self.returned_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_marker"),
            self.next_marker.clone().unwrap_or_default(),
        );
        fields.insert(
            format!("{field_prefix}truncated"),
            self.has_more.to_string(),
        );
        fields.insert(
            format!("{field_prefix}bucket_count"),
            self.buckets.len().to_string(),
        );
        fields.extend(
            object_read_proof_from_readiness(&self.readiness).to_control_fields(field_prefix),
        );
        for (index, bucket) in self.buckets.iter().enumerate() {
            fields.insert(format!("{field_prefix}bucket.{index}"), bucket.clone());
        }
        fields
    }
}

impl BucketQuota {
    pub fn disabled(&self) -> bool {
        self.max_live_bytes == 0 && self.max_live_objects == 0
    }

    pub fn to_control_fields(
        &self,
        live_bytes: u64,
        live_objects: u64,
        field_prefix: &str,
    ) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}max_live_bytes"),
            self.max_live_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_live_objects"),
            self.max_live_objects.to_string(),
        );
        fields.insert(format!("{field_prefix}live_bytes"), live_bytes.to_string());
        fields.insert(
            format!("{field_prefix}live_objects"),
            live_objects.to_string(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketPolicyDocument {
    pub document: String,
}

impl BucketPolicyDocument {
    pub fn disabled(&self) -> bool {
        self.document.is_empty()
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}policy_document"),
            self.document.clone(),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketTagSet {
    pub tags: BTreeMap<String, String>,
}

impl BucketTagSet {
    pub fn disabled(&self) -> bool {
        self.tags.is_empty()
    }

    pub fn to_flat_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_flat_fields(&self.tags, field_prefix)
    }

    pub fn to_indexed_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        string_map_to_indexed_fields(&self.tags, field_prefix)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        self.to_indexed_fields(&format!("{field_prefix}tag."))
    }

    pub fn from_fields(
        fields: &BTreeMap<String, String>,
        field_prefix: &str,
    ) -> Result<Self, ObjectError> {
        let tags = string_map_from_fields(fields, field_prefix)?;
        validate_tags(&tags)?;
        Ok(Self { tags })
    }
}

fn string_map_to_flat_fields(
    values: &BTreeMap<String, String>,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| (format!("{field_prefix}{key}"), value.clone()))
        .collect()
}

pub(crate) fn string_map_to_indexed_fields(
    values: &BTreeMap<String, String>,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    fields.insert(metadata_count_field(field_prefix), values.len().to_string());
    for (index, (key, value)) in values.iter().enumerate() {
        let prefix = format!("{field_prefix}{index}.");
        fields.insert(format!("{prefix}key"), key.clone());
        fields.insert(format!("{prefix}value"), value.clone());
    }
    fields
}

fn string_map_from_fields(
    fields: &BTreeMap<String, String>,
    field_prefix: &str,
) -> Result<BTreeMap<String, String>, ObjectError> {
    let mut out = BTreeMap::new();
    let mut has_indexed_groups = false;
    if let Some(count_value) = fields.get(&metadata_count_field(field_prefix)) {
        let count = count_value.parse::<usize>().map_err(|_| {
            ObjectError::InvalidArgument(format!(
                "{} count must be a non-negative integer",
                field_prefix.trim_end_matches('.')
            ))
        })?;
        for index in 0..count {
            let prefix = format!("{field_prefix}{index}.");
            let key = fields.get(&(prefix.clone() + "key")).ok_or_else(|| {
                ObjectError::InvalidArgument(format!(
                    "missing indexed metadata key field {prefix}key"
                ))
            })?;
            let value = fields.get(&(prefix + "value")).ok_or_else(|| {
                ObjectError::InvalidArgument(format!(
                    "missing indexed metadata value field {field_prefix}{index}.value"
                ))
            })?;
            has_indexed_groups = true;
            out.insert(key.clone(), value.clone());
        }
    }
    for (field, value) in fields {
        if let Some(suffix) = field.strip_prefix(field_prefix) {
            if !has_indexed_groups || !is_indexed_metadata_field_suffix(suffix) {
                out.insert(suffix.to_string(), value.clone());
            }
        }
    }
    Ok(out)
}

fn metadata_count_field(field_prefix: &str) -> String {
    format!("{}_count", field_prefix.trim_end_matches('.'))
}

fn is_indexed_metadata_field_suffix(suffix: &str) -> bool {
    let Some((index, name)) = suffix.split_once('.') else {
        return false;
    };
    !index.is_empty()
        && index.chars().all(|ch| ch.is_ascii_digit())
        && matches!(name, "key" | "value")
}
