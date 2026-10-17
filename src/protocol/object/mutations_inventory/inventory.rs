#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectInventoryRecord {
    pub metadata: ObjectMetadata,
    pub placement: Option<ObjectPlacement>,
}

impl ObjectInventoryRecord {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}placement_present"),
            self.placement.is_some().to_string(),
        );
        if let Some(placement) = &self.placement {
            fields.extend(placement.to_control_fields(&format!("{field_prefix}placement.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectInventoryPage {
    pub records: Vec<ObjectInventoryRecord>,
    pub bucket: String,
    pub prefix: String,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
    pub listed_objects: u64,
    pub payload_bytes: u64,
    pub has_more: bool,
    pub read_proof: ObjectReadProof,
    pub object_list_page_proof_format: String,
    pub object_list_page_proof_checksum: String,
    pub expected_object_list_page_proof_checksum: String,
}

impl ObjectInventoryPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}prefix"), self.prefix.clone());
        fields.insert(
            format!("{field_prefix}marker"),
            self.marker.clone().unwrap_or_default(),
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
            format!("{field_prefix}object_count"),
            self.records.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}listed_objects"),
            self.listed_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.extend(self.read_proof.to_control_fields(field_prefix));
        fields.insert(
            format!("{field_prefix}object_list_page_proof_format"),
            self.object_list_page_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}object_list_page_proof_checksum"),
            self.object_list_page_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_object_list_page_proof_checksum"),
            self.expected_object_list_page_proof_checksum.clone(),
        );
        for (index, record) in self.records.iter().enumerate() {
            fields.extend(record.to_control_fields(&format!("{field_prefix}object.{index}.")));
        }
        fields
    }
}

