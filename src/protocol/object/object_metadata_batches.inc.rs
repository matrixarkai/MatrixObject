#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectMetadataBatchRead {
    pub objects: Vec<ObjectMetadata>,
    pub missing: Vec<ObjectId>,
    pub requested_objects: u64,
    pub found_objects: u64,
    pub missing_objects: u64,
    pub payload_bytes: u64,
    pub expected_metadata_shard_topology_epoch: Option<u64>,
    pub expected_metadata_shard_topology_checksum: String,
    pub metadata_batch_read_proof_format: String,
    pub metadata_batch_read_proof_checksum: String,
    pub expected_metadata_batch_read_proof_checksum: String,
}

impl ObjectMetadataBatchRead {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}requested_count"),
            self.requested_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}requested_objects"),
            self.requested_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}object_count"),
            self.found_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}found_objects"),
            self.found_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}missing_count"),
            self.missing_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}missing_objects"),
            self.missing_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}expected_metadata_shard_topology_epoch_present"),
            self.expected_metadata_shard_topology_epoch
                .is_some()
                .to_string(),
        );
        if let Some(expected_epoch) = self.expected_metadata_shard_topology_epoch {
            fields.insert(
                format!("{field_prefix}expected_metadata_shard_topology_epoch"),
                expected_epoch.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}expected_metadata_shard_topology_checksum"),
            self.expected_metadata_shard_topology_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_batch_read_proof_format"),
            self.metadata_batch_read_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_batch_read_proof_checksum"),
            self.metadata_batch_read_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_metadata_batch_read_proof_checksum"),
            self.expected_metadata_batch_read_proof_checksum.clone(),
        );
        for (index, object) in self.objects.iter().enumerate() {
            fields.extend(object.to_control_fields(&format!("{field_prefix}object.{index}.")));
        }
        for (index, missing) in self.missing.iter().enumerate() {
            fields.extend(missing.to_control_fields(&format!("{field_prefix}missing.{index}.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalObjectMetadataRequest {
    pub bucket: String,
    pub key: String,
    pub version: Option<u64>,
    pub preconditions: ObjectReadPreconditions,
}

impl ConditionalObjectMetadataRequest {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}bucket"), self.bucket.clone());
        fields.insert(format!("{field_prefix}key"), self.key.clone());
        if let Some(version) = self.version {
            fields.insert(format!("{field_prefix}version"), version.to_string());
        }
        fields.insert(
            format!("{field_prefix}if_match_etag_count"),
            self.preconditions.if_match_etags.len().to_string(),
        );
        for (index, etag) in self.preconditions.if_match_etags.iter().enumerate() {
            fields.insert(format!("{field_prefix}if_match_etag.{index}"), etag.clone());
        }
        fields.insert(
            format!("{field_prefix}if_none_match_etag_count"),
            self.preconditions.if_none_match_etags.len().to_string(),
        );
        for (index, etag) in self.preconditions.if_none_match_etags.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}if_none_match_etag.{index}"),
                etag.clone(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionalObjectMetadataItem {
    pub request: ConditionalObjectMetadataRequest,
    pub metadata: Option<ConditionalObjectMetadata>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ConditionalObjectMetadataItem {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .request
            .to_control_fields(&format!("{field_prefix}request."));
        fields.insert(
            format!("{field_prefix}found"),
            self.metadata.is_some().to_string(),
        );
        if let Some(metadata) = &self.metadata {
            fields.insert(
                format!("{field_prefix}not_modified"),
                metadata.not_modified.to_string(),
            );
            fields.extend(
                metadata
                    .metadata
                    .to_control_fields(&format!("{field_prefix}metadata.")),
            );
        }
        if let Some(error_code) = &self.error_code {
            fields.insert(format!("{field_prefix}error_code"), error_code.clone());
        }
        if let Some(error_message) = &self.error_message {
            fields.insert(
                format!("{field_prefix}error_message"),
                error_message.clone(),
            );
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConditionalObjectMetadataBatchRead {
    pub items: Vec<ConditionalObjectMetadataItem>,
    pub requested_objects: u64,
    pub found_objects: u64,
    pub not_modified_objects: u64,
    pub missing_objects: u64,
    pub failed_objects: u64,
    pub payload_bytes: u64,
    pub metadata_batch_read_proof_format: String,
    pub metadata_batch_read_proof_checksum: String,
    pub expected_metadata_batch_read_proof_checksum: String,
}

impl ConditionalObjectMetadataBatchRead {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}requested_count"),
            self.requested_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}requested_objects"),
            self.requested_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}object_count"),
            self.found_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}found_objects"),
            self.found_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}not_modified_objects"),
            self.not_modified_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}missing_count"),
            self.missing_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}missing_objects"),
            self.missing_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}failed_objects"),
            self.failed_objects.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}metadata_batch_read_proof_format"),
            self.metadata_batch_read_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}metadata_batch_read_proof_checksum"),
            self.metadata_batch_read_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_metadata_batch_read_proof_checksum"),
            self.expected_metadata_batch_read_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}item_count"),
            self.items.len().to_string(),
        );
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(&format!("{field_prefix}item.{index}.")));
        }
        fields
    }
}

