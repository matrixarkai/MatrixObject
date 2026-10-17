#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLegalHoldMutationReport {
    pub metadata: ObjectMetadata,
    pub enabled: bool,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectLegalHoldMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(format!("{field_prefix}enabled"), self.enabled.to_string());
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRetentionMutationReport {
    pub metadata: ObjectMetadata,
    pub retention_until_ms: u64,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectRetentionMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}retention_until_ms"),
            self.retention_until_ms.to_string(),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectTagsMutationReport {
    pub metadata: ObjectMetadata,
    pub tags: BTreeMap<String, String>,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectTagsMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.extend(string_map_to_indexed_fields(
            &self.tags,
            &format!("{field_prefix}tag."),
        ));
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectTransitionMutationReport {
    pub metadata: ObjectMetadata,
    pub storage_class: String,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectTransitionMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}storage_class"),
            self.storage_class.clone(),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRestoreMutationReport {
    pub metadata: ObjectMetadata,
    pub restore_until_ms: u64,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ObjectRestoreMutationReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}restore_until_ms"),
            self.restore_until_ms.to_string(),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}
