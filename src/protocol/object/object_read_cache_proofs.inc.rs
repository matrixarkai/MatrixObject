#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectReadRepairReport {
    pub report: ObjectReadRepairReport,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenConditionalObjectBatchReadReport {
    pub report: ConditionalObjectBatchReadReport,
    pub proof: ObjectReadProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenCacheControlStats {
    pub stats: CacheControlStats,
    pub proof: ObjectReadProof,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ProvenCacheControlStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}object_read_proof.")),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenObjectBatchCacheControlReport {
    pub report: ObjectBatchCacheControlReport,
    pub proof: ObjectReadProof,
}

impl ProvenObjectBatchCacheControlReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.report.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}object_read_proof.")),
        );
        fields
    }
}
