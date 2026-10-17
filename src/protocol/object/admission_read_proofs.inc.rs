#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdmissionReadProof {
    pub requested_admission_read_index: u64,
    pub requested_admission_commit_index: u64,
    pub admission_applied_index: u64,
    pub admission_read_index_lag: u64,
    pub admission_commit_index_lag: u64,
    pub control_kind: String,
    pub control_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenAdmissionRead<T> {
    pub value: T,
    pub proof: AdmissionReadProof,
}

impl AdmissionReadProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            (
                "requested_admission_read_index",
                self.requested_admission_read_index,
            ),
            (
                "requested_admission_commit_index",
                self.requested_admission_commit_index,
            ),
            ("admission_applied_index", self.admission_applied_index),
            ("admission_read_index_lag", self.admission_read_index_lag),
            (
                "admission_commit_index_lag",
                self.admission_commit_index_lag,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}control_kind"),
            self.control_kind.clone(),
        );
        fields.insert(
            format!("{field_prefix}control_checksum"),
            self.control_checksum.clone(),
        );
        fields
    }
}

impl<T> ProvenAdmissionRead<T> {
    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        self.proof.to_control_fields(field_prefix)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenPageRouteFencedAdmissionRead<T> {
    pub value: T,
    pub proof: AdmissionReadProof,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl<T> ProvenPageRouteFencedAdmissionRead<T> {
    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}admission_read_proof.")),
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
pub struct ProvenChunkControlFencedAdmissionRead<T> {
    pub value: T,
    pub proof: AdmissionReadProof,
    pub control_proof: ChunkServiceControlProof,
    pub expected_control_checksum: String,
}

impl<T> ProvenChunkControlFencedAdmissionRead<T> {
    pub fn proof_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}admission_read_proof.")),
        );
        fields.extend(
            self.control_proof
                .to_control_fields(&format!("{field_prefix}chunk_control_proof.")),
        );
        fields.insert(
            format!("{field_prefix}expected_control_checksum"),
            self.expected_control_checksum.clone(),
        );
        fields
    }
}

