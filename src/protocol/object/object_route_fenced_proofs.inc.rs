// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvenRouteFencedMetadataRead<T> {
    pub value: T,
    pub proof: ObjectReadProof,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ProvenRouteFencedMetadataRead<ObjectData> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.value.to_control_fields(field_prefix);
        append_route_fenced_read_control_fields(
            &self.proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

impl ProvenRouteFencedMetadataRead<ConditionalObjectData> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.value.to_control_fields(field_prefix);
        append_route_fenced_read_control_fields(
            &self.proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

impl ProvenRouteFencedMetadataRead<AdmittedObjectData> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.value.to_control_fields(field_prefix);
        append_route_fenced_read_control_fields(
            &self.proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

impl ProvenRouteFencedMetadataRead<AdmittedConditionalObjectData> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.value.to_control_fields(field_prefix);
        append_route_fenced_read_control_fields(
            &self.proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

fn append_route_fenced_read_control_fields(
    proof: &ObjectReadProof,
    route_control_proof: &ChunkRouteControlProof,
    expected_route_control_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.extend(proof.to_control_fields(field_prefix));
    fields.insert(
        format!("{field_prefix}expected_route_control_checksum"),
        expected_route_control_checksum.to_string(),
    );
    fields.extend(route_control_proof.to_control_fields(&format!("{field_prefix}route_control.")));
}
