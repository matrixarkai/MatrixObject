// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartCreateReport {
    pub upload: MultipartUpload,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub multipart_create_proof_format: String,
    pub multipart_create_proof_checksum: String,
    pub expected_multipart_create_proof_checksum: String,
}

impl MultipartCreateReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.upload.to_control_fields(field_prefix);
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        append_multipart_proof_fields(
            "create",
            &self.multipart_create_proof_format,
            &self.multipart_create_proof_checksum,
            &self.expected_multipart_create_proof_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartPartReport {
    pub part: MultipartPart,
    pub received_bytes: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub multipart_part_proof_format: String,
    pub multipart_part_proof_checksum: String,
    pub expected_multipart_part_proof_checksum: String,
}

impl MultipartPartReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.part.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}received_bytes"),
            self.received_bytes.to_string(),
        );
        fields.insert(format!("{field_prefix}checksum"), self.part.etag.clone());
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        append_multipart_proof_fields(
            "part",
            &self.multipart_part_proof_format,
            &self.multipart_part_proof_checksum,
            &self.expected_multipart_part_proof_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartCompleteReport {
    pub metadata: ObjectMetadata,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub multipart_complete_proof_format: String,
    pub multipart_complete_proof_checksum: String,
    pub expected_multipart_complete_proof_checksum: String,
}

impl MultipartCompleteReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.metadata.to_control_fields(field_prefix);
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        append_multipart_proof_fields(
            "complete",
            &self.multipart_complete_proof_format,
            &self.multipart_complete_proof_checksum,
            &self.expected_multipart_complete_proof_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartCompleteReceipt {
    pub idempotency_key: String,
    pub upload_id: String,
    pub request_checksum: String,
    pub metadata: ObjectMetadata,
    pub commit_index: u64,
    pub update_time_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartAbortReceipt {
    pub idempotency_key: String,
    pub upload_id: String,
    pub request_checksum: String,
    pub upload: MultipartUpload,
    pub commit_index: u64,
    pub update_time_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartAbortReport {
    pub upload: MultipartUpload,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub multipart_abort_proof_format: String,
    pub multipart_abort_proof_checksum: String,
    pub expected_multipart_abort_proof_checksum: String,
}

impl MultipartAbortReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.upload.to_control_fields(field_prefix);
        fields.insert(
            format!("{field_prefix}aborted_bytes"),
            self.upload.total_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}aborted_part_count"),
            self.upload.parts.len().to_string(),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        append_multipart_proof_fields(
            "abort",
            &self.multipart_abort_proof_format,
            &self.multipart_abort_proof_checksum,
            &self.expected_multipart_abort_proof_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

pub(crate) fn append_route_control_fields(
    route_control_proof: &ChunkRouteControlProof,
    expected_route_control_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}expected_route_control_checksum"),
        expected_route_control_checksum.to_string(),
    );
    fields.extend(route_control_proof.to_control_fields(&format!("{field_prefix}route_control.")));
}

fn append_multipart_proof_fields(
    proof_kind: &str,
    proof_format: &str,
    proof_checksum: &str,
    expected_proof_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}multipart_{proof_kind}_proof_format"),
        proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}multipart_{proof_kind}_proof_checksum"),
        proof_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_multipart_{proof_kind}_proof_checksum"),
        expected_proof_checksum.to_string(),
    );
}
