#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedConditionalObjectReadItem {
    pub request: VersionedConditionalObjectReadRequest,
    pub object: Option<ConditionalObjectData>,
    pub stats: PageReadStats,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl VersionedConditionalObjectReadItem {
    pub fn to_control_fields(
        &self,
        field_prefix: &str,
        payload_offset: &mut u64,
    ) -> BTreeMap<String, String> {
        let not_modified = self
            .object
            .as_ref()
            .is_some_and(|object| object.not_modified);
        let mut fields = batch_read_item_status_fields(
            self.error_code.as_deref(),
            self.error_message.as_deref(),
            self.object.is_some(),
            field_prefix,
        );
        fields.insert(
            format!("{field_prefix}not_modified"),
            not_modified.to_string(),
        );
        fields.extend(
            self.request
                .to_control_fields(&format!("{field_prefix}request.")),
        );
        fields.extend(self.stats.to_control_fields(field_prefix));
        if let Some(object) = &self.object {
            let payload_length = object
                .data
                .as_ref()
                .map(|data| data.len() as u64)
                .unwrap_or(0);
            append_payload_slice_fields(payload_length, payload_offset, field_prefix, &mut fields);
            fields.extend(object.to_control_fields(&format!("{field_prefix}info.")));
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VersionedConditionalObjectBatchReadReport {
    pub items: Vec<VersionedConditionalObjectReadItem>,
    pub requested_objects: u64,
    pub successful_objects: u64,
    pub not_modified_objects: u64,
    pub failed_objects: u64,
    pub payload_bytes: u64,
    pub read_stats: PageReadStats,
    pub read_proof: ObjectReadProof,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub object_batch_read_proof_format: String,
    pub object_batch_read_proof_checksum: String,
    pub expected_object_batch_read_proof_checksum: String,
}

impl VersionedConditionalObjectBatchReadReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = batch_read_report_base_fields(
            self.items.len(),
            self.requested_objects,
            self.successful_objects,
            self.not_modified_objects,
            0,
            self.failed_objects,
            self.payload_bytes,
            field_prefix,
        );
        append_route_fenced_read_control_fields(
            &self.read_proof,
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields.extend(
            self.read_stats
                .to_control_fields(&format!("{field_prefix}read_stats.")),
        );
        append_object_batch_read_proof_control_fields(
            &self.object_batch_read_proof_format,
            &self.object_batch_read_proof_checksum,
            &self.expected_object_batch_read_proof_checksum,
            field_prefix,
            &mut fields,
        );
        let mut payload_offset = 0;
        for (index, item) in self.items.iter().enumerate() {
            fields.extend(item.to_control_fields(
                &format!("{field_prefix}result.{index}."),
                &mut payload_offset,
            ));
        }
        fields
    }
}
