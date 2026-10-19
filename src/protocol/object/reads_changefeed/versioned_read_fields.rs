// SPDX-License-Identifier: Apache-2.0
fn append_range_control_fields(
    range: &Option<DataRange>,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    if let Some(range) = range {
        fields.insert(format!("{field_prefix}offset"), range.offset.to_string());
        fields.insert(format!("{field_prefix}length"), range.length.to_string());
    }
}

fn append_precondition_control_fields(
    preconditions: &ObjectReadPreconditions,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}if_match_etag_count"),
        preconditions.if_match_etags.len().to_string(),
    );
    for (index, etag) in preconditions.if_match_etags.iter().enumerate() {
        fields.insert(format!("{field_prefix}if_match_etag.{index}"), etag.clone());
    }
    fields.insert(
        format!("{field_prefix}if_none_match_etag_count"),
        preconditions.if_none_match_etags.len().to_string(),
    );
    for (index, etag) in preconditions.if_none_match_etags.iter().enumerate() {
        fields.insert(
            format!("{field_prefix}if_none_match_etag.{index}"),
            etag.clone(),
        );
    }
}

fn append_payload_slice_fields(
    payload_length: u64,
    payload_offset: &mut u64,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}payload_offset"),
        payload_offset.to_string(),
    );
    fields.insert(
        format!("{field_prefix}payload_length"),
        payload_length.to_string(),
    );
    *payload_offset = payload_offset.saturating_add(payload_length);
}

fn append_object_batch_read_proof_control_fields(
    proof_format: &str,
    proof_checksum: &str,
    expected_proof_checksum: &str,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}object_batch_read_proof_format"),
        proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}object_batch_read_proof_checksum"),
        proof_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_object_batch_read_proof_checksum"),
        expected_proof_checksum.to_string(),
    );
}

fn batch_read_item_status_fields(
    error_code: Option<&str>,
    error_message: Option<&str>,
    object_present: bool,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    let status_code = error_code.unwrap_or("OK");
    fields.insert(
        format!("{field_prefix}status_code"),
        status_code.to_string(),
    );
    fields.insert(
        format!("{field_prefix}status_message"),
        error_message.unwrap_or_default().to_string(),
    );
    fields.insert(
        format!("{field_prefix}object_present"),
        object_present.to_string(),
    );
    fields
}

fn batch_read_report_base_fields(
    result_count: usize,
    requested_objects: u64,
    successful_objects: u64,
    not_modified_objects: u64,
    admission_rejected_objects: u64,
    failed_objects: u64,
    payload_bytes: u64,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for (name, value) in [
        ("result_count", result_count as u64),
        ("requested_objects", requested_objects),
        ("successful_objects", successful_objects),
        ("not_modified_objects", not_modified_objects),
        ("admission_rejected_objects", admission_rejected_objects),
        ("failed_objects", failed_objects),
        ("payload_bytes", payload_bytes),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields
}

#[allow(clippy::too_many_arguments)]
fn admitted_batch_read_report_base_fields(
    result_count: usize,
    requested_objects: u64,
    successful_objects: u64,
    not_modified_objects: u64,
    admission_rejected_objects: u64,
    failed_objects: u64,
    payload_bytes: u64,
    max_payload_bytes: u64,
    reserved_chunk_read_count: u64,
    max_chunk_read_count: u64,
    budget_limited: bool,
    storage_admission_proof_format: &str,
    storage_admission_proof_checksum: &str,
    expected_storage_admission_proof_checksum: &str,
    field_prefix: &str,
) -> BTreeMap<String, String> {
    let mut fields = batch_read_report_base_fields(
        result_count,
        requested_objects,
        successful_objects,
        not_modified_objects,
        admission_rejected_objects,
        failed_objects,
        payload_bytes,
        field_prefix,
    );
    for (name, value) in [
        ("max_payload_bytes", max_payload_bytes),
        ("reserved_chunk_read_count", reserved_chunk_read_count),
        ("max_chunk_read_count", max_chunk_read_count),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields.insert(
        format!("{field_prefix}budget_limited"),
        budget_limited.to_string(),
    );
    fields.insert(
        format!("{field_prefix}storage_admission_proof_format"),
        storage_admission_proof_format.to_string(),
    );
    fields.insert(
        format!("{field_prefix}storage_admission_proof_checksum"),
        storage_admission_proof_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}expected_storage_admission_proof_checksum"),
        expected_storage_admission_proof_checksum.to_string(),
    );
    fields
}
