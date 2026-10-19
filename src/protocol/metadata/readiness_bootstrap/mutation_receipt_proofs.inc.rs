// SPDX-License-Identifier: Apache-2.0
pub(crate) fn object_batch_delete_proof_checksum(
    operation: &str,
    report: &ObjectBatchDeleteReport,
) -> String {
    let mut reserve_hint = encoded_string_len(DELETE_BATCH_PROOF_FORMAT)
        + encoded_string_len(operation)
        + encoded_string_len(&report.expected_route_control_checksum)
        + encoded_string_len(&report.route_control_proof.state_format)
        + encoded_string_len(&report.route_control_proof.state_checksum)
        + std::mem::size_of::<u64>() * 25;
    for item in &report.items {
        reserve_hint += encoded_string_len(&item.request.bucket)
            + encoded_string_len(&item.request.key)
            + encoded_string_len(item.request.if_match_etag.as_deref().unwrap_or_default())
            + encoded_string_len(item.error_code.as_deref().unwrap_or_default())
            + encoded_string_len(item.error_message.as_deref().unwrap_or_default())
            + std::mem::size_of::<u64>() * 3;
        if let Some(metadata) = item.metadata.as_ref() {
            reserve_hint += object_metadata_encoded_len(metadata);
        }
    }
    let mut material = Vec::with_capacity(reserve_hint);
    append_string(&mut material, DELETE_BATCH_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.unique_objects);
    append_u64(&mut material, report.duplicate_objects);
    append_u64(&mut material, report.deleted_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.deleted_bytes);
    append_object_read_proof(&mut material, &report.read_proof);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_string(&mut material, &item.request.bucket);
        append_string(&mut material, &item.request.key);
        append_string(
            &mut material,
            item.request.if_match_etag.as_deref().unwrap_or_default(),
        );
        append_u64(&mut material, item.request.version.unwrap_or_default());
        append_u64(&mut material, item.request.version.is_some() as u64);
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(metadata) = item.metadata.as_ref() {
            append_u64(&mut material, 1);
            append_metadata(&mut material, metadata);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

fn object_metadata_encoded_len(metadata: &ObjectMetadata) -> usize {
    encoded_string_len(&metadata.object_id.bucket)
        + encoded_string_len(&metadata.object_id.key)
        + encoded_string_len(&metadata.etag)
        + encoded_string_len(&metadata.content_type)
        + encoded_string_len(&metadata.storage_class)
        + metadata
            .user_metadata
            .iter()
            .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
            .sum::<usize>()
        + metadata
            .tags
            .iter()
            .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
            .sum::<usize>()
        + std::mem::size_of::<u64>() * (10 + metadata.extents.len() * 4)
}

fn string_map_encoded_len(values: &BTreeMap<String, String>) -> usize {
    std::mem::size_of::<u64>()
        + values
            .iter()
            .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
            .sum::<usize>()
}

fn string_list_encoded_len(values: &[String]) -> usize {
    std::mem::size_of::<u64>()
        + values
            .iter()
            .map(|value| encoded_string_len(value))
            .sum::<usize>()
}

fn put_options_encoded_len(options: &PutOptions) -> usize {
    std::mem::size_of::<u64>() * 2
        + encoded_string_len(options.if_match_etag.as_deref().unwrap_or(""))
        + encoded_string_len(options.expected_checksum.as_deref().unwrap_or(""))
        + encoded_string_len(options.lease_owner.as_deref().unwrap_or(""))
        + string_map_encoded_len(&options.user_metadata)
        + string_map_encoded_len(&options.tags)
        + encoded_string_len(options.storage_class.as_deref().unwrap_or(""))
        + string_list_encoded_len(&options.if_match_etags)
}

fn multipart_part_encoded_len(part: &MultipartPart) -> usize {
    std::mem::size_of::<u64>() * 4
        + encoded_string_len(&part.etag)
        + part
            .idempotency_key
            .as_ref()
            .map(|idempotency_key| encoded_string_len(idempotency_key))
            .unwrap_or(0)
}

fn multipart_upload_encoded_len(upload: &MultipartUpload) -> usize {
    let mut encoded_len = encoded_string_len(&upload.upload_id)
        + encoded_string_len(&upload.bucket)
        + encoded_string_len(&upload.key)
        + encoded_string_len(&upload.content_type)
        + std::mem::size_of::<u64>()
        + upload
            .create_idempotency_key
            .as_ref()
            .map(|idempotency_key| encoded_string_len(idempotency_key))
            .unwrap_or(0)
        + put_options_encoded_len(&upload.options)
        + std::mem::size_of::<u64>() * 5;
    for (part_number, part) in &upload.parts {
        encoded_len += std::mem::size_of::<u64>()
            + multipart_part_encoded_len(part)
            + std::mem::size_of::<u64>()
            + upload
                .part_payloads
                .get(part_number)
                .map(|payload| payload.len())
                .unwrap_or(0);
    }
    encoded_len
}

fn chunk_route_control_proof_encoded_len(proof: &ChunkRouteControlProof) -> usize {
    encoded_string_len(&proof.state_format)
        + encoded_string_len(&proof.state_checksum)
        + std::mem::size_of::<u64>() * 34
}

fn multipart_proof_common_encoded_len(
    format: &str,
    operation: &str,
    route_control_proof: &ChunkRouteControlProof,
    expected_route_control_checksum: &str,
) -> usize {
    encoded_string_len(format)
        + encoded_string_len(operation)
        + chunk_route_control_proof_encoded_len(route_control_proof)
        + encoded_string_len(expected_route_control_checksum)
}

pub(crate) fn stamp_object_batch_delete_proof(
    operation: &str,
    report: &mut ObjectBatchDeleteReport,
) {
    report.delete_batch_proof_format = DELETE_BATCH_PROOF_FORMAT.to_string();
    report.delete_batch_proof_checksum = object_batch_delete_proof_checksum(operation, report);
}

pub(crate) fn object_batch_put_proof_checksum(
    operation: &str,
    report: &ObjectBatchPutReport,
) -> String {
    let mut reserve_hint = encoded_string_len(PUT_BATCH_PROOF_FORMAT)
        + encoded_string_len(operation)
        + encoded_string_len(&report.expected_route_control_checksum)
        + encoded_string_len(&report.route_control_proof.state_format)
        + encoded_string_len(&report.route_control_proof.state_checksum)
        + std::mem::size_of::<u64>() * 24;
    for item in &report.items {
        reserve_hint += encoded_string_len(&item.bucket)
            + encoded_string_len(&item.key)
            + encoded_string_len(item.error_code.as_deref().unwrap_or_default())
            + encoded_string_len(item.error_message.as_deref().unwrap_or_default())
            + std::mem::size_of::<u64>() * 2;
        if let Some(metadata) = item.metadata.as_ref() {
            reserve_hint += object_metadata_encoded_len(metadata);
        }
    }
    let mut material = Vec::with_capacity(reserve_hint);
    append_string(&mut material, PUT_BATCH_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.put_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.payload_bytes);
    append_u64(&mut material, report.max_payload_bytes);
    append_object_read_proof(&mut material, &report.read_proof);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_string(&mut material, &item.bucket);
        append_string(&mut material, &item.key);
        append_u64(&mut material, item.payload_bytes);
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(metadata) = item.metadata.as_ref() {
            append_u64(&mut material, 1);
            append_metadata(&mut material, metadata);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn stamp_object_batch_put_proof(operation: &str, report: &mut ObjectBatchPutReport) {
    report.put_batch_proof_format = PUT_BATCH_PROOF_FORMAT.to_string();
    report.put_batch_proof_checksum = object_batch_put_proof_checksum(operation, report);
}

pub(crate) fn object_batch_copy_proof_checksum(
    operation: &str,
    report: &ObjectBatchCopyReport,
) -> String {
    let mut reserve_hint = encoded_string_len(COPY_BATCH_PROOF_FORMAT)
        + encoded_string_len(operation)
        + encoded_string_len(&report.expected_route_control_checksum)
        + encoded_string_len(&report.route_control_proof.state_format)
        + encoded_string_len(&report.route_control_proof.state_checksum)
        + std::mem::size_of::<u64>() * 23;
    for item in &report.items {
        reserve_hint += encoded_string_len(&item.request.source_bucket)
            + encoded_string_len(&item.request.source_key)
            + encoded_string_len(&item.request.destination_bucket)
            + encoded_string_len(&item.request.destination_key)
            + encoded_string_len(item.request.content_type.as_deref().unwrap_or_default())
            + encoded_string_len(item.error_code.as_deref().unwrap_or_default())
            + encoded_string_len(item.error_message.as_deref().unwrap_or_default())
            + std::mem::size_of::<u64>() * 3;
        if let Some(metadata) = item.metadata.as_ref() {
            reserve_hint += object_metadata_encoded_len(metadata);
        }
    }
    let mut material = Vec::with_capacity(reserve_hint);
    append_string(&mut material, COPY_BATCH_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_u64(&mut material, report.requested_objects);
    append_u64(&mut material, report.copied_objects);
    append_u64(&mut material, report.failed_objects);
    append_u64(&mut material, report.copied_bytes);
    append_object_read_proof(&mut material, &report.read_proof);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    append_u64(&mut material, report.items.len() as u64);
    for item in &report.items {
        append_string(&mut material, &item.request.source_bucket);
        append_string(&mut material, &item.request.source_key);
        append_u64(
            &mut material,
            item.request.source_version.unwrap_or_default(),
        );
        append_u64(&mut material, item.request.source_version.is_some() as u64);
        append_string(&mut material, &item.request.destination_bucket);
        append_string(&mut material, &item.request.destination_key);
        append_string(
            &mut material,
            item.request.content_type.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_code.as_deref().unwrap_or_default(),
        );
        append_string(
            &mut material,
            item.error_message.as_deref().unwrap_or_default(),
        );
        if let Some(metadata) = item.metadata.as_ref() {
            append_u64(&mut material, 1);
            append_metadata(&mut material, metadata);
        } else {
            append_u64(&mut material, 0);
        }
    }
    etag(&material)
}

pub(crate) fn stamp_object_batch_copy_proof(operation: &str, report: &mut ObjectBatchCopyReport) {
    report.copy_batch_proof_format = COPY_BATCH_PROOF_FORMAT.to_string();
    report.copy_batch_proof_checksum = object_batch_copy_proof_checksum(operation, report);
}

pub(crate) fn check_expected_object_batch_delete_proof_checksum(
    operation: &str,
    actual: &str,
    expected_delete_batch_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_delete_batch_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} delete batch proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn check_expected_object_batch_put_proof_checksum(
    operation: &str,
    actual: &str,
    expected_put_batch_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_put_batch_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} put batch proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn check_expected_object_batch_copy_proof_checksum(
    operation: &str,
    actual: &str,
    expected_copy_batch_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_copy_batch_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} copy batch proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn multipart_create_proof_checksum(
    operation: &str,
    report: &MultipartCreateReport,
) -> String {
    let mut material = Vec::with_capacity(
        multipart_proof_common_encoded_len(
            MULTIPART_CREATE_PROOF_FORMAT,
            operation,
            &report.route_control_proof,
            &report.expected_route_control_checksum,
        ) + multipart_upload_encoded_len(&report.upload),
    );
    append_string(&mut material, MULTIPART_CREATE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_multipart_upload(&mut material, &report.upload);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn stamp_multipart_create_proof(operation: &str, report: &mut MultipartCreateReport) {
    report.multipart_create_proof_format = MULTIPART_CREATE_PROOF_FORMAT.to_string();
    report.multipart_create_proof_checksum = multipart_create_proof_checksum(operation, report);
}

pub(crate) fn check_expected_multipart_create_proof_checksum(
    operation: &str,
    actual: &str,
    expected_multipart_create_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_multipart_create_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} multipart create proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn multipart_part_proof_checksum(
    operation: &str,
    report: &MultipartPartReport,
) -> String {
    let mut material = Vec::with_capacity(
        multipart_proof_common_encoded_len(
            MULTIPART_PART_PROOF_FORMAT,
            operation,
            &report.route_control_proof,
            &report.expected_route_control_checksum,
        ) + multipart_part_encoded_len(&report.part)
            + std::mem::size_of::<u64>(),
    );
    append_string(&mut material, MULTIPART_PART_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_multipart_part(&mut material, &report.part);
    append_u64(&mut material, report.received_bytes);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn stamp_multipart_part_proof(operation: &str, report: &mut MultipartPartReport) {
    report.multipart_part_proof_format = MULTIPART_PART_PROOF_FORMAT.to_string();
    report.multipart_part_proof_checksum = multipart_part_proof_checksum(operation, report);
}

pub(crate) fn check_expected_multipart_part_proof_checksum(
    operation: &str,
    actual: &str,
    expected_multipart_part_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_multipart_part_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} multipart part proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn multipart_abort_proof_checksum(
    operation: &str,
    report: &MultipartAbortReport,
) -> String {
    let mut material = Vec::with_capacity(
        multipart_proof_common_encoded_len(
            MULTIPART_ABORT_PROOF_FORMAT,
            operation,
            &report.route_control_proof,
            &report.expected_route_control_checksum,
        ) + multipart_upload_encoded_len(&report.upload),
    );
    append_string(&mut material, MULTIPART_ABORT_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_multipart_upload(&mut material, &report.upload);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn stamp_multipart_abort_proof(operation: &str, report: &mut MultipartAbortReport) {
    report.multipart_abort_proof_format = MULTIPART_ABORT_PROOF_FORMAT.to_string();
    report.multipart_abort_proof_checksum = multipart_abort_proof_checksum(operation, report);
}

pub(crate) fn check_expected_multipart_abort_proof_checksum(
    operation: &str,
    actual: &str,
    expected_multipart_abort_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_multipart_abort_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} multipart abort proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn multipart_complete_proof_checksum(
    operation: &str,
    report: &MultipartCompleteReport,
) -> String {
    let mut material = Vec::with_capacity(
        multipart_proof_common_encoded_len(
            MULTIPART_COMPLETE_PROOF_FORMAT,
            operation,
            &report.route_control_proof,
            &report.expected_route_control_checksum,
        ) + object_metadata_encoded_len(&report.metadata),
    );
    append_string(&mut material, MULTIPART_COMPLETE_PROOF_FORMAT);
    append_string(&mut material, operation);
    append_metadata(&mut material, &report.metadata);
    append_page_route_control_proof(&mut material, &report.route_control_proof);
    append_string(&mut material, &report.expected_route_control_checksum);
    etag(&material)
}

pub(crate) fn stamp_multipart_complete_proof(
    operation: &str,
    report: &mut MultipartCompleteReport,
) {
    report.multipart_complete_proof_format = MULTIPART_COMPLETE_PROOF_FORMAT.to_string();
    report.multipart_complete_proof_checksum = multipart_complete_proof_checksum(operation, report);
}

pub(crate) fn check_expected_multipart_complete_proof_checksum(
    operation: &str,
    actual: &str,
    expected_multipart_complete_proof_checksum: Option<&str>,
) -> Result<(), ObjectError> {
    if let Some(expected) = expected_multipart_complete_proof_checksum {
        if !expected.is_empty() && actual != expected {
            return Err(ObjectError::PreconditionFailed(format!(
                "{} multipart complete proof checksum mismatch: expected {} got {}",
                operation, expected, actual
            )));
        }
    }
    Ok(())
}

pub(crate) fn proven_receipt_read<T>(
    value: T,
    readiness: &MetadataRaftReadiness,
) -> ProvenReceiptRead<T> {
    ProvenReceiptRead {
        value,
        proof: receipt_read_proof_from_readiness(readiness),
        storage_page_proof: None,
    }
}

pub(crate) fn proven_storage_receipt_page_read<T>(
    value: T,
    readiness: &MetadataRaftReadiness,
) -> ProvenReceiptRead<T>
where
    T: StorageReceiptPageProofSource,
{
    let proof = receipt_read_proof_from_readiness(readiness);
    let storage_page_proof = Some(storage_receipt_page_proof(&value, &proof));
    ProvenReceiptRead {
        value,
        proof,
        storage_page_proof,
    }
}
