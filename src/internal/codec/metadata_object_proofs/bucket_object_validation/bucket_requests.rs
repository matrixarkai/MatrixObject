// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn validate_bucket(bucket: &str) -> Result<(), ObjectError> {
    if bucket.is_empty() || bucket.len() > 255 {
        return Err(ObjectError::InvalidArgument(
            "bucket must be 1..255 bytes".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_key(key: &str) -> Result<(), ObjectError> {
    if key.is_empty() || key.len() > 4096 {
        return Err(ObjectError::InvalidArgument(
            "key must be 1..4096 bytes".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn canonicalize_string_list(values: &mut Vec<String>) {
    values.retain(|value| !value.is_empty());
    values.sort();
    values.dedup();
}

pub(crate) fn validate_cors_policy(policy: &BucketCorsPolicy) -> Result<(), ObjectError> {
    validate_cors_list("cors allowed origins", &policy.allowed_origins, 64, 2048)?;
    validate_cors_list("cors allowed methods", &policy.allowed_methods, 16, 64)?;
    validate_cors_list("cors allowed headers", &policy.allowed_headers, 128, 1024)?;
    validate_cors_list("cors expose headers", &policy.expose_headers, 128, 1024)?;
    if !policy.disabled() && policy.allowed_origins.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "cors policy requires at least one allowed origin".to_string(),
        ));
    }
    if !policy.disabled() && policy.allowed_methods.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "cors policy requires at least one allowed method".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_bucket_policy_document(
    policy: &BucketPolicyDocument,
) -> Result<(), ObjectError> {
    if policy.document.len() > MAX_BUCKET_POLICY_DOCUMENT_BYTES {
        return Err(ObjectError::InvalidArgument(
            "bucket policy document is too large".to_string(),
        ));
    }
    if !policy.document.is_empty() && policy.document.trim().is_empty() {
        return Err(ObjectError::InvalidArgument(
            "bucket policy document must not be blank".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn bucket_retention_request_checksum(
    bucket: &str,
    retain_for_ms: u64,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-BUCKET-RETENTION-REQUEST1")
            + encoded_string_len(bucket)
            + std::mem::size_of::<u64>(),
    );
    append_string(&mut material, "MOSRS-BUCKET-RETENTION-REQUEST1");
    append_string(&mut material, bucket);
    append_u64(&mut material, retain_for_ms);
    Ok(etag(&material))
}

pub(crate) fn bucket_quota_request_checksum(
    bucket: &str,
    quota: &BucketQuota,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-BUCKET-QUOTA-REQUEST1")
            + encoded_string_len(bucket)
            + std::mem::size_of::<u64>() * 2,
    );
    append_string(&mut material, "MOSRS-BUCKET-QUOTA-REQUEST1");
    append_string(&mut material, bucket);
    append_u64(&mut material, quota.max_live_bytes);
    append_u64(&mut material, quota.max_live_objects);
    Ok(etag(&material))
}

pub(crate) fn bucket_versioning_request_checksum(
    bucket: &str,
    enabled: bool,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-BUCKET-VERSIONING-REQUEST1")
            + encoded_string_len(bucket)
            + std::mem::size_of::<u64>(),
    );
    append_string(&mut material, "MOSRS-BUCKET-VERSIONING-REQUEST1");
    append_string(&mut material, bucket);
    append_u64(&mut material, u64::from(enabled));
    Ok(etag(&material))
}

pub(crate) fn bucket_membership_request_checksum(
    bucket: &str,
    label: &str,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    let mut material =
        Vec::with_capacity(encoded_string_len(label) + encoded_string_len(bucket));
    append_string(&mut material, label);
    append_string(&mut material, bucket);
    Ok(etag(&material))
}

pub(crate) fn bucket_create_request_checksum(bucket: &str) -> Result<String, ObjectError> {
    bucket_membership_request_checksum(bucket, "MOSRS-BUCKET-CREATE-REQUEST1")
}

pub(crate) fn bucket_delete_request_checksum(bucket: &str) -> Result<String, ObjectError> {
    bucket_membership_request_checksum(bucket, "MOSRS-BUCKET-DELETE-REQUEST1")
}

pub(crate) fn multipart_complete_request_checksum(
    upload_id: &str,
    expected_size: Option<u64>,
    expected_etag: Option<&str>,
    expected_checksum: Option<&str>,
) -> Result<String, ObjectError> {
    validate_upload_id(upload_id)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-MULTIPART-COMPLETE-REQUEST1")
            + encoded_string_len(upload_id)
            + std::mem::size_of::<u64>() * 2
            + encoded_string_len(expected_etag.unwrap_or_default())
            + encoded_string_len(expected_checksum.unwrap_or_default()),
    );
    append_string(&mut material, "MOSRS-MULTIPART-COMPLETE-REQUEST1");
    append_string(&mut material, upload_id);
    match expected_size {
        Some(size) => {
            append_u64(&mut material, 1);
            append_u64(&mut material, size);
        }
        None => append_u64(&mut material, 0),
    }
    append_string(&mut material, expected_etag.unwrap_or_default());
    append_string(&mut material, expected_checksum.unwrap_or_default());
    Ok(etag(&material))
}

pub(crate) fn multipart_abort_request_checksum(upload_id: &str) -> Result<String, ObjectError> {
    validate_upload_id(upload_id)?;
    let mut material = Vec::new();
    append_string(&mut material, "MOSRS-MULTIPART-ABORT-REQUEST1");
    append_string(&mut material, upload_id);
    Ok(etag(&material))
}

pub(crate) fn trim_multipart_complete_receipts(
    receipts: &mut BTreeMap<String, MultipartCompleteReceipt>,
    retain: usize,
) -> usize {
    if receipts.len() <= retain {
        return 0;
    }
    let mut victims: Vec<(u64, String)> = receipts
        .iter()
        .map(|(key, receipt)| (receipt.update_time_ms, key.clone()))
        .collect();
    victims.sort();
    let trim_count = receipts.len() - retain;
    for (_, key) in victims.into_iter().take(trim_count) {
        receipts.remove(&key);
    }
    trim_count
}

pub(crate) fn trim_multipart_abort_receipts(
    receipts: &mut BTreeMap<String, MultipartAbortReceipt>,
    retain: usize,
) -> usize {
    if receipts.len() <= retain {
        return 0;
    }
    let mut victims: Vec<(u64, String)> = receipts
        .iter()
        .map(|(key, receipt)| (receipt.update_time_ms, key.clone()))
        .collect();
    victims.sort();
    let trim_count = receipts.len() - retain;
    for (_, key) in victims.into_iter().take(trim_count) {
        receipts.remove(&key);
    }
    trim_count
}

