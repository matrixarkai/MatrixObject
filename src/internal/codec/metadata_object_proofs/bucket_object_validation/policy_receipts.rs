// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

pub(crate) fn validate_bucket_policy_receipt(
    receipt: &BucketPolicyMutationReceipt,
) -> Result<(), ObjectError> {
    validate_idempotency_key(&receipt.idempotency_key)?;
    validate_bucket(&receipt.bucket)?;
    if receipt.metadata.object_id.bucket != receipt.bucket
        || !receipt.metadata.object_id.key.is_empty()
    {
        return Err(ObjectError::Corruption(
            "bucket policy receipt metadata bucket mismatch".to_string(),
        ));
    }
    match receipt.method.as_str() {
        "CreateBucket"
        | "DeleteBucket"
        | "ConfigureBucketRetention"
        | "ConfigureBucketQuota"
        | "ConfigureBucketVersioning"
        | "ConfigureBucketLifecycle"
        | "ConfigureBucketPolicy"
        | "ConfigureBucketTagging" => {}
        _ => {
            return Err(ObjectError::Corruption(
                "bucket policy receipt method is invalid".to_string(),
            ))
        }
    }
    if receipt.request_checksum.is_empty() {
        return Err(ObjectError::Corruption(
            "bucket policy receipt request checksum is empty".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn bucket_cors_request_checksum(
    bucket: &str,
    policy: &BucketCorsPolicy,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    let mut policy = policy.clone();
    policy.canonicalize();
    validate_cors_policy(&policy)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-BUCKET-CORS-REQUEST1")
            + encoded_string_len(bucket)
            + cors_policy_encoded_len(&policy),
    );
    append_string(&mut material, "MOSRS-BUCKET-CORS-REQUEST1");
    append_string(&mut material, bucket);
    append_cors_policy(&mut material, &policy);
    Ok(etag(&material))
}

fn string_list_encoded_len(values: &[String]) -> usize {
    std::mem::size_of::<u64>() + values.iter().map(|value| encoded_string_len(value)).sum::<usize>()
}

fn cors_policy_encoded_len(policy: &BucketCorsPolicy) -> usize {
    string_list_encoded_len(&policy.allowed_origins)
        + string_list_encoded_len(&policy.allowed_methods)
        + string_list_encoded_len(&policy.allowed_headers)
        + string_list_encoded_len(&policy.expose_headers)
        + std::mem::size_of::<u64>()
}

pub(crate) fn validate_bucket_cors_receipt(
    receipt: &BucketCorsMutationReceipt,
) -> Result<(), ObjectError> {
    validate_idempotency_key(&receipt.idempotency_key)?;
    validate_bucket(&receipt.bucket)?;
    if receipt.report.metadata.object_id.bucket != receipt.bucket
        || !receipt.report.metadata.object_id.key.is_empty()
    {
        return Err(ObjectError::Corruption(
            "bucket cors receipt metadata bucket mismatch".to_string(),
        ));
    }
    let expected = bucket_cors_request_checksum(&receipt.bucket, &receipt.report.policy)?;
    if receipt.request_checksum != expected {
        return Err(ObjectError::Corruption(
            "bucket cors receipt request checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_cors_list(
    label: &str,
    values: &[String],
    max_count: usize,
    max_total_bytes: usize,
) -> Result<(), ObjectError> {
    if values.len() > max_count {
        return Err(ObjectError::InvalidArgument(format!(
            "{label} exceeds maximum entry count"
        )));
    }
    let mut total = 0usize;
    let mut previous = "";
    for value in values {
        if value.is_empty() || value.len() > 256 || value.contains('\0') || value.contains('\n') {
            return Err(ObjectError::InvalidArgument(format!(
                "{label} contains an invalid value"
            )));
        }
        if !previous.is_empty() && previous >= value.as_str() {
            return Err(ObjectError::InvalidArgument(format!(
                "{label} must be sorted and unique"
            )));
        }
        previous = value;
        total = total.saturating_add(value.len());
        if total > max_total_bytes {
            return Err(ObjectError::InvalidArgument(format!(
                "{label} exceeds maximum encoded size"
            )));
        }
    }
    Ok(())
}

