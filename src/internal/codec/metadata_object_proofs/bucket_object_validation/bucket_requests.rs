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

