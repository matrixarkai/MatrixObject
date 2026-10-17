pub(crate) fn bucket_membership_metadata(
    bucket: &str,
    content_type: &str,
    update_time_ms: u64,
    deleted: bool,
) -> ObjectMetadata {
    ObjectMetadata {
        object_id: ObjectId::new(bucket, ""),
        version: 0,
        length: 0,
        etag: String::new(),
        content_type: content_type.to_string(),
        user_metadata: BTreeMap::new(),
        storage_class: k_standard_storage_class().to_string(),
        restore_until_ms: 0,
        tags: BTreeMap::new(),
        legal_hold: false,
        legal_hold_update_time_ms: 0,
        retention_until_ms: 0,
        update_time_ms,
        extents: Vec::new(),
        deleted,
    }
}

pub(crate) fn bucket_lifecycle_request_checksum(
    bucket: &str,
    policy: &BucketLifecyclePolicy,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    validate_lifecycle_policy(policy)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-BUCKET-LIFECYCLE-REQUEST1")
            + encoded_string_len(bucket)
            + lifecycle_policy_encoded_len(policy),
    );
    append_string(&mut material, "MOSRS-BUCKET-LIFECYCLE-REQUEST1");
    append_string(&mut material, bucket);
    append_lifecycle_policy(&mut material, policy);
    Ok(etag(&material))
}

pub(crate) fn bucket_policy_document_request_checksum(
    bucket: &str,
    policy: &BucketPolicyDocument,
) -> Result<String, ObjectError> {
    validate_bucket(bucket)?;
    validate_bucket_policy_document(policy)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("MOSRS-BUCKET-POLICY-DOCUMENT-REQUEST1")
            + encoded_string_len(bucket)
            + bucket_policy_document_encoded_len(policy),
    );
    append_string(&mut material, "MOSRS-BUCKET-POLICY-DOCUMENT-REQUEST1");
    append_string(&mut material, bucket);
    append_bucket_policy_document(&mut material, policy);
    Ok(etag(&material))
}

pub(crate) fn bucket_tagging_checksum_material(
    bucket: &str,
    tag_set: &BucketTagSet,
) -> Result<Vec<u8>, ObjectError> {
    validate_bucket(bucket)?;
    validate_tags(&tag_set.tags)?;
    let mut material = Vec::with_capacity(
        encoded_string_len("ConfigureBucketTagging")
            + encoded_string_len(bucket)
            + bucket_tag_set_encoded_len(tag_set),
    );
    append_string(&mut material, "ConfigureBucketTagging");
    append_string(&mut material, bucket);
    append_bucket_tag_set(&mut material, tag_set);
    Ok(material)
}

pub(crate) fn bucket_tagging_request_checksum(
    bucket: &str,
    tag_set: &BucketTagSet,
) -> Result<String, ObjectError> {
    Ok(etag(&bucket_tagging_checksum_material(bucket, tag_set)?))
}

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

pub(crate) fn checked_bucket_policy_receipt_retry(
    receipt: &BucketPolicyMutationReceipt,
    method: &str,
    bucket: &str,
    request_checksum: &str,
) -> Result<BucketPolicyMutationReceipt, ObjectError> {
    if receipt.method != method
        || receipt.bucket != bucket
        || receipt.request_checksum != request_checksum
    {
        return Err(ObjectError::InvalidArgument(
            "bucket policy idempotency key reused with different request".to_string(),
        ));
    }
    Ok(receipt.clone())
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

fn lifecycle_policy_encoded_len(policy: &BucketLifecyclePolicy) -> usize {
    std::mem::size_of::<u64>() * 6
        + encoded_string_len(policy.transition_storage_class.as_deref().unwrap_or(""))
        + encoded_string_len(policy.filter_tag_key.as_deref().unwrap_or(""))
        + encoded_string_len(policy.filter_tag_value.as_deref().unwrap_or(""))
        + encoded_string_len(policy.filter_prefix.as_deref().unwrap_or(""))
}

fn bucket_policy_document_encoded_len(policy: &BucketPolicyDocument) -> usize {
    encoded_string_len(&policy.document)
}

fn bucket_tag_set_encoded_len(tag_set: &BucketTagSet) -> usize {
    std::mem::size_of::<u64>()
        + tag_set
            .tags
            .iter()
            .map(|(key, value)| encoded_string_len(key) + encoded_string_len(value))
            .sum::<usize>()
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

pub(crate) fn checked_bucket_cors_receipt_retry(
    receipt: &BucketCorsMutationReceipt,
    bucket: &str,
    request_checksum: &str,
) -> Result<BucketCorsMutationReceipt, ObjectError> {
    if receipt.bucket != bucket || receipt.request_checksum != request_checksum {
        return Err(ObjectError::InvalidArgument(
            "bucket cors idempotency key reused with different request".to_string(),
        ));
    }
    Ok(receipt.clone())
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

