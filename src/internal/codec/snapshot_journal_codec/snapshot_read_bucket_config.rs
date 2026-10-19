// SPDX-License-Identifier: Apache-2.0
pub(crate) fn read_lifecycle_policy(
    cursor: &mut SnapshotByteCursor<'_>,
    store_snapshot_version: u64,
) -> Result<BucketLifecyclePolicy, ObjectError> {
    let noncurrent_versions_to_keep =
        cursor.read_count("snapshot lifecycle noncurrent versions")?;
    let noncurrent_older_than_ms = cursor.read_u64()?;
    let transition_after_ms = cursor.read_u64()?;
    let transition_storage_class = empty_string_to_none(cursor.read_string()?);
    let expire_current_after_ms = cursor.read_u64()?;
    let expire_delete_markers = cursor.read_bool()?;
    let filter_tag_key = empty_string_to_none(cursor.read_string()?);
    let filter_tag_value = empty_string_to_none(cursor.read_string()?);
    let filter_prefix = empty_string_to_none(cursor.read_string()?);
    let abort_incomplete_multipart_after_ms = if store_snapshot_version >= 11 {
        cursor.read_u64()?
    } else {
        0
    };
    Ok(BucketLifecyclePolicy {
        noncurrent_versions_to_keep,
        noncurrent_older_than_ms,
        transition_after_ms,
        transition_storage_class,
        expire_current_after_ms,
        expire_delete_markers,
        abort_incomplete_multipart_after_ms,
        filter_tag_key,
        filter_tag_value,
        filter_prefix,
    })
}

pub(crate) fn read_bucket_policy_document(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<BucketPolicyDocument, ObjectError> {
    let policy = BucketPolicyDocument {
        document: cursor.read_string()?,
    };
    validate_bucket_policy_document(&policy)?;
    Ok(policy)
}

pub(crate) fn read_bucket_tag_set(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<BucketTagSet, ObjectError> {
    let tag_set = BucketTagSet {
        tags: read_string_map(cursor, "bucket tag set count")?,
    };
    validate_tags(&tag_set.tags)?;
    Ok(tag_set)
}

pub(crate) fn read_cors_policy(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<BucketCorsPolicy, ObjectError> {
    let mut policy = BucketCorsPolicy {
        allowed_origins: read_string_list(cursor, "store snapshot cors allowed origin count")?,
        allowed_methods: read_string_list(cursor, "store snapshot cors allowed method count")?,
        allowed_headers: read_string_list(cursor, "store snapshot cors allowed header count")?,
        expose_headers: read_string_list(cursor, "store snapshot cors expose header count")?,
        max_age_seconds: cursor.read_u64()?,
    };
    policy.canonicalize();
    validate_cors_policy(&policy)?;
    Ok(policy)
}

pub(crate) fn read_bucket_policy_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_user_metadata: bool,
) -> Result<BucketPolicyMutationReceipt, ObjectError> {
    let receipt = BucketPolicyMutationReceipt {
        idempotency_key: cursor.read_string()?,
        method: cursor.read_string()?,
        bucket: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        metadata: read_metadata(cursor, has_user_metadata)?,
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
    };
    validate_bucket_policy_receipt(&receipt)?;
    Ok(receipt)
}

pub(crate) fn read_bucket_cors_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_user_metadata: bool,
) -> Result<BucketCorsMutationReceipt, ObjectError> {
    let receipt = BucketCorsMutationReceipt {
        idempotency_key: cursor.read_string()?,
        bucket: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        report: BucketCorsMutationReport {
            metadata: read_metadata(cursor, has_user_metadata)?,
            policy: read_cors_policy(cursor)?,
        },
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
    };
    validate_bucket_cors_receipt(&receipt)?;
    Ok(receipt)
}

