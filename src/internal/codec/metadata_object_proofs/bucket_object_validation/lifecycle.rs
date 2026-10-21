// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn validate_lifecycle_policy(policy: &BucketLifecyclePolicy) -> Result<(), ObjectError> {
    if policy.transition_after_ms > 0 && policy.transition_storage_class.is_none() {
        return Err(ObjectError::InvalidArgument(
            "lifecycle transition requires target storage class".to_string(),
        ));
    }
    if policy.transition_after_ms == 0 && policy.transition_storage_class.is_some() {
        return Err(ObjectError::InvalidArgument(
            "lifecycle transition storage class requires age".to_string(),
        ));
    }
    if let Some(storage_class) = &policy.transition_storage_class {
        validate_storage_class(storage_class)?;
        if storage_class == k_standard_storage_class() {
            return Err(ObjectError::InvalidArgument(
                "lifecycle transition target must be cold storage".to_string(),
            ));
        }
    }
    if let Some(key) = &policy.filter_tag_key {
        validate_tag_key(key)?;
        if policy.filter_tag_value.is_none() {
            return Err(ObjectError::InvalidArgument(
                "lifecycle tag filter key requires value".to_string(),
            ));
        }
    }
    if let Some(value) = &policy.filter_tag_value {
        validate_tag_value(value)?;
        if policy.filter_tag_key.is_none() {
            return Err(ObjectError::InvalidArgument(
                "lifecycle tag filter value requires key".to_string(),
            ));
        }
    }
    if let Some(prefix) = &policy.filter_prefix {
        if prefix.len() > 4096 || prefix.contains('\0') || prefix.contains('\n') {
            return Err(ObjectError::InvalidArgument(
                "lifecycle prefix filter is invalid".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn lifecycle_policy_matches(
    policy: &BucketLifecyclePolicy,
    metadata: &ObjectMetadata,
) -> bool {
    if let Some(prefix) = &policy.filter_prefix {
        if !metadata.object_id.key.starts_with(prefix) {
            return false;
        }
    }
    if let (Some(key), Some(value)) = (&policy.filter_tag_key, &policy.filter_tag_value) {
        return metadata.tags.get(key).map_or(false, |found| found == value);
    }
    true
}

pub(crate) fn lifecycle_policy_matches_upload(
    policy: &BucketLifecyclePolicy,
    upload: &MultipartUpload,
) -> bool {
    if let Some(prefix) = &policy.filter_prefix {
        if !upload.key.starts_with(prefix) {
            return false;
        }
    }
    if let (Some(key), Some(value)) = (&policy.filter_tag_key, &policy.filter_tag_value) {
        return upload
            .options
            .tags
            .get(key)
            .map_or(false, |found| found == value);
    }
    true
}

pub(crate) fn multipart_upload_common_prefix(
    prefix: &str,
    delimiter: Option<&str>,
    key: &str,
) -> Option<String> {
    object_common_prefix(prefix, delimiter, key)
}

pub(crate) fn object_common_prefix(
    prefix: &str,
    delimiter: Option<&str>,
    key: &str,
) -> Option<String> {
    let delimiter = delimiter?;
    let search_begin = if key.starts_with(prefix) {
        prefix.len()
    } else {
        0
    };
    key.get(search_begin..)
        .and_then(|suffix| suffix.find(delimiter))
        .map(|position| key[..search_begin + position + delimiter.len()].to_string())
}

pub(crate) fn lifecycle_age_reached(update_time_ms: u64, age_ms: u64, now_ms: u64) -> bool {
    update_time_ms.saturating_add(age_ms) <= now_ms
}

