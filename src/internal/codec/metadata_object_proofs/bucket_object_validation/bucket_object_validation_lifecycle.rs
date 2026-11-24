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

