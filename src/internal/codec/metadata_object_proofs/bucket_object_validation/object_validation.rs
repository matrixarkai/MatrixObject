// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn validate_tags(tags: &BTreeMap<String, String>) -> Result<(), ObjectError> {
    if tags.len() > 10 {
        return Err(ObjectError::InvalidArgument(
            "object tags must contain at most 10 entries".to_string(),
        ));
    }
    let mut total = 0usize;
    for (key, value) in tags {
        validate_tag_key(key)?;
        validate_tag_value(value)?;
        total = total.saturating_add(key.len()).saturating_add(value.len());
        if total > 2048 {
            return Err(ObjectError::InvalidArgument(
                "object tags exceed maximum total size".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_user_metadata(
    metadata: &BTreeMap<String, String>,
) -> Result<(), ObjectError> {
    if metadata.len() > MAX_OBJECT_METADATA_ENTRIES {
        return Err(ObjectError::InvalidArgument(
            "object metadata has too many entries".to_string(),
        ));
    }
    let mut total = 0usize;
    for (key, value) in metadata {
        if key.is_empty()
            || key.len() > MAX_OBJECT_METADATA_KEY_BYTES
            || key.contains('\0')
            || key.contains('\n')
        {
            return Err(ObjectError::InvalidArgument(
                "object metadata key is invalid".to_string(),
            ));
        }
        if value.len() > MAX_OBJECT_METADATA_VALUE_BYTES
            || value.contains('\0')
            || value.contains('\n')
        {
            return Err(ObjectError::InvalidArgument(
                "object metadata value is invalid".to_string(),
            ));
        }
        total = total.saturating_add(key.len()).saturating_add(value.len());
        if total > MAX_OBJECT_METADATA_TOTAL_BYTES {
            return Err(ObjectError::InvalidArgument(
                "object metadata exceeds maximum total size".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_tag_key(key: &str) -> Result<(), ObjectError> {
    if key.is_empty() || key.len() > 128 || key.contains('\0') || key.contains('\n') {
        return Err(ObjectError::InvalidArgument(
            "object tag key must be 1..128 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_tag_value(value: &str) -> Result<(), ObjectError> {
    if value.len() > 256 || value.contains('\0') || value.contains('\n') {
        return Err(ObjectError::InvalidArgument(
            "object tag value must be 0..256 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_storage_class(storage_class: &str) -> Result<(), ObjectError> {
    if matches!(
        storage_class,
        "STANDARD" | "ARCHIVE" | "GLACIER" | "DEEP_ARCHIVE"
    ) {
        Ok(())
    } else {
        Err(ObjectError::InvalidArgument(
            "storage class must be STANDARD, ARCHIVE, GLACIER, or DEEP_ARCHIVE".to_string(),
        ))
    }
}

pub(crate) fn validate_chunk_id(chunk_id: &ChunkId) -> Result<(), ObjectError> {
    if chunk_id.id() == 0 {
        return Err(ObjectError::InvalidArgument(
            "chunk id must be positive".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn k_standard_storage_class() -> &'static str {
    "STANDARD"
}

pub(crate) fn k_max_raft_apply_receipts() -> usize {
    4096
}

pub(crate) fn k_max_raft_membership_receipt_list_limit() -> usize {
    10_000
}

pub(crate) fn k_max_metadata_receipt_list_limit() -> usize {
    10_000
}

pub(crate) fn k_max_storage_receipt_list_limit() -> usize {
    10_000
}

pub(crate) fn k_max_metadata_transaction_list_limit() -> usize {
    10_000
}

pub(crate) fn check_payload_read_allowed(
    metadata: &ObjectMetadata,
    now_ms: u64,
) -> Result<(), ObjectError> {
    if metadata.storage_class.is_cold() && metadata.lock.restore_until_ms <= now_ms {
        return Err(ObjectError::PreconditionFailed(format!(
            "object in {} requires restore before payload read",
            metadata.storage_class
        )));
    }
    Ok(())
}

