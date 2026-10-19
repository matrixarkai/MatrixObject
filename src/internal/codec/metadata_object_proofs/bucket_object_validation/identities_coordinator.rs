// SPDX-License-Identifier: Apache-2.0
pub(crate) fn validate_meta_key(key: &str) -> Result<(), ObjectError> {
    if key.is_empty() || key.len() > 8192 {
        return Err(ObjectError::InvalidArgument(
            "metadata key must be 1..8192 bytes".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_changefeed_consumer_id(consumer_id: &str) -> Result<(), ObjectError> {
    if consumer_id.is_empty()
        || consumer_id.len() > 256
        || consumer_id.contains('\0')
        || consumer_id.contains('/')
        || consumer_id.contains('\\')
        || consumer_id.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "object changefeed consumer id must be 1..256 bytes without path separators"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_upload_id(upload_id: &str) -> Result<(), ObjectError> {
    if upload_id.is_empty()
        || upload_id.len() > 256
        || upload_id.contains('\0')
        || upload_id.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "multipart upload id must be 1..256 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_idempotency_key(idempotency_key: &str) -> Result<(), ObjectError> {
    if idempotency_key.is_empty()
        || idempotency_key.len() > 256
        || idempotency_key.contains('\0')
        || idempotency_key.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "idempotency key must be 1..256 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_session_id(session_id: &str) -> Result<(), ObjectError> {
    if session_id.is_empty()
        || session_id.len() > 256
        || session_id.contains('\0')
        || session_id.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "coordinator session id must be 1..256 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_session_endpoint(endpoint: &str) -> Result<(), ObjectError> {
    if endpoint.is_empty()
        || endpoint.len() > 4096
        || endpoint.contains('\0')
        || endpoint.contains('\n')
    {
        return Err(ObjectError::InvalidArgument(
            "coordinator endpoint must be 1..4096 bytes without control separators".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_coordinator_backend_token(backend: &str) -> String {
    backend.trim().to_ascii_lowercase().replace('_', "-")
}

pub(crate) fn is_embedded_coordinator_backend(backend: &str) -> bool {
    matches!(
        canonical_coordinator_backend_token(backend).as_str(),
        "" | "embedded" | "embedded-raft" | "raft" | "matrix-raft"
    )
}

pub(crate) fn normalize_coordinator_backend(backend: &str) -> String {
    let canonical = canonical_coordinator_backend_token(backend);
    if is_embedded_coordinator_backend(&canonical) {
        "embedded-raft".to_string()
    } else if canonical == "zk" || canonical == "zoo-keeper" {
        "zookeeper".to_string()
    } else {
        canonical
    }
}

pub(crate) fn is_supported_coordinator_backend(backend: &str) -> bool {
    matches!(
        normalize_coordinator_backend(backend).as_str(),
        "embedded-raft" | "etcd" | "zookeeper"
    )
}

pub(crate) fn coordinator_policy_format() -> &'static str {
    "MXOS-COORDINATOR-POLICY1"
}
