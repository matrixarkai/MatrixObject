// SPDX-License-Identifier: Apache-2.0
fn export_page_service_snapshot_chunk_slice_with_checksum(
    snapshot_bytes: &[u8],
    full_checksum: &str,
    offset: u64,
    max_payload_bytes: usize,
) -> Result<ChunkServiceSnapshotChunk, ObjectError> {
    if max_payload_bytes == 0 {
        return Err(ObjectError::InvalidArgument(
            "max_payload_bytes must be positive".to_string(),
        ));
    }
    if offset > snapshot_bytes.len() as u64 {
        return Err(ObjectError::InvalidArgument(
            "page service snapshot offset exceeds payload".to_string(),
        ));
    }
    let start = usize::try_from(offset).map_err(|_| {
        ObjectError::InvalidArgument("page service snapshot offset exceeds memory size".to_string())
    })?;
    let remaining = snapshot_bytes.len().saturating_sub(start);
    let emit = remaining.min(max_payload_bytes);
    let end = start.checked_add(emit).ok_or_else(|| {
        ObjectError::InvalidArgument("page service snapshot chunk range overflows".to_string())
    })?;
    let payload_slice = &snapshot_bytes[start..end];
    let total_bytes = snapshot_bytes.len() as u64;
    let page_checksum = if offset == 0 && end == snapshot_bytes.len() {
        full_checksum.to_string()
    } else {
        etag(payload_slice)
    };
    Ok(ChunkServiceSnapshotChunk {
        offset,
        total_bytes,
        page_checksum,
        full_checksum: full_checksum.to_string(),
        eof: end == snapshot_bytes.len(),
        payload: payload_slice.to_vec(),
    })
}

pub(crate) fn default_chunk_endpoints(replica_count: usize) -> Vec<String> {
    (0..replica_count)
        .map(|index| format!("local-chunk-{index}"))
        .collect()
}

fn validate_page_chunk_endpoint(endpoint: &str) -> Result<(), ObjectError> {
    if endpoint.is_empty() || endpoint.len() > 512 {
        return Err(ObjectError::InvalidArgument(
            "chunk endpoint must be 1..512 bytes".to_string(),
        ));
    }
    if endpoint
        .chars()
        .any(|ch| ch.is_control() || ch.is_whitespace())
    {
        return Err(ObjectError::InvalidArgument(
            "chunk endpoint must not contain whitespace or control characters".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_page_chunk_endpoint_topology(
    endpoints: &[String],
    disabled_endpoints: &BTreeSet<usize>,
    write_quorum: usize,
    read_quorum: usize,
    replica_count: usize,
) -> Result<(), ObjectError> {
    if endpoints.len() != replica_count {
        return Err(ObjectError::InvalidArgument(
            "chunk endpoint count must match replica count".to_string(),
        ));
    }
    if endpoints.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "blockserver requires at least one chunk endpoint".to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    for endpoint in endpoints {
        validate_page_chunk_endpoint(endpoint)?;
        if !seen.insert(endpoint.clone()) {
            return Err(ObjectError::InvalidArgument(
                "chunk endpoint list contains duplicates".to_string(),
            ));
        }
    }
    for index in disabled_endpoints {
        if *index >= endpoints.len() {
            return Err(ObjectError::InvalidArgument(
                "disabled chunk endpoint index exceeds endpoint count".to_string(),
            ));
        }
    }
    let enabled = endpoints.len().saturating_sub(disabled_endpoints.len());
    if read_quorum == 0 || read_quorum > endpoints.len() {
        return Err(ObjectError::InvalidArgument(
            "configured read quorum exceeds chunk endpoints".to_string(),
        ));
    }
    if write_quorum == 0 || write_quorum > enabled {
        return Err(ObjectError::InvalidArgument(
            "configured write quorum exceeds writable chunk endpoints".to_string(),
        ));
    }
    Ok(())
}
