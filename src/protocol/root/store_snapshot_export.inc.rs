// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn export_store_snapshot_chunk_bytes_with_checksum(
    snapshot_bytes: &[u8],
    full_checksum: &str,
    last_applied_index: u64,
    term: u64,
    offset: u64,
    max_payload_bytes: usize,
) -> Result<StoreSnapshotChunk, ObjectError> {
    if last_applied_index > 0 && term == 0 {
        return Err(ObjectError::InvalidArgument(
            "store snapshot term is required with applied index".to_string(),
        ));
    }
    if max_payload_bytes == 0 {
        return Err(ObjectError::InvalidArgument(
            "store snapshot chunk budget must be positive".to_string(),
        ));
    }
    let total_bytes = snapshot_bytes.len() as u64;
    if offset > total_bytes {
        return Err(ObjectError::InvalidArgument(
            "store snapshot chunk offset exceeds image size".to_string(),
        ));
    }
    let remaining = (total_bytes - offset) as usize;
    let emit = remaining.min(max_payload_bytes);
    let start = offset as usize;
    let payload_slice = &snapshot_bytes[start..start + emit];
    let page_checksum = if offset == 0 && emit as u64 == total_bytes {
        full_checksum.to_string()
    } else {
        etag(payload_slice)
    };
    Ok(StoreSnapshotChunk {
        offset,
        total_bytes,
        page_checksum,
        full_checksum: full_checksum.to_string(),
        last_applied_index,
        term,
        eof: offset + emit as u64 >= total_bytes,
        payload: payload_slice.to_vec(),
    })
}
