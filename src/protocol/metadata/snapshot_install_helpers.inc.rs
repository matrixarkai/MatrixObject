// SPDX-License-Identifier: Apache-2.0
pub(crate) fn export_meta_snapshot_chunk_slice_with_checksum(
    snapshot_bytes: &[u8],
    full_checksum: &str,
    last_applied_index: u64,
    offset: u64,
    max_payload_bytes: usize,
) -> Result<MetaSnapshotChunk, ObjectError> {
    if last_applied_index == 0 {
        return Err(ObjectError::InvalidArgument(
            "metadata snapshot last applied index must be positive".to_string(),
        ));
    }
    if max_payload_bytes == 0 {
        return Err(ObjectError::InvalidArgument(
            "metadata snapshot chunk budget must be positive".to_string(),
        ));
    }
    let total_bytes = snapshot_bytes.len() as u64;
    if offset > total_bytes {
        return Err(ObjectError::InvalidArgument(
            "metadata snapshot chunk offset exceeds image size".to_string(),
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
    Ok(MetaSnapshotChunk {
        offset,
        total_bytes,
        page_checksum,
        full_checksum: full_checksum.to_string(),
        last_applied_index,
        eof: offset + emit as u64 >= total_bytes,
        payload: payload_slice.to_vec(),
    })
}

pub(crate) fn snapshot_install_progress(
    resume_supported: bool,
    stream_active: bool,
    next_offset: u64,
    total_bytes: u64,
    full_checksum: String,
    last_applied_index: u64,
    term: u64,
) -> SnapshotInstallProgress {
    let remaining_bytes = total_bytes.saturating_sub(next_offset);
    let stream_id = if total_bytes == 0 || full_checksum.is_empty() {
        String::new()
    } else {
        let mut material = Vec::with_capacity(
            std::mem::size_of::<u64>() * 3 + encoded_string_len(&full_checksum),
        );
        append_u64(&mut material, total_bytes);
        append_string(&mut material, &full_checksum);
        append_u64(&mut material, last_applied_index);
        append_u64(&mut material, term);
        etag(&material)
    };
    SnapshotInstallProgress {
        resume_supported,
        stream_active,
        next_offset,
        remaining_bytes,
        stream_id,
        total_bytes,
        last_applied_index,
        term,
        full_checksum,
    }
}

pub(crate) fn metadata_snapshot_install_report(
    snapshot: &MetaSnapshot,
    snapshot_bytes: u64,
    snapshot_checksum: String,
    installed: bool,
    raft_snapshot_install: bool,
) -> MetadataSnapshotInstallReport {
    let record_count = snapshot.records.len() as u64;
    let deleted_record_count = snapshot
        .records
        .values()
        .filter(|record| record.deleted)
        .count() as u64;
    let last_applied_index = snapshot.next_commit_index.saturating_sub(1);
    MetadataSnapshotInstallReport {
        installed,
        validated: true,
        raft_snapshot_install,
        snapshot_bytes,
        snapshot_checksum: snapshot_checksum.clone(),
        last_applied_index,
        next_log_index: snapshot.next_commit_index,
        record_count,
        live_record_count: record_count.saturating_sub(deleted_record_count),
        deleted_record_count,
        metadata_lease_count: snapshot.leases.len() as u64,
        applied_mutation_count: snapshot.batch_request_receipts.len() as u64,
        change_log_count: snapshot.changes.len() as u64,
        metadata_shard_count: snapshot.shards.len() as u64,
        install_progress: snapshot_install_progress(
            true,
            false,
            snapshot_bytes,
            snapshot_bytes,
            snapshot_checksum,
            last_applied_index,
            0,
        ),
    }
}

pub(crate) fn abort_snapshot_install_progress(
    progress: SnapshotInstallProgress,
    expected_stream_id: Option<&str>,
) -> Result<SnapshotAbortReport, ObjectError> {
    if let Some(expected) = expected_stream_id {
        if !expected.is_empty() && expected != progress.stream_id {
            return Err(ObjectError::InvalidArgument(
                "snapshot install stream id mismatch".to_string(),
            ));
        }
    }
    Ok(SnapshotAbortReport {
        aborted: progress.stream_active || progress.next_offset > 0,
        previous_stream_active: progress.stream_active,
        previous_next_offset: progress.next_offset,
        previous_remaining_bytes: progress.remaining_bytes,
        previous_stream_id: progress.stream_id,
        cleared_progress: snapshot_install_progress(
            progress.resume_supported,
            false,
            0,
            0,
            String::new(),
            0,
            0,
        ),
    })
}
