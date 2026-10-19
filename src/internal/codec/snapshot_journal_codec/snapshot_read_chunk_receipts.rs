pub(crate) fn read_chunk_batch_write_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaBatchWriteReceipt, ObjectError> {
    Ok(ReplicaBatchWriteReceipt {
        chunk_id: read_chunk_id(cursor)?,
        request_checksum: cursor.read_string()?,
        payload_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        stats: read_chunk_write_batch_stats(cursor)?,
        deduped: false,
    })
}

pub(crate) fn read_chunk_write_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaWriteReceipt, ObjectError> {
    Ok(ReplicaWriteReceipt {
        chunk_id: read_chunk_id(cursor)?,
        offset: cursor.read_u64()?,
        length: cursor.read_u64()?,
        request_checksum: cursor.read_string()?,
        payload_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        deduped: false,
    })
}

pub(crate) fn read_chunk_discard_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaDiscardReceipt, ObjectError> {
    Ok(ReplicaDiscardReceipt {
        chunk_id: read_chunk_id(cursor)?,
        offset: cursor.read_u64()?,
        length: cursor.read_u64()?,
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        deduped: false,
    })
}

pub(crate) fn read_chunk_batch_discard_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaBatchDiscardReceipt, ObjectError> {
    Ok(ReplicaBatchDiscardReceipt {
        chunk_id: read_chunk_id(cursor)?,
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        stats: read_chunk_discard_batch_stats(cursor)?,
        deduped: false,
    })
}

pub(crate) fn read_chunk_state_install_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaStateInstallReceipt, ObjectError> {
    Ok(ReplicaStateInstallReceipt {
        chunk_id: read_chunk_id(cursor)?,
        request_checksum: cursor.read_string()?,
        payload_checksum: cursor.read_string()?,
        state_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        logical_length: cursor.read_u64()?,
        write_sequence: cursor.read_u64()?,
        discard_range_count: cursor.read_u64()?,
        deduped: false,
    })
}

pub(crate) fn read_chunk_partial_reclaim_report(
    cursor: &mut SnapshotByteCursor<'_>,
    snapshot_version: u64,
) -> Result<ReplicaPartialReclaimReport, ObjectError> {
    Ok(ReplicaPartialReclaimReport {
        scanned_chunks: cursor.read_u64()?,
        candidate_chunks: cursor.read_u64()?,
        reclaimed_chunks: cursor.read_u64()?,
        skipped_chunks: cursor.read_u64()?,
        reclaimed_discard_bytes: cursor.read_u64()?,
        rewritten_bytes: cursor.read_u64()?,
        trimmed_bytes: cursor.read_u64()?,
        skipped_rewrite_bytes: cursor.read_u64()?,
        remaining_discard_bytes: cursor.read_u64()?,
        budget_limited: cursor.read_u64()? != 0,
        trim_flow_limited: cursor.read_u64()? != 0,
        requested_trim_bytes: cursor.read_u64()?,
        effective_trim_bytes: cursor.read_u64()?,
        trim_flow_available_bytes: cursor.read_u64()?,
        trim_flow_throttled_bytes: cursor.read_u64()?,
        trim_flow_throttled_ops: if snapshot_version >= 17 {
            cursor.read_u64()?
        } else {
            0
        },
        partial_reclaim_failures: if snapshot_version >= 26 {
            cursor.read_u64()?
        } else {
            0
        },
        last_partial_reclaim_failure_status: if snapshot_version >= 27 {
            cursor.read_string()?
        } else {
            "ok".to_string()
        },
    })
}

pub(crate) fn read_chunk_reclaim_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    snapshot_version: u64,
) -> Result<ReplicaReclaimReceipt, ObjectError> {
    Ok(ReplicaReclaimReceipt {
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        max_chunks: cursor.read_u64()?,
        max_rewrite_bytes: cursor.read_u64()?,
        min_discarded_bytes: cursor.read_u64()?,
        min_discard_ratio_ppm: cursor.read_u64()?,
        max_discard_ranges: cursor.read_u64()?,
        max_trim_bytes: cursor.read_u64()?,
        report: read_chunk_partial_reclaim_report(cursor, snapshot_version)?,
        deduped: false,
    })
}

