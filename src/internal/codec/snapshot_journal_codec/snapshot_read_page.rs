// SPDX-License-Identifier: Apache-2.0
pub(crate) fn read_page_write_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkWriteReceipt, ObjectError> {
    Ok(ChunkWriteReceipt {
        chunk_id: read_chunk_id(cursor)?,
        offset: cursor.read_u64()?,
        length: cursor.read_u64()?,
        request_checksum: cursor.read_string()?,
        payload_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        deduped: false,
    })
}

pub(crate) fn read_page_batch_write_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_metadata_head_stats: bool,
) -> Result<ChunkBatchWriteReceipt, ObjectError> {
    Ok(ChunkBatchWriteReceipt {
        request_checksum: cursor.read_string()?,
        payload_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        stats: read_page_batch_stats(cursor, has_metadata_head_stats)?,
        deduped: false,
    })
}

pub(crate) fn read_page_discard_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkDiscardReceipt, ObjectError> {
    Ok(ChunkDiscardReceipt {
        chunk_id: read_chunk_id(cursor)?,
        offset: cursor.read_u64()?,
        length: cursor.read_u64()?,
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        deduped: false,
    })
}

pub(crate) fn read_page_read_stats(
    cursor: &mut SnapshotByteCursor<'_>,
    has_metadata_head_stats: bool,
) -> Result<ChunkReadStats, ObjectError> {
    let chunk_reads = cursor.read_u64()?;
    let cache_hits = cursor.read_u64()?;
    let coalesced_chunk_read_count = cursor.read_u64()?;
    let physical_chunk_read_count = cursor.read_u64()?;
    let (
        metadata_head_batch_requests,
        metadata_physical_head_count,
        metadata_coalesced_head_count,
        metadata_cache_hits,
        metadata_cache_misses,
    ) = if has_metadata_head_stats {
        (
            cursor.read_u64()?,
            cursor.read_u64()?,
            cursor.read_u64()?,
            cursor.read_u64()?,
            cursor.read_u64()?,
        )
    } else {
        (0, 0, 0, 0, 0)
    };
    Ok(ChunkReadStats {
        chunk_reads,
        cache_hits,
        coalesced_chunk_read_count,
        physical_chunk_read_count,
        metadata_head_batch_requests,
        metadata_physical_head_count,
        metadata_coalesced_head_count,
        metadata_cache_hits,
        metadata_cache_misses,
        replica_fallback_reads: cursor.read_u64()?,
        quorum_verified_chunks: cursor.read_u64()?,
        replica_read_repairs: cursor.read_u64()?,
        replica_read_repair_failures: cursor.read_u64()?,
        quorum_read_degraded_chunks: cursor.read_u64()?,
    })
}

pub(crate) fn read_page_batch_stats(
    cursor: &mut SnapshotByteCursor<'_>,
    has_metadata_head_stats: bool,
) -> Result<ChunkBatchStats, ObjectError> {
    Ok(ChunkBatchStats {
        input_ranges: cursor.read_u64()?,
        mapped_chunk_ranges: cursor.read_u64()?,
        coalesced_chunk_ranges: cursor.read_u64()?,
        physical_chunk_write_count: cursor.read_u64()?,
        physical_chunk_write_bytes: cursor.read_u64()?,
        physical_chunk_discard_ranges: cursor.read_u64()?,
        physical_chunk_discard_bytes: cursor.read_u64()?,
        payload_bytes: cursor.read_u64()?,
        read_stats: read_page_read_stats(cursor, has_metadata_head_stats)?,
    })
}

pub(crate) fn read_page_batch_discard_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_metadata_head_stats: bool,
) -> Result<ChunkBatchDiscardReceipt, ObjectError> {
    Ok(ChunkBatchDiscardReceipt {
        request_checksum: cursor.read_string()?,
        idempotency_key: cursor.read_string()?,
        stats: read_page_batch_stats(cursor, has_metadata_head_stats)?,
        deduped: false,
    })
}

