// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn read_string_set(
    cursor: &mut SnapshotByteCursor<'_>,
    label: &str,
) -> Result<BTreeSet<String>, ObjectError> {
    let mut values = BTreeSet::new();
    for _ in 0..cursor.read_count(label)? {
        values.insert(cursor.read_string()?);
    }
    Ok(values)
}

pub(crate) fn read_string_list(
    cursor: &mut SnapshotByteCursor<'_>,
    label: &str,
) -> Result<Vec<String>, ObjectError> {
    let count = cursor.read_count(label)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(cursor.read_string()?);
    }
    Ok(values)
}

pub(crate) fn read_string_map(
    cursor: &mut SnapshotByteCursor<'_>,
    label: &str,
) -> Result<BTreeMap<String, String>, ObjectError> {
    let mut values = BTreeMap::new();
    for _ in 0..cursor.read_count(label)? {
        values.insert(cursor.read_string()?, cursor.read_string()?);
    }
    Ok(values)
}

pub(crate) fn read_object_id(cursor: &mut SnapshotByteCursor<'_>) -> Result<ObjectId, ObjectError> {
    Ok(ObjectId {
        bucket: cursor.read_string()?.into(),
        key: cursor.read_string()?.into(),
    })
}

pub(crate) fn read_chunk_id(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkId, ObjectError> {
    Ok(ChunkId {
        volume_id: cursor.read_u64()?,
        id: cursor.read_u64()?,
    })
}

pub(crate) fn read_page_id(cursor: &mut SnapshotByteCursor<'_>) -> Result<PageId, ObjectError> {
    Ok(PageId {
        chunk_id: read_chunk_id(cursor)?,
        page_index: cursor.read_u64()?,
    })
}

pub(crate) fn read_chunk_write_batch_stats(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaWriteBatchStats, ObjectError> {
    Ok(ReplicaWriteBatchStats {
        input_ranges: cursor.read_u64()?,
        input_bytes: cursor.read_u64()?,
        coalesced_ranges: cursor.read_u64()?,
        coalesced_bytes: cursor.read_u64()?,
        physical_write_count: cursor.read_u64()?,
        physical_write_bytes: cursor.read_u64()?,
        invalidated_cache_entries: cursor.read_u64()?,
    })
}

pub(crate) fn read_chunk_discard_batch_stats(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ReplicaDiscardBatchStats, ObjectError> {
    Ok(ReplicaDiscardBatchStats {
        input_ranges: cursor.read_u64()?,
        input_bytes: cursor.read_u64()?,
        coalesced_ranges: cursor.read_u64()?,
        coalesced_bytes: cursor.read_u64()?,
        physical_discard_ranges: cursor.read_u64()?,
        physical_discard_bytes: cursor.read_u64()?,
        invalidated_cache_entries: cursor.read_u64()?,
    })
}

