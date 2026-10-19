// SPDX-License-Identifier: Apache-2.0
pub(crate) fn read_range(cursor: &mut SnapshotByteCursor<'_>) -> Result<DataRange, ObjectError> {
    Ok(DataRange {
        offset: cursor.read_u64()?,
        length: cursor.read_u64()?,
    })
}

pub(crate) fn read_metadata(
    cursor: &mut SnapshotByteCursor<'_>,
    has_user_metadata: bool,
) -> Result<ObjectMetadata, ObjectError> {
    let object_id = read_object_id(cursor)?;
    let version = cursor.read_u64()?;
    let length = cursor.read_u64()?;
    let etag = cursor.read_string()?;
    let content_type = cursor.read_string()?;
    let user_metadata = if has_user_metadata {
        read_string_map(cursor, "snapshot metadata user metadata count")?
    } else {
        BTreeMap::new()
    };
    let storage_class = cursor.read_string()?;
    let restore_until_ms = cursor.read_u64()?;
    let tags = read_string_map(cursor, "snapshot metadata tag count")?;
    let legal_hold = cursor.read_bool()?;
    let legal_hold_update_time_ms = cursor.read_u64()?;
    let retention_until_ms = cursor.read_u64()?;
    let update_time_ms = cursor.read_u64()?;
    let deleted = cursor.read_bool()?;
    let extent_count = cursor.read_count("snapshot metadata extent count")?;
    let mut extents = Vec::with_capacity(extent_count);
    for _ in 0..extent_count {
        extents.push(ObjectExtent {
            chunk_id: read_chunk_id(cursor)?,
            offset: cursor.read_u64()?,
            length: cursor.read_u64()?,
        });
    }
    Ok(ObjectMetadata {
        object_id,
        version,
        length,
        etag: etag.into(),
        content_type: content_type.into(),
        user_metadata: user_metadata.into(),
        storage_class: interned_storage_class(&storage_class),
        tags: tags.into(),
        lock: ObjectLock::new(
            restore_until_ms,
            legal_hold,
            legal_hold_update_time_ms,
            retention_until_ms,
        ),
        update_time_ms,
        extents: extents.into(),
        deleted,
    })
}

pub(crate) fn read_raft_apply_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_user_metadata: bool,
) -> Result<RaftApplyReceipt, ObjectError> {
    Ok(RaftApplyReceipt {
        index: cursor.read_u64()?,
        term: cursor.read_u64()?,
        command_checksum: cursor.read_string()?,
        result: read_metadata(cursor, has_user_metadata)?,
    })
}

pub(crate) fn read_raft_membership_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<RaftMembershipReceipt, ObjectError> {
    let mutation_id = cursor.read_string()?;
    let voter_count = cursor.read_count("snapshot raft membership voter count")?;
    let mut target_voter_node_ids = Vec::with_capacity(voter_count);
    for _ in 0..voter_count {
        target_voter_node_ids.push(cursor.read_string()?);
    }
    Ok(RaftMembershipReceipt {
        mutation_id,
        target_voter_node_ids,
        target_voter_checksum: cursor.read_string()?,
        target_healthy_peer_count: cursor.read_u64()?,
        expected_previous_commit_index: cursor.read_u64()?,
        membership_epoch: cursor.read_u64()?,
        command_checksum: cursor.read_string()?,
    })
}

pub(crate) fn read_chunk_info(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ChunkInfo, ObjectError> {
    let chunk_id = read_chunk_id(cursor)?;
    let logical_bytes = cursor.read_u64()?;
    let live_bytes = cursor.read_u64()?;
    let stale_bytes = cursor.read_u64()?;
    let discarded_bytes = cursor.read_u64()?;
    let next_offset = cursor.read_u64()?;
    let stale_range_count = cursor.read_count("snapshot segment stale range count")?;
    let mut stale_ranges = Vec::with_capacity(stale_range_count);
    for _ in 0..stale_range_count {
        stale_ranges.push(read_range(cursor)?);
    }
    let discarded_range_count = cursor.read_count("snapshot segment discarded range count")?;
    let mut discarded_ranges = Vec::with_capacity(discarded_range_count);
    for _ in 0..discarded_range_count {
        discarded_ranges.push(read_range(cursor)?);
    }
    Ok(ChunkInfo {
        chunk_id,
        logical_bytes,
        live_bytes,
        stale_bytes,
        discarded_bytes,
        next_offset,
        stale_ranges,
        discarded_ranges,
    })
}

pub(crate) fn read_put_options(
    cursor: &mut SnapshotByteCursor<'_>,
    has_expected_checksum: bool,
    has_if_match_etags: bool,
    has_user_metadata: bool,
) -> Result<PutOptions, ObjectError> {
    let if_none_match = cursor.read_bool()?;
    let if_match_etag = empty_string_to_none(cursor.read_string()?);
    let expected_checksum = if has_expected_checksum {
        empty_string_to_none(cursor.read_string()?)
    } else {
        None
    };
    let lease_owner = empty_string_to_none(cursor.read_string()?);
    let lease_epoch_raw = cursor.read_u64()?;
    let user_metadata = if has_user_metadata {
        read_string_map(cursor, "snapshot put option user metadata count")?
    } else {
        BTreeMap::new()
    };
    let tags = read_string_map(cursor, "snapshot put option tag count")?;
    let storage_class = empty_string_to_none(cursor.read_string()?);
    let if_match_etags = if has_if_match_etags {
        read_string_list(cursor, "snapshot put option if-match etag count")?
    } else {
        Vec::new()
    };
    let options = PutOptions {
        if_none_match,
        if_match_etag,
        if_match_etags,
        expected_checksum,
        lease_owner,
        lease_epoch: if lease_epoch_raw == 0 {
            None
        } else {
            Some(lease_epoch_raw)
        },
        user_metadata,
        tags,
        storage_class,
        min_read_index: 0,
        min_commit_index: 0,
    };
    validate_user_metadata(&options.user_metadata)?;
    validate_tags(&options.tags)?;
    Ok(options)
}

