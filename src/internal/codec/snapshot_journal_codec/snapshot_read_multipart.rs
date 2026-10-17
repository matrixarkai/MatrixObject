pub(crate) fn read_multipart_upload(
    cursor: &mut SnapshotByteCursor<'_>,
    store_snapshot_version: u64,
) -> Result<MultipartUpload, ObjectError> {
    let upload_id = cursor.read_string()?;
    let bucket = cursor.read_string()?;
    let key = cursor.read_string()?;
    let content_type = cursor.read_string()?;
    let create_idempotency_key = if store_snapshot_version >= 22 {
        if cursor.read_u64()? == 0 {
            None
        } else {
            Some(cursor.read_string()?)
        }
    } else {
        None
    };
    let options = read_put_options(
        cursor,
        store_snapshot_version >= 8,
        store_snapshot_version >= 15,
        store_snapshot_version >= 21,
    )?;
    let create_time_ms = cursor.read_u64()?;
    let update_time_ms = cursor.read_u64()?;
    let next_part_number = cursor.read_u64()? as u32;
    let total_bytes = cursor.read_u64()?;
    let mut parts = BTreeMap::new();
    let mut part_payloads = BTreeMap::new();
    for _ in 0..cursor.read_count("snapshot multipart part count")? {
        let map_part_number = cursor.read_u64()? as u32;
        let part = MultipartPart {
            part_number: cursor.read_u64()? as u32,
            length: cursor.read_u64()?,
            etag: cursor.read_string()?,
            update_time_ms: cursor.read_u64()?,
            idempotency_key: if store_snapshot_version >= 23 {
                if cursor.read_u64()? == 0 {
                    None
                } else {
                    Some(cursor.read_string()?)
                }
            } else {
                None
            },
        };
        let payload_len =
            bounded_snapshot_len(cursor.read_u64()?, "snapshot multipart part payload bytes")?;
        let payload = cursor.read_bytes(payload_len)?.to_vec();
        parts.insert(map_part_number, part);
        part_payloads.insert(map_part_number, payload);
    }
    Ok(MultipartUpload {
        upload_id,
        bucket,
        key,
        content_type,
        create_idempotency_key,
        options,
        create_time_ms,
        update_time_ms,
        next_part_number,
        total_bytes,
        parts,
        part_payloads,
    })
}

pub(crate) fn read_multipart_complete_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_user_metadata: bool,
) -> Result<MultipartCompleteReceipt, ObjectError> {
    Ok(MultipartCompleteReceipt {
        idempotency_key: cursor.read_string()?,
        upload_id: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        metadata: read_metadata(cursor, has_user_metadata)?,
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
    })
}

pub(crate) fn read_multipart_abort_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    store_snapshot_version: u64,
) -> Result<MultipartAbortReceipt, ObjectError> {
    Ok(MultipartAbortReceipt {
        idempotency_key: cursor.read_string()?,
        upload_id: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        upload: read_multipart_upload(cursor, store_snapshot_version)?,
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
    })
}

