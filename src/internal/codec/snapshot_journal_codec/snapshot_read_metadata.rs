pub(crate) fn read_metadata_record(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataRecord, ObjectError> {
    Ok(MetadataRecord {
        key: cursor.read_string()?.into(),
        value: cursor.read_string()?,
        version: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
        deleted: cursor.read_bool()?,
    })
}

pub(crate) fn read_metadata_lease(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataLease, ObjectError> {
    Ok(MetadataLease {
        namespace: cursor.read_string()?,
        owner: cursor.read_string()?,
        epoch: cursor.read_u64()?,
        expire_time_ms: cursor.read_u64()?,
        ttl_ms: cursor.read_u64()?,
    })
}

pub(crate) fn read_metadata_lease_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
    has_release_flag: bool,
) -> Result<MetadataLeaseReceipt, ObjectError> {
    Ok(MetadataLeaseReceipt {
        idempotency_key: cursor.read_string()?,
        operation: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        lease: read_metadata_lease(cursor)?,
        released: if has_release_flag {
            cursor.read_bool()?
        } else {
            false
        },
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
        deduped: false,
    })
}

pub(crate) fn read_coordinator_session(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<CoordinatorSession, ObjectError> {
    Ok(CoordinatorSession {
        session_id: cursor.read_string()?,
        owner: cursor.read_string()?,
        endpoint: cursor.read_string()?,
        epoch: cursor.read_u64()?,
        acquire_time_ms: cursor.read_u64()?,
        last_renew_ms: cursor.read_u64()?,
        expire_time_ms: cursor.read_u64()?,
        ttl_ms: cursor.read_u64()?,
    })
}

pub(crate) fn read_metadata_coordinator_config(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataCoordinatorConfig, ObjectError> {
    let backend = cursor.read_string()?;
    let endpoint_count = cursor.read_count("metadata coordinator config endpoint count")?;
    let mut endpoints = Vec::with_capacity(endpoint_count);
    for _ in 0..endpoint_count {
        endpoints.push(cursor.read_string()?);
    }
    let config = MetadataCoordinatorConfig {
        backend: normalize_coordinator_backend(&backend),
        endpoints,
        namespace_path: cursor.read_string()?,
        session_ttl_ms: cursor.read_u64()?,
        revision: cursor.read_u64()?,
    };
    validate_metadata_coordinator_config(&config)?;
    Ok(config)
}

pub(crate) fn read_metadata_coordinator_config_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataCoordinatorConfigReceipt, ObjectError> {
    Ok(MetadataCoordinatorConfigReceipt {
        idempotency_key: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        config: read_metadata_coordinator_config(cursor)?,
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
        deduped: false,
    })
}

pub(crate) fn read_coordinator_session_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<CoordinatorSessionReceipt, ObjectError> {
    Ok(CoordinatorSessionReceipt {
        idempotency_key: cursor.read_string()?,
        operation: cursor.read_string()?,
        request_checksum: cursor.read_string()?,
        session: read_coordinator_session(cursor)?,
        released: cursor.read_bool()?,
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
        deduped: false,
    })
}

pub(crate) fn read_metadata_batch_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataBatchReceipt, ObjectError> {
    let idempotency_key = cursor.read_string()?;
    let request_checksum = cursor.read_string()?;
    let owner_fenced = cursor.read_bool()?;
    let expected_owner = cursor.read_string()?;
    let expected_epoch = cursor.read_u64()?;
    let lease_owner = match cursor.read_string()? {
        owner if owner.is_empty() => None,
        owner => Some(owner),
    };
    let lease_epoch_value = cursor.read_u64()?;
    let lease_epoch = if lease_epoch_value == 0 {
        None
    } else {
        Some(lease_epoch_value)
    };
    let now_ms = cursor.read_u64()?;
    let first_commit_index = cursor.read_u64()?;
    let last_commit_index = cursor.read_u64()?;
    let record_count = cursor.read_count("metadata batch receipt record count")?;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(read_metadata_record(cursor)?);
    }
    Ok(MetadataBatchReceipt {
        idempotency_key,
        request_checksum,
        owner_fenced,
        expected_owner,
        expected_epoch,
        lease_owner,
        lease_epoch,
        now_ms,
        records,
        first_commit_index,
        last_commit_index,
        deduped: false,
    })
}

pub(crate) fn read_metadata_transaction(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataTransaction, ObjectError> {
    let transaction_id = cursor.read_string()?;
    let owner = cursor.read_string()?;
    let expire_time_ms = cursor.read_u64()?;
    let state = cursor.read_string()?;
    let begin_time_ms = cursor.read_u64()?;
    let update_time_ms = cursor.read_u64()?;
    let mutation_count = cursor.read_count("metadata transaction mutation count")?;
    let mut mutations = Vec::with_capacity(mutation_count);
    for _ in 0..mutation_count {
        mutations.push(MetadataMutation {
            key: cursor.read_string()?.into(),
            value: cursor.read_string()?,
            expected_version: cursor.read_u64()?,
            delete: cursor.read_bool()?,
        });
    }
    let mut read_versions = BTreeMap::new();
    for _ in 0..cursor.read_count("metadata transaction read version count")? {
        read_versions.insert(cursor.read_string()?, cursor.read_u64()?);
    }
    Ok(MetadataTransaction {
        transaction_id,
        owner,
        expire_time_ms,
        state,
        mutations,
        read_versions,
        begin_time_ms,
        update_time_ms,
    })
}

pub(crate) fn read_metadata_transaction_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataTransactionReceipt, ObjectError> {
    let idempotency_key = cursor.read_string()?;
    let operation = cursor.read_string()?;
    let request_checksum = cursor.read_string()?;
    let transaction = read_metadata_transaction(cursor)?;
    let record_count = cursor.read_count("metadata transaction receipt record count")?;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(read_metadata_record(cursor)?);
    }
    Ok(MetadataTransactionReceipt {
        idempotency_key,
        operation,
        request_checksum,
        transaction,
        records,
        commit_index: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
        deduped: false,
    })
}

pub(crate) fn read_metadata_shard_image(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataShard, ObjectError> {
    Ok(MetadataShard {
        shard_id: cursor.read_string()?,
        start_key: cursor.read_string()?,
        end_key: cursor.read_string()?,
        owner_node_id: cursor.read_string()?,
        epoch: cursor.read_u64()?,
        draining: cursor.read_bool()?,
    })
}

pub(crate) fn read_metadata_shard_receipt(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataShardReceipt, ObjectError> {
    let idempotency_key = cursor.read_string()?;
    let operation = cursor.read_string()?;
    let request_checksum = cursor.read_string()?;
    let commit_index = cursor.read_u64()?;
    let update_time_ms = cursor.read_u64()?;
    let shard_count = cursor.read_count("metadata shard receipt image count")?;
    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        shards.push(read_metadata_shard_image(cursor)?);
    }
    Ok(MetadataShardReceipt {
        idempotency_key,
        operation,
        request_checksum,
        shards,
        commit_index,
        update_time_ms,
        deduped: false,
    })
}

