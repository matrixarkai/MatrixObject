impl Default for MatrixReplicaService {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn validate_storage_receipt_retention(retention: usize) -> Result<(), ObjectError> {
    if retention == 0 || retention > MAX_STORAGE_RECEIPT_RETENTION {
        return Err(ObjectError::InvalidArgument(format!(
            "storage receipt retention must be 1..{}",
            MAX_STORAGE_RECEIPT_RETENTION
        )));
    }
    Ok(())
}

fn chunk_rle_encoded_len(data: &[u8]) -> usize {
    if data.is_empty() {
        return 0;
    }
    let mut encoded_len = 0usize;
    let mut previous = data[0];
    for &byte in &data[1..] {
        if byte == previous {
            continue;
        } else {
            encoded_len = encoded_len.saturating_add(9);
            previous = byte;
        }
    }
    encoded_len.saturating_add(9)
}

fn chunk_compression_stats(state: &PageState) -> (u64, u64) {
    let raw_len = state.data.len();
    if raw_len == 0 {
        return (0, 0);
    }
    let encoded_len = chunk_rle_encoded_len(&state.data);
    if encoded_len < raw_len {
        (1, (raw_len - encoded_len) as u64)
    } else {
        (0, 0)
    }
}

fn chunk_map_compression_stats(
    pages: &BTreeMap<PageId, std::sync::Arc<PageState>>,
) -> (u64, u64) {
    pages.values().fold((0u64, 0u64), |(count, bytes), state| {
        let (page_count, saved_bytes) = chunk_compression_stats(state);
        (
            count.saturating_add(page_count),
            bytes.saturating_add(saved_bytes),
        )
    })
}

pub(crate) fn trim_btree_map_to_retention<K: Clone + Ord, V>(
    map: &mut BTreeMap<K, V>,
    retention: usize,
) -> u64 {
    let overflow = map.len().saturating_sub(retention);
    if overflow == 0 {
        return 0;
    }
    let keys = map.keys().take(overflow).cloned().collect::<Vec<_>>();
    for key in keys {
        map.remove(&key);
    }
    overflow as u64
}

pub(crate) fn trim_vec_front_to_retention<T>(items: &mut Vec<T>, retention: usize) -> u64 {
    let overflow = items.len().saturating_sub(retention);
    if overflow > 0 {
        items.drain(0..overflow);
    }
    overflow as u64
}

pub(crate) fn validate_meta_retention_limit(
    field: &str,
    retention: usize,
) -> Result<(), ObjectError> {
    if retention == 0 || retention > MAX_META_RETENTION_LIMIT {
        return Err(ObjectError::InvalidArgument(format!(
            "{field} must be 1..{MAX_META_RETENTION_LIMIT}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_metadata_admission_policy(
    policy: &MetadataAdmissionPolicy,
) -> Result<(), ObjectError> {
    for (field, value) in [
        ("max_batch_mutations", policy.max_batch_mutations),
        ("max_open_transactions", policy.max_open_transactions),
        ("max_namespace_leases", policy.max_namespace_leases),
        ("max_coordinator_sessions", policy.max_coordinator_sessions),
        ("max_shards", policy.max_shards),
    ] {
        if value == 0 || value > MAX_META_ADMISSION_LIMIT {
            return Err(ObjectError::InvalidArgument(format!(
                "metadata admission {field} must be 1..{MAX_META_ADMISSION_LIMIT}"
            )));
        }
    }
    if policy.max_batch_value_bytes == 0
        || policy.max_batch_value_bytes > MAX_META_BATCH_VALUE_BYTES
    {
        return Err(ObjectError::InvalidArgument(format!(
            "metadata admission max_batch_value_bytes must be 1..{MAX_META_BATCH_VALUE_BYTES}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_metadata_qos_config(config: &MetadataQosConfig) -> Result<(), ObjectError> {
    let enabled = config.max_write_ops_per_window > 0 || config.max_write_bytes_per_window > 0;
    if enabled && config.window_ms == 0 {
        return Err(ObjectError::InvalidArgument(
            "metadata qos window_ms must be positive when limits are enabled".to_string(),
        ));
    }
    if config.window_ms > MAX_META_QOS_WINDOW_MS {
        return Err(ObjectError::InvalidArgument(format!(
            "metadata qos window_ms must be <= {MAX_META_QOS_WINDOW_MS}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_metadata_route_cache_config(
    config: &MetadataRouteCacheConfig,
) -> Result<(), ObjectError> {
    if config.entries == 0 || config.entries > MAX_METADATA_ROUTE_CACHE_ENTRIES {
        return Err(ObjectError::InvalidArgument(format!(
            "metadata route cache entries must be 1..{MAX_METADATA_ROUTE_CACHE_ENTRIES}"
        )));
    }
    if config.ttl_ms == 0 || config.ttl_ms > MAX_METADATA_ROUTE_CACHE_TTL_MS {
        return Err(ObjectError::InvalidArgument(format!(
            "metadata route cache ttl_ms must be 1..{MAX_METADATA_ROUTE_CACHE_TTL_MS}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_page_route_qos_config(
    config: &ChunkRouteQosConfig,
) -> Result<(), ObjectError> {
    let enabled = config.max_read_ops_per_window > 0
        || config.max_read_bytes_per_window > 0
        || config.max_write_ops_per_window > 0
        || config.max_write_bytes_per_window > 0
        || config.max_discard_ops_per_window > 0
        || config.max_discard_bytes_per_window > 0;
    if enabled && config.window_ms == 0 {
        return Err(ObjectError::InvalidArgument(
            "block route qos window_ms must be positive when limits are enabled".to_string(),
        ));
    }
    if config.window_ms > MAX_PAGE_ROUTE_QOS_WINDOW_MS {
        return Err(ObjectError::InvalidArgument(format!(
            "block route qos window_ms must be <= {MAX_PAGE_ROUTE_QOS_WINDOW_MS}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_chunk_mutation_qos_config(
    config: &ReplicaMutationQosConfig,
) -> Result<(), ObjectError> {
    let enabled = config.max_write_ops_per_window > 0
        || config.max_write_bytes_per_window > 0
        || config.max_discard_ops_per_window > 0
        || config.max_discard_bytes_per_window > 0;
    if enabled && config.window_ms == 0 {
        return Err(ObjectError::InvalidArgument(
            "chunk mutation qos window_ms must be positive when limits are enabled".to_string(),
        ));
    }
    if config.window_ms > MAX_CHUNK_MUTATION_QOS_WINDOW_MS {
        return Err(ObjectError::InvalidArgument(format!(
            "chunk mutation qos window_ms must be <= {MAX_CHUNK_MUTATION_QOS_WINDOW_MS}"
        )));
    }
    Ok(())
}
