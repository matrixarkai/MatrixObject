// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_metadata_change_checkpoint(
    out: &mut Vec<u8>,
    checkpoint: &MetadataChangeCheckpoint,
) {
    append_string(out, &checkpoint.consumer_id);
    append_string(out, &checkpoint.prefix);
    append_u64(out, checkpoint.after_change_sequence);
    append_u64(out, checkpoint.after_commit_index);
    append_u64(out, checkpoint.update_time_ms);
    append_string(out, &checkpoint.owner);
    append_string(out, &checkpoint.note);
}

pub(crate) fn append_metadata_admission_policy(
    out: &mut Vec<u8>,
    policy: &MetadataAdmissionPolicy,
) {
    append_u64(out, policy.max_batch_mutations as u64);
    append_u64(out, policy.max_batch_value_bytes);
    append_u64(out, policy.max_open_transactions as u64);
    append_u64(out, policy.max_namespace_leases as u64);
    append_u64(out, policy.max_coordinator_sessions as u64);
    append_u64(out, policy.max_shards as u64);
}

pub(crate) fn read_metadata_admission_policy(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataAdmissionPolicy, ObjectError> {
    let policy = MetadataAdmissionPolicy {
        max_batch_mutations: cursor.read_count("metadata admission max batch mutations")?,
        max_batch_value_bytes: cursor.read_u64()?,
        max_open_transactions: cursor.read_count("metadata admission max open transactions")?,
        max_namespace_leases: cursor.read_count("metadata admission max namespace leases")?,
        max_coordinator_sessions: cursor
            .read_count("metadata admission max coordinator sessions")?,
        max_shards: cursor.read_count("metadata admission max shards")?,
    };
    validate_metadata_admission_policy(&policy)?;
    Ok(policy)
}

pub(crate) fn append_metadata_qos_config(out: &mut Vec<u8>, config: &MetadataQosConfig) {
    append_u64(out, config.window_ms);
    append_u64(out, config.max_write_ops_per_window);
    append_u64(out, config.max_write_bytes_per_window);
}

pub(crate) fn read_metadata_qos_config(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataQosConfig, ObjectError> {
    let config = MetadataQosConfig {
        window_ms: cursor.read_u64()?,
        max_write_ops_per_window: cursor.read_u64()?,
        max_write_bytes_per_window: cursor.read_u64()?,
    };
    validate_metadata_qos_config(&config)?;
    Ok(config)
}

pub(crate) fn append_metadata_qos_state(out: &mut Vec<u8>, state: &MetadataQosState) {
    append_u64(out, state.window_start_ms);
    append_u64(out, state.write_ops_in_window);
    append_u64(out, state.write_bytes_in_window);
    append_u64(out, state.throttled_write_ops);
    append_u64(out, state.throttled_write_bytes);
}

pub(crate) fn read_metadata_qos_state(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<MetadataQosState, ObjectError> {
    Ok(MetadataQosState {
        window_start_ms: cursor.read_u64()?,
        write_ops_in_window: cursor.read_u64()?,
        write_bytes_in_window: cursor.read_u64()?,
        throttled_write_ops: cursor.read_u64()?,
        throttled_write_bytes: cursor.read_u64()?,
    })
}
