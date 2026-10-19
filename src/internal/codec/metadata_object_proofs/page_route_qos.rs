// SPDX-License-Identifier: Apache-2.0
pub(crate) fn append_page_route_qos_config(out: &mut Vec<u8>, config: &ChunkRouteQosConfig) {
    append_u64(out, config.window_ms);
    append_u64(out, config.max_read_ops_per_window);
    append_u64(out, config.max_read_bytes_per_window);
    append_u64(out, config.max_write_ops_per_window);
    append_u64(out, config.max_write_bytes_per_window);
    append_u64(out, config.max_discard_ops_per_window);
    append_u64(out, config.max_discard_bytes_per_window);
}

pub(crate) fn read_page_route_qos_config(
    cursor: &mut SnapshotByteCursor<'_>,
    has_read_limits: bool,
) -> Result<ChunkRouteQosConfig, ObjectError> {
    let config = ChunkRouteQosConfig {
        window_ms: cursor.read_u64()?,
        max_read_ops_per_window: if has_read_limits {
            cursor.read_u64()?
        } else {
            0
        },
        max_read_bytes_per_window: if has_read_limits {
            cursor.read_u64()?
        } else {
            0
        },
        max_write_ops_per_window: cursor.read_u64()?,
        max_write_bytes_per_window: cursor.read_u64()?,
        max_discard_ops_per_window: cursor.read_u64()?,
        max_discard_bytes_per_window: cursor.read_u64()?,
    };
    validate_page_route_qos_config(&config)?;
    Ok(config)
}

pub(crate) fn append_page_route_qos_state(out: &mut Vec<u8>, state: &ChunkRouteQosState) {
    append_u64(out, state.window_start_ms);
    append_u64(out, state.read_ops_in_window);
    append_u64(out, state.read_bytes_in_window);
    append_u64(out, state.write_ops_in_window);
    append_u64(out, state.write_bytes_in_window);
    append_u64(out, state.discard_ops_in_window);
    append_u64(out, state.discard_bytes_in_window);
    append_u64(out, state.throttled_ops);
    append_u64(out, state.throttled_bytes);
}

pub(crate) fn read_page_route_qos_state(
    cursor: &mut SnapshotByteCursor<'_>,
    has_read_limits: bool,
) -> Result<ChunkRouteQosState, ObjectError> {
    Ok(ChunkRouteQosState {
        window_start_ms: cursor.read_u64()?,
        read_ops_in_window: if has_read_limits {
            cursor.read_u64()?
        } else {
            0
        },
        read_bytes_in_window: if has_read_limits {
            cursor.read_u64()?
        } else {
            0
        },
        write_ops_in_window: cursor.read_u64()?,
        write_bytes_in_window: cursor.read_u64()?,
        discard_ops_in_window: cursor.read_u64()?,
        discard_bytes_in_window: cursor.read_u64()?,
        throttled_ops: cursor.read_u64()?,
        throttled_bytes: cursor.read_u64()?,
    })
}
