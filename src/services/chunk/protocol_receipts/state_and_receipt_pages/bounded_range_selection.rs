pub(crate) fn select_bounded_chunk_write_ranges_from(
    writes: Vec<(u64, Vec<u8>)>,
    max_write_bytes: u64,
    write_start_range_index: u64,
) -> Result<Vec<(u64, Vec<u8>)>, ObjectError> {
    let requested_start = write_start_range_index as usize;
    let mut selected = Vec::with_capacity(writes.len().saturating_sub(requested_start));
    let mut selected_bytes = 0u64;
    let mut non_empty_index = 0usize;
    for (offset, data) in writes {
        if data.is_empty() {
            continue;
        }
        if non_empty_index < requested_start {
            non_empty_index += 1;
            continue;
        }
        non_empty_index += 1;
        validate_pipeline_range("chunk write", offset, data.len() as u64)?;
        if max_write_bytes > 0
            && selected_bytes
                .checked_add(data.len() as u64)
                .map_or(true, |bytes| bytes > max_write_bytes)
        {
            break;
        }
        selected_bytes = selected_bytes
            .checked_add(data.len() as u64)
            .ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "chunk bounded batch write byte count overflows".to_string(),
                )
            })?;
        selected.push((offset, data));
    }
    if selected.is_empty() && requested_start > non_empty_index {
        return Err(ObjectError::InvalidArgument(format!(
            "chunk prepared batch write start range index {} exceeds {} non-empty ranges",
            write_start_range_index, non_empty_index
        )));
    }
    if selected.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "chunk prepared batch write selected no ranges within max_write_bytes".to_string(),
        ));
    }
    Ok(selected)
}

pub(crate) fn select_bounded_chunk_discard_ranges_from(
    ranges: Vec<DataRange>,
    max_discard_bytes: u64,
    discard_start_range_index: u64,
) -> Result<Vec<DataRange>, ObjectError> {
    let requested_start = discard_start_range_index as usize;
    let mut selected = Vec::with_capacity(ranges.len().saturating_sub(requested_start));
    let mut selected_bytes = 0u64;
    let mut non_empty_index = 0usize;
    for range in ranges {
        if range.length == 0 {
            continue;
        }
        if non_empty_index < requested_start {
            non_empty_index += 1;
            continue;
        }
        non_empty_index += 1;
        validate_pipeline_data_range("chunk discard", &range)?;
        if max_discard_bytes > 0
            && selected_bytes
                .checked_add(range.length)
                .map_or(true, |bytes| bytes > max_discard_bytes)
        {
            break;
        }
        selected_bytes = selected_bytes.checked_add(range.length).ok_or_else(|| {
            ObjectError::InvalidArgument(
                "chunk bounded batch discard byte count overflows".to_string(),
            )
        })?;
        selected.push(range);
    }
    if selected.is_empty() && requested_start > non_empty_index {
        return Err(ObjectError::InvalidArgument(format!(
            "chunk prepared batch discard start range index {} exceeds {} non-empty ranges",
            discard_start_range_index, non_empty_index
        )));
    }
    if selected.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "chunk prepared batch discard selected no ranges within max_discard_bytes".to_string(),
        ));
    }
    Ok(selected)
}
