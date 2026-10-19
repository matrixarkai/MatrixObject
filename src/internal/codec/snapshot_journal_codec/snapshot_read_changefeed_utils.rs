pub(crate) fn read_object_change_record(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ObjectChangeRecord, ObjectError> {
    Ok(ObjectChangeRecord {
        sequence: cursor.read_u64()?,
        previous_sequence: cursor.read_u64()?,
        operation: cursor.read_string()?.into(),
        object_id: read_object_id(cursor)?,
        version: cursor.read_u64()?,
        length: cursor.read_u64()?,
        etag: cursor.read_string()?.into(),
        deleted: cursor.read_bool()?,
        event_time_ms: cursor.read_u64()?,
    })
}

pub(crate) fn read_object_change_cursor(
    cursor: &mut SnapshotByteCursor<'_>,
) -> Result<ObjectChangeCursor, ObjectError> {
    Ok(ObjectChangeCursor {
        consumer_id: cursor.read_string()?,
        sequence: cursor.read_u64()?,
        update_time_ms: cursor.read_u64()?,
    })
}

pub(crate) fn empty_string_to_none(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

pub(crate) fn bounded_snapshot_len(len: u64, label: &str) -> Result<usize, ObjectError> {
    if len > 4 * 1024 * 1024 * 1024 {
        return Err(ObjectError::Corruption(format!("{label} is too large")));
    }
    usize::try_from(len).map_err(|_| ObjectError::Corruption(format!("{label} overflows usize")))
}

fn ranges_are_merge_ordered(ranges: &[DataRange]) -> bool {
    let mut previous: Option<&DataRange> = None;
    for range in ranges.iter().filter(|range| range.length > 0) {
        if let Some(last) = previous {
            if last.offset > range.offset
                || (last.offset == range.offset && last.length > range.length)
            {
                return false;
            }
        }
        previous = Some(range);
    }
    true
}

pub(crate) fn merge_range_slice(ranges: &[DataRange]) -> Vec<DataRange> {
    let mut out: Vec<DataRange> = Vec::new();
    if ranges_are_merge_ordered(ranges) {
        out.reserve(ranges.len());
        for range in ranges.iter().filter(|range| range.length > 0) {
            if let Some(last) = out.last_mut() {
                let last_end = last.offset.saturating_add(last.length);
                let range_end = range.offset.saturating_add(range.length);
                if range.offset <= last_end {
                    last.length = range_end.saturating_sub(last.offset).max(last.length);
                    continue;
                }
            }
            out.push(DataRange {
                offset: range.offset,
                length: range.length,
            });
        }
        return out;
    }
    let mut sorted_ranges: Vec<&DataRange> =
        ranges.iter().filter(|range| range.length > 0).collect();
    sorted_ranges.sort_by(|a, b| a.offset.cmp(&b.offset).then(a.length.cmp(&b.length)));
    out.reserve(sorted_ranges.len());
    for range in sorted_ranges {
        if let Some(last) = out.last_mut() {
            let last_end = last.offset.saturating_add(last.length);
            let range_end = range.offset.saturating_add(range.length);
            if range.offset <= last_end {
                last.length = range_end.saturating_sub(last.offset).max(last.length);
                continue;
            }
        }
        out.push(DataRange {
            offset: range.offset,
            length: range.length,
        });
    }
    out
}

/// Removes `[offset, offset + length)` from `ranges`, splitting any range that
/// straddles it. Used when a write lands on space that was discarded: the bytes
/// are real again, so the discard over them no longer holds.
pub(crate) fn subtract_range(ranges: &[DataRange], offset: u64, length: u64) -> Vec<DataRange> {
    let end = offset.saturating_add(length);
    let mut out = Vec::with_capacity(ranges.len().saturating_add(1));
    for range in ranges {
        let range_end = range.offset.saturating_add(range.length);
        if range_end <= offset || range.offset >= end {
            out.push(range.clone());
            continue;
        }
        if range.offset < offset {
            out.push(DataRange {
                offset: range.offset,
                length: offset.saturating_sub(range.offset),
            });
        }
        if range_end > end {
            out.push(DataRange {
                offset: end,
                length: range_end.saturating_sub(end),
            });
        }
    }
    out
}

pub(crate) fn merge_ranges(mut ranges: Vec<DataRange>) -> Vec<DataRange> {
    let ranges_in_merge_order = ranges_are_merge_ordered(&ranges);
    ranges.retain(|range| range.length > 0);
    if !ranges_in_merge_order {
        ranges.sort_by(|a, b| a.offset.cmp(&b.offset).then(a.length.cmp(&b.length)));
    }
    let mut out: Vec<DataRange> = Vec::new();
    out.reserve(ranges.len());
    for range in ranges {
        if let Some(last) = out.last_mut() {
            let last_end = last.offset.saturating_add(last.length);
            let range_end = range.offset.saturating_add(range.length);
            if range.offset <= last_end {
                last.length = range_end.saturating_sub(last.offset).max(last.length);
                continue;
            }
        }
        out.push(range);
    }
    out
}

pub(crate) fn ranges_covered_by(ranges: &[DataRange], covering_ranges: &[DataRange]) -> bool {
    let covering_ranges = merge_range_slice(covering_ranges);
    ranges.iter().all(|range| {
        let Some(range_end) = range.offset.checked_add(range.length) else {
            return false;
        };
        covering_ranges.iter().any(|cover| {
            let Some(cover_end) = cover.offset.checked_add(cover.length) else {
                return false;
            };
            cover.offset <= range.offset && range_end <= cover_end
        })
    })
}
