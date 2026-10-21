// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn repair_object_range(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<RepairStats, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        let range_end = bounded_object_range_end(offset, length, metadata.length)?;
        let mut aggregate = RepairStats::default();
        let mut object_cursor = 0u64;
        for extent in &metadata.extents {
            let extent_start = object_cursor;
            let extent_end = object_cursor.checked_add(extent.length()).ok_or_else(|| {
                ObjectError::InvalidArgument("object extent overflows".to_string())
            })?;
            object_cursor = extent_end;
            if extent_end <= offset {
                continue;
            }
            if extent_start >= range_end {
                break;
            }
            let read_start = offset.max(extent_start);
            let read_end = range_end.min(extent_end);
            let local_offset = read_start - extent_start;
            let local_len = read_end - read_start;
            let stats = self.chunk_service.repair_chunk_range(
                &extent.chunk_id,
                extent.offset() + local_offset,
                local_len,
            )?;
            merge_repair_stats(&mut aggregate, stats);
        }
        Ok(aggregate)
    }

    pub fn repair_object_range_bounded(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
    ) -> Result<BoundedRepairReport, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        let range_end = bounded_object_range_end(offset, length, metadata.length)?;
        let mut aggregate = BoundedRepairReport {
            start_chunk_index,
            next_chunk_index: start_chunk_index,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..BoundedRepairReport::default()
        };
        let mut object_cursor = 0u64;
        let mut object_chunk_cursor = 0u64;
        let mut remaining_chunks = if max_repair_chunks == 0 {
            usize::MAX
        } else {
            max_repair_chunks
        };
        for extent in &metadata.extents {
            let extent_start = object_cursor;
            let extent_end = object_cursor.checked_add(extent.length()).ok_or_else(|| {
                ObjectError::InvalidArgument("object extent overflows".to_string())
            })?;
            object_cursor = extent_end;
            if extent_end <= offset {
                continue;
            }
            if extent_start >= range_end {
                break;
            }
            let read_start = offset.max(extent_start);
            let read_end = range_end.min(extent_end);
            let local_offset = read_start - extent_start;
            let local_len = read_end - read_start;
            let extent_locations = self.chunk_service.map_range(
                &extent.chunk_id,
                extent.offset() + local_offset,
                local_len,
            )?;
            let extent_chunk_count = extent_locations
                .iter()
                .map(|location| location.page_id.clone())
                .collect::<BTreeSet<_>>()
                .len() as u64;
            if extent_chunk_count == 0 {
                continue;
            }
            aggregate.total_chunk_count = aggregate
                .total_chunk_count
                .saturating_add(extent_chunk_count);
            let extent_repair_bytes = extent_locations.iter().fold(0u64, |total, location| {
                total.saturating_add(location.length)
            });
            let extent_start_chunk_index = extent_locations
                .first()
                .map(|location| location.page_id.page_index)
                .unwrap_or(0);
            if start_chunk_index >= object_chunk_cursor.saturating_add(extent_chunk_count) {
                object_chunk_cursor = object_chunk_cursor.saturating_add(extent_chunk_count);
                aggregate.next_chunk_index = aggregate.next_chunk_index.max(object_chunk_cursor);
                continue;
            }
            if remaining_chunks == 0 {
                aggregate.has_more = true;
                aggregate.skipped_page_count = aggregate
                    .skipped_page_count
                    .saturating_add(extent_chunk_count);
                aggregate.skipped_repair_bytes = aggregate
                    .skipped_repair_bytes
                    .saturating_add(extent_repair_bytes);
                object_chunk_cursor = object_chunk_cursor.saturating_add(extent_chunk_count);
                continue;
            }
            let local_start_chunk_index = start_chunk_index.saturating_sub(object_chunk_cursor);
            let chunk_start_chunk_index =
                extent_start_chunk_index.saturating_add(local_start_chunk_index);
            let report = self.chunk_service.repair_chunk_range_bounded(
                &extent.chunk_id,
                extent.offset() + local_offset,
                local_len,
                remaining_chunks,
                chunk_start_chunk_index,
            )?;
            let selected_chunk_count = report.selected_chunk_count;
            let mut object_report = report;
            object_report.total_chunk_count = 0;
            object_report.start_chunk_index = object_chunk_cursor.saturating_add(
                object_report
                    .start_chunk_index
                    .saturating_sub(extent_start_chunk_index),
            );
            object_report.next_chunk_index = object_chunk_cursor.saturating_add(
                object_report
                    .next_chunk_index
                    .saturating_sub(extent_start_chunk_index),
            );
            merge_bounded_repair_report(&mut aggregate, object_report);
            object_chunk_cursor = object_chunk_cursor.saturating_add(extent_chunk_count);
            remaining_chunks = remaining_chunks.saturating_sub(selected_chunk_count as usize);
            if aggregate.has_more {
                break;
            }
        }
        if start_chunk_index > aggregate.total_chunk_count {
            return Err(ObjectError::InvalidArgument(
                "object repair start_chunk_index exceeds mapped repair chunk count".to_string(),
            ));
        }
        Ok(aggregate)
    }

}
