impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    fn propose_repair_object_metadata_range_bounded(
        &mut self,
        metadata: &ObjectMetadata,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_page_route_control_checksum: Option<&str>,
    ) -> Result<BoundedRepairReport, ObjectError> {
        let mut route_checksum = expected_page_route_control_checksum.map_or_else(
            || {
                self.store
                    .page_service
                    .route_control_proof()
                    .state_checksum
            },
            ToString::to_string,
        );
        let projected = self.store.clone().repair_object_range_bounded(
            &metadata.object_id.bucket,
            &metadata.object_id.key,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
        )?;
        let range_end = bounded_object_range_end(offset, length, metadata.length)?;
        let mut object_cursor = 0u64;
        let mut remaining_chunks = if max_repair_chunks == 0 {
            usize::MAX
        } else {
            max_repair_chunks
        };
        let mut next_chunk_index = start_chunk_index;
        for extent in &metadata.extents {
            if remaining_chunks == 0 {
                break;
            }
            let extent_start = object_cursor;
            let extent_end = object_cursor.checked_add(extent.length).ok_or_else(|| {
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
            let selected_chunks = self
                .store
                .page_service
                .map_range(&extent.slab_id, extent.offset + local_offset, local_len)?
                .into_iter()
                .filter(|loc| loc.chunk_id.page_index >= next_chunk_index)
                .map(|loc| loc.chunk_id)
                .collect::<BTreeSet<_>>();
            let selected_chunk_count = selected_chunks.len();
            if selected_chunk_count == 0 {
                continue;
            }
            let chunk_limit = remaining_chunks.min(selected_chunk_count);
            let proposed_next_chunk_index = selected_chunks
                .iter()
                .take(chunk_limit)
                .map(|chunk_id| chunk_id.page_index)
                .max()
                .unwrap_or(next_chunk_index)
                .saturating_add(1);
            self.propose_repair_page_range_bounded_linearizable_fenced(
                extent.slab_id.clone(),
                extent.offset + local_offset,
                local_len,
                chunk_limit,
                next_chunk_index,
                &route_checksum,
            )?;
            route_checksum = self
                .store
                .page_service
                .route_control_proof()
                .state_checksum;
            remaining_chunks = remaining_chunks.saturating_sub(chunk_limit);
            next_chunk_index = proposed_next_chunk_index;
            if remaining_chunks == 0 {
                break;
            }
        }
        Ok(projected)
    }
}
