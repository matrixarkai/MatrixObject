// SPDX-License-Identifier: Apache-2.0
impl MatrixChunkService {
    pub fn can_accept_slab_read(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_read_bounded(chunk_id, offset, length, max_payload_bytes, 0)
    }

    pub fn can_accept_chunk_read_bounded(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        let mapped_chunk_ranges = self.mapped_chunk_count(offset, length)?;
        let byte_budget_limited = max_payload_bytes > 0 && length > max_payload_bytes;
        let chunk_budget_limited =
            max_chunk_read_count > 0 && mapped_chunk_ranges > max_chunk_read_count;
        let budget_limited = byte_budget_limited || chunk_budget_limited;
        let mut read_stats = ChunkReadStats::default();
        let mut readable_chunk_count = 0u64;
        let mut failed_chunk_count = 0u64;
        let mut min_ready = u64::MAX;
        let mut first_error: Option<ObjectError> = None;
        if !budget_limited {
            let locations = self.map_range(chunk_id, offset, length)?;
            read_stats = if locations.len() == 1 {
                self.collect_metadata_head_admission_stats(std::slice::from_ref(
                    &locations[0].page_id,
                ))
            } else {
                let mut page_ids = Vec::with_capacity(locations.len());
                for loc in &locations {
                    page_ids.push(loc.page_id.clone());
                }
                self.collect_metadata_head_admission_stats(&page_ids)
            };
            for loc in &locations {
                let mut ready = 0u64;
                for (index, replica) in self.replicas.iter().enumerate() {
                    if !self.replica_active(index) {
                        continue;
                    }
                    match replica.head_chunk(&loc.page_id) {
                        Ok(_) => ready = ready.saturating_add(1),
                        Err(err) if first_error.is_none() => first_error = Some(err),
                        Err(_) => {}
                    }
                }
                min_ready = min_ready.min(ready);
                if ready >= self.read_quorum as u64 {
                    readable_chunk_count = readable_chunk_count.saturating_add(1);
                } else {
                    failed_chunk_count = failed_chunk_count.saturating_add(1);
                }
            }
        }
        if mapped_chunk_ranges == 0 {
            min_ready = 0;
        }
        read_stats.chunk_reads = mapped_chunk_ranges;
        let can_accept = !budget_limited && failed_chunk_count == 0;
        let partial_status = if can_accept {
            "ok".to_string()
        } else if byte_budget_limited {
            "block read payload budget exceeded".to_string()
        } else if chunk_budget_limited {
            "block read chunk fanout exceeds max_chunk_read_count".to_string()
        } else {
            first_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "block read quorum was not met".to_string())
        };
        let admission_reason = if can_accept {
            "read_ready"
        } else if budget_limited {
            "response_budget_limited"
        } else {
            "read_quorum_unavailable"
        };
        let mut admission = ChunkRangeAdmission {
            can_accept,
            input_ranges: if length == 0 { 0 } else { 1 },
            mapped_chunk_ranges,
            payload_bytes: length,
            max_payload_bytes,
            max_chunk_read_count,
            budget_limited,
            ready_replica_count: min_ready,
            required_quorum: self.read_quorum as u64,
            readable_chunk_count,
            failed_chunk_count,
            read_stats,
            partial_status,
            admission_reason: admission_reason.to_string(),
            read_quorum_limited: !budget_limited && failed_chunk_count != 0,
            route_control_proof: self.route_control_proof(),
            route_control_fence_matched: true,
            ..ChunkRangeAdmission::default()
        };
        stamp_page_range_admission_proof("CanReadBlock", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_chunk_read_bounded_fenced(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_chunk_read_bounded(
            chunk_id,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanReadBlock", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_slab_reads(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_reads_bounded(requests, max_payload_bytes, 0)
    }

    pub fn can_accept_chunk_reads_bounded(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_reads_bounded_borrowed(
            &requests,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    pub fn can_accept_chunk_reads_bounded_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        let mut admission = ChunkRangeAdmission {
            input_ranges: requests.iter().filter(|request| request.length > 0).count() as u64,
            max_payload_bytes,
            max_chunk_read_count,
            required_quorum: self.read_quorum as u64,
            ..ChunkRangeAdmission::default()
        };
        let mut grouped: BTreeMap<PageId, Vec<DataRange>> = BTreeMap::new();
        for request in requests {
            validate_pipeline_range("block batch read", request.offset, request.length)?;
            admission.payload_bytes = admission
                .payload_bytes
                .checked_add(request.length)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch read payload byte count overflows".to_string(),
                    )
                })?;
            if max_payload_bytes > 0 && admission.payload_bytes > max_payload_bytes {
                admission.budget_limited = true;
                admission.partial_status = "block batch read payload budget exceeded".to_string();
                admission.admission_reason = "response_budget_limited".to_string();
                stamp_page_range_admission_proof("CanReadBlockBatch", &mut admission);
                return Ok(admission);
            }
            let mapped_chunk_ranges = self.mapped_chunk_count(request.offset, request.length)?;
            admission.mapped_chunk_ranges = admission
                .mapped_chunk_ranges
                .checked_add(mapped_chunk_ranges)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page batch read chunk fanout overflows".to_string(),
                    )
                })?;
            if max_chunk_read_count > 0 && admission.mapped_chunk_ranges > max_chunk_read_count {
                admission.budget_limited = true;
                admission.partial_status =
                    "block batch read chunk fanout exceeds max_chunk_read_count".to_string();
                admission.admission_reason = "response_budget_limited".to_string();
                stamp_page_range_admission_proof("CanReadBlockBatch", &mut admission);
                return Ok(admission);
            }
            for loc in self.map_range(&request.chunk_id, request.offset, request.length)? {
                grouped.entry(loc.page_id).or_default().push(DataRange {
                    offset: loc.page_offset,
                    length: loc.length,
                });
            }
        }

        let mut page_ids = Vec::with_capacity(grouped.len());
        for page_id in grouped.keys() {
            page_ids.push(page_id.clone());
        }
        admission.read_stats = self.collect_metadata_head_admission_stats(&page_ids);
        let mut first_error: Option<ObjectError> = None;
        let mut min_ready = u64::MAX;
        for (page_id, ranges) in grouped {
            for range in merge_ranges(ranges) {
                admission.coalesced_chunk_ranges =
                    admission.coalesced_chunk_ranges.saturating_add(1);
                admission.read_stats.chunk_reads =
                    admission.read_stats.chunk_reads.saturating_add(1);
                let range_end = range.end()?;
                let mut ready = 0u64;
                for (index, replica) in self.replicas.iter().enumerate() {
                    if !self.replica_active(index) {
                        continue;
                    }
                    match replica.head_chunk(&page_id) {
                        Ok(head) => {
                            let readable = range_end <= head.logical_length
                                && !head.discarded_ranges.iter().any(|discarded| {
                                    ranges_intersect(
                                        range.offset,
                                        range.length,
                                        discarded.offset,
                                        discarded.length,
                                    )
                                });
                            if readable {
                                ready = ready.saturating_add(1);
                            } else if first_error.is_none() {
                                first_error = Some(ObjectError::Corruption(
                                    "page batch read range is unavailable on chunk replica"
                                        .to_string(),
                                ));
                            }
                        }
                        Err(err) if first_error.is_none() => first_error = Some(err),
                        Err(_) => {}
                    }
                }
                min_ready = min_ready.min(ready);
                if ready >= self.read_quorum as u64 {
                    admission.readable_chunk_count =
                        admission.readable_chunk_count.saturating_add(1);
                } else {
                    admission.failed_chunk_count = admission.failed_chunk_count.saturating_add(1);
                }
            }
        }

        if admission.coalesced_chunk_ranges == 0 {
            min_ready = 0;
        }
        admission.ready_replica_count = min_ready;
        admission.required_quorum = self.read_quorum as u64;
        admission.can_accept = admission.failed_chunk_count == 0 && !admission.budget_limited;
        admission.partial_status = if admission.can_accept {
            "ok".to_string()
        } else {
            first_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "block batch read quorum was not met".to_string())
        };
        admission.read_quorum_limited = admission.failed_chunk_count != 0;
        admission.admission_reason = if admission.can_accept {
            "read_ready".to_string()
        } else {
            "read_quorum_unavailable".to_string()
        };
        stamp_page_range_admission_proof("CanReadBlockBatch", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_chunk_reads_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_slab_reads(requests, max_payload_bytes)?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanReadBlockBatch", &mut admission);
        Ok(admission)
    }

    pub fn can_accept_chunk_reads_bounded_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.can_accept_chunk_reads_bounded_fenced_borrowed(
            &requests,
            max_payload_bytes,
            max_chunk_read_count,
            expected_route_control_checksum,
        )
    }

    pub fn can_accept_chunk_reads_bounded_fenced_borrowed(
        &self,
        requests: &[ChunkRangeRequest],
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ChunkRangeAdmission, ObjectError> {
        self.check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut admission = self.can_accept_chunk_reads_bounded_borrowed(
            requests,
            max_payload_bytes,
            max_chunk_read_count,
        )?;
        self.stamp_route_control_admission(&mut admission, expected_route_control_checksum);
        stamp_page_range_admission_proof("CanReadBlockBatch", &mut admission);
        Ok(admission)
    }

}
