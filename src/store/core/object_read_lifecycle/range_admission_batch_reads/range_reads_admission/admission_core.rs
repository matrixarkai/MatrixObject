// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn can_accept_object_range_read(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        self.can_accept_object_range_read_bounded(bucket, key, offset, length, max_payload_bytes, 0)
    }

    pub fn can_accept_object_range_read_bounded(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        check_payload_read_allowed(&metadata, now_millis())?;
        self.can_accept_object_metadata_range_read_bounded(
            &metadata,
            offset,
            length,
            max_payload_bytes,
            max_chunk_read_count,
        )
    }

    fn can_accept_object_metadata_range_read_bounded(
        &self,
        metadata: &ObjectMetadata,
        offset: u64,
        length: u64,
        max_payload_bytes: u64,
        max_chunk_read_count: u64,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        if offset > metadata.length {
            return Err(ObjectError::InvalidArgument(
                "range offset exceeds object length".to_string(),
            ));
        }
        let selected_length = length.min(metadata.length - offset);
        let range_end = offset.checked_add(selected_length).ok_or_else(|| {
            ObjectError::InvalidArgument("object range admission overflows".to_string())
        })?;
        let mut report = ObjectRangeAdmission {
            bucket: metadata.object_id.bucket.to_string(),
            key: metadata.object_id.key.to_string(),
            can_accept: true,
            object_length: metadata.length,
            requested_offset: offset,
            requested_length: length,
            selected_bytes: selected_length,
            max_payload_bytes,
            max_chunk_read_count,
            min_ready_replica_count: u64::MAX,
            partial_status: "ok".to_string(),
            storage_admission_reason: "read_ready".to_string(),
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectRangeAdmission::default()
        };
        self.attach_storage_admission_context(&mut report, now_millis());
        if max_payload_bytes > 0 && selected_length > max_payload_bytes {
            report.can_accept = false;
            report.budget_limited = true;
            report.payload_bytes = selected_length;
            report.partial_status = "object range read payload budget exceeded".to_string();
            report.storage_admission_reason = "response_budget_limited".to_string();
            stamp_object_range_admission_proof("CanReadObjectRange", &mut report);
            return Ok(report);
        }
        let mut object_cursor = 0u64;
        for extent in &metadata.extents {
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
            let admission = self.chunk_service.can_accept_chunk_read_bounded(
                &extent.chunk_id,
                extent.offset + local_offset,
                local_len,
                max_payload_bytes,
                max_chunk_read_count,
            )?;
            report.extent_count = report.extent_count.saturating_add(1);
            report.mapped_chunk_ranges = report
                .mapped_chunk_ranges
                .saturating_add(admission.mapped_chunk_ranges);
            report.payload_bytes = report.payload_bytes.saturating_add(admission.payload_bytes);
            report.budget_limited |= admission.budget_limited;
            report.required_quorum = report.required_quorum.max(admission.required_quorum);
            report.readable_chunk_count = report
                .readable_chunk_count
                .saturating_add(admission.readable_chunk_count);
            report.failed_chunk_count = report
                .failed_chunk_count
                .saturating_add(admission.failed_chunk_count);
            report.storage_read_quorum_limited |= admission.read_quorum_limited;
            report.storage_write_quorum_limited |= admission.write_quorum_limited;
            report.storage_discard_budget_limited |= admission.discard_budget_limited;
            if report.storage_admission_reason == "read_ready"
                && admission.admission_reason != "read_ready"
            {
                report.storage_admission_reason = admission.admission_reason.clone();
            }
            report.min_ready_replica_count = report
                .min_ready_replica_count
                .min(admission.ready_replica_count);
            merge_page_read_stats(&mut report.read_stats, admission.read_stats);
            if !admission.can_accept && report.can_accept {
                report.can_accept = false;
                report.partial_status = admission.partial_status;
            }
            if max_chunk_read_count > 0 && report.mapped_chunk_ranges > max_chunk_read_count {
                report.can_accept = false;
                report.budget_limited = true;
                report.partial_status =
                    "object range read chunk fanout exceeds max_chunk_read_count".to_string();
                report.storage_admission_reason = "response_budget_limited".to_string();
                break;
            }
        }
        if report.min_ready_replica_count == u64::MAX {
            report.min_ready_replica_count = 0;
        }
        stamp_object_range_admission_proof("CanReadObjectRange", &mut report);
        Ok(report)
    }

}
