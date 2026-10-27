// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn can_accept_object_delete_discard(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        let metadata = self.head_object(bucket, key)?;
        let mut report = ObjectRangeAdmission {
            bucket: bucket.to_string(),
            key: key.to_string(),
            can_accept: true,
            object_length: metadata.length,
            requested_offset: 0,
            requested_length: metadata.length,
            selected_bytes: if max_discard_bytes > 0 {
                metadata.length.min(max_discard_bytes)
            } else {
                metadata.length
            },
            max_discard_bytes,
            budget_limited: max_discard_bytes > 0 && metadata.length > max_discard_bytes,
            min_ready_replica_count: u64::MAX,
            partial_status: "ok".to_string(),
            storage_admission_reason: "discard_ready".to_string(),
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectRangeAdmission::default()
        };
        self.attach_storage_admission_context(&mut report, now_millis());
        let mut consumed_discard_budget = 0u64;
        for extent in metadata.extents.iter() {
            let mut admitted_extent_len = extent.length();
            if max_discard_bytes > 0 {
                let remaining_budget = max_discard_bytes.saturating_sub(consumed_discard_budget);
                if remaining_budget == 0 {
                    report.extent_count = report.extent_count.saturating_add(1);
                    report.budget_limited = true;
                    report.skipped_discard_bytes =
                        report.skipped_discard_bytes.saturating_add(extent.length());
                    if report.can_accept {
                        report.can_accept = false;
                        report.partial_status = "object delete discard budget exceeded".to_string();
                    }
                    continue;
                }
                admitted_extent_len = admitted_extent_len.min(remaining_budget);
                if admitted_extent_len < extent.length() {
                    report.budget_limited = true;
                    report.skipped_discard_bytes = report
                        .skipped_discard_bytes
                        .saturating_add(extent.length() - admitted_extent_len);
                }
            }
            let admission = self.chunk_service.can_accept_page_discard(
                &extent.chunk_id,
                extent.offset(),
                admitted_extent_len,
                0,
            )?;
            report.extent_count = report.extent_count.saturating_add(1);
            report.mapped_chunk_ranges = report
                .mapped_chunk_ranges
                .saturating_add(admission.mapped_chunk_ranges);
            report.payload_bytes = report.payload_bytes.saturating_add(admission.payload_bytes);
            report.budget_limited |= admission.budget_limited;
            report.required_quorum = report.required_quorum.max(admission.required_quorum);
            report.selected_discard_bytes = report
                .selected_discard_bytes
                .saturating_add(admission.selected_discard_bytes);
            report.skipped_discard_bytes = report
                .skipped_discard_bytes
                .saturating_add(admission.skipped_discard_bytes);
            report.storage_read_quorum_limited |= admission.read_quorum_limited;
            report.storage_write_quorum_limited |= admission.write_quorum_limited;
            report.storage_discard_budget_limited |= admission.discard_budget_limited;
            if report.storage_admission_reason == "discard_ready"
                && admission.admission_reason != "discard_ready"
            {
                report.storage_admission_reason = admission.admission_reason.clone();
            }
            report.min_ready_replica_count = report
                .min_ready_replica_count
                .min(admission.ready_replica_count);
            if max_discard_bytes > 0 {
                consumed_discard_budget =
                    consumed_discard_budget.saturating_add(admission.selected_discard_bytes);
            }
            if !admission.can_accept && report.can_accept {
                report.can_accept = false;
                report.partial_status = admission.partial_status;
            }
        }
        if report.budget_limited && report.can_accept {
            report.can_accept = false;
            report.partial_status = "object delete discard budget exceeded".to_string();
        }
        if report.min_ready_replica_count == u64::MAX {
            report.min_ready_replica_count = 0;
        }
        stamp_object_range_admission_proof("CanDiscardObject", &mut report);
        Ok(report)
    }

    pub fn can_accept_object_delete_discard_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.can_accept_object_delete_discard(bucket, key, max_discard_bytes)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        stamp_object_range_admission_proof("CanDiscardObject", &mut report);
        Ok(report)
    }

    pub fn can_accept_object_delete_discard_storage_fenced(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        expected_storage_admission_proof_checksum: Option<&str>,
    ) -> Result<ObjectRangeAdmission, ObjectError> {
        let report = self.can_accept_object_delete_discard_fenced(
            bucket,
            key,
            max_discard_bytes,
            expected_route_control_checksum,
        )?;
        check_expected_object_storage_admission_proof_checksum(
            "object delete discard admission",
            &report,
            expected_storage_admission_proof_checksum,
        )?;
        Ok(report)
    }

    pub fn can_accept_object_delete_discard_with_metadata_read(
        &self,
        bucket: &str,
        key: &str,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(ObjectRangeAdmission, MetadataRaftReadiness), ObjectError> {
        let (_, readiness) = self.head_object_with_metadata_read(bucket, key, options)?;
        let admission = self.can_accept_object_delete_discard(bucket, key, max_discard_bytes)?;
        Ok((admission, readiness))
    }
}
