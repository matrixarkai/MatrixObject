// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn gc_report(&self) -> GcReport {
        self.gc_report_with_discard_budget(0)
    }

    pub fn gc_report_with_discard_budget(&self, max_discard_bytes: u64) -> GcReport {
        self.gc_report_with_discard_policy(1, max_discard_bytes)
    }

    pub fn gc_report_with_discard_policy(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
    ) -> GcReport {
        let mut logical_bytes = 0u64;
        let mut live_bytes = 0u64;
        let mut stale_bytes = 0u64;
        let mut discarded_bytes = 0u64;
        let mut candidate_count = 0u64;
        let mut discard_candidate_bytes = 0u64;
        let mut pending_discard_range_count = 0u64;
        let mut merged_discard_range_count = 0u64;
        let mut fragmented_range_count = 0u64;
        let mut compaction_candidate_count = 0u64;
        let mut compaction_candidate_bytes = 0u64;
        let mut compaction_candidate_range_count = 0u64;
        let mut discard_ranges = Vec::new();
        for chunk in self.chunks.values() {
            logical_bytes = saturating_add(logical_bytes, chunk.logical_bytes);
            live_bytes = saturating_add(live_bytes, chunk.live_bytes);
            stale_bytes = saturating_add(stale_bytes, chunk.stale_bytes);
            discarded_bytes = saturating_add(discarded_bytes, chunk.discarded_bytes);
            if chunk.stale_bytes > 0 {
                candidate_count += 1;
                discard_candidate_bytes =
                    saturating_add(discard_candidate_bytes, chunk.stale_bytes);
            }
            let pending_ranges = chunk
                .stale_ranges
                .iter()
                .filter(|range| range.length > 0)
                .count();
            pending_discard_range_count =
                saturating_add(pending_discard_range_count, pending_ranges as u64);
            let merged_ranges = merge_range_slice(&chunk.stale_ranges);
            merged_discard_range_count =
                saturating_add(merged_discard_range_count, merged_ranges.len() as u64);
            for range in &merged_ranges {
                if range.length >= min_discard_bytes {
                    discard_ranges.push((chunk.chunk_id.clone(), range.clone()));
                }
            }
            if merged_ranges.len() > self.options.compaction_fragment_threshold {
                compaction_candidate_count += 1;
                compaction_candidate_bytes =
                    saturating_add(compaction_candidate_bytes, chunk.stale_bytes);
                compaction_candidate_range_count =
                    saturating_add(compaction_candidate_range_count, merged_ranges.len() as u64);
                fragmented_range_count =
                    saturating_add(fragmented_range_count, merged_ranges.len() as u64);
            }
        }
        let logical_write_amplification_ppm = if self.logical_written_bytes == 0 {
            0
        } else {
            self.physical_written_bytes.saturating_mul(1_000_000) / self.logical_written_bytes
        };
        let space_amplification_ppm = if live_bytes == 0 {
            0
        } else {
            logical_bytes.saturating_mul(1_000_000) / live_bytes
        };
        discard_ranges.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.offset.cmp(&right.1.offset))
                .then_with(|| left.1.length.cmp(&right.1.length))
        });
        let mut selected_discard_bytes = 0u64;
        let mut selected_discard_range_count = 0u64;
        let mut budget_skipped_discard_range_count = 0u64;
        let mut discard_budget_limited = false;
        let mut discard_candidates = Vec::new();
        for (chunk_id, range) in discard_ranges {
            if max_discard_bytes > 0
                && range.length > max_discard_bytes.saturating_sub(selected_discard_bytes)
            {
                discard_budget_limited = true;
                budget_skipped_discard_range_count =
                    budget_skipped_discard_range_count.saturating_add(1);
                continue;
            }
            selected_discard_bytes = saturating_add(selected_discard_bytes, range.length);
            selected_discard_range_count = selected_discard_range_count.saturating_add(1);
            discard_candidates.push(ObjectGcDiscardCandidate { chunk_id, range });
        }
        GcReport {
            chunk_count: self.chunks.len() as u64,
            logical_bytes,
            live_bytes,
            stale_bytes,
            discarded_bytes,
            logical_write_amplification_ppm,
            space_amplification_ppm,
            candidate_count,
            discard_candidate_bytes,
            min_discard_bytes,
            selected_discard_bytes,
            selected_discard_range_count,
            budget_skipped_discard_range_count,
            max_discard_bytes,
            discard_budget_limited,
            pending_discard_range_count,
            merged_discard_range_count,
            fragmented_range_count,
            compaction_candidate_count,
            compaction_candidate_bytes,
            compaction_candidate_range_count,
            discard_candidates,
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: String::new(),
        }
    }

    pub fn gc_report_fenced(
        &self,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<GcReport, ObjectError> {
        self.gc_report_with_discard_budget_fenced(0, expected_route_control_checksum)
    }

    pub fn gc_report_with_discard_budget_fenced(
        &self,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<GcReport, ObjectError> {
        self.gc_report_with_discard_policy_fenced(
            1,
            max_discard_bytes,
            expected_route_control_checksum,
        )
    }

    pub fn gc_report_with_discard_policy_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<GcReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.gc_report_with_discard_policy(min_discard_bytes, max_discard_bytes);
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn gc_report_with_expected_proof(
        &self,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<GcReport, ObjectError> {
        self.gc_report_with_discard_policy_proof_fenced(
            1,
            0,
            None,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn gc_report_with_discard_budget_proof_fenced(
        &self,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<GcReport, ObjectError> {
        self.gc_report_with_discard_policy_proof_fenced(
            1,
            max_discard_bytes,
            expected_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn gc_report_with_discard_policy_proof_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<GcReport, ObjectError> {
        let report = self.gc_report_with_discard_policy_fenced(
            min_discard_bytes,
            max_discard_bytes,
            expected_route_control_checksum,
        )?;
        report.verify_expected_proof_checksum(expected_object_gc_report_proof_checksum)?;
        Ok(report)
    }

    pub fn gc_report_with_metadata_read(
        &self,
        options: MetadataReadOptions,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.gc_report_with_discard_budget_and_metadata_read(0, options)
    }

    pub fn gc_report_with_discard_budget_and_metadata_read(
        &self,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.gc_report_with_discard_policy_and_metadata_read(1, max_discard_bytes, options)
    }

    pub fn gc_report_with_discard_policy_and_metadata_read(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object gc report read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        Ok((
            self.gc_report_with_discard_policy(min_discard_bytes, max_discard_bytes),
            readiness,
        ))
    }

    pub fn gc_report_with_metadata_read_fenced(
        &self,
        options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.gc_report_with_discard_budget_and_metadata_read_fenced(
            0,
            options,
            expected_route_control_checksum,
        )
    }

    pub fn gc_report_with_discard_budget_and_metadata_read_fenced(
        &self,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.gc_report_with_discard_policy_and_metadata_read_fenced(
            1,
            max_discard_bytes,
            options,
            expected_route_control_checksum,
        )
    }

    pub fn gc_report_with_discard_policy_and_metadata_read_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        let readiness = self.metadata_raft_readiness(options.min_read_index, options.now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object gc report read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        Ok((
            self.gc_report_with_discard_policy_fenced(
                min_discard_bytes,
                max_discard_bytes,
                expected_route_control_checksum,
            )?,
            readiness,
        ))
    }

    pub fn gc_report_with_metadata_read_proof_fenced(
        &self,
        options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.gc_report_with_discard_policy_and_metadata_read_proof_fenced(
            1,
            0,
            options,
            expected_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn gc_report_with_discard_budget_and_metadata_read_proof_fenced(
        &self,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        self.gc_report_with_discard_policy_and_metadata_read_proof_fenced(
            1,
            max_discard_bytes,
            options,
            expected_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )
    }

    pub fn gc_report_with_discard_policy_and_metadata_read_proof_fenced(
        &self,
        min_discard_bytes: u64,
        max_discard_bytes: u64,
        options: MetadataReadOptions,
        expected_route_control_checksum: Option<&str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(GcReport, MetadataRaftReadiness), ObjectError> {
        let (report, readiness) = self.gc_report_with_discard_policy_and_metadata_read_fenced(
            min_discard_bytes,
            max_discard_bytes,
            options,
            expected_route_control_checksum,
        )?;
        report.verify_expected_proof_checksum(expected_object_gc_report_proof_checksum)?;
        Ok((report, readiness))
    }

}
