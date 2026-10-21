// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl DiscardPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}segment."));
        fields.insert(
            format!("{field_prefix}range_count"),
            self.ranges.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}discard_bytes"),
            self.discard_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}fragmented"),
            self.fragmented.to_string(),
        );
        fields.insert(
            format!("{field_prefix}compact_instead"),
            self.compact_instead.to_string(),
        );
        for (index, range) in self.ranges.iter().enumerate() {
            fields.extend(range.to_control_fields(&format!("{field_prefix}range.{index}.")));
        }
        fields
    }
}

impl ObjectGcDiscardCandidate {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}segment."));
        fields.extend(
            self.range
                .to_control_fields(&format!("{field_prefix}range.")),
        );
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.chunk_id.volume_id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_id"),
            self.chunk_id.id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}offset"),
            self.range.offset.to_string(),
        );
        fields.insert(
            format!("{field_prefix}length"),
            self.range.length.to_string(),
        );
        fields
    }
}

impl ChunkDiscardRunReport {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-RUST-SEGMENT-DISCARD-RUN-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        chunk_discard_run_report_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_chunk_discard_run_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_chunk_discard_run_proof_checksum =
            expected_chunk_discard_run_proof_checksum.as_ref();
        if expected_chunk_discard_run_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_chunk_discard_run_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "chunk discard run proof checksum mismatch: expected {} got {}",
                expected_chunk_discard_run_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}segment_discard_run_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_discard_run_proof_checksum"),
            self.proof_checksum(),
        );
        for (name, value) in [
            ("planned_segments", self.planned_chunks),
            ("planned_discard_bytes", self.planned_discard_bytes),
            (
                "skipped_compaction_segments",
                self.skipped_compaction_chunks,
            ),
            ("skipped_limit_segments", self.skipped_limit_chunks),
            ("attempted_segments", self.attempted_chunks),
            ("discarded_segments", self.discarded_chunks),
            ("failed_segments", self.failed_chunks),
            ("discard_bytes", self.discard_bytes),
            (
                "discarded_segment_count",
                self.discarded_chunk_ids.len() as u64,
            ),
            (
                "compaction_segment_count",
                self.compaction_chunk_ids.len() as u64,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}first_error"),
            self.first_error.clone(),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        for (index, chunk_id) in self.discarded_chunk_ids.iter().enumerate() {
            fields.extend(
                chunk_id.to_control_fields(&format!("{field_prefix}discarded_segment.{index}.")),
            );
        }
        for (index, chunk_id) in self.compaction_chunk_ids.iter().enumerate() {
            fields.extend(
                chunk_id.to_control_fields(&format!("{field_prefix}compaction_segment.{index}.")),
            );
        }
        fields
    }
}

impl GcReport {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-RUST-OBJECT-GC-REPORT-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        object_gc_report_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        if expected_object_gc_report_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_object_gc_report_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "object GC report proof checksum mismatch: expected {} got {}",
                expected_object_gc_report_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("segment_count", self.chunk_count),
            ("logical_bytes", self.logical_bytes),
            ("live_bytes", self.live_bytes),
            ("stale_bytes", self.stale_bytes),
            ("discarded_bytes", self.discarded_bytes),
            (
                "logical_write_amplification_ppm",
                self.logical_write_amplification_ppm,
            ),
            ("space_amplification_ppm", self.space_amplification_ppm),
            ("candidate_count", self.candidate_count),
            ("discard_candidate_bytes", self.discard_candidate_bytes),
            ("min_discard_bytes", self.min_discard_bytes),
            ("selected_discard_bytes", self.selected_discard_bytes),
            (
                "selected_discard_range_count",
                self.selected_discard_range_count,
            ),
            (
                "budget_skipped_discard_range_count",
                self.budget_skipped_discard_range_count,
            ),
            ("max_discard_bytes", self.max_discard_bytes),
            (
                "pending_discard_range_count",
                self.pending_discard_range_count,
            ),
            (
                "merged_discard_range_count",
                self.merged_discard_range_count,
            ),
            ("fragmented_range_count", self.fragmented_range_count),
            (
                "compaction_candidate_count",
                self.compaction_candidate_count,
            ),
            (
                "compaction_candidate_bytes",
                self.compaction_candidate_bytes,
            ),
            (
                "compaction_candidate_range_count",
                self.compaction_candidate_range_count,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}discard_budget_limited"),
            self.discard_budget_limited.to_string(),
        );
        fields.insert(
            format!("{field_prefix}object_gc_report_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}object_gc_report_proof_checksum"),
            self.proof_checksum(),
        );
        fields.insert(
            format!("{field_prefix}discard_candidate_range_count"),
            self.discard_candidates.len().to_string(),
        );
        for (index, candidate) in self.discard_candidates.iter().enumerate() {
            fields.extend(
                candidate.to_control_fields(&format!("{field_prefix}discard_candidate.{index}.")),
            );
        }
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

impl FullCompactionPlan {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-RUST-FULL-COMPACTION-PLAN-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        full_compaction_plan_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_full_compaction_plan_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_full_compaction_plan_checksum =
            expected_full_compaction_plan_checksum.as_ref();
        if expected_full_compaction_plan_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_full_compaction_plan_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "full compaction plan checksum mismatch: expected {} got {}",
                expected_full_compaction_plan_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("min_stale_bytes", self.min_stale_bytes),
            ("candidate_count", self.candidate_count),
            ("candidate_stale_bytes", self.candidate_stale_bytes),
            ("candidate_live_bytes", self.candidate_live_bytes),
            ("candidate_logical_bytes", self.candidate_logical_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}full_compaction_plan_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}full_compaction_plan_proof_checksum"),
            self.proof_checksum(),
        );
        fields.insert(
            format!("{field_prefix}object_gc_report_proof_checksum"),
            self.object_gc_report_proof_checksum.clone(),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        for (index, chunk_id) in self.candidate_chunk_ids.iter().enumerate() {
            fields
                .extend(chunk_id.to_control_fields(&format!("{field_prefix}candidate.{index}.")));
        }
        fields
    }
}
