impl SlabInfo {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .slab_id
            .to_control_fields(&format!("{field_prefix}segment."));
        for (name, value) in [
            ("logical_bytes", self.logical_bytes),
            ("live_bytes", self.live_bytes),
            ("stale_bytes", self.stale_bytes),
            ("discarded_bytes", self.discarded_bytes),
            ("next_offset", self.next_offset),
            ("stale_range_count", self.stale_ranges.len() as u64),
            ("discarded_range_count", self.discarded_ranges.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (index, range) in self.stale_ranges.iter().enumerate() {
            fields.extend(range.to_control_fields(&format!("{field_prefix}stale_range.{index}.")));
        }
        for (index, range) in self.discarded_ranges.iter().enumerate() {
            fields.extend(
                range.to_control_fields(&format!("{field_prefix}discarded_range.{index}.")),
            );
        }
        fields
    }
}

impl ReplicaReclaimCandidate {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}chunk."));
        for (name, value) in [
            ("logical_length", self.logical_length),
            ("discarded_bytes", self.discarded_bytes),
            ("discarded_range_count", self.discarded_range_count),
            ("discard_ratio_ppm", self.discard_ratio_ppm),
            ("rewrite_bytes", self.rewrite_bytes),
            ("trim_bytes", self.trim_bytes),
            ("reclaim_score", self.reclaim_score),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields
    }
}

impl ReplicaReclaimPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}marker_present"),
            self.marker.is_some().to_string(),
        );
        if let Some(marker) = &self.marker {
            fields.extend(marker.to_control_fields(&format!("{field_prefix}marker.")));
        }
        fields.insert(
            format!("{field_prefix}next_marker_present"),
            self.next_marker.is_some().to_string(),
        );
        if let Some(next_marker) = &self.next_marker {
            fields.extend(next_marker.to_control_fields(&format!("{field_prefix}next_marker.")));
        }
        for (name, value) in [
            ("scanned_chunks", self.scanned_chunks),
            ("candidate_chunks", self.candidate_chunks),
            ("scheduled_chunks", self.scheduled_chunks),
            ("skipped_chunks", self.skipped_chunks),
            ("skipped_by_budget_chunks", self.skipped_by_budget_chunks),
            ("skipped_by_budget_bytes", self.skipped_by_budget_bytes),
            (
                "skipped_by_trim_budget_chunks",
                self.skipped_by_trim_budget_chunks,
            ),
            (
                "skipped_by_trim_budget_bytes",
                self.skipped_by_trim_budget_bytes,
            ),
            (
                "skipped_by_fragmentation_chunks",
                self.skipped_by_fragmentation_chunks,
            ),
            (
                "skipped_by_fragmentation_discarded_bytes",
                self.skipped_by_fragmentation_discarded_bytes,
            ),
            (
                "skipped_by_fragmentation_ranges",
                self.skipped_by_fragmentation_ranges,
            ),
            ("candidate_discarded_bytes", self.candidate_discarded_bytes),
            ("scheduled_rewrite_bytes", self.scheduled_rewrite_bytes),
            ("scheduled_trim_ops", self.scheduled_trim_ops),
            ("scheduled_trim_bytes", self.scheduled_trim_bytes),
            ("remaining_discard_bytes", self.remaining_discard_bytes),
            ("requested_trim_bytes", self.requested_trim_bytes),
            ("effective_trim_bytes", self.effective_trim_bytes),
            ("trim_flow_available_bytes", self.trim_flow_available_bytes),
            ("trim_flow_throttled_ops", self.trim_flow_throttled_ops),
            ("trim_flow_throttled_bytes", self.trim_flow_throttled_bytes),
            ("candidate_count", self.candidates.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("continuation_safe", self.continuation_safe),
            ("next_marker_advanced", self.next_marker_advanced),
            ("budget_limited", self.budget_limited),
            ("truncated", self.truncated),
            ("trim_flow_limited", self.trim_flow_limited),
            ("reclaim_needed", self.reclaim_needed),
            ("reclaim_can_apply", self.reclaim_can_apply),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("reclaim_reason", &self.reclaim_reason),
            ("plan_checksum", &self.plan_checksum),
            ("expected_plan_checksum", &self.expected_plan_checksum),
            ("actual_plan_checksum", &self.actual_plan_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (index, candidate) in self.candidates.iter().enumerate() {
            fields
                .extend(candidate.to_control_fields(&format!("{field_prefix}candidate.{index}.")));
        }
        fields.insert(
            format!("{field_prefix}first_budget_skipped_candidate_present"),
            self.first_budget_skipped_candidate.is_some().to_string(),
        );
        if let Some(candidate) = &self.first_budget_skipped_candidate {
            fields.extend(
                candidate
                    .to_control_fields(&format!("{field_prefix}first_budget_skipped_candidate.")),
            );
        }
        fields.insert(
            format!("{field_prefix}first_trim_budget_skipped_candidate_present"),
            self.first_trim_budget_skipped_candidate
                .is_some()
                .to_string(),
        );
        if let Some(candidate) = &self.first_trim_budget_skipped_candidate {
            fields.extend(candidate.to_control_fields(&format!(
                "{field_prefix}first_trim_budget_skipped_candidate."
            )));
        }
        fields
    }
}
