impl PageReplicaReclaimCandidate {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}replica_index"),
            self.replica_index.to_string(),
        );
        fields.extend(
            self.candidate
                .to_control_fields(&format!("{field_prefix}candidate.")),
        );
        fields
    }
}

impl PageReplicaReclaimPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("scanned_replicas", self.scanned_replicas),
            ("skipped_replicas", self.skipped_replicas),
            ("frozen_replicas", self.frozen_replicas),
            ("quarantined_replicas", self.quarantined_replicas),
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
            ("candidate_count", self.candidates.len() as u64),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("continuation_safe", self.continuation_safe),
            ("next_marker_advanced", self.next_marker_advanced),
            ("budget_limited", self.budget_limited),
            ("truncated", self.truncated),
            (
                "global_reclaim_budget_enforced",
                self.global_reclaim_budget_enforced,
            ),
            ("reclaim_needed", self.reclaim_needed),
            ("reclaim_can_apply", self.reclaim_can_apply),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            (
                "global_reclaim_budget_basis",
                &self.global_reclaim_budget_basis,
            ),
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
