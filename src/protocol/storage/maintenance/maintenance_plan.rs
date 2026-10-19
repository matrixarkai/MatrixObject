// SPDX-License-Identifier: Apache-2.0
impl StorageMaintenanceAction {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(format!("{field_prefix}action"), self.action.clone());
        fields.insert(format!("{field_prefix}reason"), self.reason.clone());
        fields.insert(format!("{field_prefix}priority"), self.priority.to_string());
        fields.insert(
            format!("{field_prefix}replica_index_present"),
            self.replica_index.is_some().to_string(),
        );
        if let Some(replica_index) = self.replica_index {
            fields.insert(
                format!("{field_prefix}replica_index"),
                replica_index.to_string(),
            );
        }
        fields.insert(
            format!("{field_prefix}chunk_present"),
            self.page_id.is_some().to_string(),
        );
        if let Some(page_id) = &self.page_id {
            fields.extend(page_id.to_control_fields(&format!("{field_prefix}chunk.")));
        }
        fields.insert(
            format!("{field_prefix}estimated_bytes"),
            self.estimated_bytes.to_string(),
        );
        fields
    }
}

impl StorageMaintenancePlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .readiness
            .to_control_fields(&format!("{field_prefix}readiness."));
        fields.extend(
            self.reclaim
                .to_control_fields(&format!("{field_prefix}reclaim.")),
        );
        for (index, discard) in self.chunk_discards.iter().enumerate() {
            fields.extend(
                discard.to_control_fields(&format!("{field_prefix}segment_discard.{index}.")),
            );
        }
        for (index, action) in self.actions.iter().enumerate() {
            fields.extend(action.to_control_fields(&format!("{field_prefix}action.{index}.")));
        }
        fields.insert(
            format!("{field_prefix}segment_discard_count"),
            self.chunk_discards.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}action_count"),
            self.actions.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            self.plan_checksum.clone(),
        );
        for (name, value) in [
            ("reclaim_continuation_safe", self.reclaim_continuation_safe),
            (
                "reclaim_next_marker_advanced",
                self.reclaim_next_marker_advanced,
            ),
            ("capacity_pressure", self.capacity_pressure),
            ("reclaim_pressure", self.reclaim_pressure),
            ("trim_flow_pressure", self.trim_flow_pressure),
            ("quorum_pressure", self.quorum_pressure),
            ("truncated", self.truncated),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}max_actions"),
            self.max_actions.to_string(),
        );
        fields
    }
}

impl StorageMaintenanceRunReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.plan.to_control_fields(&format!("{field_prefix}plan."));
        fields.extend(
            self.repair
                .to_control_fields(&format!("{field_prefix}repair.")),
        );
        fields.extend(
            self.reclaim
                .to_control_fields(&format!("{field_prefix}reclaim.")),
        );
        fields.extend(
            self.chunk_discard
                .to_control_fields(&format!("{field_prefix}segment_discard.")),
        );
        fields.extend(
            self.post_readiness
                .to_control_fields(&format!("{field_prefix}post_readiness.")),
        );
        for (name, value) in [
            ("executed_actions", self.executed_actions),
            ("skipped_actions", self.skipped_actions),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("route_control_fenced", self.route_control_fenced),
            (
                "route_control_fence_matched",
                self.route_control_fence_matched,
            ),
            ("reclaim_continuation_safe", self.reclaim_continuation_safe),
            (
                "reclaim_next_marker_advanced",
                self.reclaim_next_marker_advanced,
            ),
            (
                "reclaim_continuation_blocked",
                self.reclaim_continuation_blocked,
            ),
            ("reclaimed", self.reclaimed),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
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

