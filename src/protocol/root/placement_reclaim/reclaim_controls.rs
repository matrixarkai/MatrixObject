// SPDX-License-Identifier: Apache-2.0
impl RootReclaimNodePlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .usage
            .to_control_fields(&format!("{field_prefix}usage."));
        fields.insert(
            format!("{field_prefix}reclaim_score"),
            self.reclaim_score.to_string(),
        );
        fields.insert(
            format!("{field_prefix}reclaim_reason"),
            self.reclaim_reason.clone(),
        );
        fields
    }
}

impl RootReclaimNodePlanPage {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "marker", &self.marker);
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "service_kind_filter",
            &self.service_kind_filter,
        );
        for (name, value) in [
            ("only_online", self.only_online),
            ("include_draining", self.include_draining),
            ("has_more", self.has_more),
            ("reclaim_needed", self.reclaim_needed),
            ("reclaim_can_apply", self.reclaim_can_apply),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("plan_count", self.plans.len() as u64),
            ("min_reclaimable_bytes", self.min_reclaimable_bytes),
            ("min_overcommitted_bytes", self.min_overcommitted_bytes),
            ("min_free_bytes", self.min_free_bytes),
            (
                "min_block_physical_discard_bytes",
                self.min_page_physical_discard_bytes,
            ),
            (
                "min_block_physical_discard_ranges",
                self.min_page_physical_discard_ranges,
            ),
            ("scanned_nodes", self.scanned_nodes),
            ("candidate_nodes", self.candidate_nodes),
            ("skipped_nodes", self.skipped_nodes),
            ("total_reclaim_score", self.total_reclaim_score),
            ("total_reclaimable_bytes", self.total_reclaimable_bytes),
            ("total_overcommitted_bytes", self.total_overcommitted_bytes),
            (
                "total_capacity_pressure_bytes",
                self.total_capacity_pressure_bytes,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}reclaim_reason"),
            self.reclaim_reason.clone(),
        );
        for (index, plan) in self.plans.iter().enumerate() {
            fields.extend(plan.to_control_fields(&format!("{field_prefix}plan.{index}.")));
        }
        fields.insert(
            format!("{field_prefix}plan_checksum"),
            control_fields_checksum(&fields),
        );
        fields
    }

    pub fn plan_checksum(&self) -> String {
        self.to_control_fields("")
            .get("plan_checksum")
            .cloned()
            .unwrap_or_default()
    }
}

