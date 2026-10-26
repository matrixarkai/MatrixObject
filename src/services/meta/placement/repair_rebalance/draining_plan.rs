// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    fn finalize_placement_repair_report(report: &mut PlacementRepairReport) {
        report.repair_needed = report.repair_objects > 0;
        report.repair_can_apply =
            report.repair_needed && !report.has_more && report.failed_repairs == 0;
        report.repair_reason = if !report.repair_needed {
            "no_root_placement_repair_needed".to_string()
        } else if report.has_more {
            "root_placement_repair_plan_truncated".to_string()
        } else if report.failed_repairs > 0 {
            "root_placement_repair_plan_errors".to_string()
        } else {
            "root_placement_repair_ready".to_string()
        };
    }

    fn finalize_placement_capacity_rebalance_report(report: &mut PlacementCapacityRebalanceReport) {
        report.rebalance_needed = report.rebalance_objects > 0;
        report.rebalance_can_apply =
            report.rebalance_needed && !report.has_more && report.failed_rebalances == 0;
        report.rebalance_reason = if !report.rebalance_needed {
            "no_root_capacity_rebalance_needed".to_string()
        } else if report.has_more {
            "root_capacity_rebalance_plan_truncated".to_string()
        } else if report.failed_rebalances > 0 {
            "root_capacity_rebalance_plan_errors".to_string()
        } else {
            "root_capacity_rebalance_ready".to_string()
        };
    }

    pub fn plan_draining_placement_repairs(&self, max_objects: usize) -> PlacementRepairReport {
        let limit = if max_objects == 0 {
            usize::MAX
        } else {
            max_objects
        };
        let mut report = PlacementRepairReport::default();
        for placement in self.placements.values() {
            report.scanned_objects = report.scanned_objects.saturating_add(1);
            let Some(source_node_id) = self.repair_source_for_placement(placement) else {
                continue;
            };
            if report.items.len() >= limit {
                report.skipped_objects = report.skipped_objects.saturating_add(1);
                report.has_more = true;
                continue;
            }
            match self.plan_repair_target_for_placement(placement, &source_node_id) {
                Ok(plan) => {
                    report.repair_objects = report.repair_objects.saturating_add(1);
                    report.bytes_to_repair = report.bytes_to_repair.saturating_add(plan.length);
                    report.items.push(PlacementRepairItem { plan });
                }
                Err(err) => {
                    report.failed_repairs = report.failed_repairs.saturating_add(1);
                    report.errors.push(format!(
                        "{}/{}: {}",
                        placement.object_id.bucket, placement.object_id.key, err
                    ));
                }
            }
        }
        Self::finalize_placement_repair_report(&mut report);
        report
    }

    pub fn repair_draining_placements(&mut self, max_objects: usize) -> PlacementRepairReport {
        let mut report = self.plan_draining_placement_repairs(max_objects);
        let planned = report.items.clone();
        for item in planned {
            match self.commit_placement_migration(item.plan.clone(), &item.plan.plan_checksum) {
                Ok(placement) => {
                    report.repaired_objects = report.repaired_objects.saturating_add(1);
                    report.bytes_repaired = report.bytes_repaired.saturating_add(placement.length);
                }
                Err(err) => {
                    report.failed_repairs = report.failed_repairs.saturating_add(1);
                    report.errors.push(format!(
                        "{}/{}: {}",
                        item.plan.object_id.bucket, item.plan.object_id.key, err
                    ));
                }
            }
        }
        report
    }

    pub fn prepare_draining_placement_repair_raft_entry(
        &self,
        max_objects: usize,
    ) -> PreparedPlacementRepair {
        let report = self.plan_draining_placement_repairs(max_objects);
        let request_checksum = placement_repair_request_checksum(max_objects);
        let mut prepared = PreparedPlacementRepair {
            max_objects,
            report,
            before_state_fingerprint: self.placement_state_fingerprint(),
            request_checksum,
            expected_previous_commit_index: self.placement_commit_index.saturating_sub(1),
            prepared_commit_index: self.placement_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_placement_repair_checksum(&prepared);
        prepared
    }

}
