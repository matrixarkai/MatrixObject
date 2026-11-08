// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
const UNHEALTHY_REPAIR_PRIORITY_LOOKAHEAD: usize = 8;

impl MatrixMetaService {
    pub fn plan_unhealthy_placement_repairs(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PlacementRepairReport, ObjectError> {
        let repair_limit = if max_objects == 0 {
            usize::MAX
        } else {
            max_objects
        };
        let scan_limit = if max_objects == 0 {
            0
        } else {
            max_objects.saturating_mul(UNHEALTHY_REPAIR_PRIORITY_LOOKAHEAD)
        };
        let mut health = self.audit_placement_health(
            marker,
            scan_limit,
            expected_replica_count,
            min_free_bytes,
            true,
            max_payload_bytes,
        )?;
        let mut skipped_manifests = 0u64;
        health.items.retain(|item| {
            let repairable = self
                .object_manifests
                .get(&item.placement.object_id)
                .is_none();
            if !repairable {
                skipped_manifests = skipped_manifests.saturating_add(1);
            }
            repairable
        });
        health.items.sort_by(|left, right| {
            Self::compare_health_repair_priority(left, right, expected_replica_count)
        });
        let deferred_objects = health.items.len().saturating_sub(repair_limit);
        let has_more = health.has_more || deferred_objects > 0;
        let next_marker = if deferred_objects > 0 {
            marker.cloned()
        } else {
            health.next_marker.clone()
        };
        let mut report = PlacementRepairReport {
            marker: marker.cloned(),
            next_marker,
            has_more,
            scanned_objects: health.scanned_objects,
            skipped_objects: health
                .skipped_objects
                .saturating_add(skipped_manifests)
                .saturating_add(deferred_objects as u64),
            ..PlacementRepairReport::default()
        };
        let mut reserved_bytes = BTreeMap::<String, u64>::new();
        for item in health.items.into_iter().take(repair_limit) {
            match self.plan_health_repair_for_item(
                &item,
                expected_replica_count,
                min_free_bytes,
                &reserved_bytes,
            ) {
                Ok(plan) => {
                    let reserved = reserved_bytes.entry(plan.target_node_id.clone()).or_default();
                    *reserved = reserved.saturating_add(plan.length);
                    report.repair_objects = report.repair_objects.saturating_add(1);
                    report.bytes_to_repair = report.bytes_to_repair.saturating_add(plan.length);
                    report.items.push(PlacementRepairItem { plan });
                }
                Err(err) => {
                    report.failed_repairs = report.failed_repairs.saturating_add(1);
                    report.errors.push(format!(
                        "{}/{}: {}",
                        item.placement.object_id.bucket, item.placement.object_id.key, err
                    ));
                }
            }
        }
        Self::finalize_placement_repair_report(&mut report);
        Ok(report)
    }

    pub fn repair_unhealthy_placements(
        &mut self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PlacementRepairReport, ObjectError> {
        let mut report = self.plan_unhealthy_placement_repairs(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        let planned = report.items.clone();
        for item in planned {
            match self.commit_placement_health_repair(item.plan.clone(), &item.plan.plan_checksum) {
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
        Ok(report)
    }

    pub fn prepare_unhealthy_placement_repair_raft_entry(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<PreparedUnhealthyPlacementRepair, ObjectError> {
        let report = self.plan_unhealthy_placement_repairs(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        let request_checksum = unhealthy_placement_repair_request_checksum(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        );
        let mut prepared = PreparedUnhealthyPlacementRepair {
            marker: marker.cloned(),
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
            report,
            before_state_fingerprint: self.placement_state_fingerprint(),
            request_checksum,
            expected_previous_commit_index: self.placement_commit_index.saturating_sub(1),
            prepared_commit_index: self.placement_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_unhealthy_placement_repair_checksum(&prepared);
        Ok(prepared)
    }

}
