// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn plan_object_capacity_rebalance(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<&ObjectId>,
    ) -> ObjectCapacityRebalanceReport {
        let root_limit = if marker.is_some() { 0 } else { max_objects };
        let root_plan = self
            .meta_service
            .plan_capacity_rebalance(root_limit, min_free_bytes);
        let mut report = ObjectCapacityRebalanceReport {
            min_free_bytes,
            checked_objects: 0,
            rebalance_objects: 0,
            bytes_to_rebalance: 0,
            pressure_node_count: root_plan.pressure_node_count,
            marker: marker.cloned(),
            errors: root_plan.errors.clone(),
            ..ObjectCapacityRebalanceReport::default()
        };
        let limit = if max_objects == 0 {
            usize::MAX
        } else {
            max_objects
        };
        let mut last_selected: Option<ObjectId> = None;
        for item in root_plan.items {
            if marker.is_some_and(|marker| &item.plan.object_id <= marker) {
                continue;
            }
            report.checked_objects = report.checked_objects.saturating_add(1);
            if report.items.len() >= limit {
                report.truncated = true;
                report.next_marker = last_selected.clone();
                break;
            }
            let Some(metadata) = self.objects.get(&item.plan.object_id) else {
                report.errors.push(format!(
                    "{}/{}: object missing during capacity rebalance planning",
                    item.plan.object_id.bucket, item.plan.object_id.key
                ));
                continue;
            };
            if metadata.deleted {
                continue;
            }
            report.rebalance_objects = report.rebalance_objects.saturating_add(1);
            report.bytes_to_rebalance = report.bytes_to_rebalance.saturating_add(metadata.length);
            report.items.push(ObjectCapacityRebalanceItem {
                object: metadata.clone(),
                pressure_node_id: item.pressure_node_id,
                plan: item.plan,
            });
            last_selected = Some(metadata.object_id.clone());
        }
        if root_plan.has_more && report.next_marker.is_none() {
            report.truncated = true;
            report.next_marker = last_selected;
        }
        finalize_object_capacity_rebalance_report(&mut report);
        report
    }

    pub fn rebalance_object_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<&ObjectId>,
    ) -> ObjectCapacityRebalanceRunReport {
        let plan = self.plan_object_capacity_rebalance(max_objects, min_free_bytes, marker);
        let mut report = ObjectCapacityRebalanceRunReport {
            plan,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectCapacityRebalanceRunReport::default()
        };
        let items = report.plan.items.clone();
        for item in items {
            report.attempted_migration_objects =
                report.attempted_migration_objects.saturating_add(1);
            let current = match self.objects.get(&item.object.object_id) {
                Some(metadata) if !metadata.deleted => metadata,
                _ => {
                    report.skipped_migration_objects =
                        report.skipped_migration_objects.saturating_add(1);
                    continue;
                }
            };
            if current.etag() != item.object.etag() || current.version != item.object.version {
                report.skipped_migration_objects =
                    report.skipped_migration_objects.saturating_add(1);
                continue;
            }
            match self.meta_service.commit_placement_migration_with_min_free(
                item.plan.clone(),
                &item.plan.plan_checksum,
                min_free_bytes,
            ) {
                Ok(placement) => {
                    if placement.node_ids == item.plan.before_node_ids {
                        report.skipped_migration_objects =
                            report.skipped_migration_objects.saturating_add(1);
                    } else {
                        report.migrated_objects = report.migrated_objects.saturating_add(1);
                        report.bytes_migrated =
                            report.bytes_migrated.saturating_add(placement.length);
                        report.migrated.push(placement);
                    }
                }
                Err(err) => {
                    report.failed_migration_objects =
                        report.failed_migration_objects.saturating_add(1);
                    if report.first_error.is_none() {
                        report.first_error = Some(err.to_string());
                    }
                }
            }
        }
        report
    }

    pub fn rebalance_object_capacity_fenced(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<&ObjectId>,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectCapacityRebalanceRunReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.rebalance_object_capacity(max_objects, min_free_bytes, marker);
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

}
