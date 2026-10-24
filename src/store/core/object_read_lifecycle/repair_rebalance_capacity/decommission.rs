// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn decommission_draining_replicas(
        &mut self,
        max_objects: usize,
        max_repair_chunks_per_object: usize,
    ) -> ObjectReplicaDecommissionReport {
        let plan = self
            .meta_service
            .plan_draining_placement_repairs(max_objects);
        let mut report = ObjectReplicaDecommissionReport {
            plan,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectReplicaDecommissionReport::default()
        };
        let items = report.plan.items.clone();
        for item in items {
            let object_id = item.plan.object_id.clone();
            let metadata = match self.object(&object_id) {
                Some(metadata) if !metadata.deleted => metadata.clone(),
                _ => {
                    report.failed_repair_objects = report.failed_repair_objects.saturating_add(1);
                    if report.first_error.is_none() {
                        report.first_error = Some(format!(
                            "object missing during decommission: {}/{}",
                            object_id.bucket, object_id.key
                        ));
                    }
                    continue;
                }
            };

            report.attempted_repair_objects = report.attempted_repair_objects.saturating_add(1);
            if metadata.version != item.plan.version || metadata.length != item.plan.length {
                report.skipped_repair_objects = report.skipped_repair_objects.saturating_add(1);
                report.skipped_migration_objects =
                    report.skipped_migration_objects.saturating_add(1);
                continue;
            }
            match self.repair_object_range_bounded(
                &metadata.object_id.bucket,
                &metadata.object_id.key,
                0,
                metadata.length,
                max_repair_chunks_per_object,
                0,
            ) {
                Ok(repair) => {
                    let stats = repair.stats;
                    let failed_repairs = stats.failed_repairs;
                    let repaired_replicas = stats.repaired_replicas;
                    merge_repair_stats(&mut report.stats, stats);
                    if failed_repairs > 0 {
                        report.failed_repair_objects =
                            report.failed_repair_objects.saturating_add(1);
                        continue;
                    }
                    if repaired_replicas > 0 {
                        report.repaired_objects = report.repaired_objects.saturating_add(1);
                        report.bytes_repaired =
                            report.bytes_repaired.saturating_add(metadata.length);
                    } else {
                        report.skipped_repair_objects =
                            report.skipped_repair_objects.saturating_add(1);
                    }
                }
                Err(err) => {
                    report.failed_repair_objects = report.failed_repair_objects.saturating_add(1);
                    if report.first_error.is_none() {
                        report.first_error = Some(err.to_string());
                    }
                    continue;
                }
            }

            report.attempted_migration_objects =
                report.attempted_migration_objects.saturating_add(1);
            match self
                .meta_service
                .commit_placement_migration(item.plan.clone(), &item.plan.plan_checksum)
            {
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

    pub fn decommission_draining_replicas_fenced(
        &mut self,
        max_objects: usize,
        max_repair_chunks_per_object: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectReplicaDecommissionReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report =
            self.decommission_draining_replicas(max_objects, max_repair_chunks_per_object);
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

}
