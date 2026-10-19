// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_decommission_draining_object_replicas(
        &mut self,
        max_objects: usize,
        max_repair_chunks_per_object: usize,
    ) -> ObjectReplicaDecommissionReport {
        let plan = self.plan_object_decommission_draining_replicas(max_objects);
        self.propose_decommission_draining_object_replica_plan(
            plan,
            max_repair_chunks_per_object,
            "",
        )
        .expect("empty decommission plan checksum fence cannot fail")
    }

    fn propose_decommission_draining_object_replica_plan(
        &mut self,
        plan: RootPlacementRepairReport,
        max_repair_chunks_per_object: usize,
        expected_plan_checksum: &str,
    ) -> Result<ObjectReplicaDecommissionReport, ObjectError> {
        plan.verify_expected_plan_checksum(expected_plan_checksum)?;
        let mut report = ObjectReplicaDecommissionReport {
            plan,
            route_control_proof: self.store.chunk_service.route_control_proof(),
            expected_plan_checksum: expected_plan_checksum.to_string(),
            ..ObjectReplicaDecommissionReport::default()
        };
        let items = report.plan.items.clone();
        for item in items {
            let object_id = item.plan.object_id.clone();
            let metadata = match self.store.objects.get(&object_id) {
                Some(metadata) if !metadata.deleted => metadata.clone(),
                _ => {
                    report.failed_repair_objects = report.failed_repair_objects.saturating_add(1);
                    if report.first_error.is_none() {
                        report.first_error = Some(format!(
                            "object missing during raft decommission: {}/{}",
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
            match self.propose_repair_object_metadata_range_bounded(
                &metadata,
                0,
                metadata.length,
                max_repair_chunks_per_object,
                0,
                None,
            ) {
                Ok(repair) => {
                    let failed_repairs = repair.stats.failed_repairs;
                    let repaired_replicas = repair.stats.repaired_replicas;
                    merge_repair_stats(&mut report.stats, repair.stats);
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
            let prepared = match self.prepare_root_placement_migration_raft_entry(
                item.plan.clone(),
                &item.plan.plan_checksum,
            ) {
                Ok(prepared) => prepared,
                Err(err) => {
                    report.failed_migration_objects =
                        report.failed_migration_objects.saturating_add(1);
                    if report.first_error.is_none() {
                        report.first_error = Some(err.to_string());
                    }
                    continue;
                }
            };
            match self.propose_prepared_root_placement_migration(prepared) {
                Ok(_) => {
                    if item.plan.after_node_ids == item.plan.before_node_ids {
                        report.skipped_migration_objects =
                            report.skipped_migration_objects.saturating_add(1);
                    } else {
                        report.migrated_objects = report.migrated_objects.saturating_add(1);
                        report.bytes_migrated =
                            report.bytes_migrated.saturating_add(item.plan.length);
                        report.migrated.push(ObjectPlacement {
                            object_id: item.plan.object_id,
                            version: item.plan.version,
                            length: item.plan.length,
                            node_ids: item.plan.after_node_ids,
                            update_time_ms: metadata.update_time_ms,
                        });
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
        Ok(report)
    }

}
