impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn plan_object_capacity_rebalance(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<&ObjectId>,
    ) -> ObjectCapacityRebalanceReport {
        self.store
            .plan_object_capacity_rebalance(max_objects, min_free_bytes, marker)
    }

    pub fn plan_object_capacity_rebalance_with_read_proof_linearizable(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<&ObjectId>,
    ) -> Result<ProvenRootRead<ObjectCapacityRebalanceReport>, ObjectError> {
        let proof = self.ensure_root_read_index("plan object capacity rebalance")?;
        let value = self.plan_object_capacity_rebalance(max_objects, min_free_bytes, marker);
        Ok(ProvenRootRead { value, proof })
    }

    pub fn propose_rebalance_object_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
        marker: Option<ObjectId>,
    ) -> Result<ObjectCapacityRebalanceRunReport, ObjectError> {
        let plan =
            self.store
                .plan_object_capacity_rebalance(max_objects, min_free_bytes, marker.as_ref());
        let mut report = ObjectCapacityRebalanceRunReport {
            plan,
            route_control_proof: self.store.chunk_service.route_control_proof(),
            ..ObjectCapacityRebalanceRunReport::default()
        };
        if marker.is_none() {
            let prepared = self.prepare_root_capacity_rebalance_raft_entry_linearizable(
                max_objects,
                min_free_bytes,
            )?;
            let prepared_matches_object_window = prepared.report.items.len()
                == report.plan.items.len()
                && prepared
                    .report
                    .items
                    .iter()
                    .zip(report.plan.items.iter())
                    .all(|(prepared_item, planned_item)| prepared_item.plan == planned_item.plan);
            if prepared_matches_object_window {
                match self.propose_prepared_root_capacity_rebalance_linearizable(prepared.clone()) {
                    Ok(_) => {
                        for item in prepared.report.items {
                            let update_time_ms = report
                                .plan
                                .items
                                .iter()
                                .find(|planned| planned.object.object_id == item.plan.object_id)
                                .map(|planned| planned.object.update_time_ms)
                                .unwrap_or_default();
                            report.attempted_migration_objects =
                                report.attempted_migration_objects.saturating_add(1);
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
                                    update_time_ms,
                                });
                            }
                        }
                        return Ok(report);
                    }
                    Err(err) => {
                        report.failed_migration_objects = report
                            .failed_migration_objects
                            .saturating_add(prepared.report.items.len() as u64);
                        report.first_error = Some(err.to_string());
                        return Ok(report);
                    }
                }
            }
        }
        self.propose_rebalance_object_capacity_plan_items(&mut report);
        Ok(report)
    }

    fn propose_rebalance_object_capacity_plan(
        &mut self,
        plan: ObjectCapacityRebalanceReport,
    ) -> ObjectCapacityRebalanceRunReport {
        let mut report = ObjectCapacityRebalanceRunReport {
            plan,
            route_control_proof: self.store.chunk_service.route_control_proof(),
            ..ObjectCapacityRebalanceRunReport::default()
        };
        self.propose_rebalance_object_capacity_plan_items(&mut report);
        report
    }

    fn propose_rebalance_object_capacity_plan_items(
        &mut self,
        report: &mut ObjectCapacityRebalanceRunReport,
    ) {
        let items = report.plan.items.clone();
        for item in items {
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
                            update_time_ms: item.object.update_time_ms,
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
    }
}
