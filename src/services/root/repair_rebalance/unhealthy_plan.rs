impl MatrixRootService {
    pub fn plan_unhealthy_placement_repairs(
        &self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        let health = self.audit_placement_health(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            true,
            max_payload_bytes,
        )?;
        let mut report = RootPlacementRepairReport {
            marker: marker.cloned(),
            next_marker: health.next_marker.clone(),
            has_more: health.has_more,
            scanned_objects: health.scanned_objects,
            skipped_objects: health.skipped_objects,
            ..RootPlacementRepairReport::default()
        };
        for item in health.items {
            match self.plan_health_repair_for_item(&item, expected_replica_count, min_free_bytes) {
                Ok(plan) => {
                    report.repair_objects = report.repair_objects.saturating_add(1);
                    report.bytes_to_repair = report.bytes_to_repair.saturating_add(plan.length);
                    report.items.push(RootPlacementRepairItem { plan });
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
        Self::finalize_root_placement_repair_report(&mut report);
        Ok(report)
    }

    pub fn repair_unhealthy_placements(
        &mut self,
        marker: Option<&ObjectId>,
        max_objects: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        max_payload_bytes: u64,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
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
    ) -> Result<PreparedRootUnhealthyPlacementRepair, ObjectError> {
        let report = self.plan_unhealthy_placement_repairs(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        )?;
        let request_checksum = root_unhealthy_placement_repair_request_checksum(
            marker,
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
        );
        let mut prepared = PreparedRootUnhealthyPlacementRepair {
            marker: marker.cloned(),
            max_objects,
            expected_replica_count,
            min_free_bytes,
            max_payload_bytes,
            report,
            before_state_fingerprint: self.root_state_fingerprint(),
            request_checksum,
            expected_previous_commit_index: self.placement_commit_index.saturating_sub(1),
            prepared_commit_index: self.placement_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_root_unhealthy_placement_repair_checksum(&prepared);
        Ok(prepared)
    }

}
