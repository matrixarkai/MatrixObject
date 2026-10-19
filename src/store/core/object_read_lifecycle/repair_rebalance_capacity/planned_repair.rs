// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn repair_object_range_bounded_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<BoundedRepairReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report = self.repair_object_range_bounded(
            bucket,
            key,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
        )?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

    pub fn plan_object_replica_repairs(
        &self,
        max_objects: usize,
        marker: Option<&ObjectId>,
    ) -> Result<ObjectReplicaRepairPlan, ObjectError> {
        let mut plan = ObjectReplicaRepairPlan {
            marker: marker.cloned(),
            ..ObjectReplicaRepairPlan::default()
        };
        let limit = if max_objects == 0 {
            usize::MAX
        } else {
            max_objects
        };
        let mut last_selected: Option<ObjectId> = None;
        for (object_id, metadata) in &self.objects {
            if marker.is_some_and(|marker| object_id <= marker) {
                continue;
            }
            if metadata.deleted || metadata.length == 0 {
                continue;
            }
            plan.checked_objects = plan.checked_objects.saturating_add(1);
            let repair_estimate = self.plan_object_repair_work(metadata)?;
            if repair_estimate.0 == 0 {
                continue;
            }
            if plan.items.len() >= limit {
                plan.truncated = true;
                plan.next_marker = last_selected;
                break;
            }
            plan.repair_objects = plan.repair_objects.saturating_add(1);
            plan.estimated_chunks = plan.estimated_chunks.saturating_add(repair_estimate.0);
            plan.bytes_to_repair = plan.bytes_to_repair.saturating_add(repair_estimate.1);
            plan.items.push(ObjectReplicaRepairItem {
                object: metadata.clone(),
                estimated_chunks: repair_estimate.0,
                estimated_bytes: repair_estimate.1,
            });
            last_selected = Some(object_id.clone());
        }
        finalize_object_replica_repair_plan(&mut plan);
        Ok(plan)
    }

    pub fn repair_planned_object_replicas(
        &mut self,
        max_objects: usize,
        marker: Option<&ObjectId>,
        max_repair_chunks_per_object: usize,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        let plan = self.plan_object_replica_repairs(max_objects, marker)?;
        let mut report = ObjectReplicaRepairRunReport {
            plan,
            route_control_proof: self.chunk_service.route_control_proof(),
            ..ObjectReplicaRepairRunReport::default()
        };
        let items = report.plan.items.clone();
        for item in items {
            report.attempted_repair_objects = report.attempted_repair_objects.saturating_add(1);
            let object = item.object;
            let current = match self.objects.get(&object.object_id) {
                Some(metadata) if !metadata.deleted => metadata,
                _ => {
                    report.skipped_repair_objects = report.skipped_repair_objects.saturating_add(1);
                    continue;
                }
            };
            if current.etag() != object.etag()
                || current.version != object.version
                || current.length != object.length
            {
                report.skipped_repair_objects = report.skipped_repair_objects.saturating_add(1);
                continue;
            }
            match self.repair_object_range_bounded(
                &object.object_id.bucket,
                &object.object_id.key,
                0,
                object.length,
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
                    } else if repaired_replicas > 0 {
                        report.repaired_objects = report.repaired_objects.saturating_add(1);
                        report.bytes_repaired = report.bytes_repaired.saturating_add(object.length);
                        report.repaired.push(object);
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
                }
            }
        }
        Ok(report)
    }

    pub fn repair_planned_object_replicas_fenced(
        &mut self,
        max_objects: usize,
        marker: Option<&ObjectId>,
        max_repair_chunks_per_object: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        let mut report =
            self.repair_planned_object_replicas(max_objects, marker, max_repair_chunks_per_object)?;
        report.expected_route_control_checksum = expected_route_control_checksum
            .unwrap_or_default()
            .to_string();
        Ok(report)
    }

}
