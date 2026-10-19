// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn plan_object_replica_repairs(
        &self,
        max_objects: usize,
        marker: Option<&ObjectId>,
    ) -> Result<ObjectReplicaRepairPlan, ObjectError> {
        self.store.plan_object_replica_repairs(max_objects, marker)
    }

    pub fn plan_object_replica_repairs_with_read_proof_linearizable(
        &self,
        max_objects: usize,
        marker: Option<&ObjectId>,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectReplicaRepairPlan>, ObjectError> {
        let applied_metadata_index = self
            .store
            .metadata_raft_readiness(0, now_ms)
            .applied_commit_index;
        let readiness = self
            .store
            .metadata_raft_readiness(applied_metadata_index, now_ms);
        if !readiness.readable {
            return Err(ObjectError::PreconditionFailed(format!(
                "object replica repair plan read index is not satisfied: reason={} applied={} required={}",
                readiness.reason, readiness.applied_commit_index, readiness.min_read_index
            )));
        }
        let plan = self.plan_object_replica_repairs(max_objects, marker)?;
        Ok(proven_metadata_read(plan, &readiness))
    }

    pub fn propose_repair_planned_object_replicas(
        &mut self,
        max_objects: usize,
        marker: Option<ObjectId>,
        max_repair_chunks_per_object: usize,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        let plan = self
            .store
            .plan_object_replica_repairs(max_objects, marker.as_ref())?;
        self.propose_repair_object_replica_plan(plan, max_repair_chunks_per_object)
    }

    fn propose_repair_object_replica_plan(
        &mut self,
        plan: ObjectReplicaRepairPlan,
        max_repair_chunks_per_object: usize,
    ) -> Result<ObjectReplicaRepairRunReport, ObjectError> {
        let mut report = ObjectReplicaRepairRunReport {
            plan,
            route_control_proof: self.store.chunk_service.route_control_proof(),
            ..ObjectReplicaRepairRunReport::default()
        };
        let items = report.plan.items.clone();
        for item in items {
            report.attempted_repair_objects = report.attempted_repair_objects.saturating_add(1);
            let object = item.object;
            let current = match self.store.objects.get(&object.object_id) {
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
            match self.propose_repair_object_metadata_range_bounded(
                &object,
                0,
                object.length,
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

}
