impl MatrixRootService {
    pub fn apply_prepared_unhealthy_placement_repair(
        &mut self,
        prepared: PreparedRootUnhealthyPlacementRepair,
    ) -> Result<RootPlacementRepairReport, ObjectError> {
        if prepared.expected_previous_commit_index != self.placement_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "root prepared unhealthy placement repair previous commit fence mismatch"
                    .to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.placement_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "root prepared unhealthy placement repair commit index must be contiguous"
                    .to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "root prepared unhealthy placement repair commit index must be adjacent"
                    .to_string(),
            ));
        }
        let expected_checksum = prepared_root_unhealthy_placement_repair_checksum(
            &PreparedRootUnhealthyPlacementRepair {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            },
        );
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "root prepared unhealthy placement repair raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != root_unhealthy_placement_repair_request_checksum(
                prepared.marker.as_ref(),
                prepared.max_objects,
                prepared.expected_replica_count,
                prepared.min_free_bytes,
                prepared.max_payload_bytes,
            )
        {
            return Err(ObjectError::Corruption(
                "root prepared unhealthy placement repair request checksum mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.root_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "root prepared unhealthy placement repair state changed before apply".to_string(),
            ));
        }
        let planned = self.plan_unhealthy_placement_repairs(
            prepared.marker.as_ref(),
            prepared.max_objects,
            prepared.expected_replica_count,
            prepared.min_free_bytes,
            prepared.max_payload_bytes,
        )?;
        if planned != prepared.report {
            return Err(ObjectError::PreconditionFailed(
                "root prepared unhealthy placement repair plan changed before apply".to_string(),
            ));
        }
        let mut report = prepared.report;
        let mut updated_placements = Vec::new();
        for item in &report.items {
            let plan = &item.plan;
            if plan.plan_checksum != placement_migration_plan_checksum(plan) {
                return Err(ObjectError::Corruption(
                    "root prepared unhealthy placement repair plan checksum mismatch".to_string(),
                ));
            }
            let current = self.object_placement(&plan.object_id)?;
            if current.version != plan.version
                || current.length != plan.length
                || current.node_ids != plan.before_node_ids
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared unhealthy placement repair object fence mismatch".to_string(),
                ));
            }
            if plan.after_node_ids.len() < plan.before_node_ids.len()
                || plan.after_node_ids.len() > plan.before_node_ids.len().saturating_add(1)
                || plan.after_node_ids.iter().collect::<BTreeSet<_>>().len()
                    != plan.after_node_ids.len()
                || !plan
                    .after_node_ids
                    .iter()
                    .any(|node_id| node_id == &plan.target_node_id)
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared unhealthy placement repair replica set is invalid".to_string(),
                ));
            }
            if !plan.source_node_id.is_empty()
                && (!current
                    .node_ids
                    .iter()
                    .any(|node_id| node_id == &plan.source_node_id)
                    || plan
                        .after_node_ids
                        .iter()
                        .any(|node_id| node_id == &plan.source_node_id))
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared unhealthy placement repair source fence mismatch".to_string(),
                ));
            }
            if plan.source_node_id.is_empty()
                && plan.after_node_ids.len() != plan.before_node_ids.len() + 1
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared unhealthy placement repair add-replica fence mismatch"
                        .to_string(),
                ));
            }
            let target = self.nodes.get(&plan.target_node_id).ok_or_else(|| {
                ObjectError::NotFound("root placement health repair target missing".to_string())
            })?;
            if !Self::node_accepts_new_placement(target) {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared unhealthy placement repair target is not writable".to_string(),
                ));
            }
            if target.committed_bytes.saturating_add(plan.length)
                > Self::node_effective_capacity_bytes(target)
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared unhealthy placement repair target lacks capacity".to_string(),
                ));
            }
            updated_placements.push(ObjectPlacement {
                object_id: current.object_id.clone(),
                version: current.version,
                length: current.length,
                node_ids: plan.after_node_ids.clone(),
                update_time_ms: current.update_time_ms,
            });
        }
        for placement in updated_placements {
            if let Some(previous) = self.placements.get(&placement.object_id).cloned() {
                self.release_committed_bytes(&previous);
            }
            for node_id in &placement.node_ids {
                if let Some(node) = self.nodes.get_mut(node_id) {
                    node.committed_bytes = node.committed_bytes.saturating_add(placement.length);
                }
            }
            report.repaired_objects = report.repaired_objects.saturating_add(1);
            report.bytes_repaired = report.bytes_repaired.saturating_add(placement.length);
            self.placements
                .insert(placement.object_id.clone(), placement);
        }
        self.advance_commit_index();
        Ok(report)
    }

}
