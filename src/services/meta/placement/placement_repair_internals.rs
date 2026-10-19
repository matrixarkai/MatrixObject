// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    fn repair_source_for_placement(&self, placement: &ObjectPlacement) -> Option<String> {
        placement.node_ids.iter().find_map(|node_id| {
            let Some(node) = self.nodes.get(node_id) else {
                return Some(node_id.clone());
            };
            if !Self::node_accepts_new_placement(node)
                || node.committed_bytes > Self::node_effective_capacity_bytes(node)
            {
                Some(node_id.clone())
            } else {
                None
            }
        })
    }

    fn plan_repair_target_for_placement(
        &self,
        placement: &ObjectPlacement,
        source_node_id: &str,
    ) -> Result<PlacementMigrationPlan, ObjectError> {
        let mut candidates: Vec<&RootNode> = self
            .nodes
            .values()
            .filter(|node| {
                Self::node_accepts_new_placement(node)
                    && !placement
                        .node_ids
                        .iter()
                        .any(|current| current == &node.node_id)
                    && node.committed_bytes.saturating_add(placement.length)
                        <= Self::node_effective_capacity_bytes(node)
            })
            .collect();
        candidates.sort_by(|left, right| Self::compare_placement_target_nodes(left, right));
        let target = candidates.first().ok_or_else(|| {
            ObjectError::PreconditionFailed("root placement repair target missing".to_string())
        })?;
        self.plan_placement_migration(&placement.object_id, source_node_id, &target.node_id)
    }

    fn plan_health_repair_for_item(
        &self,
        item: &RootPlacementHealthItem,
        expected_replica_count: usize,
        min_free_bytes: u64,
    ) -> Result<PlacementMigrationPlan, ObjectError> {
        let source_node_id = item
            .missing_node_ids
            .first()
            .or_else(|| item.offline_node_ids.first())
            .or_else(|| item.draining_node_ids.first())
            .or_else(|| item.capacity_pressure_node_ids.first())
            .cloned();
        let target_node_id =
            self.select_placement_repair_target(&item.placement, min_free_bytes)?;
        let mut after_node_ids = item.placement.node_ids.clone();
        let (source_node_id, reason) = if let Some(source_node_id) = source_node_id {
            let Some(index) = after_node_ids
                .iter()
                .position(|node_id| node_id == &source_node_id)
            else {
                return Err(ObjectError::PreconditionFailed(
                    "root placement health source is no longer present".to_string(),
                ));
            };
            after_node_ids[index] = target_node_id.clone();
            (source_node_id, "health_repair".to_string())
        } else if expected_replica_count > 0 && after_node_ids.len() < expected_replica_count {
            after_node_ids.push(target_node_id.clone());
            (String::new(), "under_replicated".to_string())
        } else {
            return Err(ObjectError::PreconditionFailed(
                "root placement health item has no repairable source".to_string(),
            ));
        };
        if after_node_ids.iter().collect::<BTreeSet<_>>().len() != after_node_ids.len() {
            return Err(ObjectError::PreconditionFailed(
                "root placement health repair target duplicates placement".to_string(),
            ));
        }
        let mut plan = PlacementMigrationPlan {
            object_id: item.placement.object_id.clone(),
            version: item.placement.version,
            length: item.placement.length,
            source_node_id,
            target_node_id,
            before_node_ids: item.placement.node_ids.clone(),
            after_node_ids,
            reason,
            plan_checksum: String::new(),
        };
        plan.plan_checksum = placement_migration_plan_checksum(&plan);
        Ok(plan)
    }

    fn select_placement_repair_target(
        &self,
        placement: &ObjectPlacement,
        min_free_bytes: u64,
    ) -> Result<String, ObjectError> {
        let mut candidates: Vec<&RootNode> = self
            .nodes
            .values()
            .filter(|node| {
                Self::node_accepts_new_placement(node)
                    && !placement
                        .node_ids
                        .iter()
                        .any(|current| current == &node.node_id)
                    && Self::node_effective_free_bytes(node)
                        >= placement.length.saturating_add(min_free_bytes)
            })
            .collect();
        candidates.sort_by(|left, right| Self::compare_placement_target_nodes(left, right));
        candidates
            .first()
            .map(|node| node.node_id.clone())
            .ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "root placement health repair target missing".to_string(),
                )
            })
    }

    fn commit_placement_health_repair(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        if plan.plan_checksum != expected_plan_checksum
            || placement_migration_plan_checksum(&plan) != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement health repair plan checksum mismatch".to_string(),
            ));
        }
        let current = self.object_placement(&plan.object_id)?;
        if current.version != plan.version
            || current.length != plan.length
            || current.node_ids != plan.before_node_ids
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement health repair object fence mismatch".to_string(),
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
                "root placement health repair replica set is invalid".to_string(),
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
                "root placement health repair source fence mismatch".to_string(),
            ));
        }
        if plan.source_node_id.is_empty()
            && plan.after_node_ids.len() != plan.before_node_ids.len() + 1
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement health repair add-replica fence mismatch".to_string(),
            ));
        }
        let target = self.nodes.get(&plan.target_node_id).ok_or_else(|| {
            ObjectError::NotFound("root placement health repair target missing".to_string())
        })?;
        if !Self::node_accepts_new_placement(target) {
            return Err(ObjectError::PreconditionFailed(
                "root placement health repair target is not writable".to_string(),
            ));
        }
        if target.committed_bytes.saturating_add(plan.length)
            > Self::node_effective_capacity_bytes(target)
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement health repair target lacks capacity".to_string(),
            ));
        }
        self.release_committed_bytes(&current);
        let repaired = ObjectPlacement {
            object_id: current.object_id.clone(),
            version: current.version,
            length: current.length,
            node_ids: plan.after_node_ids,
            update_time_ms: current.update_time_ms,
        };
        for node_id in &repaired.node_ids {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.committed_bytes = node.committed_bytes.saturating_add(repaired.length);
            }
        }
        self.placements
            .insert(repaired.object_id.clone(), repaired.clone());
        self.advance_commit_index();
        Ok(repaired)
    }
}
