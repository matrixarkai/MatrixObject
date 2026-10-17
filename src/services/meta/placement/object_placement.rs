impl MatrixMetaService {
    pub fn plan_object_placement(
        &self,
        object_id: ObjectId,
        length: u64,
        replica_count: usize,
    ) -> Result<PlacementPlan, ObjectError> {
        if replica_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "root placement replica_count must be positive".to_string(),
            ));
        }
        let mut skipped_draining = 0u64;
        let mut skipped_offline = 0u64;
        let mut skipped_capacity = 0u64;
        let mut candidates = Vec::new();
        let previous = self.placements.get(&object_id);
        for node in self.nodes.values() {
            if !Self::is_data_service_node(node) {
                continue;
            }
            if !node.online {
                skipped_offline = skipped_offline.saturating_add(1);
                continue;
            }
            if !Self::node_accepts_new_placement(node) {
                skipped_draining = skipped_draining.saturating_add(1);
                continue;
            }
            let previous_bytes = previous.map_or(0, |old| {
                if old
                    .node_ids
                    .iter()
                    .any(|old_node| old_node == &node.node_id)
                {
                    old.length
                } else {
                    0
                }
            });
            let projected = node
                .committed_bytes
                .saturating_sub(previous_bytes)
                .saturating_add(length);
            if projected > Self::node_effective_capacity_bytes(node) {
                skipped_capacity = skipped_capacity.saturating_add(1);
                continue;
            }
            candidates.push(node);
        }
        candidates.sort_by(|left, right| Self::compare_placement_target_nodes(left, right));
        let mut selected = Vec::new();
        let mut used_domains = BTreeSet::new();
        for node in &candidates {
            if selected.len() >= replica_count {
                break;
            }
            if used_domains.insert(node.failure_domain.clone()) || used_domains.len() == 1 {
                selected.push(node.node_id.clone());
            }
        }
        for node in &candidates {
            if selected.len() >= replica_count {
                break;
            }
            if !selected.contains(&node.node_id) {
                selected.push(node.node_id.clone());
            }
        }
        if selected.len() < replica_count {
            return Err(ObjectError::PreconditionFailed(
                "root placement could not satisfy replica count".to_string(),
            ));
        }
        let mut plan = PlacementPlan {
            object_id,
            length,
            node_ids: selected,
            skipped_draining,
            skipped_offline,
            skipped_capacity,
            plan_checksum: String::new(),
        };
        plan.plan_checksum = placement_plan_checksum(&plan);
        Ok(plan)
    }

    pub fn commit_object_placement(
        &mut self,
        placement: ObjectPlacement,
        expected_plan_checksum: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        let plan = PlacementPlan {
            object_id: placement.object_id.clone(),
            length: placement.length,
            node_ids: placement.node_ids.clone(),
            skipped_draining: 0,
            skipped_offline: 0,
            skipped_capacity: 0,
            plan_checksum: String::new(),
        };
        let mut normalized = plan;
        normalized.plan_checksum = placement_plan_checksum(&normalized);
        if normalized.plan_checksum != expected_plan_checksum {
            return Err(ObjectError::PreconditionFailed(
                "root placement plan checksum mismatch".to_string(),
            ));
        }
        let previous = self.placements.get(&placement.object_id).cloned();
        for node_id in &placement.node_ids {
            let node = self
                .nodes
                .get(node_id)
                .ok_or_else(|| ObjectError::NotFound("root placement node missing".to_string()))?;
            if !Self::node_accepts_new_placement(node) {
                return Err(ObjectError::PreconditionFailed(
                    "root placement node is not writable".to_string(),
                ));
            }
            let previous_bytes = previous.as_ref().map_or(0, |old| {
                if old.node_ids.iter().any(|old_node| old_node == node_id) {
                    old.length
                } else {
                    0
                }
            });
            if node
                .committed_bytes
                .saturating_sub(previous_bytes)
                .saturating_add(placement.length)
                > Self::node_effective_capacity_bytes(node)
            {
                return Err(ObjectError::PreconditionFailed(
                    "root placement node lacks capacity".to_string(),
                ));
            }
        }
        if let Some(previous) = previous {
            self.release_committed_bytes(&previous);
        }
        for node_id in &placement.node_ids {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.committed_bytes = node.committed_bytes.saturating_add(placement.length);
            }
        }
        self.placements
            .insert(placement.object_id.clone(), placement.clone());
        self.advance_commit_index();
        Ok(placement)
    }

    pub fn release_object_placement(
        &mut self,
        object_id: &ObjectId,
    ) -> Result<ObjectPlacement, ObjectError> {
        let placement = self
            .placements
            .remove(object_id)
            .ok_or_else(|| ObjectError::NotFound("root placement missing".to_string()))?;
        self.release_committed_bytes(&placement);
        self.advance_commit_index();
        Ok(placement)
    }
}
