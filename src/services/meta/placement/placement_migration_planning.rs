// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn plan_placement_migration(
        &self,
        object_id: &ObjectId,
        source_node_id: &str,
        target_node_id: &str,
    ) -> Result<PlacementMigrationPlan, ObjectError> {
        validate_node_id(source_node_id)?;
        validate_node_id(target_node_id)?;
        if source_node_id == target_node_id {
            return Err(ObjectError::InvalidArgument(
                "root placement migration source and target must differ".to_string(),
            ));
        }
        let placement = self.object_placement(object_id)?;
        if !placement.node_ids.iter().any(|node| node == source_node_id) {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration source is not a current replica".to_string(),
            ));
        }
        if placement.node_ids.iter().any(|node| node == target_node_id) {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration target is already a current replica".to_string(),
            ));
        }
        let source = self.nodes.get(source_node_id).ok_or_else(|| {
            ObjectError::NotFound("root placement migration source missing".to_string())
        })?;
        let target = self.nodes.get(target_node_id).ok_or_else(|| {
            ObjectError::NotFound("root placement migration target missing".to_string())
        })?;
        if !Self::node_accepts_new_placement(target) {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration target is not writable".to_string(),
            ));
        }
        if target.committed_bytes.saturating_add(placement.length)
            > Self::node_effective_capacity_bytes(target)
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration target lacks capacity".to_string(),
            ));
        }
        let reason = if source.draining {
            "draining".to_string()
        } else if !source.online {
            "offline".to_string()
        } else if source.committed_bytes > source.capacity_bytes {
            "capacity".to_string()
        } else {
            "manual".to_string()
        };
        let mut after_node_ids = placement.node_ids.clone();
        if let Some(index) = after_node_ids
            .iter()
            .position(|node| node == source_node_id)
        {
            after_node_ids[index] = target_node_id.to_string();
        }
        let mut plan = PlacementMigrationPlan {
            object_id: placement.object_id.clone(),
            version: placement.version,
            length: placement.length,
            source_node_id: source_node_id.to_string(),
            target_node_id: target_node_id.to_string(),
            before_node_ids: placement.node_ids.clone(),
            after_node_ids,
            reason,
            plan_checksum: String::new(),
        };
        plan.plan_checksum = placement_migration_plan_checksum(&plan);
        Ok(plan)
    }

    pub fn commit_placement_migration(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
    ) -> Result<ObjectPlacement, ObjectError> {
        self.commit_placement_migration_with_min_free(plan, expected_plan_checksum, 0)
    }

    pub fn commit_placement_migration_with_min_free(
        &mut self,
        plan: PlacementMigrationPlan,
        expected_plan_checksum: &str,
        min_free_bytes: u64,
    ) -> Result<ObjectPlacement, ObjectError> {
        if plan.plan_checksum != expected_plan_checksum
            || placement_migration_plan_checksum(&plan) != expected_plan_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration plan checksum mismatch".to_string(),
            ));
        }
        let current = self.object_placement(&plan.object_id)?;
        if current.version != plan.version
            || current.length != plan.length
            || current.node_ids != plan.before_node_ids
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration object fence mismatch".to_string(),
            ));
        }
        if plan.after_node_ids.len() != plan.before_node_ids.len()
            || !plan
                .before_node_ids
                .iter()
                .any(|node| node == &plan.source_node_id)
            || plan
                .before_node_ids
                .iter()
                .any(|node| node == &plan.target_node_id)
            || plan
                .after_node_ids
                .iter()
                .any(|node| node == &plan.source_node_id)
            || !plan
                .after_node_ids
                .iter()
                .any(|node| node == &plan.target_node_id)
            || plan.after_node_ids.iter().collect::<BTreeSet<_>>().len()
                != plan.after_node_ids.len()
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration replica set is invalid".to_string(),
            ));
        }
        if !self.nodes.contains_key(&plan.source_node_id) {
            return Err(ObjectError::NotFound(
                "root placement migration source missing".to_string(),
            ));
        }
        let target = self.nodes.get(&plan.target_node_id).ok_or_else(|| {
            ObjectError::NotFound("root placement migration target missing".to_string())
        })?;
        if !Self::node_accepts_new_placement(target) {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration target is not writable".to_string(),
            ));
        }
        if target.committed_bytes.saturating_add(plan.length)
            > Self::node_effective_capacity_bytes(target)
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration target lacks capacity".to_string(),
            ));
        }
        if min_free_bytes > 0
            && Self::node_effective_free_bytes(target) < plan.length.saturating_add(min_free_bytes)
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration target lacks capacity floor".to_string(),
            ));
        }
        if !current
            .node_ids
            .iter()
            .any(|node| node == &plan.source_node_id)
            || current
                .node_ids
                .iter()
                .any(|node| node == &plan.target_node_id)
        {
            return Err(ObjectError::PreconditionFailed(
                "root placement migration replica set fence mismatch".to_string(),
            ));
        }
        self.release_committed_bytes(&current);
        let migrated = ObjectPlacement {
            object_id: current.object_id.clone(),
            version: current.version,
            length: current.length,
            node_ids: plan.after_node_ids,
            update_time_ms: current.update_time_ms,
        };
        for node_id in &migrated.node_ids {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.committed_bytes = node.committed_bytes.saturating_add(migrated.length);
            }
        }
        self.placements
            .insert(migrated.object_id.clone(), migrated.clone());
        self.advance_commit_index();
        Ok(migrated)
    }
}
