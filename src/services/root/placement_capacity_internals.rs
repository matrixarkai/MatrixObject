impl MatrixRootService {
    fn plan_capacity_rebalance_target(
        &self,
        placement: &ObjectPlacement,
        source_node_id: &str,
        min_free_bytes: u64,
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
                    && Self::node_effective_free_bytes(node)
                        >= min_free_bytes.saturating_add(placement.length)
            })
            .collect();
        candidates.sort_by(|left, right| Self::compare_placement_target_nodes(left, right));
        let target = candidates.first().ok_or_else(|| {
            ObjectError::PreconditionFailed("root capacity rebalance target missing".to_string())
        })?;
        let mut plan =
            self.plan_placement_migration(&placement.object_id, source_node_id, &target.node_id)?;
        plan.reason = "capacity_pressure".to_string();
        plan.plan_checksum = placement_migration_plan_checksum(&plan);
        Ok(plan)
    }
}
