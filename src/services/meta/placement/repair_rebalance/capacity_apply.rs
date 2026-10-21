// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn apply_prepared_capacity_rebalance(
        &mut self,
        prepared: PreparedRootCapacityRebalance,
    ) -> Result<RootCapacityRebalanceReport, ObjectError> {
        if prepared.expected_previous_commit_index != self.placement_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "root prepared capacity rebalance previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.placement_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "root prepared capacity rebalance commit index must be contiguous".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "root prepared capacity rebalance commit index must be adjacent".to_string(),
            ));
        }
        let expected_checksum =
            prepared_root_capacity_rebalance_checksum(&PreparedRootCapacityRebalance {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "root prepared capacity rebalance raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != root_capacity_rebalance_request_checksum(
                prepared.max_objects,
                prepared.min_free_bytes,
            )
        {
            return Err(ObjectError::Corruption(
                "root prepared capacity rebalance request checksum mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.root_state_fingerprint() {
            return Err(ObjectError::PreconditionFailed(
                "root prepared capacity rebalance state changed before apply".to_string(),
            ));
        }
        let planned = self.plan_capacity_rebalance(prepared.max_objects, prepared.min_free_bytes);
        if planned != prepared.report {
            return Err(ObjectError::PreconditionFailed(
                "root prepared capacity rebalance plan changed before apply".to_string(),
            ));
        }
        let mut report = prepared.report;
        let mut updated_placements = Vec::new();
        for item in &report.items {
            let plan = &item.plan;
            if plan.plan_checksum != placement_migration_plan_checksum(plan) {
                return Err(ObjectError::Corruption(
                    "root prepared capacity rebalance plan checksum mismatch".to_string(),
                ));
            }
            let current = self
                .placements
                .get(&plan.object_id)
                .cloned()
                .ok_or_else(|| ObjectError::NotFound("root placement missing".to_string()))?;
            if current.version != plan.version
                || current.length != plan.length
                || current.node_ids != plan.before_node_ids
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared capacity rebalance placement fence mismatch".to_string(),
                ));
            }
            if plan.after_node_ids.iter().collect::<BTreeSet<_>>().len()
                != plan.after_node_ids.len()
            {
                return Err(ObjectError::PreconditionFailed(
                    "root prepared capacity rebalance target duplicates placement".to_string(),
                ));
            }
            for node_id in &plan.after_node_ids {
                let node = self.nodes.get(node_id).ok_or_else(|| {
                    ObjectError::NotFound("root placement node missing".to_string())
                })?;
                if !Self::node_accepts_new_placement(node) {
                    return Err(ObjectError::PreconditionFailed(
                        "root prepared capacity rebalance node is not writable".to_string(),
                    ));
                }
                let previous_bytes = if current.node_ids.iter().any(|old| old == node_id) {
                    current.length
                } else {
                    0
                };
                if node
                    .committed_bytes
                    .saturating_sub(previous_bytes)
                    .saturating_add(plan.length)
                    > Self::node_effective_capacity_bytes(node)
                {
                    return Err(ObjectError::PreconditionFailed(
                        "root prepared capacity rebalance node lacks capacity".to_string(),
                    ));
                }
                if prepared.min_free_bytes > 0
                    && Self::node_effective_free_bytes(node).saturating_add(previous_bytes)
                        < plan.length.saturating_add(prepared.min_free_bytes)
                {
                    return Err(ObjectError::PreconditionFailed(
                        "root prepared capacity rebalance node lacks capacity floor".to_string(),
                    ));
                }
            }
            updated_placements.push(ObjectPlacement {
                object_id: plan.object_id.clone(),
                version: plan.version,
                length: plan.length,
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
            report.rebalanced_objects = report.rebalanced_objects.saturating_add(1);
            report.bytes_rebalanced = report.bytes_rebalanced.saturating_add(placement.length);
            self.placements
                .insert(placement.object_id.clone(), placement);
        }
        self.advance_commit_index();
        Ok(report)
    }
}
