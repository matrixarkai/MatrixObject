// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn object_placement(&self, object_id: &ObjectId) -> Result<ObjectPlacement, ObjectError> {
        self.placements
            .get(object_id)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("root placement missing".to_string()))
    }

    fn placement_health_item(
        &self,
        placement: &ObjectPlacement,
        expected_replica_count: usize,
        min_free_bytes: u64,
    ) -> RootPlacementHealthItem {
        let mut missing_node_ids = Vec::new();
        let mut offline_node_ids = Vec::new();
        let mut draining_node_ids = Vec::new();
        let mut capacity_pressure_node_ids = Vec::new();
        for node_id in &placement.node_ids {
            match self.nodes.get(node_id) {
                Some(node) => {
                    if !node.online {
                        offline_node_ids.push(node_id.clone());
                    }
                    if node.draining || Self::replica_service_pages_new_placement(node) {
                        draining_node_ids.push(node_id.clone());
                    }
                    if Self::node_effective_free_bytes(node) < min_free_bytes {
                        capacity_pressure_node_ids.push(node_id.clone());
                    }
                }
                None => missing_node_ids.push(node_id.clone()),
            }
        }
        let under_replicated =
            expected_replica_count > 0 && placement.node_ids.len() < expected_replica_count;
        let healthy = !under_replicated
            && missing_node_ids.is_empty()
            && offline_node_ids.is_empty()
            && draining_node_ids.is_empty()
            && capacity_pressure_node_ids.is_empty();
        RootPlacementHealthItem {
            placement: placement.clone(),
            missing_node_ids,
            offline_node_ids,
            draining_node_ids,
            capacity_pressure_node_ids,
            under_replicated,
            healthy,
        }
    }
}
