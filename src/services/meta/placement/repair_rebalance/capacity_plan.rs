// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn plan_capacity_rebalance(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PlacementCapacityRebalanceReport {
        let limit = if max_objects == 0 {
            usize::MAX
        } else {
            max_objects
        };
        let pressure_nodes: BTreeSet<String> = self
            .nodes
            .values()
            .filter(|node| {
                Self::is_data_service_node(node)
                    && node.online
                    && Self::node_effective_free_bytes(node) < min_free_bytes
            })
            .map(|node| node.node_id.clone())
            .collect();
        let mut report = PlacementCapacityRebalanceReport {
            pressure_node_count: pressure_nodes.len() as u64,
            ..PlacementCapacityRebalanceReport::default()
        };
        if pressure_nodes.is_empty() {
            Self::finalize_placement_capacity_rebalance_report(&mut report);
            return report;
        }
        for placement in self.placements.values() {
            report.scanned_objects = report.scanned_objects.saturating_add(1);
            let Some(source_node_id) = placement
                .node_ids
                .iter()
                .find(|node_id| pressure_nodes.contains(*node_id))
                .cloned()
            else {
                continue;
            };
            if report.items.len() >= limit {
                report.skipped_objects = report.skipped_objects.saturating_add(1);
                report.has_more = true;
                continue;
            }
            match self.plan_capacity_rebalance_target(placement, &source_node_id, min_free_bytes) {
                Ok(plan) => {
                    report.rebalance_objects = report.rebalance_objects.saturating_add(1);
                    report.bytes_to_rebalance =
                        report.bytes_to_rebalance.saturating_add(plan.length);
                    report.items.push(PlacementCapacityRebalanceItem {
                        plan,
                        pressure_node_id: source_node_id,
                    });
                }
                Err(err) => {
                    report.failed_rebalances = report.failed_rebalances.saturating_add(1);
                    report.errors.push(format!(
                        "{}/{}: {}",
                        placement.object_id.bucket, placement.object_id.key, err
                    ));
                }
            }
        }
        Self::finalize_placement_capacity_rebalance_report(&mut report);
        report
    }

    pub fn rebalance_capacity(
        &mut self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PlacementCapacityRebalanceReport {
        let mut report = self.plan_capacity_rebalance(max_objects, min_free_bytes);
        let planned = report.items.clone();
        for item in planned {
            match self.commit_placement_migration_with_min_free(
                item.plan.clone(),
                &item.plan.plan_checksum,
                min_free_bytes,
            ) {
                Ok(placement) => {
                    report.rebalanced_objects = report.rebalanced_objects.saturating_add(1);
                    report.bytes_rebalanced =
                        report.bytes_rebalanced.saturating_add(placement.length);
                }
                Err(err) => {
                    report.failed_rebalances = report.failed_rebalances.saturating_add(1);
                    report.errors.push(format!(
                        "{}/{}: {}",
                        item.plan.object_id.bucket, item.plan.object_id.key, err
                    ));
                }
            }
        }
        report
    }

    pub fn prepare_capacity_rebalance_raft_entry(
        &self,
        max_objects: usize,
        min_free_bytes: u64,
    ) -> PreparedPlacementCapacityRebalance {
        let report = self.plan_capacity_rebalance(max_objects, min_free_bytes);
        let request_checksum =
            placement_capacity_rebalance_request_checksum(max_objects, min_free_bytes);
        let mut prepared = PreparedPlacementCapacityRebalance {
            max_objects,
            min_free_bytes,
            report,
            before_state_fingerprint: self.root_state_fingerprint(),
            request_checksum,
            expected_previous_commit_index: self.placement_commit_index.saturating_sub(1),
            prepared_commit_index: self.placement_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_placement_capacity_rebalance_checksum(&prepared);
        prepared
    }

}
