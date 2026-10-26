// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn list_object_placements(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        node_id_filter: Option<&str>,
    ) -> Result<PlacementPage, ObjectError> {
        if let Some(marker) = marker {
            validate_bucket(&marker.bucket)?;
            validate_key(&marker.key)?;
        }
        if let Some(node_id) = node_id_filter {
            validate_node_id(node_id)?;
        }
        let take = if limit == 0 { usize::MAX } else { limit };
        let mut page = PlacementPage {
            marker: marker.cloned(),
            node_id_filter: node_id_filter.map(str::to_string),
            ..PlacementPage::default()
        };
        for (object_id, placement) in &self.placements {
            if marker.map_or(false, |marker| object_id <= marker) {
                continue;
            }
            page.scanned_objects = page.scanned_objects.saturating_add(1);
            if node_id_filter.map_or(false, |node_id| {
                !placement
                    .node_ids
                    .iter()
                    .any(|placement_node| placement_node == node_id)
            }) {
                page.skipped_objects = page.skipped_objects.saturating_add(1);
                continue;
            }
            if page.placements.len() >= take {
                page.has_more = true;
                page.next_marker = page.placements.last().map(|last| last.object_id.clone());
                break;
            }
            page.payload_bytes = page
                .payload_bytes
                .saturating_add(placement.length)
                .saturating_add(placement.node_ids.len() as u64);
            page.placements.push(placement.clone());
        }
        if !page.has_more {
            page.next_marker = None;
        }
        Ok(page)
    }

    pub fn audit_placement_health(
        &self,
        marker: Option<&ObjectId>,
        limit: usize,
        expected_replica_count: usize,
        min_free_bytes: u64,
        degraded_only: bool,
        max_payload_bytes: u64,
    ) -> Result<PlacementHealthPage, ObjectError> {
        if let Some(marker) = marker {
            validate_bucket(&marker.bucket)?;
            validate_key(&marker.key)?;
        }
        let take = if limit == 0 { usize::MAX } else { limit };
        let mut page = PlacementHealthPage {
            marker: marker.cloned(),
            expected_replica_count,
            min_free_bytes,
            degraded_only,
            ..PlacementHealthPage::default()
        };
        for (object_id, placement) in &self.placements {
            if marker.map_or(false, |marker| object_id <= marker) {
                continue;
            }
            page.scanned_objects = page.scanned_objects.saturating_add(1);
            let item =
                self.placement_health_item(placement, expected_replica_count, min_free_bytes);
            if degraded_only && item.healthy {
                page.skipped_objects = page.skipped_objects.saturating_add(1);
                continue;
            }
            if page.items.len() >= take {
                page.has_more = true;
                page.next_marker = page
                    .items
                    .last()
                    .map(|last| last.placement.object_id.clone());
                break;
            }
            let item_payload = root_placement_health_item_payload_bytes(&item);
            if max_payload_bytes > 0
                && page.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                page.budget_limited = true;
                page.has_more = true;
                if page.items.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "root placement health item exceeds payload budget".to_string(),
                    ));
                }
                break;
            }
            if item.healthy {
                page.healthy_objects = page.healthy_objects.saturating_add(1);
            } else {
                page.degraded_objects = page.degraded_objects.saturating_add(1);
            }
            if item.under_replicated {
                page.under_replicated_objects = page.under_replicated_objects.saturating_add(1);
            }
            page.missing_node_references = page
                .missing_node_references
                .saturating_add(item.missing_node_ids.len() as u64);
            page.offline_node_references = page
                .offline_node_references
                .saturating_add(item.offline_node_ids.len() as u64);
            page.draining_node_references = page
                .draining_node_references
                .saturating_add(item.draining_node_ids.len() as u64);
            page.capacity_pressure_references = page
                .capacity_pressure_references
                .saturating_add(item.capacity_pressure_node_ids.len() as u64);
            page.payload_bytes = page.payload_bytes.saturating_add(item_payload);
            page.next_marker = Some(object_id.clone());
            page.items.push(item);
        }
        if !page.has_more {
            page.next_marker = None;
        }
        Ok(page)
    }
}
