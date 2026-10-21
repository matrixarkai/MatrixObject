// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn list_node_usage(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        draining_filter: Option<bool>,
        online_filter: Option<bool>,
        capacity_pressure_filter: Option<bool>,
        reclaim_pressure_filter: Option<bool>,
        placement_eligible_filter: Option<bool>,
        min_free_bytes: u64,
        min_reclaimable_bytes: u64,
    ) -> Result<RootNodeUsagePage, ObjectError> {
        if let Some(marker) = marker {
            validate_node_id(marker)?;
        }
        if let Some(service_kind) = service_kind_filter {
            validate_service_kind(service_kind)?;
        }
        let take = if limit == 0 { usize::MAX } else { limit };
        let mut page = RootNodeUsagePage {
            marker: marker.map(str::to_string),
            service_kind_filter: service_kind_filter.map(str::to_string),
            draining_filter,
            online_filter,
            capacity_pressure_filter,
            reclaim_pressure_filter,
            placement_eligible_filter,
            min_free_bytes,
            min_reclaimable_bytes,
            ..RootNodeUsagePage::default()
        };
        let mut referenced_by_node = BTreeMap::<String, (u64, u64)>::new();
        for placement in self.placements.values() {
            page.total_referenced_object_bytes = page
                .total_referenced_object_bytes
                .saturating_add(placement.length);
            for node_id in &placement.node_ids {
                let entry = referenced_by_node.entry(node_id.clone()).or_default();
                entry.0 = entry.0.saturating_add(1);
                entry.1 = entry.1.saturating_add(placement.length);
                page.total_replicated_object_bytes = page
                    .total_replicated_object_bytes
                    .saturating_add(placement.length);
            }
        }
        let mut usage_by_node = BTreeMap::new();
        for (node_id, node) in &self.nodes {
            let (object_count, referenced_bytes) =
                referenced_by_node.get(node_id).copied().unwrap_or_default();
            usage_by_node.insert(
                node_id.clone(),
                root_node_usage(
                    node,
                    min_free_bytes,
                    min_reclaimable_bytes,
                    true,
                    object_count,
                    referenced_bytes,
                ),
            );
        }
        for (node_id, (object_count, referenced_bytes)) in &referenced_by_node {
            if self.nodes.contains_key(node_id) {
                continue;
            }
            page.unregistered_referenced_nodes =
                page.unregistered_referenced_nodes.saturating_add(1);
            usage_by_node.insert(
                node_id.clone(),
                unregistered_root_node_usage(node_id, *object_count, *referenced_bytes),
            );
        }
        page.registered_node_count = self.nodes.len() as u64;
        page.referenced_node_count = referenced_by_node.len() as u64;
        page.total_node_count = usage_by_node.len() as u64;
        for (node_id, usage) in usage_by_node {
            if marker.map_or(false, |marker| node_id.as_str() <= marker) {
                continue;
            }
            page.scanned_nodes = page.scanned_nodes.saturating_add(1);
            let filtered_out = service_kind_filter.map_or(false, |service_kind| {
                usage.node.service_kind != service_kind
            }) || draining_filter
                .map_or(false, |draining| usage.node.draining != draining)
                || online_filter.map_or(false, |online| usage.node.online != online)
                || capacity_pressure_filter
                    .map_or(false, |pressure| usage.capacity_pressure != pressure)
                || reclaim_pressure_filter
                    .map_or(false, |pressure| usage.reclaim_pressure != pressure)
                || placement_eligible_filter
                    .map_or(false, |eligible| usage.placement_eligible != eligible);
            if filtered_out {
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            if page.nodes.len() >= take {
                page.has_more = true;
                page.next_marker = page.nodes.last().map(|last| last.node.node_id.clone());
                break;
            }
            page.total_capacity_bytes = page
                .total_capacity_bytes
                .saturating_add(usage.node.capacity_bytes);
            page.total_used_bytes = page.total_used_bytes.saturating_add(usage.node.used_bytes);
            page.total_committed_bytes = page
                .total_committed_bytes
                .saturating_add(usage.node.committed_bytes);
            page.total_free_bytes = page.total_free_bytes.saturating_add(usage.free_bytes);
            page.total_reported_free_bytes = page
                .total_reported_free_bytes
                .saturating_add(usage.reported_free_bytes);
            page.total_reclaimable_bytes = page
                .total_reclaimable_bytes
                .saturating_add(usage.reclaimable_bytes);
            page.total_overcommitted_bytes = page
                .total_overcommitted_bytes
                .saturating_add(usage.overcommitted_bytes);
            if usage.reclaim_pressure {
                page.reclaim_pressure_nodes = page.reclaim_pressure_nodes.saturating_add(1);
            }
            page.nodes.push(usage);
        }
        if !page.has_more {
            page.next_marker = None;
        }
        Ok(page)
    }

    pub fn plan_reclaim_nodes(
        &self,
        marker: Option<&str>,
        limit: usize,
        service_kind_filter: Option<&str>,
        only_online: bool,
        include_draining: bool,
        min_reclaimable_bytes: u64,
        min_overcommitted_bytes: u64,
        min_free_bytes: u64,
        min_page_physical_discard_bytes: u64,
        min_page_physical_discard_ranges: u64,
    ) -> Result<RootReclaimNodePlanPage, ObjectError> {
        if let Some(marker) = marker {
            validate_node_id(marker)?;
        }
        if let Some(service_kind) = service_kind_filter {
            validate_service_kind(service_kind)?;
        }
        let take = if limit == 0 { usize::MAX } else { limit };
        let mut candidates = Vec::new();
        let mut page = RootReclaimNodePlanPage {
            marker: marker.map(str::to_string),
            service_kind_filter: service_kind_filter.map(str::to_string),
            only_online,
            include_draining,
            min_reclaimable_bytes,
            min_overcommitted_bytes,
            min_free_bytes,
            min_page_physical_discard_bytes,
            min_page_physical_discard_ranges,
            ..RootReclaimNodePlanPage::default()
        };
        for (node_id, node) in &self.nodes {
            if marker.map_or(false, |marker| node_id.as_str() <= marker) {
                continue;
            }
            page.scanned_nodes = page.scanned_nodes.saturating_add(1);
            if service_kind_filter.map_or(false, |service_kind| node.service_kind != service_kind)
                || (only_online && !node.online)
                || (!include_draining && node.draining)
            {
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            let usage = root_node_usage(node, min_free_bytes, min_reclaimable_bytes, true, 0, 0);
            let capacity_pressure_bytes = min_free_bytes.saturating_sub(usage.free_bytes);
            let has_reclaim_signal = min_reclaimable_bytes > 0
                && usage.reclaimable_bytes >= min_reclaimable_bytes
                && usage.reclaimable_bytes > 0;
            let has_overcommit_signal =
                min_overcommitted_bytes > 0 && usage.overcommitted_bytes >= min_overcommitted_bytes;
            let has_capacity_signal = min_free_bytes > 0 && usage.capacity_pressure;
            let has_page_discard_signal = (min_page_physical_discard_bytes > 0
                && node.page_health.chunk_physical_discard_bytes
                    >= min_page_physical_discard_bytes)
                || (min_page_physical_discard_ranges > 0
                    && node.page_health.chunk_physical_discard_range_count
                        >= min_page_physical_discard_ranges);
            let has_explicit_pressure_filter = min_reclaimable_bytes > 0
                || min_overcommitted_bytes > 0
                || min_free_bytes > 0
                || min_page_physical_discard_bytes > 0
                || min_page_physical_discard_ranges > 0;
            let has_trim_signal = !has_explicit_pressure_filter
                && (node.chunk_health.trailing_trim_bytes > 0
                    || node.page_health.total_trailing_trim_bytes > 0);
            let has_fragmentation_signal = !has_explicit_pressure_filter
                && (node.chunk_health.fragmented_chunk_count > 0
                    || node.chunk_health.fragmented_discarded_bytes > 0
                    || node.page_health.total_fragmented_chunk_count > 0
                    || node.page_health.total_fragmented_discarded_bytes > 0);
            if !has_reclaim_signal
                && !has_overcommit_signal
                && !has_capacity_signal
                && !has_trim_signal
                && !has_fragmentation_signal
                && !has_page_discard_signal
            {
                page.skipped_nodes = page.skipped_nodes.saturating_add(1);
                continue;
            }
            let mut score = usage
                .reclaimable_bytes
                .saturating_add(usage.overcommitted_bytes)
                .saturating_add(capacity_pressure_bytes)
                .saturating_add(node.chunk_health.trailing_trim_bytes)
                .saturating_add(node.chunk_health.fragmented_discarded_bytes)
                .saturating_add(node.page_health.total_reclaimable_bytes)
                .saturating_add(node.page_health.total_trailing_trim_bytes)
                .saturating_add(node.page_health.total_fragmented_discarded_bytes)
                .saturating_add(node.page_health.chunk_physical_discard_bytes)
                .saturating_add(
                    node.chunk_health
                        .fragmented_chunk_count
                        .saturating_mul(4096),
                )
                .saturating_add(
                    node.page_health
                        .total_fragmented_chunk_count
                        .saturating_mul(4096),
                )
                .saturating_add(node.chunk_health.discarded_range_count.saturating_mul(1024))
                .saturating_add(
                    node.page_health
                        .chunk_physical_discard_range_count
                        .saturating_mul(1024),
                );
            if node.draining {
                score = score.saturating_add(1024 * 1024);
            }
            if !node.online {
                score = score.saturating_add(512 * 1024);
            }
            let reason = if node.draining {
                "draining_reclaim".to_string()
            } else if !node.online {
                "offline_reclaim".to_string()
            } else if node.chunk_health.trailing_trim_bytes > 0 {
                "trim_pressure".to_string()
            } else if node.page_health.total_trailing_trim_bytes > 0 {
                "block_trim_pressure".to_string()
            } else if node.chunk_health.fragmented_chunk_count > 0 {
                "compaction_pressure".to_string()
            } else if node.page_health.total_fragmented_chunk_count > 0 {
                "block_compaction_pressure".to_string()
            } else if node.page_health.chunk_physical_discard_bytes > 0
                || node.page_health.chunk_physical_discard_range_count > 0
            {
                "block_discard_pressure".to_string()
            } else if has_capacity_signal {
                "capacity_pressure".to_string()
            } else if has_overcommit_signal {
                "overcommitted_usage".to_string()
            } else {
                "reclaimable_usage".to_string()
            };
            page.candidate_nodes = page.candidate_nodes.saturating_add(1);
            page.total_reclaim_score = page.total_reclaim_score.saturating_add(score);
            page.total_reclaimable_bytes = page
                .total_reclaimable_bytes
                .saturating_add(usage.reclaimable_bytes);
            page.total_overcommitted_bytes = page
                .total_overcommitted_bytes
                .saturating_add(usage.overcommitted_bytes);
            page.total_capacity_pressure_bytes = page
                .total_capacity_pressure_bytes
                .saturating_add(capacity_pressure_bytes);
            candidates.push(RootReclaimNodePlan {
                usage,
                reclaim_score: score,
                reclaim_reason: reason,
            });
        }
        candidates.sort_by(|left, right| {
            right
                .reclaim_score
                .cmp(&left.reclaim_score)
                .then_with(|| left.usage.node.node_id.cmp(&right.usage.node.node_id))
        });
        if candidates.len() > take {
            page.has_more = true;
            candidates.truncate(take);
        }
        if page.has_more {
            page.next_marker = candidates
                .last()
                .map(|plan| plan.usage.node.node_id.clone());
        }
        page.plans = candidates;
        page.reclaim_needed = !page.plans.is_empty();
        page.reclaim_can_apply = page.reclaim_needed && !page.has_more;
        page.reclaim_reason = if !page.reclaim_needed {
            "no_root_reclaim_nodes_needed".to_string()
        } else if page.has_more {
            "root_reclaim_nodes_truncated".to_string()
        } else {
            "root_reclaim_nodes_ready".to_string()
        };
        Ok(page)
    }
}
