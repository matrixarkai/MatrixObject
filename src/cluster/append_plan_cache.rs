// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::PlacementAssignment;
use crate::cluster::ChunkGroupRoute;
use prost::Message;
use std::time::{Duration, Instant};

const MAX_CACHED_PLAN_BYTES: usize = 64 * 1024;
const MAX_CACHED_GROUPS: usize = 32;

#[derive(Debug)]
struct CachedAppendPlan {
    group_count: u64,
    group_bytes: u64,
    planned_at: Instant,
    plan: PlacementAssignment,
    routes: Vec<ChunkGroupRoute>,
}

/// Two entries serve alternating log and index objects without retaining a catalog.
/// The byte limit counts names and encoded routing data, rather than process RSS.
#[derive(Debug, Default)]
pub(super) struct AppendPlanCache {
    entries: [Option<CachedAppendPlan>; 2],
}

impl AppendPlanCache {
    pub(super) fn clear(&mut self) {
        self.entries = [None, None];
    }

    pub(super) fn lookup(
        &mut self,
        bucket: &str,
        key: &str,
        group_bytes: u64,
        group_count: u64,
        ttl: Duration,
        now: Instant,
    ) -> Option<(PlacementAssignment, Vec<ChunkGroupRoute>)> {
        for entry in &mut self.entries {
            if entry
                .as_ref()
                .is_some_and(|cached| now.saturating_duration_since(cached.planned_at) >= ttl)
            {
                *entry = None;
            }
        }
        let index = self.entries.iter().position(|entry| {
            entry.as_ref().is_some_and(|cached| {
                cached.plan.bucket == bucket
                    && cached.plan.key == key
                    && cached.group_count == group_count
                    && group_bytes <= cached.group_bytes
            })
        })?;
        self.entries.swap(0, index);
        let cached = self.entries[0].as_ref().unwrap();
        Some((cached.plan.clone(), cached.routes.clone()))
    }

    pub(super) fn remember(
        &mut self,
        plan: &PlacementAssignment,
        routes: &[ChunkGroupRoute],
        group_bytes: u64,
        group_count: u64,
        now: Instant,
    ) {
        for entry in &mut self.entries {
            if entry.as_ref().is_some_and(|cached| {
                cached.plan.bucket == plan.bucket && cached.plan.key == plan.key
            }) {
                *entry = None;
            }
        }
        if plan.node_ids.len() > MAX_CACHED_GROUPS || routes.len() > MAX_CACHED_GROUPS {
            return;
        }
        let bytes = plan
            .bucket
            .len()
            .saturating_add(plan.key.len())
            .saturating_add(plan.plan_checksum.len());
        let bytes = plan
            .node_ids
            .iter()
            .fold(bytes, |sum, id| sum.saturating_add(id.len()));
        let bytes = routes
            .iter()
            .fold(bytes, |sum, route| sum.saturating_add(route.encoded_len()));
        if bytes > MAX_CACHED_PLAN_BYTES {
            return;
        }
        // Close a hole before moving the most recently used entry to the second slot.
        if self.entries[0].is_none() {
            self.entries.swap(0, 1);
        }
        self.entries[1] = self.entries[0].take();
        self.entries[0] = Some(CachedAppendPlan {
            group_count,
            group_bytes,
            planned_at: now,
            plan: plan.clone(),
            routes: routes.to_vec(),
        });
    }
}

#[cfg(test)]
#[path = "append_plan_cache_tests.rs"]
mod tests;
