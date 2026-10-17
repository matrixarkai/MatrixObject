impl MatrixPageService {
    pub fn partial_reclaim_chunk_replicas(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
    ) -> ChunkPartialReclaimReport {
        let mut aggregate = ChunkPartialReclaimReport::default();
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) || replica.is_frozen() {
                continue;
            }
            let report =
                replica.partial_reclaim_chunks(max_chunks, max_rewrite_bytes, min_discarded_bytes);
            merge_chunk_partial_reclaim_report(&mut aggregate, report);
        }
        aggregate
    }

    pub fn plan_chunk_replica_reclaim(
        &self,
        marker: Option<&ChunkId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> Result<PageReplicaReclaimPlan, ObjectError> {
        let mut aggregate = PageReplicaReclaimPlan::default();
        aggregate.global_reclaim_budget_enforced = max_chunks > 0 || max_rewrite_bytes > 0;
        aggregate.global_reclaim_budget_basis = if aggregate.global_reclaim_budget_enforced {
            "scheduled".to_string()
        } else {
            "unlimited".to_string()
        };
        let mut remaining_chunks = max_chunks;
        let mut remaining_rewrite_bytes = max_rewrite_bytes;
        for (index, replica) in self.replicas.iter().enumerate() {
            aggregate.scanned_replicas = aggregate.scanned_replicas.saturating_add(1);
            if !self.replica_active(index) {
                aggregate.skipped_replicas = aggregate.skipped_replicas.saturating_add(1);
                aggregate.quarantined_replicas = aggregate.quarantined_replicas.saturating_add(1);
                continue;
            }
            if replica.is_frozen() {
                aggregate.skipped_replicas = aggregate.skipped_replicas.saturating_add(1);
                aggregate.frozen_replicas = aggregate.frozen_replicas.saturating_add(1);
                continue;
            }
            let plan = replica.plan_reclaim_chunks(
                marker,
                0,
                0,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
            )?;
            aggregate.scanned_chunks = aggregate.scanned_chunks.saturating_add(plan.scanned_chunks);
            aggregate.candidate_chunks = aggregate
                .candidate_chunks
                .saturating_add(plan.candidate_chunks);
            aggregate.skipped_chunks = aggregate.skipped_chunks.saturating_add(plan.skipped_chunks);
            aggregate.skipped_by_trim_budget_chunks = aggregate
                .skipped_by_trim_budget_chunks
                .saturating_add(plan.skipped_by_trim_budget_chunks);
            aggregate.skipped_by_trim_budget_bytes = aggregate
                .skipped_by_trim_budget_bytes
                .saturating_add(plan.skipped_by_trim_budget_bytes);
            aggregate.skipped_by_fragmentation_chunks = aggregate
                .skipped_by_fragmentation_chunks
                .saturating_add(plan.skipped_by_fragmentation_chunks);
            aggregate.skipped_by_fragmentation_discarded_bytes = aggregate
                .skipped_by_fragmentation_discarded_bytes
                .saturating_add(plan.skipped_by_fragmentation_discarded_bytes);
            aggregate.skipped_by_fragmentation_ranges = aggregate
                .skipped_by_fragmentation_ranges
                .saturating_add(plan.skipped_by_fragmentation_ranges);
            aggregate.candidate_discarded_bytes = aggregate
                .candidate_discarded_bytes
                .saturating_add(plan.candidate_discarded_bytes);
            aggregate.remaining_discard_bytes = aggregate
                .remaining_discard_bytes
                .saturating_add(plan.remaining_discard_bytes);
            aggregate.budget_limited |= plan.budget_limited;
            aggregate.truncated |= plan.truncated;
            if aggregate.first_budget_skipped_candidate.is_none() {
                if let Some(candidate) = plan.first_budget_skipped_candidate.clone() {
                    aggregate.first_budget_skipped_candidate = Some(PageReplicaReclaimCandidate {
                        replica_index: index,
                        candidate,
                    });
                }
            }
            if aggregate.first_trim_budget_skipped_candidate.is_none() {
                if let Some(candidate) = plan.first_trim_budget_skipped_candidate.clone() {
                    aggregate.first_trim_budget_skipped_candidate =
                        Some(PageReplicaReclaimCandidate {
                            replica_index: index,
                            candidate,
                        });
                }
            }
            for candidate in plan.candidates {
                let chunk_budget_exhausted = max_chunks > 0 && remaining_chunks == 0;
                let byte_budget_exhausted =
                    max_rewrite_bytes > 0 && candidate.rewrite_bytes > remaining_rewrite_bytes;
                let replica_candidate = PageReplicaReclaimCandidate {
                    replica_index: index,
                    candidate,
                };
                if chunk_budget_exhausted || byte_budget_exhausted {
                    aggregate.skipped_chunks = aggregate.skipped_chunks.saturating_add(1);
                    aggregate.skipped_by_budget_chunks =
                        aggregate.skipped_by_budget_chunks.saturating_add(1);
                    aggregate.skipped_by_budget_bytes = aggregate
                        .skipped_by_budget_bytes
                        .saturating_add(replica_candidate.candidate.rewrite_bytes);
                    aggregate.remaining_discard_bytes = aggregate
                        .remaining_discard_bytes
                        .saturating_add(replica_candidate.candidate.discarded_bytes);
                    aggregate.budget_limited = true;
                    aggregate.truncated |= chunk_budget_exhausted;
                    if aggregate.first_budget_skipped_candidate.is_none() {
                        aggregate.first_budget_skipped_candidate = Some(replica_candidate);
                    }
                    continue;
                }
                if max_chunks > 0 {
                    remaining_chunks = remaining_chunks.saturating_sub(1);
                }
                if max_rewrite_bytes > 0 {
                    remaining_rewrite_bytes = remaining_rewrite_bytes
                        .saturating_sub(replica_candidate.candidate.rewrite_bytes);
                }
                aggregate.scheduled_chunks = aggregate.scheduled_chunks.saturating_add(1);
                aggregate.scheduled_rewrite_bytes = aggregate
                    .scheduled_rewrite_bytes
                    .saturating_add(replica_candidate.candidate.rewrite_bytes);
                if replica_candidate.candidate.trim_bytes > 0 {
                    aggregate.scheduled_trim_ops = aggregate.scheduled_trim_ops.saturating_add(1);
                }
                aggregate.scheduled_trim_bytes = aggregate
                    .scheduled_trim_bytes
                    .saturating_add(replica_candidate.candidate.trim_bytes);
                aggregate.candidates.push(replica_candidate);
            }
        }
        finalize_page_replica_reclaim_plan(&mut aggregate);
        Ok(aggregate)
    }

    pub fn reclaim_chunk_replicas_with_limits(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> ChunkPartialReclaimReport {
        self.reclaim_chunk_replicas_with_limits_at(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_millis(),
        )
    }

    pub fn reclaim_chunk_replicas_with_limits_at(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> ChunkPartialReclaimReport {
        let Ok(plan) = self.plan_chunk_replica_reclaim(
            None,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        ) else {
            return ChunkPartialReclaimReport::default();
        };
        let mut scheduled_by_replica: BTreeMap<usize, (usize, u64)> = BTreeMap::new();
        for candidate in &plan.candidates {
            let entry = scheduled_by_replica
                .entry(candidate.replica_index)
                .or_insert((0, 0));
            entry.0 = entry.0.saturating_add(1);
            entry.1 = entry.1.saturating_add(candidate.candidate.rewrite_bytes);
        }
        let mut aggregate = ChunkPartialReclaimReport::default();
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) || replica.is_frozen() {
                continue;
            }
            let Some((replica_max_chunks, replica_max_rewrite_bytes)) =
                scheduled_by_replica.get(&index).copied()
            else {
                continue;
            };
            let report = replica.reclaim_chunks_with_limits_at(
                replica_max_chunks,
                replica_max_rewrite_bytes,
                min_discarded_bytes,
                min_discard_ratio_ppm,
                max_discard_ranges,
                max_trim_bytes,
                now_ms,
            );
            merge_chunk_partial_reclaim_report(&mut aggregate, report);
        }
        aggregate.scanned_chunks = plan.scanned_chunks;
        aggregate.candidate_chunks = plan.candidate_chunks;
        aggregate.skipped_chunks = plan.skipped_chunks;
        aggregate.skipped_rewrite_bytes = plan.skipped_by_budget_bytes;
        aggregate.remaining_discard_bytes = plan.remaining_discard_bytes;
        aggregate.budget_limited |= plan.budget_limited;
        aggregate
    }
}
