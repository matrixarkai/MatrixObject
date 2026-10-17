impl MatrixChunkService {
    pub fn partial_reclaim_chunks(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
    ) -> ChunkPartialReclaimReport {
        self.reclaim_chunks_with_limits(max_chunks, max_rewrite_bytes, min_discarded_bytes, 0, 0, 0)
    }

    pub fn plan_reclaim_chunks(
        &self,
        marker: Option<&ChunkId>,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> Result<ChunkReclaimPlan, ObjectError> {
        if let Some(marker) = marker {
            validate_slab_id(&marker.slab_id)?;
        }
        let limit = if max_chunks == 0 {
            usize::MAX
        } else {
            max_chunks
        };
        let mut plan = ChunkReclaimPlan {
            marker: marker.cloned(),
            ..ChunkReclaimPlan::default()
        };
        let mut budget_left = max_rewrite_bytes;
        for (chunk_id, state) in &self.chunks {
            if marker.map_or(false, |marker| chunk_id <= marker) {
                continue;
            }
            plan.scanned_chunks = plan.scanned_chunks.saturating_add(1);
            plan.next_marker = Some(chunk_id.clone());
            let discarded_ranges = merge_range_slice(&state.discarded_ranges);
            let discarded = discarded_range_bytes(&discarded_ranges);
            if discarded == 0 {
                continue;
            }
            plan.candidate_discarded_bytes =
                plan.candidate_discarded_bytes.saturating_add(discarded);
            let ratio = discard_ratio_ppm(discarded, state.logical_length);
            if discarded < min_discarded_bytes || ratio < min_discard_ratio_ppm {
                plan.remaining_discard_bytes =
                    plan.remaining_discard_bytes.saturating_add(discarded);
                continue;
            }
            plan.candidate_chunks = plan.candidate_chunks.saturating_add(1);
            if max_discard_ranges > 0 && discarded_ranges.len() > max_discard_ranges {
                plan.skipped_chunks = plan.skipped_chunks.saturating_add(1);
                plan.skipped_by_fragmentation_chunks =
                    plan.skipped_by_fragmentation_chunks.saturating_add(1);
                plan.skipped_by_fragmentation_discarded_bytes = plan
                    .skipped_by_fragmentation_discarded_bytes
                    .saturating_add(discarded);
                plan.skipped_by_fragmentation_ranges = plan
                    .skipped_by_fragmentation_ranges
                    .saturating_add(discarded_ranges.len() as u64);
                plan.remaining_discard_bytes =
                    plan.remaining_discard_bytes.saturating_add(discarded);
                continue;
            }
            let candidate_trim_bytes =
                trailing_discarded_bytes(state.logical_length, &discarded_ranges);
            let trim_bytes = candidate_trim_bytes.min(max_trim_bytes);
            let rewrite_bytes = state.logical_length.saturating_sub(trim_bytes);
            let reason = if trim_bytes > 0 {
                "trim_and_reclaim"
            } else {
                "rewrite_reclaim"
            };
            let candidate = ChunkReclaimCandidate {
                chunk_id: chunk_id.clone(),
                logical_length: state.logical_length,
                discarded_bytes: discarded,
                discarded_range_count: discarded_ranges.len() as u64,
                discard_ratio_ppm: ratio,
                rewrite_bytes,
                trim_bytes,
                reclaim_score: discarded.saturating_mul(1_000_000).saturating_add(ratio),
                reason: reason.to_string(),
            };
            if candidate_trim_bytes > trim_bytes {
                plan.skipped_by_trim_budget_chunks =
                    plan.skipped_by_trim_budget_chunks.saturating_add(1);
                plan.skipped_by_trim_budget_bytes = plan
                    .skipped_by_trim_budget_bytes
                    .saturating_add(candidate_trim_bytes.saturating_sub(trim_bytes));
                if plan.first_trim_budget_skipped_candidate.is_none() {
                    let mut trim_candidate = candidate.clone();
                    trim_candidate.trim_bytes = candidate_trim_bytes;
                    trim_candidate.rewrite_bytes = trim_candidate
                        .logical_length
                        .saturating_sub(candidate_trim_bytes);
                    trim_candidate.reason = "trim_budget_limited".to_string();
                    plan.first_trim_budget_skipped_candidate = Some(trim_candidate);
                }
            }
            if plan.candidates.len() >= limit {
                plan.skipped_chunks = plan.skipped_chunks.saturating_add(1);
                plan.skipped_by_budget_chunks = plan.skipped_by_budget_chunks.saturating_add(1);
                plan.skipped_by_budget_bytes =
                    plan.skipped_by_budget_bytes.saturating_add(rewrite_bytes);
                plan.remaining_discard_bytes =
                    plan.remaining_discard_bytes.saturating_add(discarded);
                plan.budget_limited = true;
                plan.truncated = true;
                if plan.first_budget_skipped_candidate.is_none() {
                    plan.first_budget_skipped_candidate = Some(candidate);
                }
                break;
            }
            if max_rewrite_bytes > 0 && rewrite_bytes > budget_left {
                plan.skipped_chunks = plan.skipped_chunks.saturating_add(1);
                plan.skipped_by_budget_chunks = plan.skipped_by_budget_chunks.saturating_add(1);
                plan.skipped_by_budget_bytes =
                    plan.skipped_by_budget_bytes.saturating_add(rewrite_bytes);
                plan.remaining_discard_bytes =
                    plan.remaining_discard_bytes.saturating_add(discarded);
                plan.budget_limited = true;
                if plan.first_budget_skipped_candidate.is_none() {
                    plan.first_budget_skipped_candidate = Some(candidate);
                }
                continue;
            }
            if max_rewrite_bytes > 0 {
                budget_left = budget_left.saturating_sub(rewrite_bytes);
            }
            plan.scheduled_chunks = plan.scheduled_chunks.saturating_add(1);
            plan.scheduled_rewrite_bytes =
                plan.scheduled_rewrite_bytes.saturating_add(rewrite_bytes);
            if trim_bytes > 0 {
                plan.scheduled_trim_ops = plan.scheduled_trim_ops.saturating_add(1);
            }
            plan.scheduled_trim_bytes = plan.scheduled_trim_bytes.saturating_add(trim_bytes);
            plan.candidates.push(candidate);
        }
        finalize_chunk_reclaim_plan(&mut plan);
        Ok(plan)
    }
}
