// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixReplicaService {
    pub fn reclaim_chunks_with_limits(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
    ) -> ReplicaPartialReclaimReport {
        self.reclaim_chunks_with_limits_at(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_millis(),
        )
    }

    pub fn reclaim_chunks_with_limits_at(
        &mut self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
    ) -> ReplicaPartialReclaimReport {
        if self.ensure_writable().is_err() {
            self.partial_reclaim_failures = self.partial_reclaim_failures.saturating_add(1);
            self.last_partial_reclaim_failure_status = "reclaim_not_writable".to_string();
            return ReplicaPartialReclaimReport {
                partial_reclaim_failures: 1,
                last_partial_reclaim_failure_status: self
                    .last_partial_reclaim_failure_status
                    .clone(),
                ..ReplicaPartialReclaimReport::default()
            };
        }
        let requested_trim_bytes = max_trim_bytes;
        let (effective_trim_bytes, trim_flow_available_bytes, trim_flow_throttled_bytes) =
            self.effective_trim_budget(requested_trim_bytes, now_ms);
        let Ok(mut plan) = self.plan_reclaim_chunks(
            None,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            effective_trim_bytes,
        ) else {
            self.partial_reclaim_failures = self.partial_reclaim_failures.saturating_add(1);
            self.last_partial_reclaim_failure_status = "reclaim_plan_failed".to_string();
            return ReplicaPartialReclaimReport {
                partial_reclaim_failures: 1,
                last_partial_reclaim_failure_status: self
                    .last_partial_reclaim_failure_status
                    .clone(),
                ..ReplicaPartialReclaimReport::default()
            };
        };
        if self.trim_flow_enabled() {
            let mut trim_left = effective_trim_bytes;
            plan.scheduled_trim_bytes = 0;
            plan.scheduled_trim_ops = 0;
            plan.scheduled_rewrite_bytes = 0;
            for candidate in &mut plan.candidates {
                if candidate.trim_bytes > trim_left {
                    candidate.trim_bytes = trim_left;
                    candidate.rewrite_bytes = candidate
                        .logical_length
                        .saturating_sub(candidate.trim_bytes);
                    candidate.reason = if candidate.trim_bytes > 0 {
                        "trim_and_reclaim"
                    } else {
                        "rewrite_reclaim"
                    }
                    .to_string();
                }
                trim_left = trim_left.saturating_sub(candidate.trim_bytes);
                plan.scheduled_trim_bytes = plan
                    .scheduled_trim_bytes
                    .saturating_add(candidate.trim_bytes);
                plan.scheduled_rewrite_bytes = plan
                    .scheduled_rewrite_bytes
                    .saturating_add(candidate.rewrite_bytes);
                if candidate.trim_bytes > 0 {
                    plan.scheduled_trim_ops = plan.scheduled_trim_ops.saturating_add(1);
                }
            }
        }
        let trim_ops_before_op_budget = plan.scheduled_trim_ops;
        if self.trim_flow_ops_enabled() {
            self.refill_trim_flow(now_ms);
        }
        let trim_op_available = self.estimated_trim_flow_available_ops(now_ms);
        if self.trim_flow_ops_enabled() && plan.scheduled_trim_ops > trim_op_available {
            let mut trim_ops_left = trim_op_available;
            plan.scheduled_trim_bytes = 0;
            plan.scheduled_trim_ops = 0;
            plan.scheduled_rewrite_bytes = 0;
            for candidate in &mut plan.candidates {
                if candidate.trim_bytes > 0 {
                    if trim_ops_left == 0 {
                        candidate.rewrite_bytes =
                            candidate.rewrite_bytes.saturating_add(candidate.trim_bytes);
                        candidate.trim_bytes = 0;
                        candidate.reason = "rewrite_reclaim".to_string();
                    } else {
                        trim_ops_left = trim_ops_left.saturating_sub(1);
                    }
                }
                plan.scheduled_trim_bytes = plan
                    .scheduled_trim_bytes
                    .saturating_add(candidate.trim_bytes);
                plan.scheduled_rewrite_bytes = plan
                    .scheduled_rewrite_bytes
                    .saturating_add(candidate.rewrite_bytes);
                if candidate.trim_bytes > 0 {
                    plan.scheduled_trim_ops = plan.scheduled_trim_ops.saturating_add(1);
                }
            }
        }
        plan.trim_flow_throttled_ops =
            trim_ops_before_op_budget.saturating_sub(plan.scheduled_trim_ops);
        if plan.trim_flow_throttled_ops > 0 {
            plan.trim_flow_limited = true;
            plan.budget_limited = true;
        }
        let mut report = ReplicaPartialReclaimReport::default();
        report.scanned_chunks = plan.scanned_chunks;
        report.candidate_chunks = plan.candidate_chunks;
        report.skipped_chunks = plan.skipped_chunks;
        report.skipped_rewrite_bytes = plan.skipped_by_budget_bytes;
        report.remaining_discard_bytes = plan.remaining_discard_bytes;
        report.budget_limited = plan.budget_limited;
        report.requested_trim_bytes = requested_trim_bytes;
        report.effective_trim_bytes = effective_trim_bytes;
        report.trim_flow_available_bytes = trim_flow_available_bytes;
        report.trim_flow_throttled_ops = plan.trim_flow_throttled_ops;
        report.trim_flow_throttled_bytes = trim_flow_throttled_bytes.saturating_add(
            report
                .trim_flow_throttled_ops
                .saturating_mul(min_discarded_bytes.max(1)),
        );
        report.trim_flow_limited = report.trim_flow_throttled_bytes > 0;
        if report.trim_flow_limited {
            self.trim_flow_throttled_reclaims = self.trim_flow_throttled_reclaims.saturating_add(1);
            self.trim_flow_throttled_bytes = self
                .trim_flow_throttled_bytes
                .saturating_add(report.trim_flow_throttled_bytes);
            self.trim_flow_throttled_ops = self
                .trim_flow_throttled_ops
                .saturating_add(report.trim_flow_throttled_ops);
            report.budget_limited = true;
        }
        let consumed_trim_ops = plan.scheduled_trim_ops;
        for candidate in plan.candidates {
            // NOT AN APPEND, so the batch undo cannot be a length here.
            self.materialise_page_undo(&candidate.page_id);
            let Some(state) = self.pages.get_mut(&candidate.page_id) else {
                continue;
            };
            let state = std::sync::Arc::make_mut(state);
            let ranges = merge_ranges(std::mem::take(&mut state.discarded_ranges));
            for range in ranges {
                let Ok(start) = usize::try_from(range.offset) else {
                    continue;
                };
                if start >= state.data.len() {
                    continue;
                }
                let length = usize::try_from(range.length).unwrap_or(usize::MAX);
                let end = start.saturating_add(length).min(state.data.len());
                for byte in &mut std::sync::Arc::make_mut(&mut state.data)[start..end] {
                    *byte = 0;
                }
            }
            if candidate.trim_bytes > 0 {
                state.logical_length = state.logical_length.saturating_sub(candidate.trim_bytes);
                if let Ok(logical_length) = usize::try_from(state.logical_length) {
                    let payload = std::sync::Arc::make_mut(&mut state.data);
                    payload.truncate(logical_length);
                    // `truncate` lowers the length and keeps the allocation, so a
                    // trimmed chunk would go on holding the bytes it just gave up
                    // and the reclaim would free nothing resident. The growth path
                    // uses `reserve_exact` for the same reason: a chunk should not
                    // hold more than it stores.
                    payload.shrink_to_fit();
                }
            }
            state.write_sequence = state.write_sequence.saturating_add(1);
            state.checksum_fnv1a64 = fnv1a64(&state.data);
            if self.keep_ec_sidecar {
                refresh_chunk_ec_sidecars(state);
            } else {
                clear_chunk_ec_sidecars(state);
            }
            report.reclaimed_chunks = report.reclaimed_chunks.saturating_add(1);
            report.reclaimed_discard_bytes = report
                .reclaimed_discard_bytes
                .saturating_add(candidate.discarded_bytes);
            report.rewritten_bytes = report
                .rewritten_bytes
                .saturating_add(candidate.rewrite_bytes);
            report.trimmed_bytes = report.trimmed_bytes.saturating_add(candidate.trim_bytes);
            self.invalidate_chunk_cache(&candidate.page_id, 0, candidate.logical_length);
        }
        if self.trim_flow_enabled() {
            self.trim_flow_available_bytes = self
                .trim_flow_available_bytes
                .saturating_sub(report.trimmed_bytes);
            self.trim_flow_available_ops = self
                .trim_flow_available_ops
                .saturating_sub(consumed_trim_ops);
        }
        report
    }
}
