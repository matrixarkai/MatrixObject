impl MatrixMetaService {
    pub fn plan_change_checkpoint_lag(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_checkpoints: usize,
        target_change_sequence: Option<u64>,
        target_commit_index: Option<u64>,
        min_change_lag: u64,
        min_commit_lag: u64,
        include_not_behind: bool,
        only_compacted: bool,
        max_payload_bytes: u64,
    ) -> Result<MetadataChangeCheckpointLagPlan, ObjectError> {
        validate_meta_key(prefix)?;
        if let Some(marker) = marker {
            validate_meta_key(marker)?;
            if !marker.starts_with(prefix) {
                return Err(ObjectError::InvalidArgument(
                    "metadata change checkpoint lag marker must be within prefix".to_string(),
                ));
            }
        }
        if max_checkpoints == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata change checkpoint lag max_checkpoints must be positive".to_string(),
            ));
        }
        let target_change_sequence =
            target_change_sequence.unwrap_or_else(|| self.next_change_sequence.saturating_sub(1));
        let target_commit_index =
            target_commit_index.unwrap_or_else(|| self.next_commit_index.saturating_sub(1));
        let first_retained_change_sequence = self
            .changes
            .first()
            .map_or(self.next_change_sequence, |change| change.sequence);
        let last_retained_change_sequence = self.changes.last().map_or(0, |change| change.sequence);
        let first_retained_commit_index = self
            .changes
            .first()
            .map_or(target_commit_index, |change| change.commit_index);
        let last_retained_commit_index = self
            .changes
            .last()
            .map_or(target_commit_index, |change| change.commit_index);

        let mut plan = MetadataChangeCheckpointLagPlan {
            items: Vec::new(),
            prefix: prefix.to_string(),
            marker: marker.map(str::to_string),
            next_marker: None,
            scanned_checkpoints: 0,
            matched_checkpoints: 0,
            checkpoint_count: self.change_checkpoints.len() as u64,
            target_change_sequence,
            target_commit_index,
            first_retained_change_sequence,
            last_retained_change_sequence,
            first_retained_commit_index,
            last_retained_commit_index,
            compacted_change_count: self.raft_snapshot_compacted_change_count,
            max_change_lag: 0,
            max_commit_lag: 0,
            retained_window_compacted_count: 0,
            payload_bytes: 0,
            max_payload_bytes,
            budget_limited: false,
            truncated: false,
            lag_plan_needed: false,
            lag_plan_can_apply: false,
            lag_plan_reason: String::new(),
            plan_checksum: String::new(),
            expected_plan_checksum: String::new(),
        };

        for (checkpoint_key, checkpoint) in self.change_checkpoints.range(prefix.to_string()..) {
            if !checkpoint_key.starts_with(prefix) {
                break;
            }
            if marker.map_or(false, |marker| checkpoint_key.as_str() <= marker) {
                continue;
            }
            plan.scanned_checkpoints = plan.scanned_checkpoints.saturating_add(1);
            let change_lag =
                target_change_sequence.saturating_sub(checkpoint.after_change_sequence);
            let commit_lag = target_commit_index.saturating_sub(checkpoint.after_commit_index);
            let behind_target = change_lag > 0 || commit_lag > 0;
            let retained_window_compacted = !self.changes.is_empty()
                && (checkpoint.after_change_sequence.saturating_add(1)
                    < first_retained_change_sequence
                    || checkpoint.after_commit_index.saturating_add(1)
                        < first_retained_commit_index);
            plan.max_change_lag = plan.max_change_lag.max(change_lag);
            plan.max_commit_lag = plan.max_commit_lag.max(commit_lag);
            if change_lag < min_change_lag
                || commit_lag < min_commit_lag
                || (!include_not_behind && !behind_target)
                || (only_compacted && !retained_window_compacted)
            {
                continue;
            }
            plan.matched_checkpoints = plan.matched_checkpoints.saturating_add(1);
            if plan.items.len() >= max_checkpoints {
                plan.truncated = true;
                break;
            }
            let item_payload = metadata_change_checkpoint_lag_payload_bytes(checkpoint);
            if max_payload_bytes > 0
                && !plan.items.is_empty()
                && plan.payload_bytes.saturating_add(item_payload) > max_payload_bytes
            {
                plan.budget_limited = true;
                plan.truncated = true;
                break;
            }
            plan.payload_bytes = plan.payload_bytes.saturating_add(item_payload);
            plan.next_marker = Some(checkpoint_key.clone());
            if retained_window_compacted {
                plan.retained_window_compacted_count =
                    plan.retained_window_compacted_count.saturating_add(1);
            }
            plan.items.push(MetadataChangeCheckpointLagItem {
                checkpoint: checkpoint.clone(),
                change_lag,
                commit_lag,
                behind_target,
                retained_window_compacted,
                first_retained_change_sequence,
                first_retained_commit_index,
                target_change_sequence,
                target_commit_index,
            });
        }
        if !plan.truncated {
            plan.next_marker = None;
        }
        plan.lag_plan_needed = !plan.items.is_empty();
        plan.lag_plan_can_apply =
            plan.lag_plan_needed && !plan.truncated && plan.retained_window_compacted_count == 0;
        plan.lag_plan_reason = if !plan.lag_plan_needed {
            "no_metadata_change_checkpoint_lag".to_string()
        } else if plan.truncated {
            "metadata_change_checkpoint_lag_truncated".to_string()
        } else if plan.retained_window_compacted_count > 0 {
            "metadata_change_checkpoint_retained_window_compacted".to_string()
        } else {
            "metadata_change_checkpoint_lag_ready".to_string()
        };
        plan.plan_checksum = metadata_change_checkpoint_lag_plan_checksum(&plan);
        Ok(plan)
    }
}
