impl MatrixChunkService {
    fn record_repair_stats(&mut self, stats: &RepairStats) {
        self.cumulative_repair_checked_chunks = self
            .cumulative_repair_checked_chunks
            .saturating_add(stats.checked_chunks);
        self.cumulative_repair_degraded_chunks = self
            .cumulative_repair_degraded_chunks
            .saturating_add(stats.degraded_chunks);
        self.cumulative_repaired_replicas = self
            .cumulative_repaired_replicas
            .saturating_add(stats.repaired_replicas);
        self.cumulative_failed_repairs = self
            .cumulative_failed_repairs
            .saturating_add(stats.failed_repairs);
    }

    fn inspect_chunk_repair(&self, page_id: &PageId) -> Result<ReplicaRepairReport, ObjectError> {
        let mut report = ReplicaRepairReport {
            page_id: Some(page_id.clone()),
            checked_chunks: 1,
            route_control_proof: self.route_control_proof(),
            ..ReplicaRepairReport::default()
        };
        let mut states: Vec<(usize, PageState)> = Vec::with_capacity(self.replicas.len());
        let mut saw_chunk = false;
        let mut first_error: Option<String> = None;
        for (index, replica) in self.replicas.iter().enumerate() {
            const CORRUPT: &str = "corrupt";
            const MISSING: &str = "missing";
            const PRESENT: &str = "present";
            let frozen = replica.is_frozen();
            let quarantined =
                self.quarantined_replicas.contains(&index) || self.replica_endpoint_disabled(index);
            if frozen {
                report.frozen_replicas = report.frozen_replicas.saturating_add(1);
            }
            if quarantined {
                report.quarantined_replicas = report.quarantined_replicas.saturating_add(1);
            }
            if self.replica_endpoint_disabled(index) {
                continue;
            }
            let status = match replica.read_chunk_state(page_id) {
                Ok(state) => {
                    saw_chunk = true;
                    match validate_chunk_state(&state) {
                        Ok(()) => {
                            states.push((index, state));
                            PRESENT
                        }
                        Err(err) => {
                            if first_error.is_none() {
                                first_error = Some(err.to_string());
                            }
                            report.corrupt_replicas = report.corrupt_replicas.saturating_add(1);
                            CORRUPT
                        }
                    }
                }
                Err(ObjectError::NotFound(_)) => {
                    report.missing_replicas = report.missing_replicas.saturating_add(1);
                    MISSING
                }
                Err(ObjectError::Corruption(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    report.corrupt_replicas = report.corrupt_replicas.saturating_add(1);
                    CORRUPT
                }
                Err(err) => return Err(err),
            };
            if !quarantined && !frozen && status != PRESENT {
                report.repairable_replicas = report.repairable_replicas.saturating_add(1);
            }
        }
        if !saw_chunk && states.is_empty() {
            return Err(ObjectError::NotFound(
                "chunk repair target is absent from all replicas".to_string(),
            ));
        }
        let mut best_count = 0usize;
        let mut best_state: Option<&PageState> = None;
        for (_, candidate) in &states {
            let count = states
                .iter()
                .filter(|(_, state)| *state == *candidate)
                .count();
            if count > best_count {
                best_count = count;
                best_state = Some(candidate);
            }
        }
        let mut divergent_indices = BTreeSet::new();
        if let Some(selected) = best_state {
            for (index, state) in &states {
                if state != selected {
                    divergent_indices.insert(*index);
                }
            }
        }
        report.divergent_replicas = divergent_indices.len() as u64;
        for index in divergent_indices {
            if self.replica_writable(index) {
                report.repairable_replicas = report.repairable_replicas.saturating_add(1);
            }
        }
        let active_replicas = self
            .replicas
            .len()
            .saturating_sub(self.quarantined_replicas.len())
            .saturating_sub(self.disabled_chunk_endpoints.len());
        let degraded = report.missing_replicas > 0
            || report.corrupt_replicas > 0
            || report.divergent_replicas > 0
            || report.frozen_replicas > 0
            || report.quarantined_replicas > 0
            || states.len() < active_replicas
            || best_count < self.read_quorum;
        if degraded {
            report.degraded_chunks = 1;
            if best_count < self.read_quorum || best_state.is_none() {
                report.repairable_replicas = 0;
                report.unrepaired_chunks = 1;
                if first_error.is_none() {
                    first_error = Some(
                        "chunk repair source replicas do not have quorum agreement".to_string(),
                    );
                }
            } else if report.repairable_replicas > 0 {
                report.repaired_chunks = 1;
                report.repaired_replicas = report.repairable_replicas;
            } else {
                report.unrepaired_chunks = 1;
            }
        } else {
            report.healthy_chunks = 1;
        }
        report.first_error = first_error;
        Ok(report)
    }

    fn repair_chunk_from_quorum(&mut self, page_id: &PageId) -> Result<RepairStats, ObjectError> {
        let mut stats = RepairStats {
            checked_chunks: 1,
            ..RepairStats::default()
        };
        let mut locally_repaired_replicas = BTreeSet::new();
        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) || replica.is_frozen() {
                continue;
            }
            match replica.read_chunk_state_validated(page_id) {
                Ok(_) | Err(ObjectError::NotFound(_)) => {}
                Err(ObjectError::Corruption(_)) => {
                    if matches!(replica.repair_chunk_payload_from_ec(page_id), Ok(true)) {
                        locally_repaired_replicas.insert(index);
                        stats.repaired_replicas = stats.repaired_replicas.saturating_add(1);
                    }
                }
                Err(err) => return Err(err),
            }
        }
        let mut states: Vec<(usize, PageState)> = Vec::with_capacity(self.replicas.len());
        for (index, replica) in self.replicas.iter().enumerate() {
            if !self.replica_active(index) {
                continue;
            }
            if let Ok(state) = replica.read_chunk_state_validated(page_id) {
                states.push((index, state));
            }
        }
        if states.len() < self.read_quorum {
            return Err(ObjectError::Corruption(
                "chunk repair source read quorum was not met".to_string(),
            ));
        }

        let mut selected: Option<PageState> = None;
        let mut selected_count = 0usize;
        for (_, candidate) in &states {
            let count = states
                .iter()
                .filter(|(_, state)| *state == *candidate)
                .count();
            if count > selected_count {
                selected = Some(candidate.clone());
                selected_count = count;
            }
        }
        if selected_count < self.read_quorum {
            return Err(ObjectError::Corruption(
                "chunk repair source replicas do not have quorum agreement".to_string(),
            ));
        }
        let selected = selected.ok_or_else(|| {
            ObjectError::Corruption("chunk repair source selection failed".to_string())
        })?;

        let active_replicas = self
            .replicas
            .len()
            .saturating_sub(self.quarantined_replicas.len())
            .saturating_sub(self.disabled_chunk_endpoints.len());
        let mut degraded = states.len() < active_replicas || !self.quarantined_replicas.is_empty();
        for (_, state) in &states {
            if *state != selected {
                degraded = true;
                break;
            }
        }
        if degraded {
            stats.degraded_chunks = 1;
        }
        let payload_checksum = etag(&selected.data);
        let handoff_token = chunk_state_handoff_checksum(page_id, &selected, &payload_checksum);
        let selected_handoff = ReplicaStateHandoff {
            page_id: page_id.clone(),
            state: selected.clone(),
            payload_checksum: payload_checksum.clone(),
            state_checksum: chunk_state_handoff_checksum_v2(
                page_id,
                &handoff_token,
                &selected,
                &payload_checksum,
            ),
            handoff_token: handoff_token.clone(),
            install_id: format!("chunk-state-handoff-{handoff_token}"),
            proof_version: 2,
        };

        let quarantined = &self.quarantined_replicas;
        let disabled = &self.disabled_chunk_endpoints;
        for (index, replica) in self.replicas.iter_mut().enumerate() {
            if quarantined.contains(&index) || disabled.contains(&index) || replica.is_frozen() {
                continue;
            }
            let needs_repair = match replica.read_chunk_state_validated(page_id) {
                Ok(state) => state != selected,
                Err(_) => true,
            };
            if needs_repair {
                match replica.install_chunk_state_handoff(selected_handoff.clone()) {
                    Ok(()) => {
                        if !locally_repaired_replicas.contains(&index) {
                            stats.repaired_replicas = stats.repaired_replicas.saturating_add(1);
                        }
                    }
                    Err(_) => stats.failed_repairs = stats.failed_repairs.saturating_add(1),
                }
            }
        }
        self.record_repair_stats(&stats);
        Ok(stats)
    }
}
