// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn scrub_chunk_replicas(&mut self, repair: bool) -> Result<ReplicaScrubReport, ObjectError> {
        let mut page_ids = BTreeSet::new();
        for (index, replica) in self.replicas.iter().enumerate() {
            if !self.replica_active(index) {
                continue;
            }
            for page_id in replica.page_ids() {
                page_ids.insert(page_id);
            }
        }
        let mut report = ReplicaScrubReport::default();
        for page_id in page_ids {
            report.checked_chunks = report.checked_chunks.saturating_add(1);
            let mut valid_states: Vec<(usize, PageState)> =
                Vec::with_capacity(self.replicas.len());
            let mut missing = 0u64;
            let mut corrupt = 0u64;
            for (index, replica) in self.replicas.iter().enumerate() {
                if !self.replica_active(index) {
                    continue;
                }
                match replica.read_chunk_state_validated(&page_id) {
                    Ok(state) => valid_states.push((index, state)),
                    Err(ObjectError::NotFound(_)) => missing = missing.saturating_add(1),
                    Err(ObjectError::Corruption(_)) => corrupt = corrupt.saturating_add(1),
                    Err(err) => return Err(err),
                }
            }
            report.missing_replicas = report.missing_replicas.saturating_add(missing);
            report.corrupt_replicas = report.corrupt_replicas.saturating_add(corrupt);

            let mut best_state: Option<PageState> = None;
            let mut best_count = 0usize;
            for (_, candidate) in &valid_states {
                let count = valid_states
                    .iter()
                    .filter(|(_, state)| *state == *candidate)
                    .count();
                if count > best_count {
                    best_state = Some(candidate.clone());
                    best_count = count;
                }
            }
            let divergent = valid_states.len().saturating_sub(best_count) as u64;
            report.divergent_replicas = report.divergent_replicas.saturating_add(divergent);
            let degraded = missing > 0 || corrupt > 0 || divergent > 0;
            if !degraded {
                report.healthy_chunks = report.healthy_chunks.saturating_add(1);
                continue;
            }
            report.degraded_chunks = report.degraded_chunks.saturating_add(1);

            if !repair || best_count < self.read_quorum || best_state.is_none() {
                report.unrepaired_chunks = report.unrepaired_chunks.saturating_add(1);
                continue;
            }
            let before = report.repaired_replicas;
            let repair_stats = self.repair_chunk_from_quorum(&page_id)?;
            report.repaired_replicas = report
                .repaired_replicas
                .saturating_add(repair_stats.repaired_replicas);
            report.failed_repairs = report
                .failed_repairs
                .saturating_add(repair_stats.failed_repairs);
            if report.repaired_replicas > before {
                report.repaired_chunks = report.repaired_chunks.saturating_add(1);
            }
            if repair_stats.failed_repairs > 0 {
                report.unrepaired_chunks = report.unrepaired_chunks.saturating_add(1);
            }
        }
        Ok(report)
    }

    pub fn plan_repair_chunk_replicas(
        &self,
        page_id: &PageId,
    ) -> Result<ReplicaRepairReport, ObjectError> {
        self.inspect_chunk_repair(page_id)
    }

    pub fn repair_chunk_replicas(
        &mut self,
        page_id: &PageId,
    ) -> Result<ReplicaRepairReport, ObjectError> {
        let mut report = self.inspect_chunk_repair(page_id)?;
        if report.degraded_chunks == 0 {
            report.committed = true;
            return Ok(report);
        }
        match self.repair_chunk_from_quorum(page_id) {
            Ok(stats) => {
                report.committed = true;
                report.repaired_replicas = stats.repaired_replicas;
                report.failed_repairs = stats.failed_repairs;
                report.repaired_chunks = u64::from(stats.repaired_replicas > 0);
                report.unrepaired_chunks = u64::from(stats.failed_repairs > 0);
                if report.first_error.is_none() && stats.failed_repairs > 0 {
                    report.first_error =
                        Some("chunk exact repair left failed replicas".to_string());
                }
                Ok(report)
            }
            Err(err) => {
                report.failed_repairs = report.failed_repairs.saturating_add(1);
                report.unrepaired_chunks = 1;
                report.first_error = Some(err.to_string());
                Ok(report)
            }
        }
    }

    pub fn repair_chunk_replicas_idempotent(
        &mut self,
        page_id: &PageId,
        idempotency_key: &str,
    ) -> Result<(ReplicaRepairReport, ReplicaRepairReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = chunk_repair_request_checksum(page_id);
        if let Some(existing) = self.repair_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum || existing.page_id != *page_id {
                return Err(ObjectError::PreconditionFailed(
                    "page chunk repair idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.report.clone(), receipt));
        }
        let report = self.repair_chunk_replicas(page_id)?;
        let receipt = ReplicaRepairReceipt {
            page_id: page_id.clone(),
            request_checksum,
            idempotency_key: idempotency_key.to_string(),
            report: report.clone(),
            deduped: false,
        };
        self.repair_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok((report, receipt))
    }

    pub fn audit_chunk_replicas(
        &self,
        after_chunk_id: Option<&PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<ChunkReplicaAuditChunk, ObjectError> {
        if max_chunks == 0 {
            return Err(ObjectError::InvalidArgument(
                "page replica audit max_chunks must be positive".to_string(),
            ));
        }
        let mut page_ids = BTreeSet::new();
        for replica in &self.replicas {
            for page_id in replica.page_ids() {
                page_ids.insert(page_id);
            }
        }
        let mut page = ChunkReplicaAuditChunk {
            marker: after_chunk_id.cloned(),
            ..ChunkReplicaAuditChunk::default()
        };
        for page_id in page_ids {
            if after_chunk_id.map_or(false, |marker| &page_id <= marker) {
                continue;
            }
            if page.scanned_chunks as usize >= max_chunks {
                page.has_more = true;
                break;
            }
            let mut states: Vec<(usize, PageState)> = Vec::with_capacity(self.replicas.len());
            let mut chunk_items = Vec::with_capacity(self.replicas.len());
            let mut missing_replicas = 0u64;
            let mut corrupt_replicas = 0u64;
            let mut frozen_replicas = 0u64;
            let mut quarantined_replicas = 0u64;
            for (index, replica) in self.replicas.iter().enumerate() {
                let frozen = replica.is_frozen();
                let quarantined = self.quarantined_replicas.contains(&index)
                    || self.replica_endpoint_disabled(index);
                if self.replica_endpoint_disabled(index) {
                    quarantined_replicas = quarantined_replicas.saturating_add(1);
                    chunk_items.push(ChunkReplicaAuditItem {
                        page_id: page_id.clone(),
                        replica_index: index,
                        status: "disabled".to_string(),
                        logical_length: 0,
                        write_sequence: 0,
                        checksum: String::new(),
                        discarded_range_count: 0,
                        frozen,
                        quarantined,
                    });
                    continue;
                }
                match replica.read_chunk_state_validated(&page_id) {
                    Ok(state) => {
                        states.push((index, state.clone()));
                        chunk_items.push(ChunkReplicaAuditItem {
                            page_id: page_id.clone(),
                            replica_index: index,
                            status: "present".to_string(),
                            logical_length: state.logical_length,
                            write_sequence: state.write_sequence,
                            checksum: state.checksum(),
                            discarded_range_count: state.discarded_ranges.len() as u64,
                            frozen,
                            quarantined,
                        });
                    }
                    Err(ObjectError::NotFound(_)) => {
                        missing_replicas = missing_replicas.saturating_add(1);
                        chunk_items.push(ChunkReplicaAuditItem {
                            page_id: page_id.clone(),
                            replica_index: index,
                            status: "missing".to_string(),
                            logical_length: 0,
                            write_sequence: 0,
                            checksum: String::new(),
                            discarded_range_count: 0,
                            frozen,
                            quarantined,
                        });
                    }
                    Err(ObjectError::Corruption(_)) => {
                        corrupt_replicas = corrupt_replicas.saturating_add(1);
                        chunk_items.push(ChunkReplicaAuditItem {
                            page_id: page_id.clone(),
                            replica_index: index,
                            status: "corrupt".to_string(),
                            logical_length: 0,
                            write_sequence: 0,
                            checksum: String::new(),
                            discarded_range_count: 0,
                            frozen,
                            quarantined,
                        });
                    }
                    Err(err) => return Err(err),
                }
                if frozen {
                    frozen_replicas = frozen_replicas.saturating_add(1);
                }
                if quarantined {
                    quarantined_replicas = quarantined_replicas.saturating_add(1);
                }
            }
            let mut best_count = 0usize;
            for (_, candidate) in &states {
                let count = states
                    .iter()
                    .filter(|(_, state)| *state == *candidate)
                    .count();
                best_count = best_count.max(count);
            }
            let divergent_indices: BTreeSet<usize> = states
                .iter()
                .filter_map(|(index, candidate)| {
                    let count = states
                        .iter()
                        .filter(|(_, state)| *state == *candidate)
                        .count();
                    (count < best_count).then_some(*index)
                })
                .collect();
            for item in &mut chunk_items {
                if divergent_indices.contains(&item.replica_index) {
                    item.status = "divergent".to_string();
                }
            }
            let divergent = divergent_indices.len() as u64;
            let degraded = chunk_items.iter().any(|item| item.status != "present")
                || divergent > 0
                || states.len() < self.replicas.len()
                || best_count < self.read_quorum;
            let mut next_payload = page.payload_bytes;
            for item in &chunk_items {
                next_payload =
                    next_payload.saturating_add(chunk_replica_audit_item_payload_bytes(item));
            }
            if max_payload_bytes > 0 && next_payload > max_payload_bytes {
                page.budget_limited = true;
                page.has_more = true;
                if page.items.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "page replica audit item exceeds payload budget".to_string(),
                    ));
                }
                break;
            }
            page.scanned_chunks = page.scanned_chunks.saturating_add(1);
            if degraded {
                page.degraded_chunks = page.degraded_chunks.saturating_add(1);
            } else {
                page.healthy_chunks = page.healthy_chunks.saturating_add(1);
            }
            page.missing_replicas = page.missing_replicas.saturating_add(missing_replicas);
            page.corrupt_replicas = page.corrupt_replicas.saturating_add(corrupt_replicas);
            page.divergent_replicas = page.divergent_replicas.saturating_add(divergent);
            page.frozen_replicas = page.frozen_replicas.saturating_add(frozen_replicas);
            page.quarantined_replicas = page
                .quarantined_replicas
                .saturating_add(quarantined_replicas);
            page.payload_bytes = next_payload;
            page.next_marker = Some(page_id);
            page.items.extend(chunk_items);
        }
        Ok(page)
    }

    pub fn repair_audited_chunk_replicas(
        &mut self,
        after_chunk_id: Option<&PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
    ) -> Result<ChunkReplicaAuditRepairReport, ObjectError> {
        let route_control_proof = self.route_control_proof();
        let audit = self.audit_chunk_replicas(after_chunk_id, max_chunks, max_payload_bytes)?;
        let mut candidate_chunks = BTreeSet::new();
        for item in &audit.items {
            if item.status != "present" {
                candidate_chunks.insert(item.page_id.clone());
            }
        }
        let mut report = ChunkReplicaAuditRepairReport {
            candidate_chunks: candidate_chunks.len() as u64,
            audit,
            route_control_proof,
            ..ChunkReplicaAuditRepairReport::default()
        };
        for page_id in candidate_chunks {
            match self.repair_chunk_from_quorum(&page_id) {
                Ok(stats) => {
                    if stats.repaired_replicas > 0 && stats.failed_repairs == 0 {
                        report.repaired_chunks = report.repaired_chunks.saturating_add(1);
                    }
                    if stats.failed_repairs > 0 {
                        report.unrepaired_chunks = report.unrepaired_chunks.saturating_add(1);
                    }
                    merge_repair_stats(&mut report.stats, stats);
                }
                Err(_) => {
                    report.unrepaired_chunks = report.unrepaired_chunks.saturating_add(1);
                    report.stats.failed_repairs = report.stats.failed_repairs.saturating_add(1);
                }
            }
        }
        finalize_page_audit_repair_report(&mut report);
        Ok(report)
    }

    pub fn remove_replica_chunk_for_repair(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
    ) -> Result<(), ObjectError> {
        let replica = self.replicas.get_mut(replica_index).ok_or_else(|| {
            ObjectError::InvalidArgument("replica index is out of range".to_string())
        })?;
        replica.remove_chunk_for_repair(page_id);
        Ok(())
    }

    #[deprecated(note = "use remove_replica_chunk_for_repair")]
    pub fn remove_replica_chunk_for_repair_simulation(
        &mut self,
        replica_index: usize,
        page_id: &PageId,
    ) -> Result<(), ObjectError> {
        self.remove_replica_chunk_for_repair(replica_index, page_id)
    }
}
