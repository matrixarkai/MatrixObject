impl MatrixObjectStore {
    pub fn prepare_page_range_repair_raft_entry(
        &self,
        slab_id: SlabId,
        offset: u64,
        length: u64,
        max_repair_chunks: usize,
        start_chunk_index: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageRangeRepair, ObjectError> {
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_audit_repair_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let report = projected.repair_slab_range_bounded(
            &slab_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
        )?;
        let after_page_service_fingerprint = page_audit_repair_image_fingerprint(&projected);
        let mut prepared = PreparedPageRangeRepair {
            slab_id,
            offset,
            length,
            max_repair_chunks,
            start_chunk_index,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            route_control_proof: report.route_control_proof.clone(),
            expected_route_control_checksum: report.expected_route_control_checksum.clone(),
            chunk_control_proof: self.chunk_service.chunk_control_proof(),
            report,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_range_repair_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_range_repair(
        &mut self,
        prepared: PreparedPageRangeRepair,
    ) -> Result<(BoundedRepairReport, ChunkRangeRepairReceipt), ObjectError> {
        let expected_checksum = prepared_page_range_repair_checksum(&PreparedPageRangeRepair {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "block prepared range repair raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.route_control_proof != prepared.report.route_control_proof {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair route proof/report mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum
            != prepared.report.expected_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair route checksum/report mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.chunk_control_proof.proof_count == 0
            || prepared.chunk_control_proof.proof_checksum.is_empty()
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair chunk control proof is required".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .chunk_service
            .range_repair_request_receipts
            .get(&prepared.raft_entry_checksum)
        {
            if existing.slab_id != prepared.slab_id
                || existing.offset != prepared.offset
                || existing.length != prepared.length
                || existing.max_repair_chunks != prepared.max_repair_chunks as u64
                || existing.start_chunk_index != prepared.start_chunk_index
                || existing.report != prepared.report
            {
                return Err(ObjectError::PreconditionFailed(
                    "block prepared range repair receipt image mismatch".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.report.clone(), receipt));
        }
        let current_fingerprint = page_audit_repair_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            let receipt = ChunkRangeRepairReceipt {
                slab_id: prepared.slab_id.clone(),
                offset: prepared.offset,
                length: prepared.length,
                max_repair_chunks: prepared.max_repair_chunks as u64,
                start_chunk_index: prepared.start_chunk_index,
                request_checksum: page_range_repair_request_checksum(
                    &prepared.slab_id,
                    prepared.offset,
                    prepared.length,
                    prepared.max_repair_chunks as u64,
                    prepared.start_chunk_index,
                ),
                idempotency_key: prepared.raft_entry_checksum.clone(),
                report: prepared.report.clone(),
                deduped: false,
            };
            self.chunk_service
                .range_repair_request_receipts
                .insert(prepared.raft_entry_checksum.clone(), receipt.clone());
            self.chunk_service.trim_receipts_to_retention();
            return Ok((prepared.report, receipt));
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared range repair",
            )?;
        self.chunk_service
            .check_prepared_chunk_control_proof_cursor(
                &prepared.chunk_control_proof,
                "block prepared range repair",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair source fingerprint mismatch".to_string(),
            ));
        }
        let mut current = self.prepare_page_range_repair_raft_entry(
            prepared.slab_id.clone(),
            prepared.offset,
            prepared.length,
            prepared.max_repair_chunks,
            prepared.start_chunk_index,
            prepared.expected_previous_commit_index,
        )?;
        current.report.expected_route_control_checksum =
            prepared.report.expected_route_control_checksum.clone();
        current.expected_route_control_checksum = prepared.expected_route_control_checksum.clone();
        current.chunk_control_proof = prepared.chunk_control_proof.clone();
        current.raft_entry_checksum = prepared_page_range_repair_checksum(&current);
        if current.report != prepared.report
            || current.route_control_proof != prepared.route_control_proof
            || current.chunk_control_proof != prepared.chunk_control_proof
            || current.raft_entry_checksum != prepared.raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared range repair no longer matches current state".to_string(),
            ));
        }
        let mut report = self.chunk_service.repair_slab_range_bounded(
            &prepared.slab_id,
            prepared.offset,
            prepared.length,
            prepared.max_repair_chunks,
            prepared.start_chunk_index,
        )?;
        report.expected_route_control_checksum =
            prepared.report.expected_route_control_checksum.clone();
        if report != prepared.report
            || page_audit_repair_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "block prepared range repair deterministic apply mismatch".to_string(),
            ));
        }
        let receipt = ChunkRangeRepairReceipt {
            slab_id: prepared.slab_id.clone(),
            offset: prepared.offset,
            length: prepared.length,
            max_repair_chunks: prepared.max_repair_chunks as u64,
            start_chunk_index: prepared.start_chunk_index,
            request_checksum: page_range_repair_request_checksum(
                &prepared.slab_id,
                prepared.offset,
                prepared.length,
                prepared.max_repair_chunks as u64,
                prepared.start_chunk_index,
            ),
            idempotency_key: prepared.raft_entry_checksum.clone(),
            report: prepared.report.clone(),
            deduped: false,
        };
        self.chunk_service
            .range_repair_request_receipts
            .insert(prepared.raft_entry_checksum.clone(), receipt.clone());
        self.chunk_service.trim_receipts_to_retention();
        Ok((prepared.report, receipt))
    }

}
