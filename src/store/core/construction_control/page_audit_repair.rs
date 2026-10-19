impl MatrixObjectStore {
    pub fn prepare_page_audit_repair_raft_entry(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageAuditRepair, ObjectError> {
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_audit_repair_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let report = projected.repair_audited_chunk_replicas(
            after_chunk_id.as_ref(),
            max_chunks,
            max_payload_bytes,
        )?;
        let after_page_service_fingerprint = page_audit_repair_image_fingerprint(&projected);
        let mut prepared = PreparedPageAuditRepair {
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
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
        prepared.raft_entry_checksum = prepared_page_audit_repair_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_page_audit_repair_raft_entry_bounded(
        &self,
        after_chunk_id: Option<PageId>,
        max_chunks: usize,
        max_payload_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageAuditRepair, ObjectError> {
        if max_chunks == 0 {
            return Err(ObjectError::InvalidArgument(
                "page prepared audit repair max_chunks must be positive".to_string(),
            ));
        }
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "page prepared audit repair max_payload_bytes must be positive".to_string(),
            ));
        }
        self.prepare_page_audit_repair_raft_entry(
            after_chunk_id,
            max_chunks,
            max_payload_bytes,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_page_audit_repair(
        &mut self,
        prepared: PreparedPageAuditRepair,
    ) -> Result<(ChunkReplicaAuditRepairReport, ChunkAuditRepairReceipt), ObjectError> {
        let expected_checksum = prepared_page_audit_repair_checksum(&PreparedPageAuditRepair {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "page prepared audit repair raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.route_control_proof != prepared.report.route_control_proof {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair route proof/report mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum
            != prepared.report.expected_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair route checksum/report mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.chunk_control_proof.proof_count == 0
            || prepared.chunk_control_proof.proof_checksum.is_empty()
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair chunk control proof is required".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair commit index must be adjacent".to_string(),
            ));
        }
        let request_checksum = page_audit_repair_request_checksum(
            &prepared.after_chunk_id,
            prepared.max_chunks as u64,
            prepared.max_payload_bytes,
        );
        if let Some(existing) = self
            .chunk_service
            .audit_repair_request_receipts
            .get(&prepared.raft_entry_checksum)
        {
            if existing.after_chunk_id == prepared.after_chunk_id
                && existing.max_chunks == prepared.max_chunks as u64
                && existing.max_payload_bytes == prepared.max_payload_bytes
                && existing.request_checksum == request_checksum
                && existing.report == prepared.report
            {
                let mut receipt = existing.clone();
                receipt.deduped = true;
                return Ok((receipt.report.clone(), receipt));
            }
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair idempotency key conflict".to_string(),
            ));
        }
        let current_fingerprint = page_audit_repair_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            let receipt = ChunkAuditRepairReceipt {
                after_chunk_id: prepared.after_chunk_id.clone(),
                max_chunks: prepared.max_chunks as u64,
                max_payload_bytes: prepared.max_payload_bytes,
                request_checksum,
                idempotency_key: prepared.raft_entry_checksum.clone(),
                report: prepared.report.clone(),
                deduped: false,
            };
            self.chunk_service
                .audit_repair_request_receipts
                .insert(prepared.raft_entry_checksum.clone(), receipt.clone());
            self.chunk_service.trim_receipts_to_retention();
            return Ok((prepared.report, receipt));
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared audit repair",
            )?;
        self.chunk_service
            .check_prepared_chunk_control_proof_cursor(
                &prepared.chunk_control_proof,
                "block prepared audit repair",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair source fingerprint mismatch".to_string(),
            ));
        }
        let mut current = self.prepare_page_audit_repair_raft_entry(
            prepared.after_chunk_id.clone(),
            prepared.max_chunks,
            prepared.max_payload_bytes,
            prepared.expected_previous_commit_index,
        )?;
        current.report.expected_route_control_checksum =
            prepared.report.expected_route_control_checksum.clone();
        current.expected_route_control_checksum = prepared.expected_route_control_checksum.clone();
        current.chunk_control_proof = prepared.chunk_control_proof.clone();
        current.raft_entry_checksum = prepared_page_audit_repair_checksum(&current);
        if current.report != prepared.report
            || current.route_control_proof != prepared.route_control_proof
            || current.chunk_control_proof != prepared.chunk_control_proof
            || current.raft_entry_checksum != prepared.raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared audit repair no longer matches current state".to_string(),
            ));
        }
        let mut report = self.chunk_service.repair_audited_chunk_replicas(
            prepared.after_chunk_id.as_ref(),
            prepared.max_chunks,
            prepared.max_payload_bytes,
        )?;
        report.expected_route_control_checksum =
            prepared.report.expected_route_control_checksum.clone();
        if report != prepared.report
            || page_audit_repair_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "page prepared audit repair deterministic apply mismatch".to_string(),
            ));
        }
        let receipt = ChunkAuditRepairReceipt {
            after_chunk_id: prepared.after_chunk_id.clone(),
            max_chunks: prepared.max_chunks as u64,
            max_payload_bytes: prepared.max_payload_bytes,
            request_checksum,
            idempotency_key: prepared.raft_entry_checksum.clone(),
            report: prepared.report.clone(),
            deduped: false,
        };
        self.chunk_service
            .audit_repair_request_receipts
            .insert(prepared.raft_entry_checksum.clone(), receipt.clone());
        self.chunk_service.trim_receipts_to_retention();
        Ok((prepared.report, receipt))
    }

}
