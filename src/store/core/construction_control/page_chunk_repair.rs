impl MatrixObjectStore {
    pub fn prepare_page_chunk_repair_raft_entry(
        &self,
        chunk_id: PageId,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageChunkRepair, ObjectError> {
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_audit_repair_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let report = projected.repair_chunk_replicas(&chunk_id)?;
        let after_page_service_fingerprint = page_audit_repair_image_fingerprint(&projected);
        let mut prepared = PreparedPageChunkRepair {
            chunk_id,
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
        prepared.raft_entry_checksum = prepared_page_chunk_repair_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_chunk_repair(
        &mut self,
        prepared: PreparedPageChunkRepair,
    ) -> Result<(ReplicaRepairReport, ReplicaRepairReceipt), ObjectError> {
        let expected_checksum = prepared_page_chunk_repair_checksum(&PreparedPageChunkRepair {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "block prepared chunk repair raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.route_control_proof != prepared.report.route_control_proof {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair route proof/report mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum
            != prepared.report.expected_route_control_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair route checksum/report mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.chunk_control_proof.proof_count == 0
            || prepared.chunk_control_proof.proof_checksum.is_empty()
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair chunk control proof is required".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .chunk_service
            .repair_request_receipts
            .get(&prepared.raft_entry_checksum)
        {
            if existing.chunk_id != prepared.chunk_id || existing.report != prepared.report {
                return Err(ObjectError::PreconditionFailed(
                    "block prepared chunk repair receipt image mismatch".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.report.clone(), receipt));
        }
        let current_fingerprint = page_audit_repair_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            let receipt = ReplicaRepairReceipt {
                chunk_id: prepared.chunk_id.clone(),
                request_checksum: prepared.raft_entry_checksum.clone(),
                idempotency_key: prepared.raft_entry_checksum.clone(),
                report: prepared.report.clone(),
                deduped: false,
            };
            self.chunk_service
                .repair_request_receipts
                .insert(prepared.raft_entry_checksum.clone(), receipt.clone());
            self.chunk_service.trim_receipts_to_retention();
            return Ok((prepared.report, receipt));
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared chunk repair",
            )?;
        self.chunk_service
            .check_prepared_chunk_control_proof_cursor(
                &prepared.chunk_control_proof,
                "block prepared chunk repair",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair source fingerprint mismatch".to_string(),
            ));
        }
        let mut current = self.prepare_page_chunk_repair_raft_entry(
            prepared.chunk_id.clone(),
            prepared.expected_previous_commit_index,
        )?;
        current.report.expected_route_control_checksum =
            prepared.report.expected_route_control_checksum.clone();
        current.expected_route_control_checksum = prepared.expected_route_control_checksum.clone();
        current.chunk_control_proof = prepared.chunk_control_proof.clone();
        current.raft_entry_checksum = prepared_page_chunk_repair_checksum(&current);
        if current.report != prepared.report
            || current.route_control_proof != prepared.route_control_proof
            || current.chunk_control_proof != prepared.chunk_control_proof
            || current.raft_entry_checksum != prepared.raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared chunk repair no longer matches current state".to_string(),
            ));
        }
        let mut report = self
            .chunk_service
            .repair_chunk_replicas(&prepared.chunk_id)?;
        report.expected_route_control_checksum =
            prepared.report.expected_route_control_checksum.clone();
        if report != prepared.report
            || page_audit_repair_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "block prepared chunk repair deterministic apply mismatch".to_string(),
            ));
        }
        let receipt = ReplicaRepairReceipt {
            chunk_id: prepared.chunk_id.clone(),
            request_checksum: prepared.raft_entry_checksum.clone(),
            idempotency_key: prepared.raft_entry_checksum.clone(),
            report: prepared.report.clone(),
            deduped: false,
        };
        self.chunk_service
            .repair_request_receipts
            .insert(prepared.raft_entry_checksum.clone(), receipt.clone());
        self.chunk_service.trim_receipts_to_retention();
        Ok((prepared.report, receipt))
    }
}
