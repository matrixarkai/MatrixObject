// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn prepare_page_allocation_reservation_raft_entry(
        &self,
        length: u64,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageAllocationReservation, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation reservation previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            chunk_allocation_mutation_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let receipt = projected.allocate_page_range_idempotent(length, now_ms, idempotency_key)?;
        let request_checksum =
            chunk_allocation_reservation_request_checksum(length, now_ms, idempotency_key);
        let after_page_service_fingerprint =
            chunk_allocation_mutation_image_fingerprint(&projected);
        let route_control_proof = self.chunk_service.route_control_proof();
        let mut prepared = PreparedPageAllocationReservation {
            length,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_route_control_checksum: route_control_proof.state_checksum.clone(),
            route_control_proof,
            receipt,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            pending_allocation_count_after: projected.pending_allocations.len() as u64,
            allocation_receipt_count_after: projected.allocation_request_receipts.len() as u64,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_allocation_reservation_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_allocation_reservation(
        &mut self,
        prepared: PreparedPageAllocationReservation,
    ) -> Result<ChunkAllocationReceipt, ObjectError> {
        validate_idempotency_key(&prepared.idempotency_key)?;
        if prepared.request_checksum
            != chunk_allocation_reservation_request_checksum(
                prepared.length,
                prepared.now_ms,
                &prepared.idempotency_key,
            )
        {
            return Err(ObjectError::Corruption(
                "page prepared allocation reservation request checksum mismatch".to_string(),
            ));
        }
        let expected_checksum =
            prepared_page_allocation_reservation_checksum(&PreparedPageAllocationReservation {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "page prepared allocation reservation raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation reservation route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation reservation route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation reservation commit index must be adjacent".to_string(),
            ));
        }
        if prepared.receipt.allocation.length != prepared.length
            || prepared.receipt.idempotency_key != prepared.idempotency_key
            || prepared.receipt.released
        {
            return Err(ObjectError::Corruption(
                "page prepared allocation reservation receipt mismatch".to_string(),
            ));
        }
        if let Some(existing) = self
            .chunk_service
            .allocation_request_receipts
            .get(&prepared.idempotency_key)
        {
            if existing.length != prepared.length || *existing != prepared.receipt.allocation {
                return Err(ObjectError::PreconditionFailed(
                    "page prepared allocation reservation idempotency image mismatch".to_string(),
                ));
            }
            return Ok(ChunkAllocationReceipt {
                allocation: existing.clone(),
                idempotency_key: prepared.idempotency_key,
                released: false,
                deduped: true,
            });
        }
        let current_fingerprint = chunk_allocation_mutation_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.receipt);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation reservation previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared allocation reservation",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation reservation source fingerprint mismatch".to_string(),
            ));
        }
        let receipt = self.chunk_service.allocate_page_range_idempotent(
            prepared.length,
            prepared.now_ms,
            &prepared.idempotency_key,
        )?;
        if receipt != prepared.receipt
            || chunk_allocation_mutation_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
            || self.chunk_service.pending_allocations.len() as u64
                != prepared.pending_allocation_count_after
            || self.chunk_service.allocation_request_receipts.len() as u64
                != prepared.allocation_receipt_count_after
        {
            return Err(ObjectError::Corruption(
                "page prepared allocation reservation deterministic apply mismatch".to_string(),
            ));
        }
        Ok(receipt)
    }

}
