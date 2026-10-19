impl MatrixObjectStore {
    pub fn prepare_page_batch_allocation_reservation_raft_entry(
        &self,
        lengths: Vec<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchAllocationReservation, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        validate_page_allocation_batch_lengths(
            &lengths,
            self.chunk_service.allocator_chunk_size,
        )?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared batch allocation reservation previous commit fence mismatch"
                    .to_string(),
            ));
        }
        let before_page_service_fingerprint =
            chunk_allocation_mutation_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let (allocations, deduped) = if let Some(existing) = projected
            .batch_allocation_request_receipts
            .get(idempotency_key)
        {
            if existing.len() != lengths.len()
                || existing
                    .iter()
                    .zip(&lengths)
                    .any(|(allocation, length)| allocation.length != *length)
            {
                return Err(ObjectError::PreconditionFailed(
                    "block batch allocation idempotency key image mismatch".to_string(),
                ));
            }
            (existing.clone(), true)
        } else {
            let mut allocations = Vec::with_capacity(lengths.len());
            for length in &lengths {
                allocations.push(projected.allocate_page_range(*length, now_ms)?);
            }
            projected
                .batch_allocation_request_receipts
                .insert(idempotency_key.to_string(), allocations.clone());
            projected.trim_receipts_to_retention();
            (allocations, false)
        };
        let after_page_service_fingerprint =
            chunk_allocation_mutation_image_fingerprint(&projected);
        let request_checksum =
            page_batch_allocation_reservation_request_checksum(&lengths, now_ms, idempotency_key);
        let route_control_proof = self.chunk_service.route_control_proof();
        let mut prepared = PreparedPageBatchAllocationReservation {
            lengths,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_route_control_checksum: route_control_proof.state_checksum.clone(),
            route_control_proof,
            receipt: ChunkBatchAllocationReceipt {
                allocations,
                idempotency_key: idempotency_key.to_string(),
                deduped,
            },
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            pending_allocation_count_after: projected.pending_allocations.len() as u64,
            allocation_receipt_count_after: projected.batch_allocation_request_receipts.len()
                as u64,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum =
            prepared_page_batch_allocation_reservation_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_batch_allocation_reservation(
        &mut self,
        prepared: PreparedPageBatchAllocationReservation,
    ) -> Result<ChunkBatchAllocationReceipt, ObjectError> {
        validate_idempotency_key(&prepared.idempotency_key)?;
        validate_page_allocation_batch_lengths(
            &prepared.lengths,
            self.chunk_service.allocator_chunk_size,
        )?;
        if prepared.request_checksum
            != page_batch_allocation_reservation_request_checksum(
                &prepared.lengths,
                prepared.now_ms,
                &prepared.idempotency_key,
            )
        {
            return Err(ObjectError::Corruption(
                "block prepared batch allocation reservation request checksum mismatch".to_string(),
            ));
        }
        let expected_checksum = prepared_page_batch_allocation_reservation_checksum(
            &PreparedPageBatchAllocationReservation {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            },
        );
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "block prepared batch allocation reservation raft entry checksum mismatch"
                    .to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared batch allocation reservation route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "block prepared batch allocation reservation route proof checksum mismatch"
                    .to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared batch allocation reservation commit index must be adjacent"
                    .to_string(),
            ));
        }
        if prepared.receipt.idempotency_key != prepared.idempotency_key
            || prepared.receipt.allocations.len() != prepared.lengths.len()
        {
            return Err(ObjectError::Corruption(
                "block prepared batch allocation reservation receipt mismatch".to_string(),
            ));
        }
        for (allocation, length) in prepared.receipt.allocations.iter().zip(&prepared.lengths) {
            if allocation.length != *length {
                return Err(ObjectError::Corruption(
                    "block prepared batch allocation reservation range length mismatch".to_string(),
                ));
            }
        }
        if let Some(existing) = self
            .chunk_service
            .batch_allocation_request_receipts
            .get(&prepared.idempotency_key)
        {
            if existing != &prepared.receipt.allocations {
                return Err(ObjectError::PreconditionFailed(
                    "block prepared batch allocation reservation idempotency image mismatch"
                        .to_string(),
                ));
            }
            return Ok(ChunkBatchAllocationReceipt {
                allocations: existing.clone(),
                idempotency_key: prepared.idempotency_key,
                deduped: true,
            });
        }
        let current_fingerprint = chunk_allocation_mutation_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.receipt);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared batch allocation reservation previous commit fence mismatch"
                    .to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared batch allocation reservation",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "block prepared batch allocation reservation source fingerprint mismatch"
                    .to_string(),
            ));
        }
        let mut allocations = Vec::with_capacity(prepared.lengths.len());
        for length in &prepared.lengths {
            allocations.push(
                self.chunk_service
                    .allocate_page_range(*length, prepared.now_ms)?,
            );
        }
        self.chunk_service
            .batch_allocation_request_receipts
            .insert(prepared.idempotency_key.clone(), allocations.clone());
        self.chunk_service.trim_receipts_to_retention();
        let receipt = ChunkBatchAllocationReceipt {
            allocations,
            idempotency_key: prepared.idempotency_key.clone(),
            deduped: false,
        };
        if receipt != prepared.receipt
            || chunk_allocation_mutation_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
            || self.chunk_service.pending_allocations.len() as u64
                != prepared.pending_allocation_count_after
            || self.chunk_service.batch_allocation_request_receipts.len() as u64
                != prepared.allocation_receipt_count_after
        {
            return Err(ObjectError::Corruption(
                "block prepared batch allocation reservation deterministic apply mismatch"
                    .to_string(),
            ));
        }
        Ok(receipt)
    }

}
