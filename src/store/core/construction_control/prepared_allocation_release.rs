// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn prepare_page_allocation_release_raft_entry(
        &self,
        allocation_id: &str,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageAllocationRelease, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if allocation_id.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "page allocation release allocation_id is required".to_string(),
            ));
        }
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation release previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            chunk_allocation_mutation_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let receipt =
            projected.release_page_allocation_idempotent(allocation_id, idempotency_key)?;
        let request_checksum =
            chunk_allocation_release_request_checksum(allocation_id, idempotency_key);
        let after_page_service_fingerprint =
            chunk_allocation_mutation_image_fingerprint(&projected);
        let pending_allocation_count_after = projected.pending_allocations.len() as u64;
        let release_receipt_count_after =
            projected.release_allocation_request_receipts.len() as u64;
        let route_control_proof = self.chunk_service.route_control_proof();
        let mut prepared = PreparedPageAllocationRelease {
            allocation_id: allocation_id.to_string(),
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_route_control_checksum: route_control_proof.state_checksum.clone(),
            route_control_proof,
            receipt,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            pending_allocation_count_after,
            release_receipt_count_after,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_allocation_release_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_allocation_release(
        &mut self,
        prepared: PreparedPageAllocationRelease,
    ) -> Result<ChunkAllocationReceipt, ObjectError> {
        validate_idempotency_key(&prepared.idempotency_key)?;
        if prepared.allocation_id.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "page prepared allocation release allocation_id is required".to_string(),
            ));
        }
        if prepared.request_checksum
            != chunk_allocation_release_request_checksum(
                &prepared.allocation_id,
                &prepared.idempotency_key,
            )
        {
            return Err(ObjectError::Corruption(
                "page prepared allocation release request checksum mismatch".to_string(),
            ));
        }
        let expected_checksum =
            prepared_page_allocation_release_checksum(&PreparedPageAllocationRelease {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "page prepared allocation release raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation release route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation release route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation release commit index must be adjacent".to_string(),
            ));
        }
        if prepared.receipt.allocation.allocation_id != prepared.allocation_id
            || prepared.receipt.idempotency_key != prepared.idempotency_key
            || !prepared.receipt.released
        {
            return Err(ObjectError::Corruption(
                "page prepared allocation release receipt mismatch".to_string(),
            ));
        }
        if let Some(existing) = self
            .chunk_service
            .release_allocation_request_receipts
            .get(&prepared.idempotency_key)
        {
            if existing.allocation_id != prepared.allocation_id {
                return Err(ObjectError::PreconditionFailed(
                    "page prepared allocation release idempotency key reused with different allocation"
                        .to_string(),
                ));
            }
            if *existing != prepared.receipt.allocation {
                return Err(ObjectError::PreconditionFailed(
                    "page prepared allocation release receipt image mismatch".to_string(),
                ));
            }
            return Ok(ChunkAllocationReceipt {
                allocation: existing.clone(),
                idempotency_key: prepared.idempotency_key,
                released: true,
                deduped: true,
            });
        }
        let current_fingerprint = chunk_allocation_mutation_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.receipt);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation release previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared allocation release",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "page prepared allocation release source fingerprint mismatch".to_string(),
            ));
        }
        let receipt = self.chunk_service.release_page_allocation_idempotent(
            &prepared.allocation_id,
            &prepared.idempotency_key,
        )?;
        if receipt != prepared.receipt
            || chunk_allocation_mutation_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
            || self.chunk_service.pending_allocations.len() as u64
                != prepared.pending_allocation_count_after
            || self.chunk_service.release_allocation_request_receipts.len() as u64
                != prepared.release_receipt_count_after
        {
            return Err(ObjectError::Corruption(
                "page prepared allocation release deterministic apply mismatch".to_string(),
            ));
        }
        Ok(receipt)
    }

}
