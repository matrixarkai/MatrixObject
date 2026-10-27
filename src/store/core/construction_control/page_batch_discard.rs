// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn prepare_page_batch_discard_raft_entry(
        &self,
        requests: Vec<ChunkRangeRequest>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        self.prepare_page_batch_discard_raft_entry_fenced(
            requests,
            idempotency_key,
            expected_previous_commit_index,
            None,
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_batch_mutation_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let (_stats, receipt) =
            projected.discard_page_ranges_idempotent_borrowed(&requests, idempotency_key)?;
        let after_page_service_fingerprint = page_batch_mutation_image_fingerprint(&projected);
        let mut prepared = PreparedPageBatchDiscard {
            requests,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            receipt,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_batch_discard_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        self.prepare_page_batch_discard_raft_entry_bounded_from(
            requests,
            max_discard_bytes,
            0,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_from(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        let mut selected = Vec::with_capacity(requests.len());
        for request in requests {
            if request.length > 0 {
                selected.push(request);
            }
        }
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "page prepared batch discard must contain at least one range".to_string(),
            ));
        }
        let admission = self.chunk_service.can_accept_page_discards_from_borrowed(
            &selected,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        let selected = select_admitted_page_discard_requests(selected, &admission)?;
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_page_batch_discard_raft_entry_fenced(
            selected,
            idempotency_key,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        self.prepare_page_batch_discard_raft_entry_bounded_from_fenced(
            requests,
            max_discard_bytes,
            0,
            idempotency_key,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_page_batch_discard_raft_entry_bounded_from_fenced(
        &self,
        requests: Vec<ChunkRangeRequest>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchDiscard, ObjectError> {
        let mut selected = Vec::with_capacity(requests.len());
        for request in requests {
            if request.length > 0 {
                selected.push(request);
            }
        }
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "page prepared batch discard must contain at least one range".to_string(),
            ));
        }
        let admission = self
            .chunk_service
            .can_accept_chunk_discards_from_fenced_borrowed(
                &selected,
                max_discard_bytes,
                discard_start_range_index,
                expected_route_control_checksum,
            )?;
        let selected = select_admitted_page_discard_requests(selected, &admission)?;
        self.prepare_page_batch_discard_raft_entry_fenced(
            selected,
            idempotency_key,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn apply_prepared_page_batch_discard(
        &mut self,
        prepared: PreparedPageBatchDiscard,
    ) -> Result<ChunkBatchDiscardReceipt, ObjectError> {
        validate_idempotency_key(&prepared.receipt.idempotency_key)?;
        let expected_checksum = prepared_page_batch_discard_checksum(&PreparedPageBatchDiscard {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "page prepared batch discard raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard requires route proof checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.receipt.request_checksum
            != page_batch_discard_request_checksum(&prepared.requests)
        {
            return Err(ObjectError::Corruption(
                "page prepared batch discard request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .chunk_service
            .batch_discard_request_receipts
            .get(&prepared.receipt.idempotency_key)
        {
            if existing.request_checksum != prepared.receipt.request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "page prepared batch discard idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let current_fingerprint = page_batch_mutation_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.receipt);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard previous commit fence mismatch".to_string(),
            ));
        }
        let current_route_proof = self.chunk_service.route_control_proof();
        if prepared.route_control_proof.state_checksum != current_route_proof.state_checksum
            || prepared
                .route_control_proof
                .chunk_route_control_commit_index
                != current_route_proof.chunk_route_control_commit_index
            || prepared
                .route_control_proof
                .chunk_route_control_next_log_index
                != current_route_proof.chunk_route_control_next_log_index
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard route proof cursor mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_expected_route_control_checksum(Some(&current_route_proof.state_checksum))?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch discard source fingerprint mismatch".to_string(),
            ));
        }
        let (_stats, receipt) = self.chunk_service.discard_page_ranges_idempotent_borrowed(
            &prepared.requests,
            &prepared.receipt.idempotency_key,
        )?;
        if receipt != prepared.receipt
            || page_batch_mutation_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "page prepared batch discard deterministic apply mismatch".to_string(),
            ));
        }
        Ok(prepared.receipt)
    }

}
