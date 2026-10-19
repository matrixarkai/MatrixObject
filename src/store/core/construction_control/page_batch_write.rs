impl MatrixObjectStore {
    pub fn prepare_page_batch_write_raft_entry(
        &self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        self.prepare_page_batch_write_raft_entry_fenced(
            writes,
            idempotency_key,
            expected_previous_commit_index,
            None,
        )
    }

    pub fn prepare_page_batch_write_raft_entry_fenced(
        &self,
        writes: Vec<ChunkRangeWrite>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch write previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_batch_mutation_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let (_stats, receipt) =
            projected.write_slab_ranges_idempotent_borrowed(&writes, idempotency_key)?;
        let after_page_service_fingerprint = page_batch_mutation_image_fingerprint(&projected);
        let mut prepared = PreparedPageBatchWrite {
            writes,
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
        prepared.raft_entry_checksum = prepared_page_batch_write_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_page_batch_write_raft_entry_bounded(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        self.prepare_page_batch_write_raft_entry_bounded_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        let selected = self.select_bounded_page_batch_writes_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )?;
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "page prepared batch write must contain at least one range".to_string(),
            ));
        }
        let admission = self.chunk_service.can_accept_slab_writes_borrowed(
            &selected,
            max_write_bytes,
            max_chunk_write_count,
        )?;
        if !admission.can_accept {
            let reason = if admission.budget_limited {
                "block prepared batch write exceeds admission budget"
            } else if admission.ready_replica_count < admission.required_quorum {
                "block prepared batch write quorum was not met"
            } else {
                "block prepared batch write admission rejected"
            };
            return Err(ObjectError::PreconditionFailed(reason.to_string()));
        }
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_page_batch_write_raft_entry_fenced(
            selected,
            idempotency_key,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_fenced(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        self.prepare_page_batch_write_raft_entry_bounded_fenced_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            0,
            idempotency_key,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_page_batch_write_raft_entry_bounded_fenced_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageBatchWrite, ObjectError> {
        let selected = self.select_bounded_page_batch_writes_from(
            writes,
            max_write_bytes,
            max_chunk_write_count,
            write_start_range_index,
        )?;
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "page prepared batch write must contain at least one range".to_string(),
            ));
        }
        let admission = self.chunk_service.can_accept_chunk_writes_fenced_from_borrowed(
            &selected,
            max_write_bytes,
            max_chunk_write_count,
            0,
            expected_route_control_checksum,
        )?;
        if !admission.can_accept {
            let reason = if admission.budget_limited {
                "block prepared batch write exceeds admission budget"
            } else if admission.ready_replica_count < admission.required_quorum {
                "block prepared batch write quorum was not met"
            } else {
                "block prepared batch write admission rejected"
            };
            return Err(ObjectError::PreconditionFailed(reason.to_string()));
        }
        self.prepare_page_batch_write_raft_entry_fenced(
            selected,
            idempotency_key,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    fn select_bounded_page_batch_writes_from(
        &self,
        writes: Vec<ChunkRangeWrite>,
        max_write_bytes: u64,
        max_chunk_write_count: u64,
        write_start_range_index: u64,
    ) -> Result<Vec<ChunkRangeWrite>, ObjectError> {
        let requested_start = write_start_range_index as usize;
        let mut selected = Vec::with_capacity(writes.len().saturating_sub(requested_start));
        let mut selected_bytes = 0u64;
        let mut selected_chunk_ranges = 0u64;
        let mut non_empty_index = 0usize;
        for write in writes.into_iter().filter(|write| !write.data.is_empty()) {
            if non_empty_index < requested_start {
                non_empty_index += 1;
                continue;
            }
            non_empty_index += 1;
            validate_pipeline_range(
                "block prepared batch write",
                write.offset,
                write.data.len() as u64,
            )?;
            let next_bytes = selected_bytes
                .checked_add(write.data.len() as u64)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page prepared batch write selected byte count overflows".to_string(),
                    )
                })?;
            let mapped_chunk_ranges = self
                .chunk_service
                .mapped_chunk_count(write.offset, write.data.len() as u64)?;
            let next_chunk_ranges = selected_chunk_ranges
                .checked_add(mapped_chunk_ranges)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "page prepared batch write selected chunk fanout overflows".to_string(),
                    )
                })?;
            if (max_write_bytes > 0 && next_bytes > max_write_bytes)
                || (max_chunk_write_count > 0 && next_chunk_ranges > max_chunk_write_count)
            {
                break;
            }
            selected_bytes = next_bytes;
            selected_chunk_ranges = next_chunk_ranges;
            selected.push(write);
        }
        if selected.is_empty() && requested_start > non_empty_index {
            return Err(ObjectError::InvalidArgument(format!(
                "page prepared batch write start range index {} exceeds {} non-empty ranges",
                write_start_range_index, non_empty_index
            )));
        }
        if selected.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch write selected no ranges within admission budget".to_string(),
            ));
        }
        Ok(selected)
    }

    pub fn apply_prepared_page_batch_write(
        &mut self,
        prepared: PreparedPageBatchWrite,
    ) -> Result<ChunkBatchWriteReceipt, ObjectError> {
        validate_idempotency_key(&prepared.receipt.idempotency_key)?;
        let expected_checksum = prepared_page_batch_write_checksum(&prepared);
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "page prepared batch write raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch write requires route proof checksum".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch write route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.receipt.request_checksum != page_batch_write_request_checksum(&prepared.writes)
            || prepared.receipt.payload_checksum
                != page_batch_write_payload_checksum(&prepared.writes)
        {
            return Err(ObjectError::Corruption(
                "page prepared batch write request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch write commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .chunk_service
            .batch_write_request_receipts
            .get(&prepared.receipt.idempotency_key)
        {
            if existing.request_checksum != prepared.receipt.request_checksum
                || existing.payload_checksum != prepared.receipt.payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "page prepared batch write idempotency key reused with different request"
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
                "page prepared batch write previous commit fence mismatch".to_string(),
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
                "page prepared batch write route proof cursor mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_expected_route_control_checksum(Some(&current_route_proof.state_checksum))?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "page prepared batch write source fingerprint mismatch".to_string(),
            ));
        }
        let (_stats, receipt) = self.chunk_service.write_slab_ranges_idempotent_borrowed(
            &prepared.writes,
            &prepared.receipt.idempotency_key,
        )?;
        if receipt != prepared.receipt
            || page_batch_mutation_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "page prepared batch write deterministic apply mismatch".to_string(),
            ));
        }
        Ok(prepared.receipt)
    }

}
