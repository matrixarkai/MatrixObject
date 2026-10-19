impl MatrixObjectStore {
    pub fn prepare_chunk_batch_write_raft_entry(
        &self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_fenced(
            replica_index,
            page_id,
            writes,
            idempotency_key,
            expected_previous_commit_index,
            None,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_fenced(
        &self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write previous commit fence mismatch".to_string(),
            ));
        }
        let replica = self
            .chunk_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        replica.check_expected_control_checksum(expected_control_checksum)?;
        let control_proof = replica.control_proof();
        let expected_control_checksum = expected_control_checksum
            .unwrap_or(control_proof.state_checksum.as_str())
            .to_string();
        let mut selected = Vec::with_capacity(writes.len());
        for (offset, data) in writes {
            if !data.is_empty() {
                selected.push((offset, data));
            }
        }
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared batch write must contain at least one range".to_string(),
            ));
        }
        let mut borrowed_selected = Vec::with_capacity(selected.len());
        for (offset, data) in &selected {
            borrowed_selected.push((*offset, data.as_slice()));
        }
        let mut projected = replica.clone();
        let (_stats, receipt) = projected.write_chunk_ranges_idempotent_borrowed(
            page_id.clone(),
            &borrowed_selected,
            idempotency_key,
        )?;
        let state = projected.read_chunk_state(&page_id)?;
        let mut prepared = PreparedChunkBatchWrite {
            replica_index,
            writes: selected,
            state,
            receipt,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            control_proof,
            expected_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_batch_write_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded(
        &self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_bounded_from(
            replica_index,
            page_id,
            writes,
            max_write_bytes,
            0,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_from(
        &self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        let selected = select_bounded_chunk_write_ranges_from(
            writes,
            max_write_bytes,
            write_start_range_index,
        )?;
        let mut borrowed_selected = Vec::with_capacity(selected.len());
        for (offset, data) in &selected {
            borrowed_selected.push((*offset, data.as_slice()));
        }
        let admission = self.chunk_service.can_accept_chunk_replica_batch_write_from_borrowed(
            replica_index,
            &page_id,
            &borrowed_selected,
            max_write_bytes,
            0,
        )?;
        if !admission.can_accept {
            let reason = if admission.budget_limited {
                "chunk prepared batch write exceeds max_write_bytes"
            } else if admission.service_mode_limited {
                "chunk prepared batch write replica is not writable"
            } else if admission.capacity_limited {
                "chunk prepared batch write exceeds replica capacity"
            } else if admission.qos_limited {
                "chunk prepared batch write exceeds mutation QoS"
            } else {
                "chunk prepared batch write admission rejected"
            };
            return Err(ObjectError::PreconditionFailed(reason.to_string()));
        }
        self.prepare_chunk_batch_write_raft_entry_fenced(
            replica_index,
            page_id,
            selected,
            idempotency_key,
            expected_previous_commit_index,
            None,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_fenced(
        &self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        self.prepare_chunk_batch_write_raft_entry_bounded_fenced_from(
            replica_index,
            page_id,
            writes,
            max_write_bytes,
            0,
            idempotency_key,
            expected_previous_commit_index,
            expected_control_checksum,
        )
    }

    pub fn prepare_chunk_batch_write_raft_entry_bounded_fenced_from(
        &self,
        replica_index: usize,
        page_id: PageId,
        writes: Vec<(u64, Vec<u8>)>,
        max_write_bytes: u64,
        write_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchWrite, ObjectError> {
        let selected = select_bounded_chunk_write_ranges_from(
            writes,
            max_write_bytes,
            write_start_range_index,
        )?;
        let mut borrowed_selected = Vec::with_capacity(selected.len());
        for (offset, data) in &selected {
            borrowed_selected.push((*offset, data.as_slice()));
        }
        let admission = self
            .chunk_service
            .can_accept_chunk_replica_batch_write_fenced_borrowed(
            replica_index,
            &page_id,
            &borrowed_selected,
            max_write_bytes,
            expected_control_checksum,
        )?;
        if !admission.can_accept {
            let reason = if admission.budget_limited {
                "chunk prepared batch write exceeds max_write_bytes"
            } else if admission.service_mode_limited {
                "chunk prepared batch write replica is not writable"
            } else if admission.capacity_limited {
                "chunk prepared batch write exceeds replica capacity"
            } else if admission.qos_limited {
                "chunk prepared batch write exceeds mutation QoS"
            } else {
                "chunk prepared batch write admission rejected"
            };
            return Err(ObjectError::PreconditionFailed(reason.to_string()));
        }
        self.prepare_chunk_batch_write_raft_entry_fenced(
            replica_index,
            page_id,
            selected,
            idempotency_key,
            expected_previous_commit_index,
            expected_control_checksum,
        )
    }

    pub fn apply_prepared_chunk_batch_write(
        &mut self,
        prepared: PreparedChunkBatchWrite,
    ) -> Result<ReplicaBatchWriteReceipt, ObjectError> {
        validate_idempotency_key(&prepared.receipt.idempotency_key)?;
        validate_chunk_state(&prepared.state)?;
        let expected_checksum = prepared_chunk_batch_write_checksum(&prepared);
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared batch write raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write requires control proof checksum".to_string(),
            ));
        }
        if prepared.expected_control_checksum != prepared.control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write control proof checksum mismatch".to_string(),
            ));
        }
        let request_checksum =
            chunk_batch_write_request_checksum(&prepared.receipt.page_id, &prepared.writes);
        let payload_checksum = chunk_batch_write_payload_checksum(&prepared.writes);
        if prepared.receipt.request_checksum != request_checksum
            || prepared.receipt.payload_checksum != payload_checksum
        {
            return Err(ObjectError::Corruption(
                "chunk prepared batch write request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write commit index must be adjacent".to_string(),
            ));
        }
        let last_applied_index = self.last_applied_index();
        let replica = self
            .chunk_service
            .replicas
            .get_mut(prepared.replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        if let Some(existing) = replica
            .batch_write_request_receipts
            .get(&prepared.receipt.idempotency_key)
        {
            if existing.page_id != prepared.receipt.page_id
                || existing.request_checksum != prepared.receipt.request_checksum
                || existing.payload_checksum != prepared.receipt.payload_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk prepared batch write idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        if prepared.expected_previous_commit_index != last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write previous commit fence mismatch".to_string(),
            ));
        }
        let current_control_proof = replica.control_proof();
        if current_control_proof.state_checksum != prepared.control_proof.state_checksum
            || current_control_proof.chunk_mutation_control_commit_index
                != prepared.control_proof.chunk_mutation_control_commit_index
            || current_control_proof.chunk_mutation_control_next_log_index
                != prepared.control_proof.chunk_mutation_control_next_log_index
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write control proof cursor mismatch".to_string(),
            ));
        }
        replica.check_expected_control_checksum(Some(&current_control_proof.state_checksum))?;
        let projected_len = prepared.state.data.len() as u64;
        let projected_physical = replica
            .projected_physical_bytes_for_chunk_len(&prepared.receipt.page_id, projected_len)?;
        if replica.capacity_bytes > 0 && projected_physical > replica.capacity_bytes {
            replica.capacity_rejected_writes = replica.capacity_rejected_writes.saturating_add(1);
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch write capacity exceeded".to_string(),
            ));
        }
        replica
            .chunks
            .insert(
                prepared.receipt.page_id.clone(),
                std::sync::Arc::new(prepared.state),
            );
        let _ = replica.invalidate_chunk_cache(&prepared.receipt.page_id, 0, projected_len);
        replica.physical_write_count = replica
            .physical_write_count
            .saturating_add(prepared.receipt.stats.physical_write_count);
        replica.physical_write_bytes = replica
            .physical_write_bytes
            .saturating_add(prepared.receipt.stats.physical_write_bytes);
        std::sync::Arc::make_mut(&mut replica.batch_write_request_receipts).insert(
            prepared.receipt.idempotency_key.clone(),
            prepared.receipt.clone(),
        );
        replica.trim_receipts_to_retention();
        Ok(prepared.receipt)
    }

}
