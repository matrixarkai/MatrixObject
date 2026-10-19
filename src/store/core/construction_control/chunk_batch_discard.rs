impl MatrixObjectStore {
    pub fn prepare_chunk_batch_discard_raft_entry(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        ranges: Vec<DataRange>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkBatchDiscard, ObjectError> {
        self.prepare_chunk_batch_discard_raft_entry_fenced(
            replica_index,
            chunk_id,
            ranges,
            idempotency_key,
            expected_previous_commit_index,
            None,
        )
    }

    pub fn prepare_chunk_batch_discard_raft_entry_fenced(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        ranges: Vec<DataRange>,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchDiscard, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch discard previous commit fence mismatch".to_string(),
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
        let mut selected = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.length > 0 {
                selected.push(range);
            }
        }
        if selected.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared batch discard must contain at least one range".to_string(),
            ));
        }
        let mut projected = replica.clone();
        let (_stats, receipt) = projected.discard_chunk_ranges_idempotent_borrowed(
            &chunk_id,
            &selected,
            idempotency_key,
        )?;
        let state = projected.read_chunk_state(&chunk_id)?;
        let mut prepared = PreparedChunkBatchDiscard {
            replica_index,
            ranges: selected,
            state,
            receipt,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            control_proof,
            expected_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_batch_discard_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_chunk_batch_discard_raft_entry_bounded(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkBatchDiscard, ObjectError> {
        self.prepare_chunk_batch_discard_raft_entry_bounded_from(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            0,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_chunk_batch_discard_raft_entry_bounded_from(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkBatchDiscard, ObjectError> {
        let selected = select_bounded_chunk_discard_ranges_from(
            ranges,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        let admission = self
            .chunk_service
            .can_accept_chunk_replica_batch_discard_from_borrowed(
            replica_index,
            &chunk_id,
            &selected,
            max_discard_bytes,
            0,
        )?;
        if !admission.can_accept {
            let reason = if admission.budget_limited {
                "chunk prepared batch discard exceeds max_discard_bytes"
            } else if admission.service_mode_limited {
                "chunk prepared batch discard replica is not writable"
            } else if admission.missing_chunk {
                "chunk prepared batch discard chunk is missing"
            } else if admission.range_invalid {
                "chunk prepared batch discard range exceeds logical length"
            } else if admission.qos_limited {
                "chunk prepared batch discard exceeds mutation QoS"
            } else {
                "chunk prepared batch discard admission rejected"
            };
            return Err(ObjectError::PreconditionFailed(reason.to_string()));
        }
        self.prepare_chunk_batch_discard_raft_entry_fenced(
            replica_index,
            chunk_id,
            selected,
            idempotency_key,
            expected_previous_commit_index,
            None,
        )
    }

    pub fn prepare_chunk_batch_discard_raft_entry_bounded_fenced(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchDiscard, ObjectError> {
        self.prepare_chunk_batch_discard_raft_entry_bounded_fenced_from(
            replica_index,
            chunk_id,
            ranges,
            max_discard_bytes,
            0,
            idempotency_key,
            expected_previous_commit_index,
            expected_control_checksum,
        )
    }

    pub fn prepare_chunk_batch_discard_raft_entry_bounded_fenced_from(
        &self,
        replica_index: usize,
        chunk_id: PageId,
        ranges: Vec<DataRange>,
        max_discard_bytes: u64,
        discard_start_range_index: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
        expected_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkBatchDiscard, ObjectError> {
        let selected = select_bounded_chunk_discard_ranges_from(
            ranges,
            max_discard_bytes,
            discard_start_range_index,
        )?;
        let admission = self
            .chunk_service
            .can_accept_chunk_replica_batch_discard_fenced_borrowed(
            replica_index,
            &chunk_id,
            &selected,
            max_discard_bytes,
            expected_control_checksum,
        )?;
        if !admission.can_accept {
            let reason = if admission.budget_limited {
                "chunk prepared batch discard exceeds max_discard_bytes"
            } else if admission.service_mode_limited {
                "chunk prepared batch discard replica is not writable"
            } else if admission.missing_chunk {
                "chunk prepared batch discard chunk is missing"
            } else if admission.range_invalid {
                "chunk prepared batch discard range exceeds logical length"
            } else if admission.qos_limited {
                "chunk prepared batch discard exceeds mutation QoS"
            } else {
                "chunk prepared batch discard admission rejected"
            };
            return Err(ObjectError::PreconditionFailed(reason.to_string()));
        }
        self.prepare_chunk_batch_discard_raft_entry_fenced(
            replica_index,
            chunk_id,
            selected,
            idempotency_key,
            expected_previous_commit_index,
            expected_control_checksum,
        )
    }

    pub fn apply_prepared_chunk_batch_discard(
        &mut self,
        prepared: PreparedChunkBatchDiscard,
    ) -> Result<ReplicaBatchDiscardReceipt, ObjectError> {
        validate_idempotency_key(&prepared.receipt.idempotency_key)?;
        validate_chunk_state(&prepared.state)?;
        let expected_checksum = prepared_chunk_batch_discard_checksum(&PreparedChunkBatchDiscard {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared batch discard raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch discard requires control proof checksum".to_string(),
            ));
        }
        if prepared.expected_control_checksum != prepared.control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch discard control proof checksum mismatch".to_string(),
            ));
        }
        let request_checksum =
            chunk_batch_discard_request_checksum(&prepared.receipt.chunk_id, &prepared.ranges);
        if prepared.receipt.request_checksum != request_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared batch discard request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch discard commit index must be adjacent".to_string(),
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
            .batch_discard_request_receipts
            .get(&prepared.receipt.idempotency_key)
        {
            if existing.chunk_id != prepared.receipt.chunk_id
                || existing.request_checksum != prepared.receipt.request_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk prepared batch discard idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        if prepared.expected_previous_commit_index != last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared batch discard previous commit fence mismatch".to_string(),
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
                "chunk prepared batch discard control proof cursor mismatch".to_string(),
            ));
        }
        replica.check_expected_control_checksum(Some(&current_control_proof.state_checksum))?;
        let projected_len = prepared.state.logical_length;
        replica
            .chunks
            .insert(
                prepared.receipt.chunk_id.clone(),
                std::sync::Arc::new(prepared.state),
            );
        let _ = replica.invalidate_chunk_cache(&prepared.receipt.chunk_id, 0, projected_len);
        replica.physical_discard_ranges = replica
            .physical_discard_ranges
            .saturating_add(prepared.receipt.stats.physical_discard_ranges);
        replica.physical_discard_bytes = replica
            .physical_discard_bytes
            .saturating_add(prepared.receipt.stats.physical_discard_bytes);
        std::sync::Arc::make_mut(&mut replica.batch_discard_request_receipts).insert(
            prepared.receipt.idempotency_key.clone(),
            prepared.receipt.clone(),
        );
        replica.trim_receipts_to_retention();
        Ok(prepared.receipt)
    }

}
