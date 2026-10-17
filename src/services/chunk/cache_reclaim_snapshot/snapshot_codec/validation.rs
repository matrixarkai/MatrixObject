impl ChunkServiceSnapshot {
    pub fn validate(&self) -> Result<(), ObjectError> {
        validate_storage_receipt_retention(self.receipt_retention)?;
        if self.checksum != self.checksum_material_hash()
            && (!self.accepting_writes
                || self.draining
                || self.frozen
                || self.capacity_bytes != 0
                || self.capacity_rejected_writes != 0
                || self.service_mode_rejected_mutations != 0
                || self.metadata_dirty
                || self.metadata_dirty_mutations != 0
                || self.metadata_checkpoint_interval != 1
                || self.defer_metadata_checkpoints
                || self.deferred_metadata_checkpoint_pending
                || self.deferred_metadata_checkpoint_requests != 0
                || self.deferred_metadata_checkpoint_runs != 0
                || self.trim_flow_rate_bytes_per_sec != 0
                || self.trim_flow_burst_bytes != 0
                || self.trim_flow_available_bytes != 0
                || self.trim_flow_rate_ops_per_sec != 0
                || self.trim_flow_burst_ops != 0
                || self.trim_flow_available_ops != 0
                || self.trim_flow_last_refill_ms != 0
                || self.trim_flow_throttled_reclaims != 0
                || self.trim_flow_throttled_ops != 0
                || self.trim_flow_throttled_bytes != 0
                || self.partial_reclaim_failures != 0
                || self.last_partial_reclaim_failure_status != "ok"
                || self.physical_write_count != 0
                || self.physical_write_bytes != 0
                || self.physical_discard_ranges != 0
                || self.physical_discard_bytes != 0
                || self.compressed_chunk_count != 0
                || self.compression_saved_bytes != 0
                || self.ec_reconstructed_reads != 0
                || self.ec_reconstruction_failures != 0
                || self.chunk_mutation_control_commit_index != 0
                || self.chunk_mutation_control_next_log_index != 1
                || self.mutation_qos_config != ChunkMutationQosConfig::default()
                || self.mutation_qos_state != ChunkMutationQosState::default()
                || self.receipt_retention != DEFAULT_STORAGE_RECEIPT_RETENTION
                || !self.write_request_receipts.is_empty()
                || !self.discard_request_receipts.is_empty()
                || !self.batch_write_request_receipts.is_empty()
                || !self.batch_discard_request_receipts.is_empty()
                || !self.state_install_request_receipts.is_empty()
                || !self.reclaim_request_receipts.is_empty()
                || !self.repair_request_receipts.is_empty()
                || self.checksum != self.legacy_checksum_material_hash())
        {
            return Err(ObjectError::Corruption(
                "chunk service snapshot checksum mismatch".to_string(),
            ));
        }
        if (self.trim_flow_rate_bytes_per_sec == 0) != (self.trim_flow_burst_bytes == 0) {
            return Err(ObjectError::Corruption(
                "chunk service snapshot trim flow has mismatched rate and burst".to_string(),
            ));
        }
        if self.metadata_checkpoint_interval == 0 {
            return Err(ObjectError::Corruption(
                "chunk service snapshot metadata checkpoint interval is invalid".to_string(),
            ));
        }
        if !self.metadata_dirty && self.metadata_dirty_mutations != 0 {
            return Err(ObjectError::Corruption(
                "chunk service snapshot metadata dirty mutation count is inconsistent".to_string(),
            ));
        }
        if !self.defer_metadata_checkpoints && self.deferred_metadata_checkpoint_pending {
            return Err(ObjectError::Corruption(
                "chunk service snapshot deferred metadata checkpoint state is inconsistent"
                    .to_string(),
            ));
        }
        if self.trim_flow_burst_bytes > 0
            && self.trim_flow_available_bytes > self.trim_flow_burst_bytes
        {
            return Err(ObjectError::Corruption(
                "chunk service snapshot trim flow exceeds burst".to_string(),
            ));
        }
        if (self.trim_flow_rate_ops_per_sec == 0) != (self.trim_flow_burst_ops == 0) {
            return Err(ObjectError::Corruption(
                "chunk service snapshot trim flow has mismatched op rate and burst".to_string(),
            ));
        }
        if self.trim_flow_burst_ops > 0 && self.trim_flow_available_ops > self.trim_flow_burst_ops {
            return Err(ObjectError::Corruption(
                "chunk service snapshot trim flow ops exceed burst".to_string(),
            ));
        }
        if self.partial_reclaim_failures == 0 && self.last_partial_reclaim_failure_status != "ok" {
            return Err(ObjectError::Corruption(
                "chunk service snapshot reclaim failure status is inconsistent".to_string(),
            ));
        }
        validate_chunk_mutation_qos_config(&self.mutation_qos_config).map_err(|err| {
            ObjectError::Corruption(format!(
                "chunk service snapshot mutation qos invalid: {err}"
            ))
        })?;
        if self.chunk_mutation_control_next_log_index <= self.chunk_mutation_control_commit_index {
            return Err(ObjectError::Corruption(
                "chunk service mutation control log watermark is invalid".to_string(),
            ));
        }
        let physical_bytes = self.chunks.values().fold(0u64, |acc, state| {
            acc.saturating_add(state.data.len() as u64)
        });
        if self.capacity_bytes > 0 && physical_bytes > self.capacity_bytes {
            return Err(ObjectError::Corruption(
                "chunk service snapshot exceeds configured capacity".to_string(),
            ));
        }
        for (chunk_id, state) in &self.chunks {
            validate_chunk_state(state).map_err(|err| {
                ObjectError::Corruption(format!(
                    "chunk service snapshot invalid chunk {:?}: {}",
                    chunk_id, err
                ))
            })?;
        }
        // Deriving this walks every payload byte, so only do it when there is
        // something to compare it against.
        if (self.compressed_chunk_count != 0 || self.compression_saved_bytes != 0)
            && chunk_map_compression_stats(&self.chunks)
                != (self.compressed_chunk_count, self.compression_saved_bytes)
        {
            return Err(ObjectError::Corruption(
                "chunk service snapshot compression accounting mismatch".to_string(),
            ));
        }
        for (idempotency_key, receipt) in &self.write_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.length == 0
                || receipt.request_checksum.is_empty()
                || receipt.payload_checksum.is_empty()
                || receipt.request_checksum
                    != chunk_write_receipt_request_checksum(
                        &receipt.chunk_id,
                        receipt.offset,
                        receipt.length,
                        &receipt.payload_checksum,
                    )
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot write receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.discard_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.length == 0
                || receipt.request_checksum.is_empty()
                || receipt.request_checksum
                    != chunk_discard_request_checksum(
                        &receipt.chunk_id,
                        receipt.offset,
                        receipt.length,
                    )
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot discard receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.batch_write_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.payload_checksum.is_empty()
                || receipt.stats.input_ranges == 0
                || receipt.stats.input_bytes == 0
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot batch write receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.batch_discard_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.stats.input_ranges == 0
                || receipt.stats.input_bytes == 0
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot batch discard receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.state_install_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.payload_checksum.is_empty()
                || receipt.state_checksum.is_empty()
                || receipt.request_checksum != chunk_state_install_receipt_request_checksum(receipt)
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot state install receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.reclaim_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.request_checksum
                    != chunk_reclaim_request_checksum(
                        receipt.max_chunks as usize,
                        receipt.max_rewrite_bytes,
                        receipt.min_discarded_bytes,
                        receipt.min_discard_ratio_ppm,
                        receipt.max_discard_ranges as usize,
                        receipt.max_trim_bytes,
                    )
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot reclaim receipt is invalid".to_string(),
                ));
            }
        }
        for (idempotency_key, receipt) in &self.repair_request_receipts {
            validate_idempotency_key(idempotency_key)?;
            if idempotency_key != &receipt.idempotency_key
                || receipt.request_checksum.is_empty()
                || receipt.request_checksum != chunk_repair_request_checksum(&receipt.chunk_id)
                || receipt.report.chunk_id.as_ref() != Some(&receipt.chunk_id)
            {
                return Err(ObjectError::Corruption(
                    "chunk service snapshot repair receipt is invalid".to_string(),
                ));
            }
        }
        Ok(())
    }

}
