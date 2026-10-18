impl MatrixChunkService {
    pub fn install_chunk_state(
        &mut self,
        chunk_id: ChunkId,
        mut state: ChunkState,
    ) -> Result<(), ObjectError> {
        self.ensure_writable()?;
        if state.ec_parity_fragment.is_empty() {
            if self.keep_ec_sidecar {
                refresh_chunk_ec_sidecars(&mut state);
            } else {
                clear_chunk_ec_sidecars(&mut state);
            }
        }
        validate_chunk_state(&state)?;
        self.ensure_capacity_for_chunk_len(&chunk_id, state.data.len() as u64)?;
        self.invalidate_chunk_cache(&chunk_id, 0, state.logical_length);
        self.chunks.insert(chunk_id, std::sync::Arc::new(state));
        Ok(())
    }

    pub fn install_chunk_state_fenced(
        &mut self,
        chunk_id: ChunkId,
        state: ChunkState,
        expected_control_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        self.check_expected_control_checksum(expected_control_checksum)?;
        self.install_chunk_state(chunk_id, state)
    }

    pub fn install_chunk_state_idempotent(
        &mut self,
        chunk_id: ChunkId,
        state: ChunkState,
        idempotency_key: &str,
    ) -> Result<ChunkStateInstallReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let payload_checksum = etag(&state.data);
        let state_checksum = chunk_state_handoff_checksum(&chunk_id, &state, &payload_checksum);
        let request_checksum = chunk_state_install_request_checksum(
            &chunk_id,
            &state,
            &payload_checksum,
            &state_checksum,
        );
        if let Some(existing) = self.state_install_request_receipts.get(idempotency_key) {
            if existing.chunk_id != chunk_id
                || existing.request_checksum != request_checksum
                || existing.payload_checksum != payload_checksum
                || existing.state_checksum != state_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "chunk state install idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        self.install_chunk_state(chunk_id.clone(), state.clone())?;
        let receipt = ChunkStateInstallReceipt {
            chunk_id,
            request_checksum,
            payload_checksum,
            state_checksum,
            idempotency_key: idempotency_key.to_string(),
            logical_length: state.logical_length,
            write_sequence: state.write_sequence,
            discard_range_count: state.discarded_ranges.len() as u64,
            deduped: false,
        };
        self.state_install_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        self.trim_receipts_to_retention();
        Ok(receipt)
    }

    pub fn repair_chunk_payload_from_ec(
        &mut self,
        chunk_id: &ChunkId,
    ) -> Result<bool, ObjectError> {
        self.ensure_writable()?;
        let chunk = self
            .chunks
            .get_mut(chunk_id)
            .ok_or_else(|| ObjectError::NotFound("chunk is missing".to_string()))?;
        let chunk = std::sync::Arc::make_mut(chunk);
        if etag_matches(&chunk.data, &chunk.checksum) {
            return Ok(false);
        }
        let reconstructed = match chunk_ec_recover_data(
            &chunk.data,
            &chunk.ec_parity_fragment,
            chunk.logical_length as usize,
            &chunk.checksum,
        ) {
            Ok(reconstructed) => reconstructed,
            Err(err) => {
                self.ec_reconstruction_failures
                    .set(self.ec_reconstruction_failures.get().saturating_add(1));
                return Err(err);
            }
        };
        if !etag_matches(&reconstructed, &chunk.checksum) {
            self.ec_reconstruction_failures
                .set(self.ec_reconstruction_failures.get().saturating_add(1));
            return Err(ObjectError::Corruption(
                "chunk EC sidecars cannot reconstruct expected payload".to_string(),
            ));
        }
        chunk.data = std::sync::Arc::new(reconstructed);
        chunk.write_sequence = chunk.write_sequence.saturating_add(1);
        if self.keep_ec_sidecar {
            refresh_chunk_ec_sidecars(chunk);
        } else {
            clear_chunk_ec_sidecars(chunk);
        }
        self.ec_reconstructed_reads
            .set(self.ec_reconstructed_reads.get().saturating_add(1));
        self.invalidate_chunk_cache(chunk_id, 0, u64::MAX);
        Ok(true)
    }

    pub fn prepare_chunk_state_handoff(
        &self,
        chunk_id: &ChunkId,
    ) -> Result<ChunkStateHandoff, ObjectError> {
        let state = self.read_chunk_state_validated(chunk_id)?;
        let payload_checksum = etag(&state.data);
        let legacy_checksum = chunk_state_handoff_checksum(chunk_id, &state, &payload_checksum);
        let handoff_token = legacy_checksum.clone();
        let state_checksum =
            chunk_state_handoff_checksum_v2(chunk_id, &handoff_token, &state, &payload_checksum);
        Ok(ChunkStateHandoff {
            chunk_id: chunk_id.clone(),
            state,
            payload_checksum,
            state_checksum,
            handoff_token: handoff_token.clone(),
            install_id: format!("chunk-state-handoff-{handoff_token}"),
            proof_version: 2,
        })
    }

    pub fn prepare_chunk_state_handoff_envelope(
        &self,
        chunk_id: &ChunkId,
        max_payload_bytes: u64,
    ) -> Result<PreparedChunkLocalStateHandoff, ObjectError> {
        let handoff = self.prepare_chunk_state_handoff(chunk_id)?;
        let payload_bytes = handoff.state.data.len() as u64;
        if max_payload_bytes > 0 && payload_bytes > max_payload_bytes {
            return Err(ObjectError::InvalidArgument(
                "chunk state handoff payload exceeds budget".to_string(),
            ));
        }
        let envelope =
            make_prepared_chunk_local_state_handoff_envelope(&handoff, max_payload_bytes);
        Ok(PreparedChunkLocalStateHandoff { handoff, envelope })
    }

    pub fn install_chunk_state_handoff(
        &mut self,
        handoff: ChunkStateHandoff,
    ) -> Result<(), ObjectError> {
        validate_chunk_state_handoff(&handoff)?;
        self.install_chunk_state(handoff.chunk_id, handoff.state)
    }

    pub fn install_chunk_state_handoff_fenced(
        &mut self,
        handoff: ChunkStateHandoff,
        expected_control_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        validate_chunk_state_handoff(&handoff)?;
        self.install_chunk_state_fenced(handoff.chunk_id, handoff.state, expected_control_checksum)
    }

}
