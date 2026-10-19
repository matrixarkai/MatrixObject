impl MatrixObjectStore {
    /// How many receipts the PAGE SERVICE keeps for its own requests.
    ///
    /// This is **not** what `configure_chunk_receipt_retention` changes. That
    /// call loops over the replicas and sets each replica's bound, leaving this
    /// one alone -- so reading this back after making that call shows 100,000
    /// unchanged while the replicas' behaviour has moved, which reads exactly
    /// like a knob that did not apply.
    ///
    /// The evidence that the replica call took effect is its own return value,
    /// `(receipts_trimmed, replicas_configured)`.
    pub fn page_service_receipt_retention(&self) -> usize {
        self.chunk_service.receipt_retention()
    }

    pub fn configure_chunk_replica_receipt_retention(
        &mut self,
        replica_index: usize,
        retention: usize,
    ) -> Result<u64, ObjectError> {
        self.chunk_service
            .configure_chunk_replica_receipt_retention(replica_index, retention)
    }

    pub fn configure_chunk_receipt_retention(
        &mut self,
        retention: usize,
    ) -> Result<(u64, u64), ObjectError> {
        self.chunk_service
            .configure_chunk_receipt_retention(retention)
    }

    pub fn prepare_chunk_receipt_retention_raft_entry(
        &self,
        retention: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        let admitted_route_checksum = self.chunk_service.route_control_proof().state_checksum;
        self.prepare_chunk_receipt_retention_raft_entry_fenced(
            retention,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_receipt_retention_raft_entry_fenced(
        &self,
        retention: usize,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        self.prepare_chunk_receipt_retention_raft_entry_targeted_fenced(
            None,
            retention,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_chunk_replica_receipt_retention_raft_entry_fenced(
        &self,
        replica_index: usize,
        retention: usize,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        self.prepare_chunk_receipt_retention_raft_entry_targeted_fenced(
            Some(replica_index),
            retention,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    fn prepare_chunk_receipt_retention_raft_entry_targeted_fenced(
        &self,
        target_replica_index: Option<usize>,
        retention: usize,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReceiptRetentionConfig, ObjectError> {
        self.chunk_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared receipt retention previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            replica_receipt_retention_control_image_fingerprint(&self.chunk_service);
        let mut projected = self.chunk_service.clone();
        let (trimmed_receipts, chunk_prepared_replica_count) = if let Some(replica_index) =
            target_replica_index
        {
            (
                projected.configure_chunk_replica_receipt_retention(replica_index, retention)?,
                1,
            )
        } else {
            projected.configure_chunk_receipt_retention(retention)?
        };
        let after_page_service_fingerprint =
            replica_receipt_retention_control_image_fingerprint(&projected);
        let mut prepared = PreparedChunkReceiptRetentionConfig {
            target_replica_index,
            retention,
            trimmed_receipts,
            chunk_prepared_replica_count,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof: self.chunk_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_receipt_retention_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_chunk_receipt_retention(
        &mut self,
        prepared: PreparedChunkReceiptRetentionConfig,
    ) -> Result<u64, ObjectError> {
        let expected_checksum = prepared_chunk_receipt_retention_config_checksum(
            &PreparedChunkReceiptRetentionConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            },
        );
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared receipt retention raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared receipt retention route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared receipt retention route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared receipt retention commit index must be adjacent".to_string(),
            ));
        }
        let current_fingerprint =
            replica_receipt_retention_control_image_fingerprint(&self.chunk_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.trimmed_receipts);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared receipt retention previous commit fence mismatch".to_string(),
            ));
        }
        self.chunk_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared receipt retention",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared receipt retention source fingerprint mismatch".to_string(),
            ));
        }
        let (trimmed, configured_replica_count) = if let Some(replica_index) =
            prepared.target_replica_index
        {
            (
                self.chunk_service
                    .configure_chunk_replica_receipt_retention(replica_index, prepared.retention)?,
                1,
            )
        } else {
            self.chunk_service
                .configure_chunk_receipt_retention(prepared.retention)?
        };
        if configured_replica_count != prepared.chunk_prepared_replica_count
            || replica_receipt_retention_control_image_fingerprint(&self.chunk_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "chunk prepared receipt retention deterministic apply mismatch".to_string(),
            ));
        }
        Ok(trimmed)
    }

}
