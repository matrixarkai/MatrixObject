impl MatrixObjectStore {
    pub fn configure_page_receipt_retention(
        &mut self,
        retention: usize,
    ) -> Result<u64, ObjectError> {
        self.page_service.configure_receipt_retention(retention)
    }

    pub fn prepare_page_receipt_retention_raft_entry(
        &self,
        retention: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedPageReceiptRetentionConfig, ObjectError> {
        let admitted_route_checksum = self.page_service.route_control_proof().state_checksum;
        self.prepare_page_receipt_retention_raft_entry_fenced(
            retention,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_page_receipt_retention_raft_entry_fenced(
        &self,
        retention: usize,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageReceiptRetentionConfig, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared receipt retention previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_receipt_retention_control_image_fingerprint(&self.page_service);
        let mut projected = self.page_service.clone();
        let trimmed_receipts = projected.configure_receipt_retention(retention)?;
        let after_page_service_fingerprint =
            page_receipt_retention_control_image_fingerprint(&projected);
        let mut prepared = PreparedPageReceiptRetentionConfig {
            retention,
            trimmed_receipts,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof: self.page_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_receipt_retention_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_receipt_retention(
        &mut self,
        prepared: PreparedPageReceiptRetentionConfig,
    ) -> Result<u64, ObjectError> {
        let expected_checksum = prepared_page_receipt_retention_config_checksum(
            &PreparedPageReceiptRetentionConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            },
        );
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "block prepared receipt retention raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared receipt retention route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "block prepared receipt retention route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared receipt retention commit index must be adjacent".to_string(),
            ));
        }
        let current_fingerprint =
            page_receipt_retention_control_image_fingerprint(&self.page_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.trimmed_receipts);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared receipt retention previous commit fence mismatch".to_string(),
            ));
        }
        self.page_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared receipt retention",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "block prepared receipt retention source fingerprint mismatch".to_string(),
            ));
        }
        let trimmed = self
            .page_service
            .configure_receipt_retention(prepared.retention)?;
        if trimmed != prepared.trimmed_receipts
            || page_receipt_retention_control_image_fingerprint(&self.page_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "block prepared receipt retention deterministic apply mismatch".to_string(),
            ));
        }
        Ok(trimmed)
    }

}
