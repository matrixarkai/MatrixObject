impl MatrixMetaService {
    pub fn register_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<CoordinatorSession, ObjectError> {
        validate_session_id(session_id)?;
        validate_meta_key(owner)?;
        validate_session_endpoint(endpoint)?;
        self.admit_metadata_qos(
            1,
            session_id
                .len()
                .saturating_add(owner.len())
                .saturating_add(endpoint.len()) as u64,
            now_ms,
        )?;
        self.check_coordinator_session_admission(session_id)?;
        self.validate_coordinator_session_ttl(ttl_ms)?;
        self.validate_coordinator_session_endpoint_fence(endpoint)?;
        if let Some(existing) = self.coordinator_sessions.get(session_id) {
            if existing.expire_time_ms > now_ms && existing.owner != owner {
                return Err(ObjectError::PreconditionFailed(
                    "coordinator session is held by another owner".to_string(),
                ));
            }
        }
        let epoch = self
            .coordinator_sessions
            .get(session_id)
            .map_or(1, |session| session.epoch.saturating_add(1));
        let session = CoordinatorSession {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            endpoint: endpoint.to_string(),
            epoch,
            acquire_time_ms: now_ms,
            last_renew_ms: now_ms,
            expire_time_ms: now_ms.saturating_add(ttl_ms),
            ttl_ms,
        };
        self.coordinator_sessions
            .insert(session_id.to_string(), session.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(session)
    }

    pub fn coordinator_config(&self) -> MetadataCoordinatorConfig {
        self.coordinator_config.clone()
    }

    pub fn list_coordinator_config_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        backend: Option<&str>,
    ) -> Result<MetadataCoordinatorConfigReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(backend) = backend {
            validate_meta_key(backend)?;
        }
        let limit = limit.clamp(1, k_max_metadata_receipt_list_limit());
        let mut page = MetadataCoordinatorConfigReceiptPage {
            total_receipt_count: self.coordinator_config_receipts.len() as u64,
            limit: limit as u64,
            backend: backend.map(str::to_string),
            ..MetadataCoordinatorConfigReceiptPage::default()
        };
        let mut last_emitted_marker = None;
        let iter: Box<dyn Iterator<Item = (&String, &MetadataCoordinatorConfigReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.coordinator_config_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.coordinator_config_receipts.iter())
            };
        for (idempotency_key, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if backend.is_some_and(|backend| receipt.config.backend != backend) {
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                page.next_marker = last_emitted_marker;
                break;
            }
            last_emitted_marker = Some(idempotency_key.clone());
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

    pub fn configure_coordinator_config(
        &mut self,
        mut config: MetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfig, ObjectError> {
        config.backend = normalize_coordinator_backend(&config.backend);
        config.endpoints.sort();
        config.endpoints.dedup();
        validate_metadata_coordinator_config(&config)?;
        if config.revision <= self.coordinator_config.revision {
            config.revision = self.coordinator_config.revision.saturating_add(1);
        }
        self.coordinator_config = config.clone();
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(config)
    }

    pub fn prepare_coordinator_config_raft_entry(
        &self,
        mut config: MetadataCoordinatorConfig,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataCoordinatorConfig, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata coordinator config prepare previous commit fence mismatch".to_string(),
            ));
        }
        validate_idempotency_key(idempotency_key)?;
        config.backend = normalize_coordinator_backend(&config.backend);
        config.endpoints.sort();
        config.endpoints.dedup();
        validate_metadata_coordinator_config(&config)?;
        if config.revision <= self.coordinator_config.revision {
            config.revision = self.coordinator_config.revision.saturating_add(1);
        }
        let request_checksum = metadata_coordinator_config_request_checksum(&config);
        if let Some(existing) = self.coordinator_config_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata coordinator config idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut prepared = PreparedMetadataCoordinatorConfig {
                config: existing.config.clone(),
                idempotency_key: idempotency_key.to_string(),
                request_checksum,
                metadata_control_proof_format: String::new(),
                metadata_control_proof_checksum: String::new(),
                metadata_control_proof_bytes: 0,
                metadata_control_proof_now_ms: 0,
                expected_previous_commit_index,
                prepared_commit_index: existing.commit_index,
                raft_entry_checksum: String::new(),
            };
            prepared.raft_entry_checksum = prepared_metadata_coordinator_config_checksum(&prepared);
            return Ok(prepared);
        }
        let mut prepared = PreparedMetadataCoordinatorConfig {
            config,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            metadata_control_proof_format: String::new(),
            metadata_control_proof_checksum: String::new(),
            metadata_control_proof_bytes: 0,
            metadata_control_proof_now_ms: 0,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_coordinator_config_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_coordinator_config(
        &mut self,
        prepared: PreparedMetadataCoordinatorConfig,
    ) -> Result<MetadataCoordinatorConfigReceipt, ObjectError> {
        validate_idempotency_key(&prepared.idempotency_key)?;
        validate_metadata_coordinator_config(&prepared.config)?;
        let expected_request_checksum =
            metadata_coordinator_config_request_checksum(&prepared.config);
        if prepared.request_checksum != expected_request_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared coordinator config request checksum mismatch".to_string(),
            ));
        }
        if !prepared.metadata_control_proof_checksum.is_empty() {
            self.check_expected_metadata_control_proof(
                &prepared.metadata_control_proof_checksum,
                prepared.metadata_control_proof_now_ms,
            )?;
        }
        let expected_checksum =
            prepared_metadata_coordinator_config_checksum(&PreparedMetadataCoordinatorConfig {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared coordinator config raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator config commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self
            .coordinator_config_receipts
            .get(&prepared.idempotency_key)
        {
            if existing.request_checksum != prepared.request_checksum
                || existing.commit_index != prepared.prepared_commit_index
                || existing.config != prepared.config
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared coordinator config receipt mismatch".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator config previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator config commit index must be contiguous".to_string(),
            ));
        }
        if prepared.config.revision <= self.coordinator_config.revision {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared coordinator config revision is stale".to_string(),
            ));
        }
        self.coordinator_config = prepared.config.clone();
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        let receipt = MetadataCoordinatorConfigReceipt {
            idempotency_key: prepared.idempotency_key.clone(),
            request_checksum: prepared.request_checksum.clone(),
            config: prepared.config,
            commit_index: prepared.prepared_commit_index,
            update_time_ms: prepared.prepared_commit_index,
            deduped: false,
        };
        self.coordinator_config_receipts
            .insert(prepared.idempotency_key, receipt.clone());
        Ok(receipt)
    }
}
