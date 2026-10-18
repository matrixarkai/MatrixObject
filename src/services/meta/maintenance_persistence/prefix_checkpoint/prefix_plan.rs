impl MatrixMetaService {
    pub fn delete_by_prefix(
        &mut self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        validate_meta_key(prefix)?;
        if max_records == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prefix delete max_records must be positive".to_string(),
            ));
        }
        self.check_shard_fence(prefix)?;
        self.check_lease_fence(prefix, lease_owner, lease_epoch, now_ms)?;
        let keys: Vec<String> = self
            .records
            .iter()
            .filter(|(key, record)| key.starts_with(prefix) && !record.deleted)
            .map(|(key, _)| key.to_string())
            .take(max_records)
            .collect();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mutations: Vec<MetadataMutation> = keys
            .iter()
            .map(|key| MetadataMutation {
                key: key.clone().into(),
                value: String::new(),
                expected_version: self
                        .records
                        .get(key.as_str())
                        .map_or(0, |record| record.version),
                delete: true,
            })
            .collect();
        self.commit_batch(mutations, lease_owner, lease_epoch, now_ms)
    }

    pub fn prepare_prefix_delete_raft_entry(
        &self,
        prefix: &str,
        max_records: usize,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataPrefixDelete, ObjectError> {
        validate_meta_key(prefix)?;
        validate_idempotency_key(idempotency_key)?;
        if max_records == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prefix delete max_records must be positive".to_string(),
            ));
        }
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prefix delete prepare previous commit fence mismatch".to_string(),
            ));
        }
        self.check_shard_fence(prefix)?;
        self.check_lease_fence(prefix, lease_owner, lease_epoch, now_ms)?;
        let keys: Vec<String> = self
            .records
            .iter()
            .filter(|(key, record)| key.starts_with(prefix) && !record.deleted)
            .map(|(key, _)| key.to_string())
            .take(max_records)
            .collect();
        let mutations: Vec<MetadataMutation> = keys
            .iter()
            .map(|key| MetadataMutation {
                key: key.clone().into(),
                value: String::new(),
                expected_version: self
                        .records
                        .get(key.as_str())
                        .map_or(0, |record| record.version),
                delete: true,
            })
            .collect();
        let request_checksum = metadata_batch_request_checksum(
            &mutations,
            false,
            "",
            0,
            lease_owner,
            lease_epoch,
            now_ms,
        );
        if let Some(existing) = self.batch_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum || existing.owner_fenced {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prefix delete idempotency key reused with different request"
                        .to_string(),
                ));
            }
        }
        self.preflight_mutations(&mutations, lease_owner, lease_epoch, now_ms)?;
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let images: Vec<MetadataRecord> = mutations
            .iter()
            .map(|mutation| MetadataRecord {
                key: mutation.key.clone(),
                value: String::new(),
                version: mutation.expected_version.saturating_add(1),
                update_time_ms: now_ms,
                deleted: true,
            })
            .collect();
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataPrefixDelete {
            prefix: prefix.to_string(),
            max_records,
            mutations,
            images,
            lease_owner: lease_owner.map(str::to_string),
            lease_epoch,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            metadata_control_proof_now_ms: now_ms,
            expected_previous_commit_index,
            prepared_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_prefix_delete_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_prefix_delete_raft_entry_bounded(
        &self,
        prefix: &str,
        max_records: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataPrefixDelete, ObjectError> {
        validate_meta_key(prefix)?;
        validate_idempotency_key(idempotency_key)?;
        if max_records == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared prefix delete max_records must be positive".to_string(),
            ));
        }
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared prefix delete max_payload_bytes must be positive".to_string(),
            ));
        }
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prefix delete prepare previous commit fence mismatch".to_string(),
            ));
        }
        self.check_shard_fence(prefix)?;
        self.check_lease_fence(prefix, lease_owner, lease_epoch, now_ms)?;
        let mut payload_bytes = 0_u64;
        let mut mutations = Vec::new();
        for (key, record) in self
            .records
            .iter()
            .filter(|(key, record)| key.starts_with(prefix) && !record.deleted)
        {
            if mutations.len() >= max_records {
                break;
            }
            let next_payload = payload_bytes.checked_add(key.len() as u64).ok_or_else(|| {
                ObjectError::InvalidArgument(
                    "metadata prepared prefix delete payload bytes overflow".to_string(),
                )
            })?;
            if next_payload > max_payload_bytes {
                if mutations.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata prepared prefix delete first record exceeds max_payload_bytes"
                            .to_string(),
                    ));
                }
                break;
            }
            payload_bytes = next_payload;
            mutations.push(MetadataMutation {
                key: key.clone(),
                value: String::new(),
                expected_version: record.version,
                delete: true,
            });
        }
        let request_checksum = metadata_batch_request_checksum(
            &mutations,
            false,
            "",
            0,
            lease_owner,
            lease_epoch,
            now_ms,
        );
        if let Some(existing) = self.batch_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum || existing.owner_fenced {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prefix delete idempotency key reused with different request"
                        .to_string(),
                ));
            }
        }
        self.preflight_mutations(&mutations, lease_owner, lease_epoch, now_ms)?;
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let images: Vec<MetadataRecord> = mutations
            .iter()
            .map(|mutation| MetadataRecord {
                key: mutation.key.clone(),
                value: String::new(),
                version: mutation.expected_version.saturating_add(1),
                update_time_ms: now_ms,
                deleted: true,
            })
            .collect();
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataPrefixDelete {
            prefix: prefix.to_string(),
            max_records,
            mutations,
            images,
            lease_owner: lease_owner.map(str::to_string),
            lease_epoch,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            metadata_control_proof_now_ms: now_ms,
            expected_previous_commit_index,
            prepared_commit_index,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_prefix_delete_checksum(&prepared);
        Ok(prepared)
    }

}
