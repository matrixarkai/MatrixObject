// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn commit_metadata_batch(
        &mut self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        self.meta_service
            .commit_batch(mutations, lease_owner, lease_epoch, now_ms)
    }

    pub fn commit_metadata_batch_idempotent(
        &mut self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        self.meta_service.commit_batch_idempotent(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
        )
    }

    pub fn prepare_metadata_batch_raft_entry(
        &self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataBatch, ObjectError> {
        self.meta_service.prepare_batch_idempotent(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_metadata_batch_raft_entry_bounded(
        &self,
        mutations: Vec<MetadataMutation>,
        max_mutations: usize,
        max_payload_bytes: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataBatch, ObjectError> {
        if max_mutations == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared batch max_mutations must be positive".to_string(),
            ));
        }
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared batch max_payload_bytes must be positive".to_string(),
            ));
        }
        if mutations.len() > max_mutations {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata prepared batch mutation count {} exceeds max_mutations {}",
                mutations.len(),
                max_mutations
            )));
        }
        let payload_bytes = MatrixMetaService::metadata_write_payload_bytes(&mutations)?;
        if payload_bytes > max_payload_bytes {
            return Err(ObjectError::PreconditionFailed(format!(
                "metadata prepared batch payload bytes {} exceeds max_payload_bytes {}",
                payload_bytes, max_payload_bytes
            )));
        }
        self.prepare_metadata_batch_raft_entry(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_object_metadata_commit_raft_entry(
        &self,
        current_key: &str,
        current_value: &str,
        expected_current_version: u64,
        bucket: &str,
        key: &str,
        object_value: &str,
        delete_object: bool,
        expected_object_version: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedObjectMetadataCommit, ObjectError> {
        validate_meta_key(current_key)?;
        validate_bucket(bucket)?;
        validate_key(key)?;
        validate_idempotency_key(idempotency_key)?;
        let object_id = ObjectId::new(bucket, key);
        let object_record_key = meta_object_key(&object_id);
        let object_mutation_value = if delete_object {
            String::new()
        } else {
            object_value.to_string()
        };
        let mutations = vec![
            MetadataMutation {
                key: current_key.to_string().into(),
                record_value: current_value.into(),
                expected_version: expected_current_version,
                delete: false,
            },
            MetadataMutation {
                key: object_record_key.clone().into(),
                record_value: object_mutation_value.clone().into(),
                expected_version: expected_object_version,
                delete: delete_object,
            },
        ];
        let batch = self.prepare_metadata_batch_raft_entry(
            mutations,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key,
            expected_previous_commit_index,
        )?;
        if batch.images.len() != 2
            || batch.images[0].key != current_key
            || batch.images[0].value != current_value.into()
            || batch.images[0].deleted()
            || batch.images[1].key != object_record_key
            || batch.images[1].value.as_text() != object_mutation_value.as_str()
            || batch.images[1].deleted() != delete_object
        {
            return Err(ObjectError::Corruption(
                "prepared object metadata images do not match request".to_string(),
            ));
        }
        let object_request_checksum = metadata_batch_request_checksum(
            &batch.mutations,
            false,
            "",
            0,
            lease_owner,
            lease_epoch,
            now_ms,
        );
        let mut prepared = PreparedObjectMetadataCommit {
            current_key: current_key.to_string(),
            object_id,
            current_value: current_value.to_string(),
            object_value: object_mutation_value,
            delete_object,
            expected_current_version,
            expected_object_version,
            batch,
            object_request_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_object_metadata_commit_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_metadata_batch(
        &mut self,
        prepared: PreparedMetadataBatch,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        self.meta_service.apply_prepared_batch(prepared)
    }

    pub fn apply_prepared_object_metadata_commit(
        &mut self,
        prepared: PreparedObjectMetadataCommit,
    ) -> Result<(MetadataRecord, MetadataRecord, MetadataBatchReceipt), ObjectError> {
        prepared.verify_prepared_object_metadata_commit()?;
        let object_record_key = meta_object_key(&prepared.object_id);
        if prepared.batch.mutations.len() != 2
            || prepared.batch.images.len() != 2
            || prepared.batch.mutations[0].key != prepared.current_key
            || prepared.batch.mutations[0].record_value.as_text() != prepared.current_value
            || prepared.batch.mutations[0].delete
            || prepared.batch.mutations[0].expected_version != prepared.expected_current_version
            || prepared.batch.mutations[1].key != object_record_key
            || prepared.batch.mutations[1].record_value.as_text() != prepared.object_value
            || prepared.batch.mutations[1].delete != prepared.delete_object
            || prepared.batch.mutations[1].expected_version != prepared.expected_object_version
        {
            return Err(ObjectError::Corruption(
                "prepared object metadata batch does not match object request".to_string(),
            ));
        }
        let (records, receipt) = self.apply_prepared_metadata_batch(prepared.batch)?;
        let current = records
            .iter()
            .find(|record| record.key == prepared.current_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "prepared object metadata current image missing".to_string(),
                )
            })?;
        let object = records
            .iter()
            .find(|record| record.key == object_record_key)
            .cloned()
            .ok_or_else(|| {
                ObjectError::Corruption("prepared object metadata object image missing".to_string())
            })?;
        Ok((current, object, receipt))
    }
}
