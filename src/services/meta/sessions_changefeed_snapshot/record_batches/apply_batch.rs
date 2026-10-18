impl MatrixMetaService {
    pub fn apply_prepared_batch(
        &mut self,
        prepared: PreparedMetadataBatch,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        validate_idempotency_key(&prepared.idempotency_key)?;
        if prepared.images.len() != prepared.mutations.len() {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared batch image count mismatch".to_string(),
            ));
        }
        let expected_checksum = prepared_metadata_batch_checksum(&PreparedMetadataBatch {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared batch raft entry checksum mismatch".to_string(),
            ));
        }
        let request_checksum = metadata_batch_request_checksum(
            &prepared.mutations,
            false,
            "",
            0,
            prepared.lease_owner.as_deref(),
            prepared.lease_epoch,
            prepared.now_ms,
        );
        if request_checksum != prepared.request_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared batch request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared batch commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self.batch_request_receipts.get(&prepared.idempotency_key) {
            if existing.request_checksum != prepared.request_checksum || existing.owner_fenced {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared batch idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.records.clone(), receipt));
        }
        let write_bytes = Self::metadata_write_payload_bytes(&prepared.mutations)?;
        self.admit_metadata_qos(
            prepared.mutations.len() as u64,
            write_bytes,
            prepared.now_ms,
        )?;
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared batch previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared batch commit index must be contiguous".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.metadata_control_proof_now_ms,
        )?;
        self.preflight_mutations(
            &prepared.mutations,
            prepared.lease_owner.as_deref(),
            prepared.lease_epoch,
            prepared.now_ms,
        )?;
        let mut seen_keys = BTreeSet::new();
        for (mutation, image) in prepared.mutations.iter().zip(prepared.images.iter()) {
            if !seen_keys.insert(mutation.key.clone()) {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared batch contains duplicate keys".to_string(),
                ));
            }
            let current_version = self
                .records
                .get(&mutation.key)
                .map_or(0, |record| record.version);
            if image.key != mutation.key
                || image.deleted != mutation.delete
                || image.version != current_version.saturating_add(1)
                || (mutation.delete && !image.value.is_empty())
                || (!mutation.delete && image.value != mutation.value)
                || image.update_time_ms != prepared.now_ms
            {
                return Err(ObjectError::Corruption(
                    "metadata prepared batch image does not match mutation".to_string(),
                ));
            }
        }
        for image in &prepared.images {
            self.file_record(image.key.clone(), image.clone());
            self.append_metadata_change(MetadataChange {
                sequence: self.next_change_sequence,
                commit_index: prepared.prepared_commit_index,
                key: image.key.clone(),
                version: image.version,
                deleted: image.deleted,
                update_time_ms: image.update_time_ms,
            });
            self.next_change_sequence = self.next_change_sequence.saturating_add(1);
        }
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        let receipt = MetadataBatchReceipt {
            idempotency_key: prepared.idempotency_key.clone(),
            request_checksum: prepared.request_checksum.clone(),
            owner_fenced: false,
            expected_owner: String::new(),
            expected_epoch: 0,
            lease_owner: prepared.lease_owner.clone(),
            lease_epoch: prepared.lease_epoch,
            now_ms: prepared.now_ms,
            records: prepared.images.clone(),
            first_commit_index: prepared.prepared_commit_index,
            last_commit_index: prepared.prepared_commit_index,
            deduped: false,
        };
        self.batch_request_receipts
            .insert(prepared.idempotency_key.clone(), receipt.clone());
        Ok((prepared.images, receipt))
    }
}
