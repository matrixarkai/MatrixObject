impl MatrixMetaService {
    pub fn apply_prepared_prefix_delete(
        &mut self,
        prepared: PreparedMetadataPrefixDelete,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        validate_meta_key(&prepared.prefix)?;
        validate_idempotency_key(&prepared.idempotency_key)?;
        if prepared.max_records == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared prefix delete max_records must be positive".to_string(),
            ));
        }
        let expected_checksum =
            prepared_metadata_prefix_delete_checksum(&PreparedMetadataPrefixDelete {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared prefix delete raft entry checksum mismatch".to_string(),
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
                "metadata prepared prefix delete request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared prefix delete commit index must be adjacent".to_string(),
            ));
        }
        if let Some(existing) = self.batch_request_receipts.get(&prepared.idempotency_key) {
            if existing.request_checksum != prepared.request_checksum || existing.owner_fenced {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared prefix delete idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.records.clone(), receipt));
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared prefix delete previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared prefix delete commit index must be contiguous".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.metadata_control_proof_now_ms,
        )?;
        let current = self.prepare_prefix_delete_raft_entry(
            &prepared.prefix,
            prepared.max_records,
            prepared.lease_owner.as_deref(),
            prepared.lease_epoch,
            prepared.now_ms,
            &prepared.idempotency_key,
            prepared.expected_previous_commit_index,
        )?;
        if current.mutations != prepared.mutations || current.images != prepared.images {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared prefix delete no longer matches current state".to_string(),
            ));
        }
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(prepared.now_ms);
        self.apply_prepared_batch(PreparedMetadataBatch {
            mutations: prepared.mutations,
            images: prepared.images,
            lease_owner: prepared.lease_owner,
            lease_epoch: prepared.lease_epoch,
            now_ms: prepared.now_ms,
            idempotency_key: prepared.idempotency_key,
            request_checksum: prepared.request_checksum,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum: metadata_control_proof_checksum.clone(),
            metadata_control_proof_bytes,
            metadata_control_proof_now_ms: prepared.now_ms,
            expected_previous_commit_index: prepared.expected_previous_commit_index,
            prepared_commit_index: prepared.prepared_commit_index,
            raft_entry_checksum: prepared_metadata_batch_checksum(&PreparedMetadataBatch {
                mutations: current.mutations,
                images: current.images,
                lease_owner: current.lease_owner,
                lease_epoch: current.lease_epoch,
                now_ms: current.now_ms,
                idempotency_key: current.idempotency_key,
                request_checksum: current.request_checksum,
                metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
                metadata_control_proof_checksum,
                metadata_control_proof_bytes,
                metadata_control_proof_now_ms: prepared.now_ms,
                expected_previous_commit_index: current.expected_previous_commit_index,
                prepared_commit_index: current.prepared_commit_index,
                raft_entry_checksum: String::new(),
            }),
        })
    }

}
