// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn prepare_batch_idempotent(
        &self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataBatch, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata batch prepare previous commit fence mismatch".to_string(),
            ));
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
                    "metadata batch idempotency key reused with different request".to_string(),
                ));
            }
            let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
                self.metadata_control_proof_digest_at(now_ms);
            let mut prepared = PreparedMetadataBatch {
                mutations,
                images: existing.records.clone(),
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
                prepared_commit_index: expected_previous_commit_index.saturating_add(1),
                raft_entry_checksum: String::new(),
            };
            prepared.raft_entry_checksum = prepared_metadata_batch_checksum(&prepared);
            return Ok(prepared);
        }
        self.preflight_mutations(&mutations, lease_owner, lease_epoch, now_ms)?;
        let mut seen_keys = BTreeSet::new();
        for mutation in &mutations {
            if !seen_keys.insert(mutation.key.clone()) {
                return Err(ObjectError::InvalidArgument(
                    "metadata prepared batch contains duplicate keys".to_string(),
                ));
            }
        }
        let mut projected_versions = BTreeMap::new();
        let prepared_commit_index = expected_previous_commit_index.saturating_add(1);
        let mut images = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            let base_version = self
                .record(mutation.key.as_str())
                .map_or(0, |record| record.version());
            let current_version = projected_versions
                .get(&mutation.key)
                .copied()
                .unwrap_or(base_version);
            projected_versions.insert(mutation.key.clone(), current_version.saturating_add(1));
            images.push(MetadataRecord::new(
                mutation.key.clone(),
                if mutation.delete {
                    String::new().into()
                } else {
                    mutation.record_value.clone()
                },
                current_version.saturating_add(1),
                now_ms,
                mutation.delete,
            ));
        }
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataBatch {
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
        prepared.raft_entry_checksum = prepared_metadata_batch_checksum(&prepared);
        Ok(prepared)
    }
}
