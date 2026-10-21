// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn commit_batch(
        &mut self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        let write_bytes = Self::metadata_write_payload_bytes(&mutations)?;
        self.admit_metadata_qos(mutations.len() as u64, write_bytes, now_ms)?;
        self.preflight_mutations(&mutations, lease_owner, lease_epoch, now_ms)?;
        let mut records = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            records.push(self.apply_mutation(mutation, now_ms)?);
        }
        Ok(records)
    }

    pub fn commit_batch_idempotent(
        &mut self,
        mutations: Vec<MetadataMutation>,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
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
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.records.clone(), receipt));
        }
        let first_commit_index = self.next_commit_index;
        let records = self.commit_batch(mutations, lease_owner, lease_epoch, now_ms)?;
        let last_commit_index = self.next_commit_index.saturating_sub(1);
        let receipt = MetadataBatchReceipt {
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            owner_fenced: false,
            expected_owner: String::new(),
            expected_epoch: 0,
            lease_owner: lease_owner.map(str::to_string),
            lease_epoch,
            now_ms,
            records: records.clone(),
            first_commit_index,
            last_commit_index,
            deduped: false,
        };
        self.batch_request_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok((records, receipt))
    }
}
