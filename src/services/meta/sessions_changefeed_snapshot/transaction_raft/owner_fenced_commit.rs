// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixMetaService {
    pub fn commit_owner_fenced_batch(
        &mut self,
        mutations: Vec<MetadataMutation>,
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        validate_meta_key(expected_owner)?;
        if expected_epoch == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata owner-fenced batch epoch must be positive".to_string(),
            ));
        }
        self.preflight_owner_fenced_mutations(
            &mutations,
            expected_owner,
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
        )?;
        let mut records = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            records.push(self.apply_mutation(mutation, now_ms)?);
        }
        Ok(records)
    }

    pub fn commit_owner_fenced_batch_idempotent(
        &mut self,
        mutations: Vec<MetadataMutation>,
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<&str>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(Vec<MetadataRecord>, MetadataBatchReceipt), ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let request_checksum = metadata_batch_request_checksum(
            &mutations,
            true,
            expected_owner,
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
        );
        if let Some(existing) = self.batch_request_receipts.get(idempotency_key) {
            if existing.request_checksum != request_checksum
                || !existing.owner_fenced
                || existing.expected_owner != expected_owner
                || existing.expected_epoch != expected_epoch
            {
                return Err(ObjectError::PreconditionFailed(
                    "metadata owner-fenced batch idempotency key reused with different request"
                        .to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok((receipt.records.clone(), receipt));
        }
        let first_commit_index = self.next_commit_index;
        let records = self.commit_owner_fenced_batch(
            mutations,
            expected_owner,
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
        )?;
        let last_commit_index = self.next_commit_index.saturating_sub(1);
        let receipt = MetadataBatchReceipt {
            idempotency_key: idempotency_key.to_string(),
            request_checksum,
            owner_fenced: true,
            expected_owner: expected_owner.to_string(),
            expected_epoch,
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
