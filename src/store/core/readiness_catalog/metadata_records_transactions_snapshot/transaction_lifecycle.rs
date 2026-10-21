// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn begin_metadata_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        self.meta_service.begin_transaction_idempotent(
            transaction_id,
            owner,
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key,
        )
    }

    pub fn commit_metadata_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<Vec<MetadataRecord>, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let records = self
            .meta_service
            .commit_transaction(transaction_id, owner, now_ms)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_metadata_transaction_commit_journal_record(transaction_id, owner, now_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(records)
    }

    pub fn renew_metadata_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<MetadataTransaction, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let transaction =
            self.meta_service
                .renew_transaction(transaction_id, owner, ttl_ms, now_ms)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_metadata_transaction_renew_journal_record(
            transaction_id,
            owner,
            ttl_ms,
            now_ms,
        ) {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(transaction)
    }

    pub fn renew_metadata_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        self.meta_service.renew_transaction_idempotent(
            transaction_id,
            owner,
            ttl_ms,
            now_ms,
            idempotency_key,
        )
    }

    pub fn commit_metadata_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        self.meta_service.commit_transaction_idempotent(
            transaction_id,
            owner,
            now_ms,
            idempotency_key,
        )
    }

    pub fn abort_metadata_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<MetadataTransaction, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let transaction = self
            .meta_service
            .abort_transaction(transaction_id, owner, now_ms)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) =
            self.append_metadata_transaction_abort_journal_record(transaction_id, owner, now_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(transaction)
    }

    pub fn abort_metadata_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataTransactionReceipt, ObjectError> {
        self.meta_service.abort_transaction_idempotent(
            transaction_id,
            owner,
            now_ms,
            idempotency_key,
        )
    }

    pub fn expire_metadata_transactions(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<MetadataTransaction>, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let expired = self.meta_service.expire_transactions(now_ms);
        if !expired.is_empty() {
            self.invalidate_metadata_route_cache();
            if self.options.persistent_snapshot_flush_on_commit {
                if let Err(err) = self.flush_persistent_snapshot() {
                    if let Some(rollback) = rollback {
                        self.restore_object_mutation_rollback(rollback);
                    }
                    return Err(err);
                }
            }
            if let Err(err) = self.append_metadata_transaction_expiry_journal_record(now_ms) {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        Ok(expired)
    }

    pub fn gc_metadata_transactions(
        &mut self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
    ) -> MetadataTransactionGcReport {
        self.meta_service
            .gc_transactions(now_ms, max_transactions, min_age_ms)
    }

    pub fn list_metadata_transaction_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        transaction_id: Option<&str>,
        max_payload_bytes: u64,
    ) -> Result<MetadataTransactionReceiptPage, ObjectError> {
        self.meta_service.list_transaction_receipts(
            marker,
            limit,
            operation,
            transaction_id,
            max_payload_bytes,
        )
    }
}
