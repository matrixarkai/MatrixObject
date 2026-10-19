// SPDX-License-Identifier: Apache-2.0
impl MatrixObjectStore {
    pub fn acquire_metadata_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<MetadataLease, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let lease = self
            .meta_service
            .acquire_lease(namespace, owner, ttl_ms, now_ms)?;
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
            self.append_metadata_lease_acquire_journal_record(namespace, owner, ttl_ms, now_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(lease)
    }

    pub fn acquire_metadata_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataLeaseReceipt, ObjectError> {
        self.meta_service.acquire_lease_idempotent(
            namespace,
            owner,
            ttl_ms,
            now_ms,
            idempotency_key,
        )
    }

    pub fn renew_metadata_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<MetadataLease, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let lease = self
            .meta_service
            .renew_lease(namespace, owner, epoch, ttl_ms, now_ms)?;
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
            self.append_metadata_lease_renew_journal_record(namespace, owner, epoch, ttl_ms, now_ms)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(lease)
    }

    pub fn renew_metadata_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataLeaseReceipt, ObjectError> {
        self.meta_service.renew_lease_idempotent(
            namespace,
            owner,
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key,
        )
    }

    pub fn release_metadata_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<MetadataLease, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let lease = self.meta_service.release_lease(namespace, owner, epoch)?;
        self.invalidate_metadata_route_cache();
        if self.options.persistent_snapshot_flush_on_commit {
            if let Err(err) = self.flush_persistent_snapshot() {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        if let Err(err) = self.append_metadata_lease_release_journal_record(namespace, owner, epoch)
        {
            if let Some(rollback) = rollback {
                self.restore_object_mutation_rollback(rollback);
            }
            return Err(err);
        }
        Ok(lease)
    }

    pub fn release_metadata_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<MetadataLeaseReceipt, ObjectError> {
        self.meta_service
            .release_lease_idempotent(namespace, owner, epoch, idempotency_key)
    }

    pub fn expire_metadata_leases(
        &mut self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<MetadataLeaseExpiryReport, ObjectError> {
        let rollback = if self.persistent_journal_path.is_empty() {
            None
        } else {
            Some(self.capture_object_mutation_rollback())
        };
        let report = self.meta_service.expire_leases(prefix, now_ms, limit)?;
        if report.expired_leases > 0 {
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
                self.append_metadata_lease_expiry_journal_record(prefix, now_ms, limit)
            {
                if let Some(rollback) = rollback {
                    self.restore_object_mutation_rollback(rollback);
                }
                return Err(err);
            }
        }
        Ok(report)
    }

    pub fn prepare_metadata_lease_raft_entry(
        &self,
        operation: &str,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataLease, ObjectError> {
        self.meta_service.prepare_lease_raft_entry(
            operation,
            namespace,
            owner,
            epoch,
            ttl_ms,
            now_ms,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_lease(
        &mut self,
        prepared: PreparedMetadataLease,
    ) -> Result<MetadataLease, ObjectError> {
        self.meta_service.apply_prepared_lease(prepared)
    }

    pub fn prepare_metadata_lease_expiry_raft_entry(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataLeaseExpiry, ObjectError> {
        self.meta_service.prepare_lease_expiry_raft_entry(
            prefix,
            now_ms,
            limit,
            expected_previous_commit_index,
        )
    }

    pub fn prepare_metadata_lease_expiry_raft_entry_bounded(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataLeaseExpiry, ObjectError> {
        self.meta_service.prepare_lease_expiry_raft_entry_bounded(
            prefix,
            now_ms,
            limit,
            max_payload_bytes,
            expected_previous_commit_index,
        )
    }

    pub fn apply_prepared_metadata_lease_expiry(
        &mut self,
        prepared: PreparedMetadataLeaseExpiry,
    ) -> Result<MetadataLeaseExpiryReport, ObjectError> {
        self.meta_service.apply_prepared_lease_expiry(prepared)
    }

}
