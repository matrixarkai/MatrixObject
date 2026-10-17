impl MatrixMetaService {
    pub fn acquire_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<MetadataLease, ObjectError> {
        validate_meta_key(namespace)?;
        validate_meta_key(owner)?;
        self.admit_metadata_qos(
            1,
            namespace.len().saturating_add(owner.len()) as u64,
            now_ms,
        )?;
        self.check_namespace_lease_admission(namespace)?;
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease ttl must be positive".to_string(),
            ));
        }
        if let Some(existing) = self.leases.get(namespace) {
            if existing.expire_time_ms > now_ms && existing.owner != owner {
                return Err(ObjectError::PreconditionFailed(
                    "metadata lease is held by another owner".to_string(),
                ));
            }
        }
        let epoch = self
            .leases
            .get(namespace)
            .map_or(1, |lease| lease.epoch.saturating_add(1));
        let lease = MetadataLease {
            namespace: namespace.to_string(),
            owner: owner.to_string(),
            epoch,
            expire_time_ms: now_ms.saturating_add(ttl_ms),
            ttl_ms,
        };
        self.leases.insert(namespace.to_string(), lease.clone());
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(lease)
    }

    pub fn acquire_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataLeaseReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "acquire";
        let request_checksum =
            metadata_lease_request_checksum(operation, namespace, owner, 0, ttl_ms, now_ms);
        if let Some(existing) = self.lease_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata lease idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let lease = self.acquire_lease(namespace, owner, ttl_ms, now_ms)?;
        let receipt = MetadataLeaseReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            lease,
            released: false,
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.lease_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }

    pub fn renew_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<MetadataLease, ObjectError> {
        validate_meta_key(namespace)?;
        validate_meta_key(owner)?;
        if ttl_ms == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease ttl must be positive".to_string(),
            ));
        }
        let lease = self
            .leases
            .get_mut(namespace)
            .ok_or_else(|| ObjectError::NotFound("metadata lease missing".to_string()))?;
        if lease.owner != owner || lease.epoch != epoch || lease.expire_time_ms <= now_ms {
            return Err(ObjectError::PreconditionFailed(
                "metadata lease renew fence mismatch".to_string(),
            ));
        }
        lease.expire_time_ms = now_ms.saturating_add(ttl_ms);
        lease.ttl_ms = ttl_ms;
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(lease.clone())
    }

    pub fn renew_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<MetadataLeaseReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "renew";
        let request_checksum =
            metadata_lease_request_checksum(operation, namespace, owner, epoch, ttl_ms, now_ms);
        if let Some(existing) = self.lease_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata lease idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let lease = self.renew_lease(namespace, owner, epoch, ttl_ms, now_ms)?;
        let receipt = MetadataLeaseReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            lease,
            released: false,
            commit_index: self.next_commit_index.saturating_sub(1),
            update_time_ms: now_ms,
            deduped: false,
        };
        self.lease_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }

    pub fn release_lease(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<MetadataLease, ObjectError> {
        validate_meta_key(namespace)?;
        validate_meta_key(owner)?;
        let lease = self
            .leases
            .get(namespace)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound("metadata lease missing".to_string()))?;
        if lease.owner != owner || lease.epoch != epoch {
            return Err(ObjectError::PreconditionFailed(
                "metadata lease release fence mismatch".to_string(),
            ));
        }
        self.leases.remove(namespace);
        self.next_commit_index = self.next_commit_index.saturating_add(1);
        Ok(lease)
    }

    pub fn release_lease_idempotent(
        &mut self,
        namespace: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<MetadataLeaseReceipt, ObjectError> {
        validate_idempotency_key(idempotency_key)?;
        let operation = "release";
        let request_checksum =
            metadata_lease_request_checksum(operation, namespace, owner, epoch, 0, 0);
        if let Some(existing) = self.lease_receipts.get(idempotency_key) {
            if existing.operation != operation || existing.request_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "metadata lease idempotency key reused with different request".to_string(),
                ));
            }
            let mut receipt = existing.clone();
            receipt.deduped = true;
            return Ok(receipt);
        }
        let lease = self.release_lease(namespace, owner, epoch)?;
        let receipt = MetadataLeaseReceipt {
            idempotency_key: idempotency_key.to_string(),
            operation: operation.to_string(),
            request_checksum,
            update_time_ms: lease.expire_time_ms,
            lease,
            released: true,
            commit_index: self.next_commit_index.saturating_sub(1),
            deduped: false,
        };
        self.lease_receipts
            .insert(idempotency_key.to_string(), receipt.clone());
        Ok(receipt)
    }
}
