impl MatrixMetaService {
    pub fn apply_prepared_lease_expiry(
        &mut self,
        prepared: PreparedMetadataLeaseExpiry,
    ) -> Result<MetadataLeaseExpiryReport, ObjectError> {
        validate_meta_key(&prepared.prefix)?;
        if prepared.limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease expiry limit must be positive".to_string(),
            ));
        }
        if prepared.expired.len() > prepared.limit {
            return Err(ObjectError::InvalidArgument(
                "metadata prepared lease expiry exceeds limit".to_string(),
            ));
        }
        let expected_checksum =
            prepared_metadata_lease_expiry_checksum(&PreparedMetadataLeaseExpiry {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "metadata prepared lease expiry raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != metadata_lease_expiry_request_checksum(
                &prepared.prefix,
                prepared.now_ms,
                prepared.limit,
            )
        {
            return Err(ObjectError::Corruption(
                "metadata prepared lease expiry request checksum mismatch".to_string(),
            ));
        }
        let mut seen = BTreeSet::new();
        for lease in &prepared.expired {
            validate_meta_key(&lease.namespace)?;
            validate_meta_key(&lease.owner)?;
            if !lease.namespace.starts_with(&prepared.prefix) {
                return Err(ObjectError::Corruption(
                    "metadata prepared lease expiry namespace outside prefix".to_string(),
                ));
            }
            if lease.expire_time_ms > prepared.now_ms {
                return Err(ObjectError::Corruption(
                    "metadata prepared lease expiry contains live lease".to_string(),
                ));
            }
            if !seen.insert(lease.namespace.clone()) {
                return Err(ObjectError::Corruption(
                    "metadata prepared lease expiry contains duplicate namespace".to_string(),
                ));
            }
        }
        if self.prepared_lease_expiry_already_applied(&prepared) {
            return Ok(MetadataLeaseExpiryReport {
                scanned_leases: prepared.expired.len() as u64,
                expired_leases: prepared.expired.len() as u64,
                skipped_leases: 0,
                prefix: prepared.prefix,
                limit: prepared.limit,
                expired: prepared.expired,
            });
        }
        if prepared.expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared lease expiry previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index != self.next_commit_index {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared lease expiry commit index must be contiguous".to_string(),
            ));
        }
        self.check_expected_metadata_control_proof(
            &prepared.metadata_control_proof_checksum,
            prepared.now_ms,
        )?;
        for lease in &prepared.expired {
            let current = self.leases.get(&lease.namespace).ok_or_else(|| {
                ObjectError::PreconditionFailed(
                    "metadata prepared lease expiry lease is missing".to_string(),
                )
            })?;
            if current != lease {
                return Err(ObjectError::PreconditionFailed(
                    "metadata prepared lease expiry lease image mismatch".to_string(),
                ));
            }
        }
        let planned =
            self.plan_expired_leases(&prepared.prefix, prepared.now_ms, prepared.limit)?;
        if planned.expired != prepared.expired {
            return Err(ObjectError::PreconditionFailed(
                "metadata prepared lease expiry set changed before apply".to_string(),
            ));
        }
        for lease in &prepared.expired {
            self.leases.remove(&lease.namespace);
        }
        self.next_commit_index = prepared.prepared_commit_index.saturating_add(1);
        Ok(MetadataLeaseExpiryReport {
            scanned_leases: planned.scanned_leases,
            expired_leases: prepared.expired.len() as u64,
            skipped_leases: planned.skipped_leases,
            prefix: prepared.prefix,
            limit: prepared.limit,
            expired: prepared.expired,
        })
    }
}
