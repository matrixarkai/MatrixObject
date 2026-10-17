impl MatrixMetaService {
    pub fn expire_leases(
        &mut self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<MetadataLeaseExpiryReport, ObjectError> {
        validate_meta_key(prefix)?;
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease expiry limit must be positive".to_string(),
            ));
        }
        let mut report = MetadataLeaseExpiryReport {
            prefix: prefix.to_string(),
            limit,
            ..MetadataLeaseExpiryReport::default()
        };
        let mut expired_namespaces = Vec::new();
        for (namespace, lease) in self.leases.range(prefix.to_string()..) {
            if !namespace.starts_with(prefix) {
                break;
            }
            report.scanned_leases = report.scanned_leases.saturating_add(1);
            if lease.expire_time_ms > now_ms {
                report.skipped_leases = report.skipped_leases.saturating_add(1);
                continue;
            }
            expired_namespaces.push(namespace.clone());
            if expired_namespaces.len() >= limit {
                break;
            }
        }
        for namespace in expired_namespaces {
            if let Some(lease) = self.leases.remove(&namespace) {
                report.expired_leases = report.expired_leases.saturating_add(1);
                report.expired.push(lease);
            }
        }
        if report.expired_leases > 0 {
            self.next_commit_index = self.next_commit_index.saturating_add(1);
        }
        Ok(report)
    }

    fn plan_expired_leases(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<MetadataLeaseExpiryReport, ObjectError> {
        validate_meta_key(prefix)?;
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease expiry limit must be positive".to_string(),
            ));
        }
        let mut report = MetadataLeaseExpiryReport {
            prefix: prefix.to_string(),
            limit,
            ..MetadataLeaseExpiryReport::default()
        };
        for (namespace, lease) in self.leases.range(prefix.to_string()..) {
            if !namespace.starts_with(prefix) {
                break;
            }
            report.scanned_leases = report.scanned_leases.saturating_add(1);
            if lease.expire_time_ms > now_ms {
                report.skipped_leases = report.skipped_leases.saturating_add(1);
                continue;
            }
            report.expired_leases = report.expired_leases.saturating_add(1);
            report.expired.push(lease.clone());
            if report.expired.len() >= limit {
                break;
            }
        }
        Ok(report)
    }

    pub fn prepare_lease_expiry_raft_entry(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataLeaseExpiry, ObjectError> {
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata lease expiry prepare previous commit fence mismatch".to_string(),
            ));
        }
        let report = self.plan_expired_leases(prefix, now_ms, limit)?;
        let request_checksum = metadata_lease_expiry_request_checksum(prefix, now_ms, limit);
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataLeaseExpiry {
            prefix: prefix.to_string(),
            now_ms,
            limit,
            expired: report.expired,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_lease_expiry_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_lease_expiry_raft_entry_bounded(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedMetadataLeaseExpiry, ObjectError> {
        validate_meta_key(prefix)?;
        if limit == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease expiry limit must be positive".to_string(),
            ));
        }
        if max_payload_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata lease expiry max_payload_bytes must be positive".to_string(),
            ));
        }
        if expected_previous_commit_index != self.next_commit_index.saturating_sub(1) {
            return Err(ObjectError::PreconditionFailed(
                "metadata lease expiry prepare previous commit fence mismatch".to_string(),
            ));
        }
        let planned = self.plan_expired_leases(prefix, now_ms, limit)?;
        let mut payload_bytes = 0_u64;
        let mut expired = Vec::new();
        for lease in planned.expired {
            let next_payload = payload_bytes
                .checked_add(metadata_lease_payload_bytes(&lease)?)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument(
                        "metadata lease expiry payload bytes overflow".to_string(),
                    )
                })?;
            if next_payload > max_payload_bytes {
                if expired.is_empty() {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata lease expiry first lease exceeds max_payload_bytes".to_string(),
                    ));
                }
                break;
            }
            payload_bytes = next_payload;
            expired.push(lease);
        }
        let effective_limit = if expired.is_empty() {
            limit
        } else {
            expired.len()
        };
        let request_checksum =
            metadata_lease_expiry_request_checksum(prefix, now_ms, effective_limit);
        let (metadata_control_proof_checksum, metadata_control_proof_bytes) =
            self.metadata_control_proof_digest_at(now_ms);
        let mut prepared = PreparedMetadataLeaseExpiry {
            prefix: prefix.to_string(),
            now_ms,
            limit: effective_limit,
            expired,
            metadata_control_proof_format: "MOSRS-META-SERVICE-CONTROL3".to_string(),
            metadata_control_proof_checksum,
            metadata_control_proof_bytes,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_metadata_lease_expiry_checksum(&prepared);
        Ok(prepared)
    }
}
