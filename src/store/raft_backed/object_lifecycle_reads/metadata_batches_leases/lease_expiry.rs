// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_expire_metadata_leases(
        &mut self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ExpireMetaLeases {
            prefix: prefix.to_string(),
            now_ms,
            limit,
        })
    }

    pub fn prepare_expire_metadata_leases_raft_entry(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<PreparedMetadataLeaseExpiry, ObjectError> {
        self.store.prepare_metadata_lease_expiry_raft_entry(
            prefix,
            now_ms,
            limit,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_expire_metadata_leases_raft_entry_bounded(
        &self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<PreparedMetadataLeaseExpiry, ObjectError> {
        self.store.prepare_metadata_lease_expiry_raft_entry_bounded(
            prefix,
            now_ms,
            limit,
            max_payload_bytes,
            self.store.last_applied_index(),
        )
    }

    pub fn propose_prepared_metadata_lease_expiry(
        &mut self,
        prepared: PreparedMetadataLeaseExpiry,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease expiry requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedMetaLeaseExpiry {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedMetaLeaseExpiry { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared metadata lease expiry raft index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_expire_metadata_leases_prepared(
        &mut self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_expire_metadata_leases_raft_entry(prefix, now_ms, limit)?;
        self.propose_prepared_metadata_lease_expiry(prepared)
    }

    pub fn propose_expire_metadata_leases_prepared_bounded(
        &mut self,
        prefix: &str,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_expire_metadata_leases_raft_entry_bounded(
            prefix,
            now_ms,
            limit,
            max_payload_bytes,
        )?;
        self.propose_prepared_metadata_lease_expiry(prepared)
    }
}
