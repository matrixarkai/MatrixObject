// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_expire_meta_coordinator_sessions(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_expire_meta_coordinator_sessions_including_fenced(now_ms, limit, false)
    }

    pub fn propose_expire_meta_coordinator_sessions_including_fenced(
        &mut self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ExpireMetaCoordinatorSessions {
            now_ms,
            limit,
            include_fenced,
        })
    }

    pub fn prepare_expire_meta_coordinator_sessions_raft_entry(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.prepare_expire_meta_coordinator_sessions_raft_entry_including_fenced(
            now_ms, limit, false,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_raft_entry_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.store
            .prepare_meta_coordinator_session_expiry_raft_entry_including_fenced(
                now_ms,
                limit,
                include_fenced,
                self.store.last_applied_index(),
            )
    }

    pub fn prepare_expire_meta_coordinator_sessions_raft_entry_bounded(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.prepare_expire_meta_coordinator_sessions_raft_entry_bounded_including_fenced(
            now_ms,
            limit,
            max_payload_bytes,
            false,
        )
    }

    pub fn prepare_expire_meta_coordinator_sessions_raft_entry_bounded_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        max_payload_bytes: u64,
        include_fenced: bool,
    ) -> Result<PreparedMetadataCoordinatorSessionExpiry, ObjectError> {
        self.store
            .prepare_meta_coordinator_session_expiry_raft_entry_bounded_including_fenced(
                now_ms,
                limit,
                max_payload_bytes,
                include_fenced,
                self.store.last_applied_index(),
            )
    }

}
