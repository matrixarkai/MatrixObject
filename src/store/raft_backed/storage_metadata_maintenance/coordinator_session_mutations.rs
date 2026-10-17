impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_register_meta_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RegisterMetaCoordinatorSession {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            endpoint: endpoint.to_string(),
            ttl_ms,
            now_ms,
        })
    }

    pub fn propose_register_meta_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        endpoint: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RegisterMetaCoordinatorSessionIdempotent {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            endpoint: endpoint.to_string(),
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_renew_meta_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewMetaCoordinatorSession {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms,
            now_ms,
        })
    }

    pub fn propose_renew_meta_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewMetaCoordinatorSessionIdempotent {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_release_meta_coordinator_session(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ReleaseMetaCoordinatorSession {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            epoch,
        })
    }

    pub fn propose_release_meta_coordinator_session_idempotent(
        &mut self,
        session_id: &str,
        owner: &str,
        epoch: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ReleaseMetaCoordinatorSessionIdempotent {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            epoch,
            idempotency_key: idempotency_key.to_string(),
        })
    }

}
