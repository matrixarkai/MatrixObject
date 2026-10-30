// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {
    pub fn meta_coordinator_session(
        &self,
        session_id: &str,
    ) -> Result<CoordinatorSession, ObjectError> {
        self.meta_service.get_coordinator_session(session_id)
    }

    pub fn meta_coordinator_sessions(&self, owner: Option<&str>) -> Vec<CoordinatorSession> {
        self.meta_service.list_coordinator_sessions(owner)
    }

    pub fn list_meta_coordinator_session_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        session_id: Option<&str>,
        owner: Option<&str>,
        released: Option<bool>,
        max_payload_bytes: u64,
    ) -> Result<CoordinatorSessionReceiptPage, ObjectError> {
        self.meta_service.list_coordinator_session_receipts(
            marker,
            limit,
            operation,
            session_id,
            owner,
            released,
            max_payload_bytes,
        )
    }

    pub fn list_meta_coordinator_config_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        backend: Option<&str>,
    ) -> Result<MetadataCoordinatorConfigReceiptPage, ObjectError> {
        self.meta_service
            .list_coordinator_config_receipts(marker, limit, backend)
    }
}
