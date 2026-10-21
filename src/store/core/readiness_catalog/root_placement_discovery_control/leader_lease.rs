// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixObjectStore {

    pub fn campaign_root_leader(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<RootLeaderLease, ObjectError> {
        self.meta_service.campaign_leader(node_id, ttl_ms, now_ms)
    }


    pub fn campaign_root_leader_idempotent(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(RootLeaderLease, RootLeaderLeaseReceipt), ObjectError> {
        self.meta_service
            .campaign_leader_idempotent(node_id, ttl_ms, now_ms, idempotency_key)
    }


    pub fn renew_root_leader(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<RootLeaderLease, ObjectError> {
        self.meta_service
            .renew_leader(node_id, epoch, ttl_ms, now_ms)
    }


    pub fn renew_root_leader_idempotent(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<(RootLeaderLease, RootLeaderLeaseReceipt), ObjectError> {
        self.meta_service
            .renew_leader_idempotent(node_id, epoch, ttl_ms, now_ms, idempotency_key)
    }


    pub fn root_leader_lease(&self) -> Option<RootLeaderLease> {
        self.meta_service.leader_lease()
    }


    pub fn list_root_leader_lease_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        operation: Option<&str>,
        leader_node_id: Option<&str>,
    ) -> Result<RootLeaderLeaseReceiptPage, ObjectError> {
        self.meta_service
            .list_leader_lease_receipts(marker, limit, operation, leader_node_id)
    }
}
