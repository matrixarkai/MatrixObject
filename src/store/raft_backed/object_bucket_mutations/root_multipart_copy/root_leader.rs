// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_campaign_placement_leader_idempotent(
        &mut self,
        node_id: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CampaignRootLeaderIdempotent {
            node_id: node_id.to_string(),
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_renew_placement_leader(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewRootLeader {
            node_id: node_id.to_string(),
            epoch,
            ttl_ms,
            now_ms,
        })
    }

    pub fn propose_renew_placement_leader_idempotent(
        &mut self,
        node_id: &str,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewRootLeaderIdempotent {
            node_id: node_id.to_string(),
            epoch,
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

}
