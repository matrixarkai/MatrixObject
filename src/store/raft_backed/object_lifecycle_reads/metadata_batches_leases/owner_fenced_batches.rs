// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_commit_owner_fenced_metadata_batch(
        &mut self,
        mutations: Vec<MetadataMutation>,
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitMetaOwnerFencedBatch {
            mutations,
            expected_owner: expected_owner.to_string(),
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
        })
    }

    pub fn propose_commit_owner_fenced_metadata_batch_idempotent(
        &mut self,
        mutations: Vec<MetadataMutation>,
        expected_owner: &str,
        expected_epoch: u64,
        lease_owner: Option<String>,
        lease_epoch: Option<u64>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitMetaOwnerFencedBatchIdempotent {
            mutations,
            expected_owner: expected_owner.to_string(),
            expected_epoch,
            lease_owner,
            lease_epoch,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

}
