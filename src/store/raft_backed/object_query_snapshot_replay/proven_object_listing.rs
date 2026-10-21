// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_objects_page_with_read_proof_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectListPage, ObjectError> {
        self.list_objects_hierarchy_page_with_read_proof_linearizable(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )
    }

    pub fn list_objects_hierarchy_page_with_read_proof_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectListPage, ObjectError> {
        let (page, readiness) = self.list_objects_hierarchy_page_linearizable(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(ProvenObjectListPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_objects_page_with_read_proof_linearizable_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenObjectListPage, ObjectError> {
        self.list_objects_hierarchy_page_with_read_proof_linearizable_committed_at(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )
    }

    pub fn list_objects_hierarchy_page_with_read_proof_linearizable_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<ProvenObjectListPage, ObjectError> {
        let (page, readiness) = self.list_objects_hierarchy_page_linearizable_committed_at(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        Ok(ProvenObjectListPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }

    pub fn list_objects_page_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectListPage, ObjectError> {
        self.list_objects_hierarchy_page_with_read_proof_from_meta_shard(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn list_objects_hierarchy_page_with_read_proof_from_meta_shard(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<ProvenObjectListPage, ObjectError> {
        let (page, readiness) = self.list_objects_hierarchy_page_from_meta_shard(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(ProvenObjectListPage {
            page,
            proof: object_read_proof_from_readiness(&readiness),
        })
    }
}
