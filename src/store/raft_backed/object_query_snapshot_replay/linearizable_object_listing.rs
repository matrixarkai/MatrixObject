// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_objects_page_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_linearizable(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )
    }

    pub fn list_objects_page_linearizable_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_linearizable_list_page_proof_fenced(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            expected_object_list_page_proof_checksum,
        )
    }

    pub fn list_objects_hierarchy_page_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_linearizable_committed_at(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            0,
        )
    }

    pub fn list_objects_hierarchy_page_linearizable_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_linearizable_list_page_proof_fenced_committed_at(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            0,
            expected_object_list_page_proof_checksum,
        )
    }

    pub fn list_objects_page_linearizable_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_linearizable_committed_at(
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

    pub fn list_objects_page_linearizable_list_page_proof_fenced_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_linearizable_list_page_proof_fenced_committed_at(
            bucket,
            prefix,
            None,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            min_commit_index,
            expected_object_list_page_proof_checksum,
        )
    }

    pub fn list_objects_hierarchy_page_linearizable_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        let (mut page, readiness) = self.read_metadata_at_raft_position(
            "list object page",
            now_ms,
            min_commit_index,
            |options| {
                self.store.list_objects_hierarchy_page_with_metadata_read(
                    bucket,
                    prefix,
                    delimiter,
                    marker,
                    limit,
                    max_payload_bytes,
                    options,
                )
            },
        )?;
        page.read_proof = object_read_proof_from_readiness(&readiness);
        stamp_object_list_page_proof("ListObjects", &mut page, None)?;
        Ok((page, readiness))
    }

    pub fn list_objects_hierarchy_page_linearizable_list_page_proof_fenced_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        min_commit_index: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        let (mut page, readiness) = self.list_objects_hierarchy_page_linearizable_committed_at(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
            min_commit_index,
        )?;
        stamp_object_list_page_proof(
            "ListObjects",
            &mut page,
            Some(expected_object_list_page_proof_checksum.as_ref()),
        )?;
        Ok((page, readiness))
    }
}
