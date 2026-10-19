// SPDX-License-Identifier: Apache-2.0
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
        self.store.list_objects_hierarchy_page_with_metadata_read(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
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
        self.store
            .list_objects_hierarchy_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                prefix,
                delimiter,
                marker,
                limit,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
                Some(expected_object_list_page_proof_checksum.as_ref()),
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
        let options =
            self.metadata_read_options_committed_at("list object page", now_ms, min_commit_index)?;
        self.store.list_objects_hierarchy_page_with_metadata_read(
            bucket,
            prefix,
            delimiter,
            marker,
            limit,
            max_payload_bytes,
            options,
        )
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
        let options =
            self.metadata_read_options_committed_at("list object page", now_ms, min_commit_index)?;
        self.store
            .list_objects_hierarchy_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                prefix,
                delimiter,
                marker,
                limit,
                max_payload_bytes,
                options,
                Some(expected_object_list_page_proof_checksum.as_ref()),
            )
    }

}
