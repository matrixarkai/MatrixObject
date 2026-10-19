// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_multipart_uploads_page_linearizable(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_page_by_key_linearizable(
            bucket,
            None,
            None,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )
    }

    pub fn list_multipart_uploads_page_by_key_linearizable(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_hierarchy_page_by_key_linearizable(
            bucket,
            prefix,
            None,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            now_ms,
        )
    }

    pub fn list_multipart_uploads_hierarchy_page_by_key_linearizable(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_multipart_uploads_hierarchy_page_by_key_with_metadata_read(
                bucket,
                prefix,
                delimiter,
                key_marker,
                upload_id_marker,
                limit,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn list_multipart_uploads_page_from_meta_shard(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_page_by_key_from_meta_shard(
            bucket,
            None,
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

    pub fn list_multipart_uploads_page_by_key_from_meta_shard(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_hierarchy_page_by_key_from_meta_shard(
            bucket,
            prefix,
            None,
            key_marker,
            upload_id_marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn list_multipart_uploads_hierarchy_page_by_key_from_meta_shard(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(MultipartUploadPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_multipart_uploads_hierarchy_page_by_key_with_metadata_read(
                bucket,
                prefix,
                delimiter,
                key_marker,
                upload_id_marker,
                limit,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
            )
    }
}
