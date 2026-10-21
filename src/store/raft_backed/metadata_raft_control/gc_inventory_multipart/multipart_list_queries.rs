// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_multipart_uploads_linearizable(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<(Vec<MultipartUpload>, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_by_key_linearizable(bucket, None, None, marker, limit, now_ms)
    }

    pub fn list_multipart_uploads_by_key_linearizable(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<(Vec<MultipartUpload>, MetadataRaftReadiness), ObjectError> {
        self.store.list_multipart_uploads_by_key_with_metadata_read(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn list_multipart_uploads_from_meta_shard(
        &self,
        bucket: Option<&str>,
        marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Vec<MultipartUpload>, MetadataRaftReadiness), ObjectError> {
        self.list_multipart_uploads_by_key_from_meta_shard(
            bucket,
            None,
            None,
            marker,
            limit,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )
    }

    pub fn list_multipart_uploads_by_key_from_meta_shard(
        &self,
        bucket: Option<&str>,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Vec<MultipartUpload>, MetadataRaftReadiness), ObjectError> {
        self.store.list_multipart_uploads_by_key_with_metadata_read(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
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
