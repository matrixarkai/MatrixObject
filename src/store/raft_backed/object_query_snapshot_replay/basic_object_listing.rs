// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_objects_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_with_metadata_read(
            bucket,
            prefix,
            marker,
            limit,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn list_objects_linearizable_committed_at(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        now_ms: u64,
        min_commit_index: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        let options =
            self.metadata_read_options_committed_at("list objects", now_ms, min_commit_index)?;
        self.store
            .list_objects_with_metadata_read(bucket, prefix, marker, limit, options)
    }

    pub fn list_objects_from_meta_shard(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_with_metadata_read(
            bucket,
            prefix,
            marker,
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
