impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_objects_page_from_meta_shard(
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
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_from_meta_shard(
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

    pub fn list_objects_page_list_page_proof_fenced_from_meta_shard(
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
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.list_objects_hierarchy_page_list_page_proof_fenced_from_meta_shard(
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
            expected_object_list_page_proof_checksum,
        )
    }

    pub fn list_objects_hierarchy_page_from_meta_shard(
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
    ) -> Result<(ObjectListPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_objects_hierarchy_page_with_metadata_read(
            bucket,
            prefix,
            delimiter,
            marker,
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

    pub fn list_objects_hierarchy_page_list_page_proof_fenced_from_meta_shard(
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
                    min_read_index,
                    now_ms,
                    require_shard_owner_serving: true,
                    expected_shard_owner: Some(expected_shard_owner.to_string()),
                    expected_shard_epoch: Some(expected_shard_epoch),
                },
                Some(expected_object_list_page_proof_checksum.as_ref()),
            )
    }

}
