impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_object_versions_linearizable(
        &self,
        bucket: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        self.store.list_object_versions_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index: self.raft.commit_index(),
                now_ms,
                ..MetadataReadOptions::default()
            },
        )
    }

    pub fn list_object_versions_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(Vec<ObjectMetadata>, MetadataRaftReadiness), ObjectError> {
        self.store.list_object_versions_with_metadata_read(
            bucket,
            key,
            MetadataReadOptions {
                min_read_index,
                now_ms,
                require_shard_owner_serving: true,
                expected_shard_owner: Some(expected_shard_owner.to_string()),
                expected_shard_epoch: Some(expected_shard_epoch),
            },
        )
    }

    pub fn list_object_versions_page_linearizable(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_object_versions_page_with_metadata_read(
            bucket,
            key,
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

    pub fn list_object_versions_page_linearizable_list_page_proof_fenced(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_object_versions_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                key,
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

    pub fn list_object_versions_page_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(ObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_object_versions_page_with_metadata_read(
            bucket,
            key,
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

    pub fn list_object_versions_page_list_page_proof_fenced_from_meta_shard(
        &self,
        bucket: &str,
        key: &str,
        marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_object_versions_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                key,
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

    pub fn list_bucket_object_versions_page_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(BucketObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_bucket_object_versions_page_with_metadata_read(
                bucket,
                prefix,
                delimiter,
                key_marker,
                version_marker,
                limit,
                max_payload_bytes,
                MetadataReadOptions {
                    min_read_index: self.raft.commit_index(),
                    now_ms,
                    ..MetadataReadOptions::default()
                },
            )
    }

    pub fn list_bucket_object_versions_page_linearizable_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(BucketObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_bucket_object_versions_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                prefix,
                delimiter,
                key_marker,
                version_marker,
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

    pub fn list_bucket_object_versions_page_from_meta_shard(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
    ) -> Result<(BucketObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_bucket_object_versions_page_with_metadata_read(
                bucket,
                prefix,
                delimiter,
                key_marker,
                version_marker,
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

    pub fn list_bucket_object_versions_page_list_page_proof_fenced_from_meta_shard(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_marker: Option<u64>,
        limit: usize,
        max_payload_bytes: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(BucketObjectVersionPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_bucket_object_versions_page_with_metadata_read_list_page_proof_fenced(
                bucket,
                prefix,
                delimiter,
                key_marker,
                version_marker,
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
