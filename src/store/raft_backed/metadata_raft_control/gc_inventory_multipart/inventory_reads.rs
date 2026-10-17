impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn list_object_inventory_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<(ObjectInventoryPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_object_inventory_with_metadata_read(
            bucket,
            prefix,
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

    pub fn list_object_inventory_linearizable_list_page_proof_fenced(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
        expected_object_list_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(ObjectInventoryPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_object_inventory_with_metadata_read_list_page_proof_fenced(
                bucket,
                prefix,
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

    pub fn list_object_inventory_from_meta_shard(
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
    ) -> Result<(ObjectInventoryPage, MetadataRaftReadiness), ObjectError> {
        self.store.list_object_inventory_with_metadata_read(
            bucket,
            prefix,
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

    pub fn list_object_inventory_list_page_proof_fenced_from_meta_shard(
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
    ) -> Result<(ObjectInventoryPage, MetadataRaftReadiness), ObjectError> {
        self.store
            .list_object_inventory_with_metadata_read_list_page_proof_fenced(
                bucket,
                prefix,
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

    pub fn list_object_inventory_with_read_proof_linearizable(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<&str>,
        limit: usize,
        max_payload_bytes: u64,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectInventoryPage>, ObjectError> {
        let (page, readiness) = self.list_object_inventory_linearizable(
            bucket,
            prefix,
            marker,
            limit,
            max_payload_bytes,
            now_ms,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }

    pub fn list_object_inventory_with_read_proof_from_meta_shard(
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
    ) -> Result<ProvenMetadataRead<ObjectInventoryPage>, ObjectError> {
        let (page, readiness) = self.list_object_inventory_from_meta_shard(
            bucket,
            prefix,
            marker,
            limit,
            max_payload_bytes,
            min_read_index,
            expected_shard_owner,
            expected_shard_epoch,
            now_ms,
        )?;
        Ok(proven_metadata_read(page, &readiness))
    }
}
