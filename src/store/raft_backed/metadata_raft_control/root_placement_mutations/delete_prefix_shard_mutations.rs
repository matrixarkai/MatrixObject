impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_delete_objects_by_prefix_from_meta_shard(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(bucket, prefix)),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "delete objects by prefix from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_delete_objects_by_prefix_fenced(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_delete_objects_by_prefix_with_delete_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_delete_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(bucket, prefix)),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "delete objects by prefix from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_delete_objects_by_prefix_fenced_with_delete_proof(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_delete_batch_proof_checksum,
        )
    }

    pub fn propose_delete_objects_by_prefix_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(bucket, prefix)),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_delete_objects_by_prefix_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
