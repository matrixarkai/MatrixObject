// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_delete_objects_by_prefix_with_route_proof_from_meta_shard_fenced(
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
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_delete_objects_by_prefix_with_route_delete_proof_from_meta_shard_fenced(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            min_read_index,
            min_commit_index,
            expected_shard_owner,
            expected_shard_epoch,
            expected_page_route_control_checksum,
            "",
        )
    }

    pub fn propose_delete_objects_by_prefix_with_route_delete_proof_from_meta_shard_fenced(
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
        expected_delete_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(bucket, prefix)),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_delete_objects_by_prefix_with_route_delete_proof_linearizable_fenced_committed_at(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_delete_batch_proof_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
