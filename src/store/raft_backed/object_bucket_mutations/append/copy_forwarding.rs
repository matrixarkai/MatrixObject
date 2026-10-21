// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_copy_object_with_root_placement_with_route_proof_from_meta_shard_fenced(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(source_bucket, source_key)),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.check_meta_shard_owner_serving(
            &meta_object_key(&ObjectId::new(destination_bucket, destination_key)),
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_copy_object_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            replica_count,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
