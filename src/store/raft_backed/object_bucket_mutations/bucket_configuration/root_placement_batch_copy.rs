// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_batch_copy_objects_with_placement_with_route_proof_from_meta_shard_fenced(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        for request in &requests {
            self.store.check_meta_shard_owner_serving(
                &meta_object_key(&ObjectId::new(&request.source_bucket, &request.source_key)),
                expected_shard_owner,
                expected_shard_epoch,
                update_time_ms,
            )?;
            self.store.check_meta_shard_owner_serving(
                &meta_object_key(&ObjectId::new(
                    &request.destination_bucket,
                    &request.destination_key,
                )),
                expected_shard_owner,
                expected_shard_epoch,
                update_time_ms,
            )?;
        }
        self.propose_batch_copy_objects_with_placement_with_route_proof_linearizable_fenced_committed_at(
            requests,
            max_items,
            max_copied_bytes,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
