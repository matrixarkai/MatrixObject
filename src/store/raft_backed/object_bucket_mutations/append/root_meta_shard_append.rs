// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_append_with_placement_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.store.ensure_metadata_mutation_fence(
            "append object with root placement from meta shard",
            min_read_index.max(options.min_read_index),
            options.min_commit_index,
        )?;
        self.propose_append_with_placement_fenced(
            bucket,
            key,
            data,
            content_type,
            replica_count,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_append_with_placement_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_append_with_placement_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            data,
            content_type,
            replica_count,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_append_with_placement_with_route_proof_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_append_with_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            data,
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
