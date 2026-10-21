// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_put_object(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        options: PutOptions,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_put_object_fenced(bucket, key, data, content_type, options, update_time_ms, "")
    }

    pub fn propose_put_object_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_put_options_page_route_control_ready(
            "put object",
            expected_page_route_control_checksum.as_ref(),
            &options,
        )?;
        self.propose(ObjectCommand::PutObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            data,
            content_type: content_type.into(),
            update_time_ms,
            options,
        })
    }

    pub fn propose_put_object_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_put_object_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            data,
            content_type,
            options.clone(),
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_put_object_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "put object",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_put_object_fenced(
            bucket,
            key,
            data,
            content_type,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "put object post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_put_object_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
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
            "put object from meta shard",
            min_read_index.max(options.min_read_index),
            options.min_commit_index,
        )?;
        self.propose_put_object_fenced(
            bucket,
            key,
            data,
            content_type,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_put_object_with_read_proof_from_meta_shard(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
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
        self.propose_put_object_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            data,
            content_type,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
