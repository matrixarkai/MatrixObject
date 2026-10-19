// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_copy_object(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CopyObject {
            source_bucket: source_bucket.to_string(),
            source_key: source_key.to_string(),
            source_version,
            destination_bucket: destination_bucket.to_string(),
            destination_key: destination_key.to_string(),
            content_type,
            update_time_ms,
            options,
        })
    }

    pub fn propose_copy_object_with_read_proof_linearizable(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_copy_object_with_read_proof_linearizable_committed_at(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            options.clone(),
            update_time_ms,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_copy_object_with_read_proof_linearizable_committed_at(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "copy object",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_copy_object(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            options,
            update_time_ms,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "copy object post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_copy_object_from_meta_shard(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
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
        self.store.ensure_metadata_mutation_fence(
            "copy object from meta shard",
            min_read_index.max(options.min_read_index),
            options.min_commit_index,
        )?;
        self.propose_copy_object(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            options,
            update_time_ms,
        )
    }

    pub fn propose_copy_object_with_read_proof_from_meta_shard(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        options: PutOptions,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
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
        self.propose_copy_object_with_read_proof_linearizable_committed_at(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            options,
            update_time_ms,
            min_read_index,
            min_commit_index,
        )
    }

}
