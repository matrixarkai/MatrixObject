// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_batch_copy_objects(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_batch_copy_objects_fenced(
            requests,
            max_items,
            max_copied_bytes,
            update_time_ms,
            "",
        )
    }

    pub fn propose_batch_copy_objects_fenced(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "batch object copy",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::BatchCopyObjects {
            requests,
            max_items,
            max_copied_bytes,
            update_time_ms,
        })
    }

    pub fn propose_batch_copy_objects_fenced_with_read_proof_linearizable(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_batch_copy_objects_fenced_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            max_copied_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_batch_copy_objects_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "batch copy objects",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_batch_copy_objects_fenced(
            requests,
            max_items,
            max_copied_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "batch copy objects post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_batch_copy_objects_from_meta_shard(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
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
        let min_commit_index = requests
            .iter()
            .map(|request| request.options.min_commit_index)
            .max()
            .unwrap_or(0);
        let min_required_read_index = requests
            .iter()
            .map(|request| request.options.min_read_index)
            .max()
            .unwrap_or(0)
            .max(min_read_index);
        self.store.ensure_metadata_mutation_fence(
            "batch copy objects from meta shard",
            min_required_read_index,
            min_commit_index,
        )?;
        self.propose_batch_copy_objects_fenced(
            requests,
            max_items,
            max_copied_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_batch_copy_objects_with_read_proof_from_meta_shard(
        &mut self,
        requests: Vec<ObjectCopyRequest>,
        max_items: usize,
        max_copied_bytes: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
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
        self.propose_batch_copy_objects_fenced_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            max_copied_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
