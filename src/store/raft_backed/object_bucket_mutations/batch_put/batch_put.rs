// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_batch_put_objects(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_batch_put_objects_fenced(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            "",
        )
    }

    pub fn propose_batch_put_objects_fenced(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        let min_read_index = requests
            .iter()
            .map(|request| request.options.min_read_index)
            .max()
            .unwrap_or(0);
        let min_commit_index = requests
            .iter()
            .map(|request| request.options.min_commit_index)
            .max()
            .unwrap_or(0);
        self.ensure_page_route_control_ready_at(
            "batch object put",
            expected_page_route_control_checksum.as_ref(),
            min_read_index,
            min_commit_index,
        )?;
        self.propose(ObjectCommand::BatchPutObjects {
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
        })
    }

    pub fn propose_batch_put_objects_fenced_with_read_proof_linearizable(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        let min_read_index = requests
            .iter()
            .map(|request| request.options.min_read_index)
            .max()
            .unwrap_or(0);
        let min_commit_index = requests
            .iter()
            .map(|request| request.options.min_commit_index)
            .max()
            .unwrap_or(0);
        self.propose_batch_put_objects_fenced_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(min_read_index),
            min_commit_index,
        )
    }

    pub fn propose_batch_put_objects_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "batch put objects",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_batch_put_objects_fenced(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "batch put objects post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_batch_put_objects_from_meta_shard(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.check_put_requests_meta_shard_owner_serving(
            &requests,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        let min_required_read_index =
            min_read_index.max(Self::put_requests_min_read_index(&requests));
        self.store.ensure_metadata_mutation_fence(
            "batch put objects from meta shard",
            min_required_read_index,
            Self::put_requests_min_commit_index(&requests),
        )?;
        self.propose_batch_put_objects_fenced(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_batch_put_objects_with_read_proof_from_meta_shard(
        &mut self,
        requests: Vec<ObjectPutRequest>,
        max_items: usize,
        max_payload_bytes: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.check_put_requests_meta_shard_owner_serving(
            &requests,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_batch_put_objects_fenced_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            max_payload_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
