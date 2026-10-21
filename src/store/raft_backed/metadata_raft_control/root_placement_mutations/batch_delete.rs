// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_batch_delete_objects(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_batch_delete_objects_fenced(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            "",
        )
    }

    pub fn propose_batch_delete_objects_fenced(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "batch object delete",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::BatchDeleteObjects {
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
        })
    }

    pub fn propose_batch_delete_objects_fenced_with_read_proof_linearizable(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_batch_delete_objects_fenced_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_batch_delete_objects_with_route_proof_linearizable_fenced(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_batch_delete_objects_with_route_proof_linearizable_fenced_committed_at(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_batch_delete_objects_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "batch delete objects route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_batch_delete_objects_fenced_with_read_proof_linearizable_committed_at(
                requests,
                max_items,
                release_root_placements,
                update_time_ms,
                &expected_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            value: proven.value,
            proof: proven.proof,
            route_control_proof,
            expected_route_control_checksum: expected_checksum,
        })
    }

    pub fn propose_batch_delete_objects_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "batch delete objects",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_batch_delete_objects_fenced(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "batch delete objects post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_batch_delete_objects_from_meta_shard(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        min_read_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        for request in &requests {
            self.store.check_meta_shard_owner_serving(
                &meta_object_key(&ObjectId::new(&request.bucket, &request.key)),
                expected_shard_owner,
                expected_shard_epoch,
                update_time_ms,
            )?;
        }
        self.store.ensure_metadata_mutation_fence(
            "batch delete objects from meta shard",
            min_read_index,
            0,
        )?;
        self.propose_batch_delete_objects_fenced(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
        )
    }

    pub fn propose_batch_delete_objects_with_read_proof_from_meta_shard(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        for request in &requests {
            self.store.check_meta_shard_owner_serving(
                &meta_object_key(&ObjectId::new(&request.bucket, &request.key)),
                expected_shard_owner,
                expected_shard_epoch,
                update_time_ms,
            )?;
        }
        self.propose_batch_delete_objects_fenced_with_read_proof_linearizable_committed_at(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_batch_delete_objects_with_route_proof_from_meta_shard_fenced(
        &mut self,
        requests: Vec<ObjectDeleteRequest>,
        max_items: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        for request in &requests {
            self.store.check_meta_shard_owner_serving(
                &meta_object_key(&ObjectId::new(&request.bucket, &request.key)),
                expected_shard_owner,
                expected_shard_epoch,
                update_time_ms,
            )?;
        }
        self.propose_batch_delete_objects_with_route_proof_linearizable_fenced_committed_at(
            requests,
            max_items,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }
}
