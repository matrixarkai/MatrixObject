// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_restore_object(
        &mut self,
        bucket: &str,
        key: &str,
        restore_until_ms: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RestoreObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            restore_until_ms,
            update_time_ms,
        })
    }

    pub fn propose_restore_object_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        restore_until_ms: u64,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.propose_restore_object_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            restore_until_ms,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_restore_object_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        restore_until_ms: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "restore object",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata =
            self.propose_restore_object(bucket, key, restore_until_ms, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "restore object post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectRestoreMutationReport {
            restore_until_ms: metadata.lock.restore_until_ms,
            metadata,
            route_control_proof: ChunkRouteControlProof::default(),
            expected_route_control_checksum: String::new(),
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_restore_object_version(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RestoreObjectVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
        })
    }

    pub fn propose_restore_object_version_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.propose_restore_object_version_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            version,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_restore_object_version_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "restore object version",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_restore_object_version(bucket, key, version, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "restore object version post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectRestoreMutationReport {
            restore_until_ms: metadata.lock.restore_until_ms,
            metadata,
            route_control_proof: ChunkRouteControlProof::default(),
            expected_route_control_checksum: String::new(),
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_restore_object_version_with_placement(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_restore_object_version_with_placement_fenced(
            bucket,
            key,
            version,
            replica_count,
            update_time_ms,
            "",
        )
    }

    pub fn propose_restore_object_version_with_placement_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "restore object version with root placement",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::RestoreObjectVersionWithRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version,
            update_time_ms,
            replica_count,
        })
    }

    pub fn propose_restore_object_version_with_placement_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.propose_restore_object_version_with_placement_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            version,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_restore_object_version_with_placement_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.propose_restore_object_version_with_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            version,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_restore_object_version_with_placement_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        let proven = self
            .propose_restore_object_version_with_placement_fenced_with_read_proof_linearizable_committed_at(
                bucket,
                key,
                version,
                replica_count,
                update_time_ms,
                &expected_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenRouteFencedMetadataRead {
            route_control_proof: proven.value.route_control_proof.clone(),
            expected_route_control_checksum: expected_checksum,
            value: proven.value,
            proof: proven.proof,
        })
    }

    pub fn propose_restore_object_version_with_placement_with_route_proof_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_restore_object_version_with_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            version,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_restore_object_version_with_placement_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        version: u64,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectRestoreMutationReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "restore object version with root placement",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let metadata = self.propose_restore_object_version_with_placement_fenced(
            bucket,
            key,
            version,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "restore object version with root placement post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectRestoreMutationReport {
            restore_until_ms: metadata.lock.restore_until_ms,
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            metadata,
        };
        Ok(proven_metadata_read(report, &readiness))
    }
}
