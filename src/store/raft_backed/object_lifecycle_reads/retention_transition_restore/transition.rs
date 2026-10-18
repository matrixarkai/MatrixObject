impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_transition_object(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::TransitionObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            storage_class: storage_class.into(),
            update_time_ms,
        })
    }

    pub fn propose_transition_object_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        self.propose_transition_object_fenced_with_read_proof_linearizable(
            bucket,
            key,
            storage_class,
            update_time_ms,
            "",
        )
    }

    pub fn propose_transition_object_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        self.propose_transition_object_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            storage_class,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_transition_object_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        self.propose_transition_object_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            storage_class,
            update_time_ms,
            "",
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_transition_object_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "transition object",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        self.ensure_page_route_control_ready_at(
            "transition object",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.page_service.route_control_proof();
        let metadata =
            self.propose_transition_object(bucket, key, storage_class, update_time_ms)?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "transition object post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectTransitionMutationReport {
            storage_class: metadata.storage_class.to_string(),
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            metadata,
        };
        Ok(proven_metadata_read(report, &readiness))
    }

    pub fn propose_transition_object_with_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_transition_object_with_root_placement_fenced(
            bucket,
            key,
            storage_class,
            replica_count,
            update_time_ms,
            "",
        )
    }

    pub fn propose_transition_object_with_root_placement_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "transition object with root placement",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::TransitionObjectWithRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            storage_class: storage_class.into(),
            update_time_ms,
            replica_count,
        })
    }

    pub fn propose_transition_object_with_root_placement_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        self.propose_transition_object_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            storage_class,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_transition_object_with_root_placement_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        self.propose_transition_object_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            storage_class,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_transition_object_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        let proven = self
            .propose_transition_object_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
                bucket,
                key,
                storage_class,
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

    pub fn propose_transition_object_with_root_placement_with_route_proof_from_meta_shard_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        expected_shard_owner: &str,
        expected_shard_epoch: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        self.check_object_meta_shard_owner_serving(
            bucket,
            key,
            expected_shard_owner,
            expected_shard_epoch,
            update_time_ms,
        )?;
        self.propose_transition_object_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            storage_class,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn propose_transition_object_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        storage_class: impl Into<String>,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectTransitionMutationReport>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_metadata_read_index_committed_at(
            "transition object with root placement",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let route_control_proof = self.store.page_service.route_control_proof();
        let metadata = self.propose_transition_object_with_root_placement_fenced(
            bucket,
            key,
            storage_class,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "transition object with root placement post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        let report = ObjectTransitionMutationReport {
            storage_class: metadata.storage_class.to_string(),
            route_control_proof,
            expected_route_control_checksum: expected_page_route_control_checksum.to_string(),
            metadata,
        };
        Ok(proven_metadata_read(report, &readiness))
    }
}
