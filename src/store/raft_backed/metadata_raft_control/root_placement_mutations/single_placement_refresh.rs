impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_refresh_object_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RefreshObjectRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            replica_count,
            update_time_ms,
        })
    }

    pub fn propose_refresh_object_root_placement_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_root_read_index("refresh object root placement")?;
        self.propose_refresh_object_root_placement(bucket, key, replica_count, update_time_ms)
    }

    pub fn propose_refresh_object_root_placement_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
        update_time_ms: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_refresh_object_root_placement_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            replica_count,
            update_time_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_refresh_object_root_placement_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_refresh_object_root_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            replica_count,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_refresh_object_root_placement_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "refresh object root placement route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_refresh_object_root_placement_with_read_proof_linearizable_committed_at(
                bucket,
                key,
                replica_count,
                update_time_ms,
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

    pub fn propose_refresh_object_root_placement_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        replica_count: usize,
        update_time_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "refresh object root placement",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_refresh_object_root_placement_linearizable(
            bucket,
            key,
            replica_count,
            update_time_ms,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "refresh object root placement post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }
}
