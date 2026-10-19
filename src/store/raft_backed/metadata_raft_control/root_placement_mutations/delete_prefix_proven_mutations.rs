impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_delete_objects_by_prefix_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_delete_objects_by_prefix_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_objects_by_prefix_fenced_with_read_delete_proof_linearizable(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_delete_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_delete_objects_by_prefix_fenced_with_read_delete_proof_linearizable_committed_at(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_delete_batch_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_objects_by_prefix_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_delete_objects_by_prefix_with_route_delete_proof_linearizable_fenced_committed_at(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            "",
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_objects_by_prefix_with_route_delete_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_delete_batch_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_delete_objects_by_prefix_with_route_delete_proof_linearizable_fenced_committed_at(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_delete_batch_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn propose_delete_objects_by_prefix_with_route_delete_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_delete_batch_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "delete objects by prefix route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven =
            self.propose_delete_objects_by_prefix_fenced_with_read_delete_proof_linearizable_committed_at(
                bucket,
                prefix,
                marker,
                max_objects,
                release_root_placements,
                update_time_ms,
                &expected_checksum,
                expected_delete_batch_proof_checksum,
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

    pub fn propose_delete_objects_by_prefix_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "delete objects by prefix",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_delete_objects_by_prefix_fenced(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "delete objects by prefix post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

    pub fn propose_delete_objects_by_prefix_fenced_with_read_delete_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        max_objects: usize,
        release_root_placements: bool,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_delete_batch_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "delete objects by prefix",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_delete_objects_by_prefix_fenced_with_delete_proof(
            bucket,
            prefix,
            marker,
            max_objects,
            release_root_placements,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_delete_batch_proof_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "delete objects by prefix post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }
}
