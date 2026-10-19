// SPDX-License-Identifier: Apache-2.0
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_append_with_root_placement(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_append_with_root_placement_fenced(
            bucket,
            key,
            data,
            content_type,
            replica_count,
            options,
            update_time_ms,
            "",
        )
    }

    pub fn propose_append_with_root_placement_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_put_options_page_route_control_ready(
            "append object with root placement",
            expected_page_route_control_checksum.as_ref(),
            &options,
        )?;
        self.propose(ObjectCommand::AppendObjectWithRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            data,
            content_type: content_type.into(),
            update_time_ms,
            options,
            replica_count,
        })
    }

    pub fn propose_append_with_root_placement_fenced_with_read_proof_linearizable(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_append_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
            bucket,
            key,
            data,
            content_type,
            replica_count,
            options.clone(),
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_append_with_root_placement_with_route_proof_linearizable_fenced(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_append_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
            bucket,
            key,
            data,
            content_type,
            replica_count,
            options.clone(),
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_append_with_root_placement_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_put_options_page_route_control_ready(
            "append object with root placement route proof",
            &expected_checksum,
            &options,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_append_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
                bucket,
                key,
                data,
                content_type,
                replica_count,
                options,
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

    pub fn propose_append_with_root_placement_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        content_type: impl Into<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "append object with root placement",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_append_with_root_placement_fenced(
            bucket,
            key,
            data,
            content_type,
            replica_count,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "append object with root placement post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }
}
