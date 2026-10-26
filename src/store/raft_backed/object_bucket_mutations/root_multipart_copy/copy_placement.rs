// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_copy_object_with_placement(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose_copy_object_with_placement_fenced(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            replica_count,
            options,
            update_time_ms,
            "",
        )
    }

    pub fn propose_copy_object_with_placement_fenced(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.ensure_page_route_control_ready(
            "copy object with root placement",
            expected_page_route_control_checksum.as_ref(),
        )?;
        self.propose(ObjectCommand::CopyObjectWithRootPlacement {
            source_bucket: source_bucket.to_string(),
            source_key: source_key.to_string(),
            source_version,
            destination_bucket: destination_bucket.to_string(),
            destination_key: destination_key.to_string(),
            content_type,
            update_time_ms,
            options,
            replica_count,
        })
    }

    pub fn propose_copy_object_with_placement_fenced_with_read_proof_linearizable(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_copy_object_with_placement_fenced_with_read_proof_linearizable_committed_at(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            replica_count,
            options.clone(),
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_copy_object_with_placement_with_route_proof_linearizable_fenced(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        self.propose_copy_object_with_placement_with_route_proof_linearizable_fenced_committed_at(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            replica_count,
            options.clone(),
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index().max(options.min_read_index),
            options.min_commit_index,
        )
    }

    pub fn propose_copy_object_with_placement_with_route_proof_linearizable_fenced_committed_at(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRouteFencedMetadataRead<ObjectMetadata>, ObjectError> {
        let expected_checksum = expected_page_route_control_checksum.as_ref().to_string();
        self.ensure_page_route_control_ready(
            "copy object with root placement route proof",
            &expected_checksum,
        )?;
        let route_control_proof = self.store.chunk_service.route_control_proof();
        let proven = self
            .propose_copy_object_with_placement_fenced_with_read_proof_linearizable_committed_at(
                source_bucket,
                source_key,
                source_version,
                destination_bucket,
                destination_key,
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

    pub fn propose_copy_object_with_placement_fenced_with_read_proof_linearizable_committed_at(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        source_version: Option<u64>,
        destination_bucket: &str,
        destination_key: &str,
        content_type: Option<String>,
        replica_count: usize,
        options: PutOptions,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<ObjectMetadata>, ObjectError> {
        self.ensure_metadata_read_index_committed_at(
            "copy object with root placement",
            update_time_ms,
            min_read_index,
            min_commit_index,
        )?;
        let metadata = self.propose_copy_object_with_placement_fenced(
            source_bucket,
            source_key,
            source_version,
            destination_bucket,
            destination_key,
            content_type,
            replica_count,
            options,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let readiness = self.ensure_metadata_read_index_committed_at(
            "copy object with root placement post-commit",
            update_time_ms,
            self.raft.commit_index(),
            min_commit_index,
        )?;
        Ok(proven_metadata_read(metadata, &readiness))
    }

}
