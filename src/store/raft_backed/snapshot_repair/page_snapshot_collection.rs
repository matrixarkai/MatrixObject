impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_page_snapshot_chunks_with_read_proof_linearizable_fenced(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<Vec<ProvenStorageRead<ChunkServiceSnapshotChunk>>, ObjectError> {
        self.export_page_snapshot_chunks_with_read_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_page_snapshot_chunks_with_route_proof_linearizable_fenced(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<Vec<ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>, ObjectError>
    {
        self.export_page_snapshot_chunks_with_route_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_page_snapshot_chunks_with_route_proof_linearizable_fenced_committed_at(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>, ObjectError>
    {
        let mut chunks = Vec::new();
        self.export_page_snapshot_chunk_stream_with_route_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
            |chunk| {
                if chunks.is_empty() {
                    chunks.reserve(snapshot_chunk_vec_capacity(
                        chunk.value.total_bytes,
                        max_payload_bytes,
                    ));
                }
                chunks.push(chunk);
                Ok(())
            },
        )?;
        Ok(chunks)
    }

    pub fn export_page_snapshot_chunks_with_read_proof_linearizable_fenced_committed_at(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenStorageRead<ChunkServiceSnapshotChunk>>, ObjectError> {
        let mut chunks = Vec::new();
        self.export_page_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
            |chunk| {
                if chunks.is_empty() {
                    chunks.reserve(snapshot_chunk_vec_capacity(
                        chunk.value.total_bytes,
                        max_payload_bytes,
                    ));
                }
                chunks.push(chunk);
                Ok(())
            },
        )?;
        Ok(chunks)
    }

    pub fn export_page_snapshot_chunks_with_read_proof_report_linearizable_fenced(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<
        (
            Vec<ProvenStorageRead<ChunkServiceSnapshotChunk>>,
            StorageSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        self.export_page_snapshot_chunks_with_read_proof_report_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_page_snapshot_chunks_with_route_proof_report_linearizable_fenced(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: impl AsRef<str>,
    ) -> Result<
        (
            Vec<ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>,
            StorageSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        self.export_page_snapshot_chunks_with_route_proof_report_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_page_snapshot_chunks_with_route_proof_report_linearizable_fenced_committed_at(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        (
            Vec<ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>,
            StorageSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        let expected_checksum = expected_route_control_checksum.as_ref().to_string();
        let mut chunks = Vec::new();
        let mut material = Vec::with_capacity(
            encoded_string_len("MXOS-RUST-STORAGE-SNAPSHOT-READ-PROOFS1")
                .saturating_add(encoded_string_len("block-route")),
        );
        append_string(&mut material, "MXOS-RUST-STORAGE-SNAPSHOT-READ-PROOFS1");
        append_string(&mut material, "block-route");
        let mut source_control_checksum = String::new();
        let mut source_read_index = 0;
        let mut source_commit_index = 0;
        let mut full_snapshot_checksum = String::new();
        let mut total_payload_bytes = 0u64;
        let mut first_offset = 0u64;
        let mut last_end_offset = 0u64;
        self.export_page_snapshot_chunk_stream_with_route_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            &expected_checksum,
            min_read_index,
            min_commit_index,
            |proven| {
                if proven.proof.control_kind != "block-route" {
                    return Err(ObjectError::PreconditionFailed(
                        "block snapshot proof stream contains non block-route proof".to_string(),
                    ));
                }
                if chunks.is_empty() {
                    source_control_checksum = proven.proof.control_checksum.clone();
                    source_read_index = proven.proof.requested_storage_read_index;
                    source_commit_index = proven.proof.requested_storage_commit_index;
                    full_snapshot_checksum = proven.value.full_checksum.clone();
                    first_offset = proven.value.offset;
                    chunks.reserve(snapshot_chunk_vec_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                    material.reserve(storage_snapshot_proof_material_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                } else if source_control_checksum != proven.proof.control_checksum
                    || source_read_index != proven.proof.requested_storage_read_index
                    || source_commit_index != proven.proof.requested_storage_commit_index
                    || full_snapshot_checksum != proven.value.full_checksum
                {
                    return Err(ObjectError::PreconditionFailed(
                        "block snapshot proof stream source image changed".to_string(),
                    ));
                }
                let payload_len = proven.value.payload.len() as u64;
                total_payload_bytes = total_payload_bytes.saturating_add(payload_len);
                last_end_offset = proven.value.offset.saturating_add(payload_len);
                append_storage_snapshot_proof_material(
                    &mut material,
                    proven.value.offset,
                    payload_len,
                    &proven.value.page_checksum,
                    &proven.proof,
                );
                chunks.push(proven);
                Ok(())
            },
        )?;
        if chunks.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "block snapshot proof stream is empty".to_string(),
            ));
        }
        let report = finish_storage_snapshot_proof_stream_report(
            material,
            chunks.len() as u64,
            "block-route".to_string(),
            source_control_checksum,
            source_read_index,
            source_commit_index,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            full_snapshot_checksum,
        );
        Ok((chunks, report))
    }

    pub fn export_page_snapshot_chunks_with_read_proof_report_linearizable_fenced_committed_at(
        &self,
        max_payload_bytes: usize,
        expected_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        (
            Vec<ProvenStorageRead<ChunkServiceSnapshotChunk>>,
            StorageSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        let mut chunks = Vec::new();
        let mut material = Vec::with_capacity(
            encoded_string_len("MXOS-RUST-STORAGE-SNAPSHOT-READ-PROOFS1")
                .saturating_add(encoded_string_len("block-route")),
        );
        append_string(&mut material, "MXOS-RUST-STORAGE-SNAPSHOT-READ-PROOFS1");
        append_string(&mut material, "block-route");
        let mut source_control_checksum = String::new();
        let mut source_read_index = 0;
        let mut source_commit_index = 0;
        let mut full_snapshot_checksum = String::new();
        let mut total_payload_bytes = 0u64;
        let mut first_offset = 0u64;
        let mut last_end_offset = 0u64;
        self.export_page_snapshot_chunk_stream_with_read_proof_linearizable_fenced_committed_at(
            max_payload_bytes,
            expected_route_control_checksum,
            min_read_index,
            min_commit_index,
            |proven| {
                if proven.proof.control_kind != "block-route" {
                    return Err(ObjectError::PreconditionFailed(
                        "block snapshot proof stream contains non block-route proof".to_string(),
                    ));
                }
                if chunks.is_empty() {
                    source_control_checksum = proven.proof.control_checksum.clone();
                    source_read_index = proven.proof.requested_storage_read_index;
                    source_commit_index = proven.proof.requested_storage_commit_index;
                    full_snapshot_checksum = proven.value.full_checksum.clone();
                    first_offset = proven.value.offset;
                    chunks.reserve(snapshot_chunk_vec_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                    material.reserve(storage_snapshot_proof_material_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                } else if source_control_checksum != proven.proof.control_checksum
                    || source_read_index != proven.proof.requested_storage_read_index
                    || source_commit_index != proven.proof.requested_storage_commit_index
                    || full_snapshot_checksum != proven.value.full_checksum
                {
                    return Err(ObjectError::PreconditionFailed(
                        "block snapshot proof stream source image changed".to_string(),
                    ));
                }
                let payload_len = proven.value.payload.len() as u64;
                total_payload_bytes = total_payload_bytes.saturating_add(payload_len);
                last_end_offset = proven.value.offset.saturating_add(payload_len);
                append_storage_snapshot_proof_material(
                    &mut material,
                    proven.value.offset,
                    payload_len,
                    &proven.value.page_checksum,
                    &proven.proof,
                );
                chunks.push(proven);
                Ok(())
            },
        )?;
        if chunks.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "block snapshot proof stream is empty".to_string(),
            ));
        }
        let report = finish_storage_snapshot_proof_stream_report(
            material,
            chunks.len() as u64,
            "block-route".to_string(),
            source_control_checksum,
            source_read_index,
            source_commit_index,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            full_snapshot_checksum,
        );
        Ok((chunks, report))
    }
}
