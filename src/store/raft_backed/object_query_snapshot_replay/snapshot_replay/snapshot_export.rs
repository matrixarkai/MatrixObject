impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_snapshot_chunk(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<StoreSnapshotChunk, ObjectError> {
        self.store.export_snapshot_chunk(offset, max_payload_bytes)
    }

    pub fn export_snapshot_chunk_with_read_proof_linearizable(
        &self,
        offset: u64,
        max_payload_bytes: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<StoreSnapshotChunk>, ObjectError> {
        self.export_snapshot_chunk_with_read_proof_linearizable_committed_at(
            offset,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_snapshot_chunk_with_read_proof_linearizable_committed_at(
        &self,
        offset: u64,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<StoreSnapshotChunk>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "snapshot export",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let chunk = self.export_snapshot_chunk(offset, max_payload_bytes)?;
        Ok(proven_metadata_read(chunk, &readiness))
    }

    pub fn export_snapshot_chunk_stream_with_read_proof_linearizable<F>(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenMetadataRead<StoreSnapshotChunk>) -> Result<(), ObjectError>,
    {
        self.export_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_snapshot_chunk_stream_with_read_proof_linearizable_committed_at<F>(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenMetadataRead<StoreSnapshotChunk>) -> Result<(), ObjectError>,
    {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "snapshot export",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        self.store
            .export_snapshot_chunk_stream(max_payload_bytes, |chunk| {
                emit(proven_metadata_read(chunk, &readiness))
            })
    }

    pub fn export_snapshot_chunks_with_read_proof_linearizable(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
    ) -> Result<Vec<ProvenMetadataRead<StoreSnapshotChunk>>, ObjectError> {
        self.export_snapshot_chunks_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_snapshot_chunks_with_read_proof_linearizable_committed_at(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenMetadataRead<StoreSnapshotChunk>>, ObjectError> {
        let mut chunks = Vec::new();
        self.export_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
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

    pub fn export_snapshot_chunks_with_read_proof_report_linearizable(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
    ) -> Result<
        (
            Vec<ProvenMetadataRead<StoreSnapshotChunk>>,
            MetadataSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        self.export_snapshot_chunks_with_read_proof_report_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_snapshot_chunks_with_read_proof_report_linearizable_committed_at(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        (
            Vec<ProvenMetadataRead<StoreSnapshotChunk>>,
            MetadataSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        let mut chunks = Vec::new();
        let mut material =
            Vec::with_capacity(encoded_string_len("MXOS-RUST-METADATA-SNAPSHOT-READ-PROOFS1"));
        append_string(&mut material, "MXOS-RUST-METADATA-SNAPSHOT-READ-PROOFS1");
        let mut requested_read_index = 0;
        let mut consensus_read_index = 0;
        let mut metadata_commit_index = 0;
        let mut snapshot_last_applied_index = 0;
        let mut snapshot_term = 0;
        let mut full_snapshot_checksum = String::new();
        let mut total_payload_bytes = 0u64;
        let mut first_offset = 0u64;
        let mut last_end_offset = 0u64;
        self.export_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            min_read_index,
            min_commit_index,
            |proven| {
                if proven.proof.read_fence_read_index_lag != 0 {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "metadata snapshot proof has read-index lag {}",
                        proven.proof.read_fence_read_index_lag
                    )));
                }
                if proven.proof.read_fence_applied_lag != 0 {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "metadata snapshot proof has applied lag {}",
                        proven.proof.read_fence_applied_lag
                    )));
                }
                if chunks.is_empty() {
                    let chunk_capacity =
                        snapshot_chunk_vec_capacity(proven.value.total_bytes, max_payload_bytes);
                    requested_read_index = proven.proof.requested_read_index;
                    consensus_read_index = proven.proof.consensus_read_index;
                    metadata_commit_index = proven.proof.metadata_commit_index;
                    snapshot_last_applied_index = proven.value.last_applied_index;
                    snapshot_term = proven.value.term;
                    full_snapshot_checksum = proven.value.full_checksum.clone();
                    first_offset = proven.value.offset;
                    chunks.reserve(chunk_capacity);
                    material.reserve(metadata_snapshot_proof_material_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                } else if requested_read_index != proven.proof.requested_read_index
                    || consensus_read_index != proven.proof.consensus_read_index
                    || metadata_commit_index != proven.proof.metadata_commit_index
                    || snapshot_last_applied_index != proven.value.last_applied_index
                    || snapshot_term != proven.value.term
                    || full_snapshot_checksum != proven.value.full_checksum
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metadata snapshot proof stream source image changed".to_string(),
                    ));
                }
                let payload_len = proven.value.payload.len() as u64;
                total_payload_bytes = total_payload_bytes.saturating_add(payload_len);
                last_end_offset = proven.value.offset.saturating_add(payload_len);
                append_metadata_snapshot_proof_material(
                    &mut material,
                    proven.value.offset,
                    payload_len,
                    &proven.value.chunk_checksum,
                    proven.value.last_applied_index,
                    proven.value.term,
                    &proven.proof,
                );
                chunks.push(proven);
                Ok(())
            },
        )?;
        if chunks.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metadata snapshot proof stream is empty".to_string(),
            ));
        }
        let report = finish_metadata_snapshot_proof_stream_report(
            material,
            chunks.len() as u64,
            requested_read_index,
            consensus_read_index,
            metadata_commit_index,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            snapshot_last_applied_index,
            snapshot_term,
            full_snapshot_checksum,
        );
        Ok((chunks, report))
    }
}
