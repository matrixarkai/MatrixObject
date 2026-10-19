impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_metadata_snapshot_chunk_with_read_proof_linearizable(
        &self,
        offset: u64,
        max_payload_bytes: usize,
        now_ms: u64,
    ) -> Result<ProvenMetadataRead<MetaSnapshotChunk>, ObjectError> {
        self.export_metadata_snapshot_chunk_with_read_proof_linearizable_committed_at(
            offset,
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_metadata_snapshot_chunk_with_read_proof_linearizable_committed_at(
        &self,
        offset: u64,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenMetadataRead<MetaSnapshotChunk>, ObjectError> {
        let readiness = self.ensure_metadata_read_index_committed_at(
            "metaserver snapshot export",
            now_ms,
            min_read_index,
            min_commit_index,
        )?;
        let chunk = self
            .store
            .export_metadata_snapshot_chunk(offset, max_payload_bytes)?;
        Ok(proven_metadata_read(chunk, &readiness))
    }

    pub fn export_metadata_snapshot_chunk_stream_with_read_proof_linearizable<F>(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenMetadataRead<MetaSnapshotChunk>) -> Result<(), ObjectError>,
    {
        self.export_metadata_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_metadata_snapshot_chunk_stream_with_read_proof_linearizable_committed_at<F>(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenMetadataRead<MetaSnapshotChunk>) -> Result<(), ObjectError>,
    {
        let mut offset = 0;
        loop {
            let proven = self
                .export_metadata_snapshot_chunk_with_read_proof_linearizable_committed_at(
                    offset,
                    max_payload_bytes,
                    now_ms,
                    min_read_index,
                    min_commit_index,
                )?;
            let eof = proven.value.eof;
            let payload_len = proven.value.payload.len() as u64;
            emit(proven)?;
            if eof {
                return Ok(());
            }
            if payload_len == 0 {
                return Err(ObjectError::Corruption(
                    "metaserver snapshot export made no progress".to_string(),
                ));
            }
            offset = offset.saturating_add(payload_len);
        }
    }

    pub fn export_metadata_snapshot_chunks_with_read_proof_linearizable(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
    ) -> Result<Vec<ProvenMetadataRead<MetaSnapshotChunk>>, ObjectError> {
        self.export_metadata_snapshot_chunks_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_metadata_snapshot_chunks_with_read_proof_linearizable_committed_at(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenMetadataRead<MetaSnapshotChunk>>, ObjectError> {
        let mut pages = Vec::new();
        self.export_metadata_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            min_read_index,
            min_commit_index,
            |chunk| {
                if pages.is_empty() {
                    pages.reserve(snapshot_chunk_vec_capacity(
                        chunk.value.total_bytes,
                        max_payload_bytes,
                    ));
                }
                pages.push(chunk);
                Ok(())
            },
        )?;
        Ok(pages)
    }

    pub fn export_metadata_snapshot_chunks_with_read_proof_report_linearizable(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
    ) -> Result<
        (
            Vec<ProvenMetadataRead<MetaSnapshotChunk>>,
            MetaServerSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        self.export_metadata_snapshot_chunks_with_read_proof_report_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_metadata_snapshot_chunks_with_read_proof_report_linearizable_committed_at(
        &self,
        max_payload_bytes: usize,
        now_ms: u64,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        (
            Vec<ProvenMetadataRead<MetaSnapshotChunk>>,
            MetaServerSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        let mut pages = Vec::new();
        let mut material = Vec::with_capacity(encoded_string_len(
            "MXOS-RUST-METASERVER-SNAPSHOT-READ-PROOFS1",
        ));
        append_string(
            &mut material,
            "MXOS-RUST-METASERVER-SNAPSHOT-READ-PROOFS1",
        );
        let mut requested_read_index = 0;
        let mut consensus_read_index = 0;
        let mut metadata_commit_index = 0;
        let mut snapshot_last_applied_index = 0;
        let mut full_snapshot_checksum = String::new();
        let mut total_payload_bytes = 0u64;
        let mut first_offset = 0u64;
        let mut last_end_offset = 0u64;
        self.export_metadata_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            now_ms,
            min_read_index,
            min_commit_index,
            |proven| {
                if proven.proof.read_fence_read_index_lag != 0 {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "metaserver snapshot proof has read-index lag {}",
                        proven.proof.read_fence_read_index_lag
                    )));
                }
                if proven.proof.read_fence_applied_lag != 0 {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "metaserver snapshot proof has applied lag {}",
                        proven.proof.read_fence_applied_lag
                    )));
                }
                if pages.is_empty() {
                    requested_read_index = proven.proof.requested_read_index;
                    consensus_read_index = proven.proof.consensus_read_index;
                    metadata_commit_index = proven.proof.metadata_commit_index;
                    snapshot_last_applied_index = proven.value.last_applied_index;
                    full_snapshot_checksum = proven.value.full_checksum.clone();
                    first_offset = proven.value.offset;
                    pages.reserve(snapshot_chunk_vec_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                    material.reserve(metaserver_snapshot_proof_material_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                } else if requested_read_index != proven.proof.requested_read_index
                    || consensus_read_index != proven.proof.consensus_read_index
                    || metadata_commit_index != proven.proof.metadata_commit_index
                    || snapshot_last_applied_index != proven.value.last_applied_index
                    || full_snapshot_checksum != proven.value.full_checksum
                {
                    return Err(ObjectError::PreconditionFailed(
                        "metaserver snapshot proof stream source image changed".to_string(),
                    ));
                }
                let payload_len = proven.value.payload.len() as u64;
                total_payload_bytes = total_payload_bytes.saturating_add(payload_len);
                last_end_offset = proven.value.offset.saturating_add(payload_len);
                append_metaserver_snapshot_proof_material(
                    &mut material,
                    proven.value.offset,
                    payload_len,
                    &proven.value.page_checksum,
                    proven.value.last_applied_index,
                    &proven.proof,
                );
                pages.push(proven);
                Ok(())
            },
        )?;
        if pages.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "metaserver snapshot proof stream is empty".to_string(),
            ));
        }
        let report = finish_metaserver_snapshot_proof_stream_report(
            material,
            pages.len() as u64,
            requested_read_index,
            consensus_read_index,
            metadata_commit_index,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            snapshot_last_applied_index,
            full_snapshot_checksum,
        );
        Ok((pages, report))
    }
}
