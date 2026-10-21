// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn install_raft_metadata_snapshot_chunks_with_read_proof<I>(
        &mut self,
        pages: I,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenMetadataRead<MetaSnapshotChunk>>,
    {
        let mut source_read_index: Option<u64> = None;
        let mut source_commit_index: Option<u64> = None;
        let mut progress = SnapshotInstallProgress::default();
        let mut saw_chunk = false;
        for proven in pages {
            let proof_status = validate_metadata_snapshot_install_read_proof(
                &proven.proof,
                "metaserver snapshot",
                min_read_index,
                min_commit_index,
            );
            if let Err(err) = proof_status {
                if saw_chunk {
                    let _ = self.store.abort_metadata_snapshot_install(None);
                }
                return Err(err);
            }
            if let (Some(read_index), Some(commit_index)) = (source_read_index, source_commit_index)
            {
                if read_index != proven.proof.consensus_read_index
                    || commit_index != proven.proof.metadata_commit_index
                {
                    if saw_chunk {
                        let _ = self.store.abort_metadata_snapshot_install(None);
                    }
                    return Err(ObjectError::PreconditionFailed(
                        "metaserver snapshot proof stream mixes metadata read fences".to_string(),
                    ));
                }
            } else {
                source_read_index = Some(proven.proof.consensus_read_index);
                source_commit_index = Some(proven.proof.metadata_commit_index);
            }
            saw_chunk = true;
            progress = match self
                .store
                .install_raft_metadata_snapshot_chunks(std::iter::once(proven.value), min_commit_index)
            {
                Ok(progress) => progress,
                Err(err) => {
                    let _ = self.store.abort_metadata_snapshot_install(None);
                    return Err(err);
                }
            };
            if !progress.stream_active && progress.total_bytes > 0 {
                return Ok(progress);
            }
        }
        if !saw_chunk {
            return Err(ObjectError::InvalidArgument(
                "metaserver snapshot proof stream is empty".to_string(),
            ));
        }
        Ok(progress)
    }

    pub fn install_raft_metadata_snapshot_chunks_with_read_proof_report<I>(
        &mut self,
        pages: I,
        expected_report: &MetaServerSnapshotProofStreamReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataSnapshotInstallReport, ObjectError>
    where
        I: IntoIterator<Item = ProvenMetadataRead<MetaSnapshotChunk>>,
    {
        let pages = pages.into_iter();
        let mut assembler = MetaSnapshotChunkAssembler::default();
        let mut assembled_snapshot = None;
        let mut material = Vec::new();
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
        let mut proof_count = 0u64;
        for proven in pages {
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
            if proof_count == 0 {
                requested_read_index = proven.proof.requested_read_index;
                consensus_read_index = proven.proof.consensus_read_index;
                metadata_commit_index = proven.proof.metadata_commit_index;
                snapshot_last_applied_index = proven.value.last_applied_index;
                full_snapshot_checksum = proven.value.full_checksum.clone();
                first_offset = proven.value.offset;
                let max_payload_bytes = proven.value.payload.len().max(1);
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
            proof_count = proof_count.saturating_add(1);
            if assembled_snapshot.is_some() {
                return Err(ObjectError::InvalidArgument(
                    "metaserver snapshot proof stream has chunks after eof".to_string(),
                ));
            }
            assembled_snapshot = assembler.push_chunk_with_checksum(proven.value)?;
        }
        if proof_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "metaserver snapshot proof stream is empty".to_string(),
            ));
        }
        let assembled_snapshot = assembled_snapshot.ok_or_else(|| {
            ObjectError::InvalidArgument(
                "metaserver snapshot proof stream did not complete snapshot image".to_string(),
            )
        })?;
        let actual_report = finish_metaserver_snapshot_proof_stream_report(
            material,
            proof_count,
            requested_read_index,
            consensus_read_index,
            metadata_commit_index,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            snapshot_last_applied_index,
            full_snapshot_checksum,
        );
        verify_metaserver_snapshot_proof_stream_report(
            expected_report,
            &actual_report,
            min_read_index,
            min_commit_index,
        )?;
        self.store
            .install_raft_metadata_snapshot_bytes_report(&assembled_snapshot.bytes, min_commit_index)
    }

    pub fn install_raft_metadata_snapshot_chunks_with_read_proof_source_report<I>(
        &mut self,
        pages: I,
        expected_report: &MetaServerSnapshotProofStreamReport,
        expected_source_proof: &ServiceSnapshotInstallSourceProofReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<MetadataSnapshotInstallReport, ObjectError>
    where
        I: IntoIterator<Item = ProvenMetadataRead<MetaSnapshotChunk>>,
    {
        let actual_source_proof = metaserver_snapshot_install_source_proof_report(expected_report);
        verify_service_snapshot_install_source_proof_report(
            expected_source_proof,
            &actual_source_proof,
        )?;
        self.install_raft_metadata_snapshot_chunks_with_read_proof_report(
            pages,
            expected_report,
            min_read_index,
            min_commit_index,
        )
    }
}
