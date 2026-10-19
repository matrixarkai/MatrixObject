impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn install_page_snapshot_chunks_with_read_proof<I>(
        &mut self,
        chunks: I,
        expected_source_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenStorageRead<ChunkServiceSnapshotChunk>>,
    {
        let mut source_control_checksum: Option<String> = None;
        let mut progress = SnapshotInstallProgress::default();
        let mut saw_chunk = false;
        for proven in chunks {
            let proof_status = Self::validate_storage_snapshot_install_proof(
                &proven.proof,
                "block-route",
                expected_source_route_control_checksum,
                min_read_index,
                min_commit_index,
            );
            if let Err(err) = proof_status {
                if saw_chunk {
                    let _ = self.store.abort_page_snapshot_install(None);
                }
                return Err(err);
            }
            if let Some(observed) = &source_control_checksum {
                if observed != &proven.proof.control_checksum {
                    if saw_chunk {
                        let _ = self.store.abort_page_snapshot_install(None);
                    }
                    return Err(ObjectError::PreconditionFailed(
                        "block snapshot proof stream mixes route-control images".to_string(),
                    ));
                }
            } else {
                source_control_checksum = Some(proven.proof.control_checksum.clone());
            }
            saw_chunk = true;
            progress = match self
                .store
                .install_page_snapshot_chunks(std::iter::once(proven.value))
            {
                Ok(progress) => progress,
                Err(err) => {
                    let _ = self.store.abort_page_snapshot_install(None);
                    return Err(err);
                }
            };
            if !progress.stream_active && progress.total_bytes > 0 {
                return Ok(progress);
            }
        }
        if !saw_chunk {
            return Err(ObjectError::InvalidArgument(
                "block snapshot proof stream is empty".to_string(),
            ));
        }
        Ok(progress)
    }

    pub fn install_page_snapshot_chunks_with_read_proof_report<I>(
        &mut self,
        chunks: I,
        expected_report: &StorageSnapshotProofStreamReport,
        expected_source_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenStorageRead<ChunkServiceSnapshotChunk>>,
    {
        let chunks = chunks.into_iter();
        let mut assembler = ChunkServiceSnapshotAssembler::default();
        let mut assembled_snapshot = None;
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
        let mut proof_count = 0u64;
        for proven in chunks {
            Self::validate_storage_snapshot_install_proof(
                &proven.proof,
                "block-route",
                expected_source_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
            if proven.proof.control_kind != "block-route" {
                return Err(ObjectError::PreconditionFailed(
                    "block snapshot proof stream contains non block-route proof".to_string(),
                ));
            }
            if proof_count == 0 {
                let max_payload_bytes = proven.value.payload.len().max(1);
                source_control_checksum = proven.proof.control_checksum.clone();
                source_read_index = proven.proof.requested_storage_read_index;
                source_commit_index = proven.proof.requested_storage_commit_index;
                full_snapshot_checksum = proven.value.full_checksum.clone();
                first_offset = proven.value.offset;
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
                &proven.value.chunk_checksum,
                &proven.proof,
            );
            proof_count = proof_count.saturating_add(1);
            if assembled_snapshot.is_some() {
                return Err(ObjectError::InvalidArgument(
                    "block snapshot proof stream has chunks after eof".to_string(),
                ));
            }
            assembled_snapshot = assembler.push_chunk_with_checksum(proven.value)?;
        }
        if proof_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "block snapshot proof stream is empty".to_string(),
            ));
        }
        let assembled_snapshot = assembled_snapshot.ok_or_else(|| {
            ObjectError::InvalidArgument(
                "block snapshot proof stream did not complete snapshot image".to_string(),
            )
        })?;
        let actual_report = finish_storage_snapshot_proof_stream_report(
            material,
            proof_count,
            "block-route".to_string(),
            source_control_checksum,
            source_read_index,
            source_commit_index,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            full_snapshot_checksum,
        );
        verify_storage_snapshot_proof_stream_report(
            expected_report,
            &actual_report,
            "block-route",
            expected_source_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        self.store
            .chunk_service
            .install_snapshot_bytes_in_place(&assembled_snapshot.bytes)?;
        let snapshot_bytes = assembled_snapshot.bytes.len() as u64;
        let snapshot_checksum = assembled_snapshot.full_checksum.clone();
        drop(assembled_snapshot);
        Ok(snapshot_install_progress(
            true,
            false,
            snapshot_bytes,
            snapshot_bytes,
            snapshot_checksum,
            0,
            0,
        ))
    }

    pub fn install_page_snapshot_chunks_with_read_proof_source_report<I>(
        &mut self,
        chunks: I,
        expected_report: &StorageSnapshotProofStreamReport,
        expected_source_proof: &StorageSnapshotInstallSourceProofReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenStorageRead<ChunkServiceSnapshotChunk>>,
    {
        let actual_source_proof = storage_snapshot_install_source_proof_report(expected_report);
        verify_storage_snapshot_install_source_proof_report(
            expected_source_proof,
            &actual_source_proof,
            "block-route",
        )?;
        self.install_page_snapshot_chunks_with_read_proof_report(
            chunks,
            expected_report,
            Some(&expected_source_proof.expected_source_control_checksum),
            min_read_index,
            min_commit_index,
        )
    }

    pub fn install_page_snapshot_chunks_with_route_proof<I>(
        &mut self,
        chunks: I,
        expected_source_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>,
    {
        self.install_page_snapshot_chunks_with_read_proof(
            chunks
                .into_iter()
                .map(ProvenPageRouteFencedStorageRead::into_storage_read),
            expected_source_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn install_page_snapshot_chunks_with_route_proof_report<I>(
        &mut self,
        chunks: I,
        expected_report: &StorageSnapshotProofStreamReport,
        expected_source_route_control_checksum: Option<&str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>,
    {
        self.install_page_snapshot_chunks_with_read_proof_report(
            chunks
                .into_iter()
                .map(ProvenPageRouteFencedStorageRead::into_storage_read),
            expected_report,
            expected_source_route_control_checksum,
            min_read_index,
            min_commit_index,
        )
    }

    pub fn install_page_snapshot_chunks_with_route_proof_source_report<I>(
        &mut self,
        chunks: I,
        expected_report: &StorageSnapshotProofStreamReport,
        expected_source_proof: &StorageSnapshotInstallSourceProofReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenPageRouteFencedStorageRead<ChunkServiceSnapshotChunk>>,
    {
        self.install_page_snapshot_chunks_with_read_proof_source_report(
            chunks
                .into_iter()
                .map(ProvenPageRouteFencedStorageRead::into_storage_read),
            expected_report,
            expected_source_proof,
            min_read_index,
            min_commit_index,
        )
    }

}
