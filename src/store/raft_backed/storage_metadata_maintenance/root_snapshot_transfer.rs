impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn export_root_snapshot_chunk_with_read_proof_linearizable(
        &self,
        offset: u64,
        max_payload_bytes: usize,
    ) -> Result<ProvenRootRead<RootSnapshotChunk>, ObjectError> {
        self.export_root_snapshot_chunk_with_read_proof_linearizable_committed_at(
            offset,
            max_payload_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_root_snapshot_chunk_with_read_proof_linearizable_committed_at(
        &self,
        offset: u64,
        max_payload_bytes: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenRootRead<RootSnapshotChunk>, ObjectError> {
        let proof = self.ensure_root_read_index_committed_at(
            "root snapshot export",
            min_read_index,
            min_commit_index,
        )?;
        let value = self
            .store
            .meta_service
            .export_placement_snapshot_chunk(offset, max_payload_bytes)?;
        Ok(ProvenRootRead { value, proof })
    }

    pub fn export_root_snapshot_chunk_stream_with_read_proof_linearizable<F>(
        &self,
        max_payload_bytes: usize,
        emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenRootRead<RootSnapshotChunk>) -> Result<(), ObjectError>,
    {
        self.export_root_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            self.raft.commit_index(),
            0,
            emit,
        )
    }

    pub fn export_root_snapshot_chunk_stream_with_read_proof_linearizable_committed_at<F>(
        &self,
        max_payload_bytes: usize,
        min_read_index: u64,
        min_commit_index: u64,
        mut emit: F,
    ) -> Result<(), ObjectError>
    where
        F: FnMut(ProvenRootRead<RootSnapshotChunk>) -> Result<(), ObjectError>,
    {
        let proof = self.ensure_root_read_index_committed_at(
            "root snapshot export",
            min_read_index,
            min_commit_index,
        )?;
        let mut offset = 0;
        loop {
            let value = self
                .store
                .meta_service
                .export_placement_snapshot_chunk(offset, max_payload_bytes)?;
            let eof = value.eof;
            let payload_len = value.payload.len() as u64;
            emit(ProvenRootRead {
                value,
                proof: proof.clone(),
            })?;
            if eof {
                return Ok(());
            }
            if payload_len == 0 {
                return Err(ObjectError::Corruption(
                    "root snapshot export made no progress".to_string(),
                ));
            }
            offset = offset.saturating_add(payload_len);
        }
    }

    pub fn export_root_snapshot_chunks_with_read_proof_linearizable(
        &self,
        max_payload_bytes: usize,
    ) -> Result<Vec<ProvenRootRead<RootSnapshotChunk>>, ObjectError> {
        self.export_root_snapshot_chunks_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_root_snapshot_chunks_with_read_proof_linearizable_committed_at(
        &self,
        max_payload_bytes: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<Vec<ProvenRootRead<RootSnapshotChunk>>, ObjectError> {
        let mut pages = Vec::new();
        self.export_root_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
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

    pub fn export_root_snapshot_chunks_with_read_proof_report_linearizable(
        &self,
        max_payload_bytes: usize,
    ) -> Result<
        (
            Vec<ProvenRootRead<RootSnapshotChunk>>,
            RootSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        self.export_root_snapshot_chunks_with_read_proof_report_linearizable_committed_at(
            max_payload_bytes,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn export_root_snapshot_chunks_with_read_proof_report_linearizable_committed_at(
        &self,
        max_payload_bytes: usize,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<
        (
            Vec<ProvenRootRead<RootSnapshotChunk>>,
            RootSnapshotProofStreamReport,
        ),
        ObjectError,
    > {
        let mut pages = Vec::new();
        let mut material =
            Vec::with_capacity(encoded_string_len("MXOS-RUST-ROOT-SNAPSHOT-READ-PROOFS1"));
        append_string(&mut material, "MXOS-RUST-ROOT-SNAPSHOT-READ-PROOFS1");
        let mut operation = String::new();
        let mut requested_root_read_index = 0;
        let mut requested_root_commit_index = 0;
        let mut root_applied_index = 0;
        let mut root_raft_term = 0;
        let mut root_state_fingerprint = String::new();
        let mut full_snapshot_checksum = String::new();
        let mut total_payload_bytes = 0u64;
        let mut first_offset = 0u64;
        let mut last_end_offset = 0u64;
        self.export_root_snapshot_chunk_stream_with_read_proof_linearizable_committed_at(
            max_payload_bytes,
            min_read_index,
            min_commit_index,
            |proven| {
                if proven.proof.root_read_index_lag != 0 {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "root snapshot proof has read lag {}",
                        proven.proof.root_read_index_lag
                    )));
                }
                if proven.proof.proof_checksum != root_read_proof_checksum(&proven.proof) {
                    return Err(ObjectError::PreconditionFailed(
                        "root snapshot proof checksum mismatch".to_string(),
                    ));
                }
                if pages.is_empty() {
                    operation = proven.proof.operation.clone();
                    requested_root_read_index = proven.proof.requested_root_read_index;
                    requested_root_commit_index = proven.proof.requested_root_commit_index;
                    root_applied_index = proven.proof.root_applied_index;
                    root_raft_term = proven.proof.root_raft_term;
                    root_state_fingerprint = proven.proof.root_state_fingerprint.clone();
                    full_snapshot_checksum = proven.value.full_checksum.clone();
                    first_offset = proven.value.offset;
                    pages.reserve(snapshot_chunk_vec_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                    material.reserve(root_snapshot_proof_material_capacity(
                        proven.value.total_bytes,
                        max_payload_bytes,
                    ));
                } else if operation != proven.proof.operation
                    || requested_root_read_index != proven.proof.requested_root_read_index
                    || requested_root_commit_index != proven.proof.requested_root_commit_index
                    || root_applied_index != proven.proof.root_applied_index
                    || root_raft_term != proven.proof.root_raft_term
                    || root_state_fingerprint != proven.proof.root_state_fingerprint
                    || full_snapshot_checksum != proven.value.full_checksum
                {
                    return Err(ObjectError::PreconditionFailed(
                        "root snapshot proof stream source image changed".to_string(),
                    ));
                }
                let payload_len = proven.value.payload.len() as u64;
                total_payload_bytes = total_payload_bytes.saturating_add(payload_len);
                last_end_offset = proven.value.offset.saturating_add(payload_len);
                append_root_snapshot_proof_material(
                    &mut material,
                    proven.value.offset,
                    payload_len,
                    &proven.value.page_checksum,
                    &proven.proof,
                );
                pages.push(proven);
                Ok(())
            },
        )?;
        if pages.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "root snapshot proof stream is empty".to_string(),
            ));
        }
        let report = finish_root_snapshot_proof_stream_report(
            material,
            pages.len() as u64,
            operation,
            requested_root_read_index,
            requested_root_commit_index,
            root_applied_index,
            root_raft_term,
            root_state_fingerprint,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            full_snapshot_checksum,
        );
        Ok((pages, report))
    }

    pub fn install_root_snapshot_chunks_with_read_proof_report<I>(
        &mut self,
        pages: I,
        expected_report: &RootSnapshotProofStreamReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenRootRead<RootSnapshotChunk>>,
    {
        let mut material =
            Vec::with_capacity(encoded_string_len("MXOS-RUST-ROOT-SNAPSHOT-READ-PROOFS1"));
        append_string(&mut material, "MXOS-RUST-ROOT-SNAPSHOT-READ-PROOFS1");
        let mut operation = String::new();
        let mut requested_root_read_index = 0;
        let mut requested_root_commit_index = 0;
        let mut root_applied_index = 0;
        let mut root_raft_term = 0;
        let mut root_state_fingerprint = String::new();
        let mut full_snapshot_checksum = String::new();
        let mut total_payload_bytes = 0u64;
        let mut first_offset = 0u64;
        let mut last_end_offset = 0u64;
        let mut proof_count = 0u64;
        let mut assembler = RootSnapshotChunkAssembler::default();
        let mut assembled_snapshot = None;
        for proven in pages {
            if proven.proof.root_read_index_lag != 0 {
                return Err(ObjectError::PreconditionFailed(format!(
                    "root snapshot proof has read lag {}",
                    proven.proof.root_read_index_lag
                )));
            }
            if proven.proof.proof_checksum != root_read_proof_checksum(&proven.proof) {
                return Err(ObjectError::PreconditionFailed(
                    "root snapshot proof checksum mismatch".to_string(),
                ));
            }
            if proof_count == 0 {
                operation = proven.proof.operation.clone();
                requested_root_read_index = proven.proof.requested_root_read_index;
                requested_root_commit_index = proven.proof.requested_root_commit_index;
                root_applied_index = proven.proof.root_applied_index;
                root_raft_term = proven.proof.root_raft_term;
                root_state_fingerprint = proven.proof.root_state_fingerprint.clone();
                full_snapshot_checksum = proven.value.full_checksum.clone();
                first_offset = proven.value.offset;
                material.reserve(root_snapshot_proof_material_capacity(
                    proven.value.total_bytes,
                    proven.value.payload.len().max(1),
                ));
            } else if operation != proven.proof.operation
                || requested_root_read_index != proven.proof.requested_root_read_index
                || requested_root_commit_index != proven.proof.requested_root_commit_index
                || root_applied_index != proven.proof.root_applied_index
                || root_raft_term != proven.proof.root_raft_term
                || root_state_fingerprint != proven.proof.root_state_fingerprint
                || full_snapshot_checksum != proven.value.full_checksum
            {
                return Err(ObjectError::PreconditionFailed(
                    "root snapshot proof stream source image changed".to_string(),
                ));
            }
            let payload_len = proven.value.payload.len() as u64;
            total_payload_bytes = total_payload_bytes.saturating_add(payload_len);
            last_end_offset = proven.value.offset.saturating_add(payload_len);
            append_root_snapshot_proof_material(
                &mut material,
                proven.value.offset,
                payload_len,
                &proven.value.page_checksum,
                &proven.proof,
            );
            proof_count = proof_count.saturating_add(1);
            if assembled_snapshot.is_some() {
                return Err(ObjectError::InvalidArgument(
                    "root snapshot proof stream has chunks after eof".to_string(),
                ));
            }
            assembled_snapshot = assembler.push_chunk(proven.value)?;
        }
        if proof_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "root snapshot proof stream is empty".to_string(),
            ));
        }
        let assembled_snapshot = assembled_snapshot.ok_or_else(|| {
            ObjectError::InvalidArgument(
                "root snapshot proof stream did not complete snapshot image".to_string(),
            )
        })?;
        let snapshot_bytes = assembled_snapshot.len() as u64;
        let snapshot_checksum = full_snapshot_checksum.clone();
        let actual_report = finish_root_snapshot_proof_stream_report(
            material,
            proof_count,
            operation,
            requested_root_read_index,
            requested_root_commit_index,
            root_applied_index,
            root_raft_term,
            root_state_fingerprint,
            total_payload_bytes,
            first_offset,
            last_end_offset,
            full_snapshot_checksum,
        );
        verify_root_snapshot_proof_stream_report(
            expected_report,
            &actual_report,
            min_read_index,
            min_commit_index,
        )?;
        self.store
            .meta_service
            .install_placement_snapshot_bytes_in_place(&assembled_snapshot)?;
        drop(assembled_snapshot);
        Ok(snapshot_install_progress(
            true,
            false,
            snapshot_bytes,
            snapshot_bytes,
            snapshot_checksum,
            root_applied_index,
            root_raft_term,
        ))
    }

    pub fn install_root_snapshot_chunks_with_read_proof_source_report<I>(
        &mut self,
        pages: I,
        expected_report: &RootSnapshotProofStreamReport,
        expected_source_proof: &ServiceSnapshotInstallSourceProofReport,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<SnapshotInstallProgress, ObjectError>
    where
        I: IntoIterator<Item = ProvenRootRead<RootSnapshotChunk>>,
    {
        let actual_source_proof =
            root_service_snapshot_install_source_proof_report(expected_report);
        verify_service_snapshot_install_source_proof_report(
            expected_source_proof,
            &actual_source_proof,
        )?;
        self.install_root_snapshot_chunks_with_read_proof_report(
            pages,
            expected_report,
            min_read_index,
            min_commit_index,
        )
    }
}
