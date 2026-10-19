impl MatrixChunkService {
    fn check_expected_chunk_control_proof_checksum(
        &self,
        expected_checksum: Option<&str>,
    ) -> Result<(), ObjectError> {
        let Some(expected_checksum) = expected_checksum else {
            return Ok(());
        };
        if expected_checksum.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "block chunk control proof checksum expectation is empty".to_string(),
            ));
        }
        let actual_proof = self.chunk_control_proof();
        if actual_proof.proof_count == 0 || actual_proof.proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block chunk control proof is unavailable".to_string(),
            ));
        }
        if expected_checksum != actual_proof.proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "block chunk control proof checksum mismatch: expected {} got {}",
                expected_checksum, actual_proof.proof_checksum
            )));
        }
        Ok(())
    }

    fn stamp_route_control_admission(
        &self,
        admission: &mut ChunkRangeAdmission,
        expected_route_control_checksum: Option<&str>,
    ) {
        let route_control_proof = self.route_control_proof();
        admission.route_control_proof = route_control_proof.clone();
        admission.expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        admission.route_control_fenced = expected_route_control_checksum.is_some();
        admission.route_control_fence_matched = expected_route_control_checksum
            .map(|expected| expected == route_control_proof.state_checksum)
            .unwrap_or(true);
    }

    pub(crate) fn storage_mutation_report(
        &self,
        stats: ChunkBatchStats,
        expected_route_control_checksum: Option<&str>,
        expected_chunk_control_proof_checksum: Option<&str>,
    ) -> ChunkStorageMutationReport {
        let route_control_proof = self.route_control_proof();
        let chunk_control_proof = self.chunk_control_proof();
        ChunkStorageMutationReport {
            stats,
            route_control_proof: route_control_proof.clone(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or("")
                .to_string(),
            route_control_fenced: expected_route_control_checksum.is_some(),
            route_control_fence_matched: expected_route_control_checksum
                .map(|expected| expected == route_control_proof.state_checksum)
                .unwrap_or(true),
            chunk_control_proof: chunk_control_proof.clone(),
            expected_chunk_control_proof_checksum: expected_chunk_control_proof_checksum
                .unwrap_or("")
                .to_string(),
            chunk_control_fenced: expected_chunk_control_proof_checksum.is_some(),
            chunk_control_fence_matched: expected_chunk_control_proof_checksum
                .map(|expected| expected == chunk_control_proof.proof_checksum)
                .unwrap_or(true),
        }
    }

    pub(crate) fn check_prepared_route_control_proof_cursor(
        &self,
        prepared_proof: &ChunkRouteControlProof,
        context: &str,
    ) -> Result<(), ObjectError> {
        let current_proof = self.route_control_proof();
        if current_proof.state_checksum != prepared_proof.state_checksum
            || current_proof.chunk_route_control_commit_index
                != prepared_proof.chunk_route_control_commit_index
            || current_proof.chunk_route_control_next_log_index
                != prepared_proof.chunk_route_control_next_log_index
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "{context} route proof cursor mismatch"
            )));
        }
        self.check_expected_route_control_checksum(Some(&current_proof.state_checksum))
    }

    pub(crate) fn check_prepared_chunk_control_proof_cursor(
        &self,
        prepared_proof: &ReplicaControlProof,
        context: &str,
    ) -> Result<(), ObjectError> {
        if prepared_proof.proof_count == 0 || prepared_proof.proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(format!(
                "{context} chunk control proof is required"
            )));
        }
        let current_proof = self.chunk_control_proof();
        if current_proof.proof_count != prepared_proof.proof_count
            || current_proof.proof_checksum != prepared_proof.proof_checksum
            || current_proof.min_raft_commit_index != prepared_proof.min_raft_commit_index
            || current_proof.max_raft_commit_index != prepared_proof.max_raft_commit_index
        {
            return Err(ObjectError::PreconditionFailed(format!(
                "{context} chunk control proof cursor mismatch"
            )));
        }
        Ok(())
    }
}
