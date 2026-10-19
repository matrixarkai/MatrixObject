impl MatrixObjectStore {
    pub fn prepare_chunk_reclaim_raft_entry(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        let admitted_route_checksum = self.page_service.route_control_proof().state_checksum;
        self.prepare_chunk_reclaim_raft_entry_fenced(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_reclaim_raft_entry_fenced(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim previous commit fence mismatch".to_string(),
            ));
        }
        let service = self
            .page_service
            .replicas
            .get(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        let before_replica_service_fingerprint = replica_service_fingerprint(service);
        let plan = service.plan_reclaim_chunks(
            None,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        )?;
        let mut projected = service.clone();
        let report = projected.reclaim_chunks_with_limits_at(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
        );
        let after_replica_service_fingerprint = replica_service_fingerprint(&projected);
        let route_control_proof = self.page_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedChunkReplicaReclaim {
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            before_replica_service_fingerprint,
            after_replica_service_fingerprint,
            plan,
            report,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof,
            expected_route_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_chunk_reclaim_checksum(&prepared);
        Ok(prepared)
    }

    pub fn prepare_chunk_reclaim_raft_entry_bounded(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        if max_chunks == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared reclaim max_chunks must be positive".to_string(),
            ));
        }
        if max_rewrite_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared reclaim max_rewrite_bytes must be positive".to_string(),
            ));
        }
        if max_discard_ranges == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared reclaim max_discard_ranges must be positive".to_string(),
            ));
        }
        let admitted_route_checksum = self.page_service.route_control_proof().state_checksum;
        self.prepare_chunk_reclaim_raft_entry_fenced(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_chunk_reclaim_raft_entry_bounded_fenced(
        &self,
        replica_index: usize,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedChunkReplicaReclaim, ObjectError> {
        if max_chunks == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared reclaim max_chunks must be positive".to_string(),
            ));
        }
        if max_rewrite_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared reclaim max_rewrite_bytes must be positive".to_string(),
            ));
        }
        if max_discard_ranges == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk prepared reclaim max_discard_ranges must be positive".to_string(),
            ));
        }
        self.prepare_chunk_reclaim_raft_entry_fenced(
            replica_index,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn apply_prepared_chunk_reclaim(
        &mut self,
        replica_index: usize,
        prepared: PreparedChunkReplicaReclaim,
    ) -> Result<ChunkPartialReclaimReport, ObjectError> {
        if prepared.replica_index != replica_index {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim replica fence mismatch".to_string(),
            ));
        }
        let expected_checksum = prepared_chunk_reclaim_checksum(&PreparedChunkReplicaReclaim {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "chunk prepared reclaim raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim commit index must be adjacent".to_string(),
            ));
        }
        {
            let replica = self
                .page_service
                .replicas
                .get(replica_index)
                .ok_or_else(|| {
                    ObjectError::InvalidArgument("replica index out of range".to_string())
                })?;
            if replica_service_fingerprint(replica) == prepared.after_replica_service_fingerprint {
                return Ok(prepared.report);
            }
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim previous commit fence mismatch".to_string(),
            ));
        }
        self.page_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "chunk prepared reclaim",
            )?;
        let current = self.prepare_chunk_reclaim_raft_entry_fenced(
            replica_index,
            prepared.max_chunks,
            prepared.max_rewrite_bytes,
            prepared.min_discarded_bytes,
            prepared.min_discard_ratio_ppm,
            prepared.max_discard_ranges,
            prepared.max_trim_bytes,
            prepared.now_ms,
            prepared.expected_previous_commit_index,
            Some(prepared.route_control_proof.state_checksum.as_str()),
        )?;
        if current.plan != prepared.plan
            || current.report != prepared.report
            || current.route_control_proof != prepared.route_control_proof
            || current.raft_entry_checksum != prepared.raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim no longer matches current state".to_string(),
            ));
        }
        let replica = self
            .page_service
            .replicas
            .get_mut(replica_index)
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica index out of range".to_string())
            })?;
        if replica_service_fingerprint(replica) != prepared.before_replica_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "chunk prepared reclaim source fingerprint mismatch".to_string(),
            ));
        }
        let report = replica.reclaim_chunks_with_limits_at(
            prepared.max_chunks,
            prepared.max_rewrite_bytes,
            prepared.min_discarded_bytes,
            prepared.min_discard_ratio_ppm,
            prepared.max_discard_ranges,
            prepared.max_trim_bytes,
            prepared.now_ms,
        );
        if report != prepared.report
            || replica_service_fingerprint(replica) != prepared.after_replica_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "chunk prepared reclaim deterministic apply mismatch".to_string(),
            ));
        }
        Ok(prepared.report)
    }
}
