impl MatrixObjectStore {
    pub fn prepare_page_reclaim_raft_entry_fenced(
        &self,
        max_chunks: usize,
        max_rewrite_bytes: u64,
        min_discarded_bytes: u64,
        min_discard_ratio_ppm: u64,
        max_discard_ranges: usize,
        max_trim_bytes: u64,
        now_ms: u64,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedPageReplicaReclaim, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim previous commit fence mismatch".to_string(),
            ));
        }
        let before_page_service_fingerprint =
            page_batch_mutation_image_fingerprint(&self.page_service);
        let plan = self.page_service.plan_chunk_replica_reclaim(
            None,
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
        )?;
        let mut projected = self.page_service.clone();
        let report = projected.reclaim_chunk_replicas_with_limits_at(
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
        );
        let after_page_service_fingerprint = page_batch_mutation_image_fingerprint(&projected);
        let mut prepared = PreparedPageReplicaReclaim {
            max_chunks,
            max_rewrite_bytes,
            min_discarded_bytes,
            min_discard_ratio_ppm,
            max_discard_ranges,
            max_trim_bytes,
            now_ms,
            before_page_service_fingerprint,
            after_page_service_fingerprint,
            plan,
            report,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof: self.page_service.route_control_proof(),
            expected_route_control_checksum: expected_route_control_checksum
                .unwrap_or_default()
                .to_string(),
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_page_reclaim_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_page_reclaim(
        &mut self,
        prepared: PreparedPageReplicaReclaim,
    ) -> Result<ChunkPartialReclaimReport, ObjectError> {
        let expected_checksum = prepared_page_reclaim_checksum(&PreparedPageReplicaReclaim {
            raft_entry_checksum: String::new(),
            ..prepared.clone()
        });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "block prepared reclaim raft entry checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim route proof checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim commit index must be adjacent".to_string(),
            ));
        }
        let current_fingerprint = page_batch_mutation_image_fingerprint(&self.page_service);
        if current_fingerprint == prepared.after_page_service_fingerprint {
            return Ok(prepared.report);
        }
        if prepared.expected_previous_commit_index != self.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim previous commit fence mismatch".to_string(),
            ));
        }
        self.page_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "block prepared reclaim",
            )?;
        if current_fingerprint != prepared.before_page_service_fingerprint {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim source fingerprint mismatch".to_string(),
            ));
        }
        let mut current = self.prepare_page_reclaim_raft_entry_fenced(
            prepared.max_chunks,
            prepared.max_rewrite_bytes,
            prepared.min_discarded_bytes,
            prepared.min_discard_ratio_ppm,
            prepared.max_discard_ranges,
            prepared.max_trim_bytes,
            prepared.now_ms,
            prepared.expected_previous_commit_index,
            Some(&prepared.expected_route_control_checksum),
        )?;
        current.raft_entry_checksum = prepared_page_reclaim_checksum(&current);
        if current.plan != prepared.plan
            || current.report != prepared.report
            || current.route_control_proof != prepared.route_control_proof
            || current.raft_entry_checksum != prepared.raft_entry_checksum
        {
            return Err(ObjectError::PreconditionFailed(
                "block prepared reclaim no longer matches current state".to_string(),
            ));
        }
        let report = self.page_service.reclaim_chunk_replicas_with_limits_at(
            prepared.max_chunks,
            prepared.max_rewrite_bytes,
            prepared.min_discarded_bytes,
            prepared.min_discard_ratio_ppm,
            prepared.max_discard_ranges,
            prepared.max_trim_bytes,
            prepared.now_ms,
        );
        if report != prepared.report
            || page_batch_mutation_image_fingerprint(&self.page_service)
                != prepared.after_page_service_fingerprint
        {
            return Err(ObjectError::Corruption(
                "block prepared reclaim deterministic apply mismatch".to_string(),
            ));
        }
        Ok(prepared.report)
    }

}
