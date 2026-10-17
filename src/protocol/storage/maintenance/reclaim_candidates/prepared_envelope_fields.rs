impl PreparedChunkReclaimEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("replica_index", self.replica_index as u64),
            ("max_chunks", self.max_chunks),
            ("max_rewrite_bytes", self.max_rewrite_bytes),
            ("min_discarded_bytes", self.min_discarded_bytes),
            ("min_discard_ratio_ppm", self.min_discard_ratio_ppm),
            ("max_discard_ranges", self.max_discard_ranges),
            ("max_trim_bytes", self.max_trim_bytes),
            ("now_ms", self.now_ms),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("route_control_proof_bytes", self.route_control_proof_bytes),
            (
                "route_control_commit_index",
                self.route_control_commit_index,
            ),
            (
                "route_control_next_log_index",
                self.route_control_next_log_index,
            ),
            ("candidate_chunks", self.candidate_chunks),
            ("candidate_discarded_bytes", self.candidate_discarded_bytes),
            ("scheduled_reclaim_chunks", self.scheduled_reclaim_chunks),
            ("scheduled_rewrite_bytes", self.scheduled_rewrite_bytes),
            ("scheduled_trim_ops", self.scheduled_trim_ops),
            ("scheduled_trim_bytes", self.scheduled_trim_bytes),
            ("reclaimed_chunks", self.reclaimed_chunks),
            ("reclaimed_discard_bytes", self.reclaimed_discard_bytes),
            ("rewritten_bytes", self.rewritten_bytes),
            ("trimmed_bytes", self.trimmed_bytes),
            (
                "first_reclaim_discarded_bytes",
                self.first_reclaim_discarded_bytes,
            ),
            (
                "first_reclaim_discard_ratio_ppm",
                self.first_reclaim_discard_ratio_ppm,
            ),
            ("first_reclaim_trim_bytes", self.first_reclaim_trim_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("budget_limited", self.budget_limited),
            ("trim_flow_limited", self.trim_flow_limited),
            ("truncated", self.truncated),
            ("continuation_safe", self.continuation_safe),
            ("next_marker_advanced", self.next_marker_advanced),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("plan_checksum", &self.plan_checksum),
            (
                "before_chunk_service_fingerprint",
                &self.before_chunk_service_fingerprint,
            ),
            (
                "after_chunk_service_fingerprint",
                &self.after_chunk_service_fingerprint,
            ),
            (
                "route_control_proof_format",
                &self.route_control_proof_format,
            ),
            (
                "route_control_proof_checksum",
                &self.route_control_proof_checksum,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(
            format!("{field_prefix}first_reclaim_chunk_id_present"),
            self.first_reclaim_chunk_id.is_some().to_string(),
        );
        if let Some(chunk_id) = &self.first_reclaim_chunk_id {
            fields
                .extend(chunk_id.to_control_fields(&format!("{field_prefix}first_reclaim_chunk.")));
        }
        fields
    }
}

impl ProvenPreparedChunkReclaimEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.envelope.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}
