impl PreparedChunkReplicaReclaim {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.plan.to_control_fields(&format!("{field_prefix}plan."));
        fields.extend(
            self.report
                .to_control_fields(&format!("{field_prefix}report.")),
        );
        for (name, value) in [
            ("replica_index", self.replica_index as u64),
            ("max_chunks", self.max_chunks as u64),
            ("max_rewrite_bytes", self.max_rewrite_bytes),
            ("min_discarded_bytes", self.min_discarded_bytes),
            ("min_discard_ratio_ppm", self.min_discard_ratio_ppm),
            ("max_discard_ranges", self.max_discard_ranges as u64),
            ("max_trim_bytes", self.max_trim_bytes),
            ("now_ms", self.now_ms),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}before_chunk_service_fingerprint"),
            self.before_chunk_service_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}after_chunk_service_fingerprint"),
            self.after_chunk_service_fingerprint.clone(),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}raft_entry_checksum"),
            self.raft_entry_checksum.clone(),
        );
        fields
    }
}
