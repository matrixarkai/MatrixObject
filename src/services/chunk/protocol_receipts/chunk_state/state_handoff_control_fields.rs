impl ReplicaStateHandoffApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .stats
            .to_control_fields(&format!("{field_prefix}stats."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_commit_index"),
            self.mutation_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_log_index"),
            self.next_log_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}committed"),
            self.committed.to_string(),
        );
        fields.insert(format!("{field_prefix}prepared"), self.prepared.to_string());
        fields.insert(
            format!("{field_prefix}external_raft_required"),
            self.external_raft_required.to_string(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.insert(
            format!("{field_prefix}target_replica_index"),
            self.target_replica_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}handoff_count"),
            self.handoff_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_bytes"),
            self.payload_bytes.to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_checksum"),
            self.payload_checksum.clone(),
        );
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

impl ReplicaStateHandoff {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}chunk."));
        fields.extend(
            self.state
                .to_control_fields(&format!("{field_prefix}state.")),
        );
        for (name, value) in [
            ("payload_checksum", &self.payload_checksum),
            ("state_checksum", &self.state_checksum),
            ("handoff_token", &self.handoff_token),
            ("install_id", &self.install_id),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(
            format!("{field_prefix}proof_version"),
            self.proof_version.to_string(),
        );
        fields
    }
}

impl PreparedChunkLocalStateHandoffEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .chunk_id
            .to_control_fields(&format!("{field_prefix}chunk."));
        for (name, value) in [
            ("max_payload_bytes", self.max_payload_bytes),
            ("payload_bytes", self.payload_bytes),
            ("write_sequence", self.write_sequence),
            ("discard_range_count", self.discard_range_count),
            ("handoff_proof_version", self.handoff_proof_version),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("request_checksum", &self.request_checksum),
            ("payload_checksum", &self.payload_checksum),
            ("state_checksum", &self.state_checksum),
            ("handoff_token", &self.handoff_token),
            ("handoff_state_checksum", &self.handoff_state_checksum),
            ("install_id", &self.install_id),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields
    }
}

impl PreparedChunkLocalStateHandoff {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .envelope
            .to_control_fields(&format!("{field_prefix}envelope."));
        fields.extend(
            self.handoff
                .to_control_fields(&format!("{field_prefix}handoff.")),
        );
        fields
    }
}

impl PreparedChunkStateHandoffEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .slab_id
            .to_control_fields(&format!("{field_prefix}segment."));
        for (name, value) in [
            ("offset", self.offset),
            ("length", self.length),
            ("source_replica_index", self.source_replica_index as u64),
            ("max_payload_bytes", self.max_payload_bytes),
            ("payload_bytes", self.payload_bytes),
            ("handoff_count", self.handoff_count),
            ("route_control_proof_bytes", self.route_control_proof_bytes),
            (
                "route_control_commit_index",
                self.route_control_commit_index,
            ),
            (
                "route_control_next_log_index",
                self.route_control_next_log_index,
            ),
            (
                "first_handoff_payload_bytes",
                self.first_handoff_payload_bytes,
            ),
            (
                "handoff_state_checksum_count",
                self.handoff_state_checksums.len() as u64,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}budget_limited"),
            self.budget_limited.to_string(),
        );
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("request_checksum", &self.request_checksum),
            ("payload_checksum", &self.payload_checksum),
            (
                "route_control_proof_format",
                &self.route_control_proof_format,
            ),
            (
                "route_control_proof_checksum",
                &self.route_control_proof_checksum,
            ),
            (
                "first_handoff_state_checksum",
                &self.first_handoff_state_checksum,
            ),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(
            format!("{field_prefix}first_handoff_chunk_present"),
            self.first_handoff_chunk_id.is_some().to_string(),
        );
        if let Some(chunk_id) = &self.first_handoff_chunk_id {
            fields
                .extend(chunk_id.to_control_fields(&format!("{field_prefix}first_handoff_chunk.")));
        }
        for (index, checksum) in self.handoff_state_checksums.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}handoff_state_checksum.{index}"),
                checksum.clone(),
            );
        }
        fields
    }
}

impl ProvenPreparedChunkStateHandoffEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .envelope
            .to_control_fields(&format!("{field_prefix}envelope."));
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}

impl ProvenStorageRead<Vec<ReplicaStateHandoff>> {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}handoff_count"),
            self.value.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}payload_checksum"),
            chunk_state_handoff_payload_checksum(&self.value),
        );
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        for (index, handoff) in self.value.iter().enumerate() {
            fields.extend(handoff.to_control_fields(&format!("{field_prefix}handoff.{index}.")));
        }
        fields
    }
}
