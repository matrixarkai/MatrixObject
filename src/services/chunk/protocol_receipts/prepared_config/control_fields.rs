fn append_prepared_chunk_target_replica_fields(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    target_replica_index: Option<usize>,
) {
    fields.insert(
        format!("{field_prefix}target_replica_index_present"),
        target_replica_index.is_some().to_string(),
    );
    if let Some(replica_index) = target_replica_index {
        fields.insert(
            format!("{field_prefix}target_replica_index"),
            replica_index.to_string(),
        );
    }
}

fn append_prepared_chunk_common_fields(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    chunk_prepared_replica_count: u64,
    expected_previous_commit_index: u64,
    prepared_commit_index: u64,
    expected_route_control_checksum: &str,
    raft_entry_checksum: &str,
    route_control_proof: &PageRouteControlProof,
) {
    for (name, value) in [
        ("chunk_prepared_replica_count", chunk_prepared_replica_count),
        (
            "expected_previous_commit_index",
            expected_previous_commit_index,
        ),
        ("prepared_commit_index", prepared_commit_index),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
    fields.insert(
        format!("{field_prefix}expected_route_control_checksum"),
        expected_route_control_checksum.to_string(),
    );
    fields.insert(
        format!("{field_prefix}raft_entry_checksum"),
        raft_entry_checksum.to_string(),
    );
    fields.extend(route_control_proof.to_control_fields(&format!("{field_prefix}route_control.")));
}

fn append_prepared_chunk_page_fingerprint_fields(
    fields: &mut BTreeMap<String, String>,
    field_prefix: &str,
    before_page_service_fingerprint: &str,
    after_page_service_fingerprint: &str,
) {
    fields.insert(
        format!("{field_prefix}before_block_service_fingerprint"),
        before_page_service_fingerprint.to_string(),
    );
    fields.insert(
        format!("{field_prefix}after_block_service_fingerprint"),
        after_page_service_fingerprint.to_string(),
    );
}

impl ChunkMutationQosConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("window_ms", self.window_ms),
            ("max_write_ops_per_window", self.max_write_ops_per_window),
            (
                "max_write_bytes_per_window",
                self.max_write_bytes_per_window,
            ),
            (
                "max_discard_ops_per_window",
                self.max_discard_ops_per_window,
            ),
            (
                "max_discard_bytes_per_window",
                self.max_discard_bytes_per_window,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl PreparedChunkMutationQosConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_prepared_chunk_target_replica_fields(
            &mut fields,
            field_prefix,
            self.target_replica_index,
        );
        fields.extend(
            self.config
                .to_control_fields(&format!("{field_prefix}config.")),
        );
        fields.insert(
            format!("{field_prefix}chunk_mutation_qos_prepared_aggregate_checksum"),
            self.chunk_mutation_qos_prepared_aggregate_checksum.clone(),
        );
        append_prepared_chunk_common_fields(
            &mut fields,
            field_prefix,
            self.chunk_prepared_replica_count,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.expected_route_control_checksum,
            &self.raft_entry_checksum,
            &self.route_control_proof,
        );
        fields
    }
}

impl PreparedChunkTrimFlowConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_prepared_chunk_target_replica_fields(
            &mut fields,
            field_prefix,
            self.target_replica_index,
        );
        for (name, value) in [
            ("rate_bytes_per_sec", self.rate_bytes_per_sec),
            ("burst_bytes", self.burst_bytes),
            ("rate_ops_per_sec", self.rate_ops_per_sec),
            ("burst_ops", self.burst_ops),
            ("now_ms", self.now_ms),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}chunk_trim_flow_prepared_aggregate_checksum"),
            self.chunk_trim_flow_prepared_aggregate_checksum.clone(),
        );
        append_prepared_chunk_page_fingerprint_fields(
            &mut fields,
            field_prefix,
            &self.before_page_service_fingerprint,
            &self.after_page_service_fingerprint,
        );
        append_prepared_chunk_common_fields(
            &mut fields,
            field_prefix,
            self.chunk_prepared_replica_count,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.expected_route_control_checksum,
            &self.raft_entry_checksum,
            &self.route_control_proof,
        );
        fields
    }
}

impl PreparedChunkReadCacheConfig {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_prepared_chunk_target_replica_fields(
            &mut fields,
            field_prefix,
            self.target_replica_index,
        );
        for (name, value) in [
            ("max_bytes_per_replica", self.max_bytes_per_replica),
            (
                "max_cacheable_read_bytes_per_replica",
                self.max_cacheable_read_bytes_per_replica,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}chunk_read_cache_prepared_aggregate_checksum"),
            self.chunk_read_cache_prepared_aggregate_checksum.clone(),
        );
        append_prepared_chunk_page_fingerprint_fields(
            &mut fields,
            field_prefix,
            &self.before_page_service_fingerprint,
            &self.after_page_service_fingerprint,
        );
        append_prepared_chunk_common_fields(
            &mut fields,
            field_prefix,
            self.chunk_prepared_replica_count,
            self.expected_previous_commit_index,
            self.prepared_commit_index,
            &self.expected_route_control_checksum,
            &self.raft_entry_checksum,
            &self.route_control_proof,
        );
        fields
    }
}

