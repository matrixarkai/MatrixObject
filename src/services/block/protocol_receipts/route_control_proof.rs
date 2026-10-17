#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageRouteControlProof {
    pub control_receipt: bool,
    pub state_format: String,
    pub state_bytes: u64,
    pub state_checksum: String,
    pub page_route_control_commit_index: u64,
    pub page_route_control_next_log_index: u64,
    pub chunk_endpoint_count: u64,
    pub disabled_chunk_endpoint_count: u64,
    pub write_quorum: u64,
    pub configured_write_quorum: u64,
    pub read_quorum: u64,
    pub configured_read_quorum: u64,
    pub disabled_replica_count: u64,
    pub qos_window_ms: u64,
    pub qos_max_read_ops_per_window: u64,
    pub qos_max_read_bytes_per_window: u64,
    pub qos_max_write_ops_per_window: u64,
    pub qos_max_write_bytes_per_window: u64,
    pub qos_max_discard_ops_per_window: u64,
    pub qos_max_discard_bytes_per_window: u64,
    pub qos_throttled_ops: u64,
    pub qos_throttled_bytes: u64,
    pub receipt_retention: u64,
    pub primary_batch_read_repair_probe_limit: u64,
    pub cumulative_quorum_write_degraded_chunks: u64,
    pub cumulative_quorum_read_degraded_chunks: u64,
    pub cumulative_quorum_discard_degraded_chunks: u64,
    pub cumulative_replica_fallback_reads: u64,
    pub cumulative_physical_chunk_read_count: u64,
    pub cumulative_coalesced_chunk_read_count: u64,
    pub cumulative_repair_checked_chunks: u64,
    pub cumulative_repair_degraded_chunks: u64,
    pub cumulative_repaired_replicas: u64,
    pub cumulative_failed_repairs: u64,
    pub cumulative_replica_read_repairs: u64,
    pub cumulative_replica_read_repair_failures: u64,
}

impl PageRouteControlProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}control_receipt"),
            self.control_receipt.to_string(),
        );
        fields.insert(
            format!("{field_prefix}state_format"),
            self.state_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}state_checksum"),
            self.state_checksum.clone(),
        );
        for (name, value) in [
            ("state_bytes", self.state_bytes),
            (
                "block_route_control_commit_index",
                self.page_route_control_commit_index,
            ),
            (
                "block_route_control_next_log_index",
                self.page_route_control_next_log_index,
            ),
            ("chunk_endpoint_count", self.chunk_endpoint_count),
            (
                "disabled_chunk_endpoint_count",
                self.disabled_chunk_endpoint_count,
            ),
            ("write_quorum", self.write_quorum),
            ("configured_write_quorum", self.configured_write_quorum),
            ("read_quorum", self.read_quorum),
            ("configured_read_quorum", self.configured_read_quorum),
            ("disabled_replica_count", self.disabled_replica_count),
            ("qos_window_ms", self.qos_window_ms),
            (
                "qos_max_read_ops_per_window",
                self.qos_max_read_ops_per_window,
            ),
            (
                "qos_max_read_bytes_per_window",
                self.qos_max_read_bytes_per_window,
            ),
            (
                "qos_max_write_ops_per_window",
                self.qos_max_write_ops_per_window,
            ),
            (
                "qos_max_write_bytes_per_window",
                self.qos_max_write_bytes_per_window,
            ),
            (
                "qos_max_discard_ops_per_window",
                self.qos_max_discard_ops_per_window,
            ),
            (
                "qos_max_discard_bytes_per_window",
                self.qos_max_discard_bytes_per_window,
            ),
            ("qos_throttled_ops", self.qos_throttled_ops),
            ("qos_throttled_bytes", self.qos_throttled_bytes),
            ("receipt_retention", self.receipt_retention),
            (
                "primary_batch_read_repair_probe_limit",
                self.primary_batch_read_repair_probe_limit,
            ),
            (
                "cumulative_quorum_write_degraded_chunks",
                self.cumulative_quorum_write_degraded_chunks,
            ),
            (
                "cumulative_quorum_read_degraded_chunks",
                self.cumulative_quorum_read_degraded_chunks,
            ),
            (
                "cumulative_quorum_discard_degraded_chunks",
                self.cumulative_quorum_discard_degraded_chunks,
            ),
            (
                "cumulative_replica_fallback_reads",
                self.cumulative_replica_fallback_reads,
            ),
            (
                "cumulative_physical_chunk_read_count",
                self.cumulative_physical_chunk_read_count,
            ),
            (
                "cumulative_coalesced_chunk_read_count",
                self.cumulative_coalesced_chunk_read_count,
            ),
            (
                "cumulative_repair_checked_chunks",
                self.cumulative_repair_checked_chunks,
            ),
            (
                "cumulative_repair_degraded_chunks",
                self.cumulative_repair_degraded_chunks,
            ),
            (
                "cumulative_repaired_replicas",
                self.cumulative_repaired_replicas,
            ),
            ("cumulative_failed_repairs", self.cumulative_failed_repairs),
            (
                "cumulative_replica_read_repairs",
                self.cumulative_replica_read_repairs,
            ),
            (
                "cumulative_replica_read_repair_failures",
                self.cumulative_replica_read_repair_failures,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}
