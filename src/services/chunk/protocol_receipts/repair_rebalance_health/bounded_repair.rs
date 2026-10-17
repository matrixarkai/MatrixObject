impl RepairStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("checked_chunks", self.checked_chunks),
            ("repaired_replicas", self.repaired_replicas),
            ("failed_repairs", self.failed_repairs),
            ("degraded_chunks", self.degraded_chunks),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BoundedRepairReport {
    pub total_chunk_count: u64,
    pub selected_chunk_count: u64,
    pub skipped_chunk_count: u64,
    pub candidate_repair_bytes: u64,
    pub selected_repair_bytes: u64,
    pub skipped_repair_bytes: u64,
    pub start_chunk_index: u64,
    pub next_chunk_index: u64,
    pub has_more: bool,
    pub stats: RepairStats,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl BoundedRepairReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("total_chunk_count", self.total_chunk_count),
            ("selected_chunk_count", self.selected_chunk_count),
            ("skipped_chunk_count", self.skipped_chunk_count),
            ("candidate_repair_bytes", self.candidate_repair_bytes),
            ("selected_repair_bytes", self.selected_repair_bytes),
            ("skipped_repair_bytes", self.skipped_repair_bytes),
            ("start_chunk_index", self.start_chunk_index),
            ("next_chunk_index", self.next_chunk_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(format!("{field_prefix}has_more"), self.has_more.to_string());
        fields.extend(
            self.stats
                .to_control_fields(&format!("{field_prefix}stats.")),
        );
        append_route_control_fields(
            &self.route_control_proof,
            &self.expected_route_control_checksum,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

