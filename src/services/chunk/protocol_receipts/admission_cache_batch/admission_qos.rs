// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaWriteAdmission {
    pub can_accept: bool,
    pub write_count: u64,
    pub coalesced_ranges: u64,
    pub payload_bytes: u64,
    pub max_write_bytes: u64,
    pub selected_write_range_count: u64,
    pub selected_write_payload_bytes: u64,
    pub write_start_range_index: u64,
    pub next_write_range_index: u64,
    pub write_has_more: bool,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub projected_physical_bytes: u64,
    pub capacity_bytes: u64,
    pub capacity_free_bytes: u64,
    pub service_mode_limited: bool,
    pub capacity_limited: bool,
    pub budget_limited: bool,
    pub qos_limited: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaDiscardAdmission {
    pub can_accept: bool,
    pub discard_count: u64,
    pub coalesced_ranges: u64,
    pub discard_bytes: u64,
    pub max_discard_bytes: u64,
    pub selected_discard_bytes: u64,
    pub skipped_discard_bytes: u64,
    pub skipped_discard_range_count: u64,
    pub discard_start_range_index: u64,
    pub next_discard_range_index: u64,
    pub discard_has_more: bool,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub service_mode_limited: bool,
    pub budget_limited: bool,
    pub qos_limited: bool,
    pub missing_chunk: bool,
    pub range_invalid: bool,
}

impl ReplicaWriteAdmission {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("write_count", self.write_count),
            ("coalesced_ranges", self.coalesced_ranges),
            ("payload_bytes", self.payload_bytes),
            ("max_write_bytes", self.max_write_bytes),
            (
                "selected_write_range_count",
                self.selected_write_range_count,
            ),
            (
                "selected_write_payload_bytes",
                self.selected_write_payload_bytes,
            ),
            ("write_start_range_index", self.write_start_range_index),
            ("next_write_range_index", self.next_write_range_index),
            ("min_read_index", self.min_read_index),
            ("min_commit_index", self.min_commit_index),
            ("projected_physical_bytes", self.projected_physical_bytes),
            ("capacity_bytes", self.capacity_bytes),
            ("capacity_free_bytes", self.capacity_free_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("can_accept", self.can_accept),
            ("write_has_more", self.write_has_more),
            ("service_mode_limited", self.service_mode_limited),
            ("capacity_limited", self.capacity_limited),
            ("budget_limited", self.budget_limited),
            ("qos_limited", self.qos_limited),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl ReplicaDiscardAdmission {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("discard_count", self.discard_count),
            ("coalesced_ranges", self.coalesced_ranges),
            ("discard_bytes", self.discard_bytes),
            ("max_discard_bytes", self.max_discard_bytes),
            ("selected_discard_bytes", self.selected_discard_bytes),
            ("skipped_discard_bytes", self.skipped_discard_bytes),
            (
                "skipped_discard_range_count",
                self.skipped_discard_range_count,
            ),
            ("discard_start_range_index", self.discard_start_range_index),
            ("next_discard_range_index", self.next_discard_range_index),
            ("min_read_index", self.min_read_index),
            ("min_commit_index", self.min_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("can_accept", self.can_accept),
            ("discard_has_more", self.discard_has_more),
            ("service_mode_limited", self.service_mode_limited),
            ("budget_limited", self.budget_limited),
            ("qos_limited", self.qos_limited),
            ("missing_chunk", self.missing_chunk),
            ("range_invalid", self.range_invalid),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaMutationQosConfig {
    pub window_ms: u64,
    pub max_write_ops_per_window: u64,
    pub max_write_bytes_per_window: u64,
    pub max_discard_ops_per_window: u64,
    pub max_discard_bytes_per_window: u64,
}

impl Default for ReplicaMutationQosConfig {
    fn default() -> Self {
        Self {
            window_ms: DEFAULT_CHUNK_MUTATION_QOS_WINDOW_MS,
            max_write_ops_per_window: 0,
            max_write_bytes_per_window: 0,
            max_discard_ops_per_window: 0,
            max_discard_bytes_per_window: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaMutationQosState {
    pub window_start_ms: u64,
    pub write_ops_in_window: u64,
    pub write_bytes_in_window: u64,
    pub discard_ops_in_window: u64,
    pub discard_bytes_in_window: u64,
    pub throttled_ops: u64,
    pub throttled_bytes: u64,
}

