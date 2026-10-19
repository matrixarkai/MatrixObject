// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaScrubReport {
    pub checked_chunks: u64,
    pub healthy_chunks: u64,
    pub degraded_chunks: u64,
    pub repaired_chunks: u64,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub missing_replicas: u64,
    pub corrupt_replicas: u64,
    pub divergent_replicas: u64,
    pub unrepaired_chunks: u64,
}

impl ReplicaScrubReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_chunk_health_control_fields(
            self.checked_chunks,
            self.healthy_chunks,
            self.degraded_chunks,
            self.repaired_chunks,
            self.repaired_replicas,
            self.failed_repairs,
            self.missing_replicas,
            self.corrupt_replicas,
            self.divergent_replicas,
            self.unrepaired_chunks,
            field_prefix,
            &mut fields,
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaRepairReport {
    pub page_id: Option<PageId>,
    pub checked_chunks: u64,
    pub healthy_chunks: u64,
    pub degraded_chunks: u64,
    pub repaired_chunks: u64,
    pub repaired_replicas: u64,
    pub failed_repairs: u64,
    pub missing_replicas: u64,
    pub corrupt_replicas: u64,
    pub divergent_replicas: u64,
    pub frozen_replicas: u64,
    pub quarantined_replicas: u64,
    pub repairable_replicas: u64,
    pub unrepaired_chunks: u64,
    pub committed: bool,
    pub first_error: Option<String>,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
}

impl ReplicaRepairReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}chunk_present"),
            self.page_id.is_some().to_string(),
        );
        if let Some(page_id) = &self.page_id {
            fields.extend(page_id.to_control_fields(&format!("{field_prefix}chunk.")));
        }
        append_chunk_health_control_fields(
            self.checked_chunks,
            self.healthy_chunks,
            self.degraded_chunks,
            self.repaired_chunks,
            self.repaired_replicas,
            self.failed_repairs,
            self.missing_replicas,
            self.corrupt_replicas,
            self.divergent_replicas,
            self.unrepaired_chunks,
            field_prefix,
            &mut fields,
        );
        fields.insert(
            format!("{field_prefix}frozen_replicas"),
            self.frozen_replicas.to_string(),
        );
        fields.insert(
            format!("{field_prefix}quarantined_replicas"),
            self.quarantined_replicas.to_string(),
        );
        fields.insert(
            format!("{field_prefix}repairable_replicas"),
            self.repairable_replicas.to_string(),
        );
        fields.insert(
            format!("{field_prefix}committed"),
            self.committed.to_string(),
        );
        fields.insert(
            format!("{field_prefix}first_error"),
            self.first_error.clone().unwrap_or_default(),
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

fn append_chunk_health_control_fields(
    checked_chunks: u64,
    healthy_chunks: u64,
    degraded_chunks: u64,
    repaired_chunks: u64,
    repaired_replicas: u64,
    failed_repairs: u64,
    missing_replicas: u64,
    corrupt_replicas: u64,
    divergent_replicas: u64,
    unrepaired_chunks: u64,
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    for (name, value) in [
        ("checked_chunks", checked_chunks),
        ("healthy_chunks", healthy_chunks),
        ("degraded_chunks", degraded_chunks),
        ("repaired_chunks", repaired_chunks),
        ("repaired_replicas", repaired_replicas),
        ("failed_repairs", failed_repairs),
        ("missing_replicas", missing_replicas),
        ("corrupt_replicas", corrupt_replicas),
        ("divergent_replicas", divergent_replicas),
        ("unrepaired_chunks", unrepaired_chunks),
    ] {
        fields.insert(format!("{field_prefix}{name}"), value.to_string());
    }
}

