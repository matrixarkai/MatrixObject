// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PlacementReadProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("requested_placement_read_index", self.requested_placement_read_index),
            (
                "requested_placement_commit_index",
                self.requested_placement_commit_index,
            ),
            ("placement_applied_index", self.placement_applied_index),
            ("placement_read_index_lag", self.placement_read_index_lag),
            ("placement_raft_term", self.placement_raft_term),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}proof_format"),
            self.proof_format.clone(),
        );
        fields.insert(format!("{field_prefix}operation"), self.operation.clone());
        fields.insert(
            format!("{field_prefix}root_state_fingerprint"),
            self.placement_state_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}proof_checksum"),
            self.proof_checksum.clone(),
        );
        fields
    }
}

