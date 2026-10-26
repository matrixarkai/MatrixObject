// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PlacementReadProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("requested_root_read_index", self.requested_root_read_index),
            (
                "requested_root_commit_index",
                self.requested_root_commit_index,
            ),
            ("root_applied_index", self.root_applied_index),
            ("root_read_index_lag", self.root_read_index_lag),
            ("root_raft_term", self.root_raft_term),
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
            self.root_state_fingerprint.clone(),
        );
        fields.insert(
            format!("{field_prefix}proof_checksum"),
            self.proof_checksum.clone(),
        );
        fields
    }
}

