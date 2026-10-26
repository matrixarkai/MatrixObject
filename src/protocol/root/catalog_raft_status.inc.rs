// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl PlacementRaftStatus {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = placement_raft_status_identity_fields(self, field_prefix);
        fields.extend(placement_raft_status_readiness_fields(self, field_prefix));
        fields.extend(placement_raft_status_metadata_fields(self, field_prefix));
        fields.extend(placement_raft_status_page_fields(self, field_prefix));
        fields.extend(placement_raft_status_chunk_fields(self, field_prefix));
        fields
    }
}
