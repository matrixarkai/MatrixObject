impl RootRaftStatus {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = root_raft_status_identity_fields(self, field_prefix);
        fields.extend(root_raft_status_readiness_fields(self, field_prefix));
        fields.extend(root_raft_status_metadata_fields(self, field_prefix));
        fields.extend(root_raft_status_page_fields(self, field_prefix));
        fields.extend(root_raft_status_chunk_fields(self, field_prefix));
        fields
    }
}
