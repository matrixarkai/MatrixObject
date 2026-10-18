#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageChunkControlProof {
    pub proof_count: u64,
    pub proof_checksum: String,
    pub min_raft_commit_index: u64,
    pub max_raft_commit_index: u64,
    pub replica_service_state_checksums: BTreeMap<usize, String>,
}

impl PageChunkControlProof {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}proof_count"),
            self.proof_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}proof_checksum"),
            self.proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}min_raft_commit_index"),
            self.min_raft_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}max_raft_commit_index"),
            self.max_raft_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}replica_service_state_checksum_count"),
            self.replica_service_state_checksums.len().to_string(),
        );
        if let Some((index, checksum)) = self.replica_service_state_checksums.iter().next() {
            fields.insert(
                format!("{field_prefix}first_replica_service_state_index"),
                index.to_string(),
            );
            fields.insert(
                format!("{field_prefix}first_replica_service_state_checksum"),
                checksum.clone(),
            );
        }
        for (index, checksum) in &self.replica_service_state_checksums {
            fields.insert(
                format!("{field_prefix}replica_service_state_checksum.{index}"),
                checksum.clone(),
            );
        }
        fields
    }
}
