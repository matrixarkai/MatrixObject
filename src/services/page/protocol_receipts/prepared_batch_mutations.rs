// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchWrite {
    pub writes: Vec<ChunkRangeWrite>,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub receipt: ChunkBatchWriteReceipt,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedPageBatchWrite {
    pub fn verify_expected_raft_entry_checksum(
        &self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Ok(());
        }
        if self.raft_entry_checksum != expected_raft_entry_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared page batch write raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchWriteApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub receipt: ChunkBatchWriteReceipt,
    pub metadata: ObjectMetadata,
}

impl PreparedPageBatchWriteApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .receipt
            .to_control_fields(&format!("{field_prefix}receipt."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_commit_index"),
            self.mutation_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_log_index"),
            self.next_log_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}committed"),
            self.committed.to_string(),
        );
        fields.insert(format!("{field_prefix}prepared"), self.prepared.to_string());
        fields.insert(
            format!("{field_prefix}external_raft_required"),
            self.external_raft_required.to_string(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchDiscard {
    pub requests: Vec<ChunkRangeRequest>,
    pub before_page_service_fingerprint: String,
    pub after_page_service_fingerprint: String,
    pub receipt: ChunkBatchDiscardReceipt,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedPageBatchDiscard {
    pub fn verify_expected_raft_entry_checksum(
        &self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_raft_entry_checksum = expected_raft_entry_checksum.as_ref();
        if expected_raft_entry_checksum.is_empty() {
            return Ok(());
        }
        if self.raft_entry_checksum != expected_raft_entry_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "prepared page batch discard raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedPageBatchDiscardApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub receipt: ChunkBatchDiscardReceipt,
    pub metadata: ObjectMetadata,
}

impl PreparedPageBatchDiscardApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .receipt
            .to_control_fields(&format!("{field_prefix}receipt."));
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_commit_index"),
            self.mutation_commit_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}next_log_index"),
            self.next_log_index.to_string(),
        );
        fields.insert(
            format!("{field_prefix}committed"),
            self.committed.to_string(),
        );
        fields.insert(format!("{field_prefix}prepared"), self.prepared.to_string());
        fields.insert(
            format!("{field_prefix}external_raft_required"),
            self.external_raft_required.to_string(),
        );
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

impl ProvenStorageRead<PreparedPageBatchWrite> {
    pub fn require_page_batch_write_raft_entry_checksum(
        self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        Ok(self)
    }
}

impl ProvenStorageRead<PreparedPageBatchDiscard> {
    pub fn require_page_batch_discard_raft_entry_checksum(
        self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        Ok(self)
    }
}

