// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicaMutationBatch {
    batch_id: String,
    started_control_commit_index: u64,
    /// The undo, when this batch owns it. `None` when the caller that opened
    /// the batch holds the undo instead, which is what the page service does:
    /// its own batch snapshot already carries every replica, and its abort
    /// rebuilds the replicas from it, so a second copy here is never read.
    snapshot: Option<Box<ReplicaServiceSnapshot>>,
    /// Pages whose undo is a LENGTH rather than a copy of their old state.
    ///
    /// A page listed here is NOT in `snapshot.pages`: the undo gave up its
    /// handle so that the append could land in the live payload instead of in a
    /// copy of it. `MatrixReplicaService::materialise_page_undo` puts it back
    /// the moment anything happens to the page that a truncation cannot undo,
    /// and `abort_mutation_batch` rebuilds it by truncating.
    page_append_undo: BTreeMap<PageId, PageAppendUndo>,
}

/// What it takes to undo an append, when an append is all that happened.
///
/// Every field a write changes is either here or provably unchanged -- see the
/// note above `MatrixReplicaService::keep_page_undo_cheap_for_write`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PageAppendUndo {
    /// Truncate the payload back to this many bytes.
    payload_len: usize,
    logical_length: u64,
    write_sequence: u64,
    checksum_fnv1a64: u64,
    discarded_ranges: Vec<DataRange>,
    /// The sidecar as it was, by HANDLE and not by copy.
    ///
    /// `refresh_chunk_ec_sidecars` REPLACES both of these rather than writing
    /// through them, so holding the old handles copies nothing and keeps nothing
    /// alive that the page would otherwise have dropped. That is what lets the
    /// fast path apply to a replica that keeps a sidecar -- a single-replica
    /// store -- instead of only to one that has peers.
    ec_parity_fragment: Option<std::sync::Arc<Vec<u8>>>,
    ec_fragment_digests: Option<std::sync::Arc<Vec<u64>>>,
}

pub struct ReplicaMutationBatchStatus {
    pub active: bool,
    pub batch_id: String,
    pub started_control_commit_index: u64,
    pub current_control_commit_index: u64,
    pub page_count: u64,
    pub physical_bytes: u64,
    pub discarded_bytes: u64,
    pub write_receipt_count: u64,
    pub discard_receipt_count: u64,
    pub batch_write_receipt_count: u64,
    pub batch_discard_receipt_count: u64,
    pub committed: bool,
    pub aborted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkBatchWrite {
    pub replica_index: usize,
    pub writes: Vec<(u64, Vec<u8>)>,
    pub state: PageState,
    pub receipt: ReplicaBatchWriteReceipt,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub control_proof: ReplicaServiceControlProof,
    pub expected_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedChunkBatchWrite {
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
                "prepared chunk batch write raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkBatchWriteApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub receipt: ReplicaBatchWriteReceipt,
    pub metadata: ObjectMetadata,
}

impl PreparedChunkBatchWriteApplyReport {
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
pub struct PreparedChunkBatchDiscard {
    pub replica_index: usize,
    pub ranges: Vec<DataRange>,
    pub state: PageState,
    pub receipt: ReplicaBatchDiscardReceipt,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub control_proof: ReplicaServiceControlProof,
    pub expected_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedChunkBatchDiscard {
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
                "prepared chunk batch discard raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedChunkBatchDiscardApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub receipt: ReplicaBatchDiscardReceipt,
    pub metadata: ObjectMetadata,
}

impl PreparedChunkBatchDiscardApplyReport {
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

impl ProvenStorageRead<PreparedChunkBatchWrite> {
    pub fn require_chunk_batch_write_raft_entry_checksum(
        self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        Ok(self)
    }
}

impl ProvenStorageRead<PreparedChunkBatchDiscard> {
    pub fn require_chunk_batch_discard_raft_entry_checksum(
        self,
        expected_raft_entry_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.value
            .verify_expected_raft_entry_checksum(expected_raft_entry_checksum)?;
        Ok(self)
    }
}

pub(crate) fn verify_storage_receipt_page_invariants(
    service: &str,
    receipt_count: u64,
    total_receipt_count: u64,
    scanned_count: u64,
    limit: u64,
    truncated: bool,
    next_marker: &Option<String>,
    actual_receipt_count: usize,
    skipped_filter_count: u64,
) -> Result<(), ObjectError> {
    let actual_receipt_count = actual_receipt_count as u64;
    if receipt_count != actual_receipt_count {
        return Err(ObjectError::Corruption(format!(
            "{service} receipt page count mismatch: declared {receipt_count}, actual {actual_receipt_count}"
        )));
    }
    if actual_receipt_count > total_receipt_count {
        return Err(ObjectError::Corruption(format!(
            "{service} receipt page total count is smaller than returned receipt count"
        )));
    }
    if limit == 0 {
        return Err(ObjectError::Corruption(format!(
            "{service} receipt page limit must be positive"
        )));
    }
    if actual_receipt_count > limit {
        return Err(ObjectError::Corruption(format!(
            "{service} receipt page exceeds requested limit"
        )));
    }
    if scanned_count < actual_receipt_count.saturating_add(skipped_filter_count) {
        return Err(ObjectError::Corruption(format!(
            "{service} receipt page scan counters are inconsistent"
        )));
    }
    if truncated
        && next_marker
            .as_ref()
            .map_or(true, |marker| marker.is_empty())
    {
        return Err(ObjectError::Corruption(format!(
            "{service} truncated receipt page is missing next marker"
        )));
    }
    if !truncated && next_marker.is_some() {
        return Err(ObjectError::Corruption(format!(
            "{service} non-truncated receipt page has unexpected next marker"
        )));
    }
    Ok(())
}
