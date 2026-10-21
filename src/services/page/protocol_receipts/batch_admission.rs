// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkBatchStats {
    pub input_ranges: u64,
    pub mapped_chunk_ranges: u64,
    pub coalesced_chunk_ranges: u64,
    pub physical_chunk_write_count: u64,
    pub physical_chunk_write_bytes: u64,
    pub physical_chunk_discard_ranges: u64,
    pub physical_chunk_discard_bytes: u64,
    pub payload_bytes: u64,
    pub read_stats: ChunkReadStats,
}

impl ChunkBatchStats {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("input_ranges", self.input_ranges),
            ("mapped_chunk_ranges", self.mapped_chunk_ranges),
            ("coalesced_chunk_ranges", self.coalesced_chunk_ranges),
            (
                "physical_chunk_write_count",
                self.physical_chunk_write_count,
            ),
            (
                "physical_chunk_write_bytes",
                self.physical_chunk_write_bytes,
            ),
            (
                "physical_chunk_discard_ranges",
                self.physical_chunk_discard_ranges,
            ),
            (
                "physical_chunk_discard_bytes",
                self.physical_chunk_discard_bytes,
            ),
            ("payload_bytes", self.payload_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.extend(
            self.read_stats
                .to_control_fields(&format!("{field_prefix}read_stats.")),
        );
        fields
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkRangeAdmission {
    pub can_accept: bool,
    pub input_ranges: u64,
    pub mapped_chunk_ranges: u64,
    pub coalesced_chunk_ranges: u64,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub min_read_index: u64,
    pub min_commit_index: u64,
    pub max_chunk_read_count: u64,
    pub max_write_bytes: u64,
    pub max_chunk_write_count: u64,
    pub selected_write_range_count: u64,
    pub selected_write_payload_bytes: u64,
    pub write_start_range_index: u64,
    pub next_write_range_index: u64,
    pub write_has_more: bool,
    pub max_discard_bytes: u64,
    pub budget_limited: bool,
    pub ready_replica_count: u64,
    pub required_quorum: u64,
    pub writable_replica_count: u64,
    pub readable_chunk_count: u64,
    pub failed_chunk_count: u64,
    pub selected_discard_bytes: u64,
    pub skipped_discard_bytes: u64,
    pub skipped_discard_range_count: u64,
    pub discard_start_range_index: u64,
    pub next_discard_range_index: u64,
    pub discard_has_more: bool,
    pub read_stats: ChunkReadStats,
    pub partial_status: String,
    pub admission_reason: String,
    pub admission_proof_format: String,
    pub admission_proof_checksum: String,
    pub expected_admission_proof_checksum: String,
    pub actual_admission_proof_checksum: String,
    pub read_quorum_limited: bool,
    pub write_quorum_limited: bool,
    pub discard_budget_limited: bool,
    pub route_control_proof: ChunkRouteControlProof,
    pub expected_route_control_checksum: String,
    pub route_control_fenced: bool,
    pub route_control_fence_matched: bool,
}

impl ChunkRangeAdmission {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("input_ranges", self.input_ranges),
            ("mapped_chunk_ranges", self.mapped_chunk_ranges),
            ("coalesced_chunk_ranges", self.coalesced_chunk_ranges),
            ("payload_bytes", self.payload_bytes),
            ("max_payload_bytes", self.max_payload_bytes),
            ("min_read_index", self.min_read_index),
            ("min_commit_index", self.min_commit_index),
            ("max_chunk_read_count", self.max_chunk_read_count),
            ("max_write_bytes", self.max_write_bytes),
            ("max_chunk_write_count", self.max_chunk_write_count),
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
            ("max_discard_bytes", self.max_discard_bytes),
            ("ready_replica_count", self.ready_replica_count),
            ("required_quorum", self.required_quorum),
            ("writable_replica_count", self.writable_replica_count),
            ("readable_chunk_count", self.readable_chunk_count),
            ("failed_chunk_count", self.failed_chunk_count),
            ("selected_discard_bytes", self.selected_discard_bytes),
            ("skipped_discard_bytes", self.skipped_discard_bytes),
            (
                "skipped_discard_range_count",
                self.skipped_discard_range_count,
            ),
            ("discard_start_range_index", self.discard_start_range_index),
            ("next_discard_range_index", self.next_discard_range_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("can_accept", self.can_accept),
            ("budget_limited", self.budget_limited),
            ("read_quorum_limited", self.read_quorum_limited),
            ("write_quorum_limited", self.write_quorum_limited),
            ("write_has_more", self.write_has_more),
            ("discard_budget_limited", self.discard_budget_limited),
            ("discard_has_more", self.discard_has_more),
            ("route_control_fenced", self.route_control_fenced),
            (
                "route_control_fence_matched",
                self.route_control_fence_matched,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}partial_status"),
            self.partial_status.clone(),
        );
        fields.insert(
            format!("{field_prefix}admission_reason"),
            self.admission_reason.clone(),
        );
        fields.insert(
            format!("{field_prefix}admission_proof_format"),
            self.admission_proof_format.clone(),
        );
        fields.insert(
            format!("{field_prefix}admission_proof_checksum"),
            self.admission_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_admission_proof_checksum"),
            self.expected_admission_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}actual_admission_proof_checksum"),
            self.actual_admission_proof_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}expected_route_control_checksum"),
            self.expected_route_control_checksum.clone(),
        );
        fields.extend(
            self.read_stats
                .to_control_fields(&format!("{field_prefix}read_stats.")),
        );
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control.")),
        );
        fields
    }

    pub fn verify_admission_proof_format(&self) -> Result<(), ObjectError> {
        if self.admission_proof_format == STORAGE_ADMISSION_PROOF_FORMAT {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "page admission proof format mismatch: expected {} got {}",
            STORAGE_ADMISSION_PROOF_FORMAT, self.admission_proof_format
        )))
    }

    pub fn verify_expected_admission_proof_checksum(
        &self,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.verify_admission_proof_format()?;
        let expected_admission_proof_checksum = expected_admission_proof_checksum.as_ref();
        if expected_admission_proof_checksum.is_empty()
            || self.admission_proof_checksum == expected_admission_proof_checksum
        {
            return Ok(());
        }
        Err(ObjectError::PreconditionFailed(format!(
            "page admission proof checksum mismatch: expected {} got {}",
            expected_admission_proof_checksum, self.admission_proof_checksum
        )))
    }
}

pub(crate) fn select_admitted_page_discard_requests(
    requests: Vec<ChunkRangeRequest>,
    admission: &ChunkRangeAdmission,
) -> Result<Vec<ChunkRangeRequest>, ObjectError> {
    if !admission.can_accept {
        return Err(ObjectError::PreconditionFailed(
            "page batch discard admission rejected".to_string(),
        ));
    }
    if admission.next_discard_range_index < admission.discard_start_range_index {
        return Err(ObjectError::Corruption(
            "page batch discard admission cursor moved backwards".to_string(),
        ));
    }
    let request_count = requests.len() as u64;
    if admission.discard_start_range_index > request_count
        || admission.next_discard_range_index > request_count
    {
        return Err(ObjectError::Corruption(
            "page batch discard admission cursor exceeds request count".to_string(),
        ));
    }
    let selected_capacity = usize::try_from(
        admission
            .next_discard_range_index
            .saturating_sub(admission.discard_start_range_index),
    )
    .unwrap_or(requests.len())
    .min(requests.len());
    let mut selected = Vec::with_capacity(selected_capacity);
    let mut selected_bytes = 0u64;
    for (index, request) in requests.into_iter().enumerate() {
        let index = index as u64;
        if index < admission.discard_start_range_index
            || index >= admission.next_discard_range_index
        {
            continue;
        }
        if request.length == 0 {
            continue;
        }
        selected_bytes = selected_bytes.checked_add(request.length).ok_or_else(|| {
            ObjectError::InvalidArgument(
                "page batch discard selected byte count overflows".to_string(),
            )
        })?;
        selected.push(request);
    }
    if selected.is_empty() {
        return Err(ObjectError::PreconditionFailed(
            "page batch discard selected no ranges within max_discard_bytes".to_string(),
        ));
    }
    if selected_bytes != admission.selected_discard_bytes {
        return Err(ObjectError::Corruption(
            "page batch discard selected bytes do not match admission".to_string(),
        ));
    }
    Ok(selected)
}

impl ProvenAdmissionRead<ChunkRangeAdmission> {
    pub fn require_page_admission_proof_checksum(
        mut self,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        let expected_admission_proof_checksum = expected_admission_proof_checksum.as_ref();
        self.value
            .verify_expected_admission_proof_checksum(expected_admission_proof_checksum)?;
        self.value.expected_admission_proof_checksum =
            expected_admission_proof_checksum.to_string();
        self.value.actual_admission_proof_checksum = self.value.admission_proof_checksum.clone();
        Ok(self)
    }
}

impl ProvenPageRouteFencedAdmissionRead<ChunkRangeAdmission> {
    pub fn require_page_admission_proof_checksum(
        mut self,
        expected_admission_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        let expected_admission_proof_checksum = expected_admission_proof_checksum.as_ref();
        self.value
            .verify_expected_admission_proof_checksum(expected_admission_proof_checksum)?;
        self.value.expected_admission_proof_checksum =
            expected_admission_proof_checksum.to_string();
        self.value.actual_admission_proof_checksum = self.value.admission_proof_checksum.clone();
        Ok(self)
    }
}
