// SPDX-License-Identifier: Apache-2.0
impl PreparedPageAllocationReservationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("length", self.length),
            ("now_ms", self.now_ms),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("allocation_offset", self.allocation_offset),
            ("allocation_length", self.allocation_length),
            ("allocation_create_time_ms", self.allocation_create_time_ms),
            (
                "pending_allocation_count_after",
                self.pending_allocation_count_after,
            ),
            (
                "allocation_receipt_count_after",
                self.allocation_receipt_count_after,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("idempotency_key", &self.idempotency_key),
            ("request_checksum", &self.request_checksum),
            (
                "before_block_service_fingerprint",
                &self.before_page_service_fingerprint,
            ),
            (
                "after_block_service_fingerprint",
                &self.after_page_service_fingerprint,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("allocation_id", &self.allocation_id),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields.extend(
            self.allocation_chunk_id
                .to_control_fields(&format!("{field_prefix}allocation_segment.")),
        );
        fields
    }
}

impl ProvenPreparedPageAllocationReservationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.envelope.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}

impl PreparedPageBatchAllocationReservationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("length_count", self.lengths.len() as u64),
            ("now_ms", self.now_ms),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("allocation_count", self.allocation_count),
            (
                "pending_allocation_count_after",
                self.pending_allocation_count_after,
            ),
            (
                "allocation_receipt_count_after",
                self.allocation_receipt_count_after,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (index, length) in self.lengths.iter().enumerate() {
            fields.insert(format!("{field_prefix}length.{index}"), length.to_string());
        }
        for (index, allocation_id) in self.allocation_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}allocation.{index}.allocation_id"),
                allocation_id.clone(),
            );
        }
        for (index, chunk_id) in self.allocation_chunks.iter().enumerate() {
            fields.extend(
                chunk_id.to_control_fields(&format!("{field_prefix}allocation.{index}.segment.")),
            );
        }
        for (index, offset) in self.allocation_offsets.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}allocation.{index}.offset"),
                offset.to_string(),
            );
        }
        for (index, length) in self.allocation_lengths.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}allocation.{index}.length"),
                length.to_string(),
            );
        }
        for (index, create_time_ms) in self.allocation_create_time_ms.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}allocation.{index}.create_time_ms"),
                create_time_ms.to_string(),
            );
        }
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("idempotency_key", &self.idempotency_key),
            ("request_checksum", &self.request_checksum),
            (
                "before_block_service_fingerprint",
                &self.before_page_service_fingerprint,
            ),
            (
                "after_block_service_fingerprint",
                &self.after_page_service_fingerprint,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields
    }
}

impl ProvenPreparedPageBatchAllocationReservationEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.envelope.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}

impl PreparedPageAllocationReleaseEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("allocation_offset", self.allocation_offset),
            ("allocation_length", self.allocation_length),
            ("allocation_create_time_ms", self.allocation_create_time_ms),
            (
                "pending_allocation_count_after",
                self.pending_allocation_count_after,
            ),
            (
                "release_receipt_count_after",
                self.release_receipt_count_after,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            ("allocation_id", &self.allocation_id),
            ("idempotency_key", &self.idempotency_key),
            ("request_checksum", &self.request_checksum),
            (
                "before_block_service_fingerprint",
                &self.before_page_service_fingerprint,
            ),
            (
                "after_block_service_fingerprint",
                &self.after_page_service_fingerprint,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(format!("{field_prefix}released"), self.released.to_string());
        fields.insert(format!("{field_prefix}deduped"), self.deduped.to_string());
        fields.extend(
            self.route_control_proof
                .to_control_fields(&format!("{field_prefix}route_control_proof.")),
        );
        fields.extend(
            self.allocation_chunk_id
                .to_control_fields(&format!("{field_prefix}allocation_segment.")),
        );
        fields
    }
}

impl ProvenPreparedPageAllocationReleaseEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.envelope.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}

impl PreparedPageAuditRepairEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "first_candidate_status",
            &Some(self.first_candidate_status.clone()).filter(|value| !value.is_empty()),
        );
        for (name, value) in [
            ("max_chunks", self.max_chunks),
            ("max_payload_bytes", self.max_payload_bytes),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
            ("route_control_proof_bytes", self.route_control_proof_bytes),
            (
                "route_control_commit_index",
                self.route_control_commit_index,
            ),
            (
                "route_control_next_log_index",
                self.route_control_next_log_index,
            ),
            ("scanned_chunks", self.scanned_chunks),
            ("candidate_chunks", self.candidate_chunks),
            ("repaired_chunks", self.repaired_chunks),
            ("unrepaired_chunks", self.unrepaired_chunks),
            ("missing_replicas", self.missing_replicas),
            ("corrupt_replicas", self.corrupt_replicas),
            ("divergent_replicas", self.divergent_replicas),
            ("quarantined_replicas", self.quarantined_replicas),
            ("frozen_replicas", self.frozen_replicas),
            ("payload_bytes", self.payload_bytes),
            ("checked_chunks", self.checked_chunks),
            ("degraded_chunks", self.degraded_chunks),
            ("repaired_replicas", self.repaired_replicas),
            ("failed_repairs", self.failed_repairs),
            (
                "first_candidate_logical_length",
                self.first_candidate_logical_length,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("envelope_format", &self.envelope_format),
            (
                "before_block_service_fingerprint",
                &self.before_page_service_fingerprint,
            ),
            (
                "after_block_service_fingerprint",
                &self.after_page_service_fingerprint,
            ),
            (
                "route_control_proof_format",
                &self.route_control_proof_format,
            ),
            (
                "route_control_proof_checksum",
                &self.route_control_proof_checksum,
            ),
            (
                "expected_route_control_checksum",
                &self.expected_route_control_checksum,
            ),
            ("raft_entry_checksum", &self.raft_entry_checksum),
            ("repair_reason", &self.repair_reason),
            ("envelope_checksum", &self.envelope_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("has_more", self.has_more),
            ("repair_needed", self.repair_needed),
            ("repair_can_apply", self.repair_can_apply),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        if let Some(page_id) = &self.after_chunk_id {
            fields.extend(page_id.to_control_fields(&format!("{field_prefix}after_chunk.")));
        }
        if let Some(page_id) = &self.first_candidate_chunk_id {
            fields.extend(
                page_id.to_control_fields(&format!("{field_prefix}first_candidate_chunk.")),
            );
        }
        if let Some(replica_index) = self.first_candidate_replica_index {
            fields.insert(
                format!("{field_prefix}first_candidate_replica_index"),
                replica_index.to_string(),
            );
        }
        fields
    }
}

impl ProvenPreparedPageAuditRepairEnvelope {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self.envelope.to_control_fields(field_prefix);
        fields.extend(
            self.proof
                .to_control_fields(&format!("{field_prefix}storage_read_proof.")),
        );
        fields
    }
}
