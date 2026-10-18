#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscardPlan {
    pub slab_id: SlabId,
    pub ranges: Vec<DataRange>,
    pub discard_bytes: u64,
    pub fragmented: bool,
    pub compact_instead: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSlabDiscard {
    pub slab_id: SlabId,
    pub ranges: Vec<DataRange>,
    pub discard_bytes: u64,
    pub source_stale_ranges: Vec<DataRange>,
    pub source_discarded_ranges: Vec<DataRange>,
    pub before_state_fingerprint: String,
    pub after_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
}

impl PreparedSlabDiscard {
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
                "prepared segment discard raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSlabDiscardEnvelope {
    pub envelope_format: String,
    pub slab_id: SlabId,
    pub range_count: u64,
    pub discard_bytes: u64,
    pub source_stale_range_count: u64,
    pub source_discarded_range_count: u64,
    pub first_range_offset: u64,
    pub first_range_length: u64,
    pub before_state_fingerprint: String,
    pub after_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub expected_route_control_checksum: String,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SlabDiscardRunReport {
    pub planned_slabs: u64,
    pub planned_discard_bytes: u64,
    pub skipped_compaction_slabs: u64,
    pub skipped_limit_slabs: u64,
    pub attempted_slabs: u64,
    pub discarded_slabs: u64,
    pub failed_slabs: u64,
    pub discard_bytes: u64,
    pub first_error: String,
    pub discarded_slab_ids: Vec<SlabId>,
    pub compaction_slab_ids: Vec<SlabId>,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSlabCompaction {
    pub slab_id: SlabId,
    pub min_stale_bytes: u64,
    pub update_time_ms: u64,
    pub live_objects: Vec<ObjectId>,
    pub source_stale_ranges: Vec<DataRange>,
    pub source_discarded_ranges: Vec<DataRange>,
    pub before_state_fingerprint: String,
    pub after_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub expected_object_gc_report_proof_checksum: String,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSlabCompactionApplyReport {
    pub mutation_id: String,
    pub mutation_request_checksum: String,
    pub mutation_commit_index: u64,
    pub next_log_index: u64,
    pub committed: bool,
    pub prepared: bool,
    pub external_raft_required: bool,
    pub deduped: bool,
    pub slab_id: SlabId,
    pub min_stale_bytes: u64,
    pub live_object_count: u64,
    pub source_stale_range_count: u64,
    pub source_stale_bytes: u64,
    pub source_discarded_range_count: u64,
    pub source_discarded_bytes: u64,
    pub expected_object_gc_report_proof_checksum: String,
    pub metadata: ObjectMetadata,
}

impl PreparedSlabCompactionApplyReport {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .slab_id
            .to_control_fields(&format!("{field_prefix}segment."));
        for (name, value) in [
            ("mutation_commit_index", self.mutation_commit_index),
            ("next_log_index", self.next_log_index),
            ("min_stale_bytes", self.min_stale_bytes),
            ("live_object_count", self.live_object_count),
            ("source_stale_range_count", self.source_stale_range_count),
            ("source_stale_bytes", self.source_stale_bytes),
            (
                "source_discarded_range_count",
                self.source_discarded_range_count,
            ),
            ("source_discarded_bytes", self.source_discarded_bytes),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}mutation_request_checksum"),
            self.mutation_request_checksum.clone(),
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
        fields.insert(
            format!("{field_prefix}expected_object_gc_report_proof_checksum"),
            self.expected_object_gc_report_proof_checksum.clone(),
        );
        fields.extend(
            self.metadata
                .to_control_fields(&format!("{field_prefix}metadata.")),
        );
        fields
    }
}

impl PreparedSlabCompaction {
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
                "prepared segment compaction raft entry checksum mismatch: expected {} got {}",
                expected_raft_entry_checksum, self.raft_entry_checksum
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSlabCompactionEnvelope {
    pub envelope_format: String,
    pub slab_id: SlabId,
    pub min_stale_bytes: u64,
    pub update_time_ms: u64,
    pub live_object_count: u64,
    pub source_stale_range_count: u64,
    pub source_discarded_range_count: u64,
    pub first_live_object_bucket: String,
    pub first_live_object_key: String,
    pub before_state_fingerprint: String,
    pub after_state_fingerprint: String,
    pub request_checksum: String,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub route_control_proof_format: String,
    pub route_control_proof_checksum: String,
    pub route_control_proof_bytes: u64,
    pub route_control_commit_index: u64,
    pub route_control_next_log_index: u64,
    pub expected_route_control_checksum: String,
    pub expected_object_gc_report_proof_checksum: String,
    pub raft_entry_checksum: String,
    pub envelope_checksum: String,
}

