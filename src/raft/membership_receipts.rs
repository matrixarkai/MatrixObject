#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMembershipReceipt {
    pub mutation_id: String,
    pub target_voter_node_ids: Vec<String>,
    pub target_voter_checksum: String,
    pub target_healthy_peer_count: u64,
    pub expected_previous_commit_index: u64,
    pub membership_epoch: u64,
    pub command_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedRaftMembershipChange {
    pub mutation_id: String,
    pub target_voter_node_ids: Vec<String>,
    pub target_healthy_peer_count: u64,
    pub expected_previous_commit_index: u64,
    pub prepared_commit_index: u64,
    pub before_state_fingerprint: String,
    pub request_checksum: String,
    pub plan: RaftMembershipPlan,
    pub raft_entry_checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMembershipRollout {
    pub mutation_id_prefix: String,
    pub final_target_voter_node_ids: Vec<String>,
    pub final_target_healthy_peer_count: u64,
    pub initial_plan: RaftMembershipPlan,
    pub final_plan: RaftMembershipPlan,
    pub prepared_phases: Vec<PreparedRaftMembershipChange>,
    pub applied_phases: Vec<ObjectMetadata>,
    pub completed: bool,
    pub phase_budget_limited: bool,
    pub needs_read_index_refresh: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RaftMembershipReceiptPage {
    pub receipts: Vec<RaftMembershipReceipt>,
    pub receipt_count: u64,
    pub total_receipt_count: u64,
    pub scanned_count: u64,
    pub limit: u64,
    pub truncated: bool,
    pub next_marker: Option<String>,
    pub target_voter_node_id: Option<String>,
    pub target_voter_checksum: Option<String>,
    pub quorum_preserved: Option<bool>,
    pub membership_plan_safe: Option<bool>,
    pub skipped_target_voter_count: u64,
    pub skipped_target_checksum_count: u64,
    pub skipped_quorum_preserved_count: u64,
    pub skipped_plan_safe_count: u64,
}

impl RaftMembershipReceipt {
    pub fn proof_checksum(&self) -> String {
        raft_membership_receipt_proof_checksum(self)
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}mutation_id"),
            self.mutation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}target_voter_checksum"),
            self.target_voter_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}command_checksum"),
            self.command_checksum.clone(),
        );
        fields.insert(
            format!("{field_prefix}target_voter_count"),
            self.target_voter_node_ids.len().to_string(),
        );
        for (index, node_id) in self.target_voter_node_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}target_voter.{index}"),
                node_id.clone(),
            );
        }
        for (name, value) in [
            ("target_healthy_peer_count", self.target_healthy_peer_count),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("membership_epoch", self.membership_epoch),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}

impl PreparedRaftMembershipChange {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, value) in [
            ("mutation_id", &self.mutation_id),
            ("before_state_fingerprint", &self.before_state_fingerprint),
            ("request_checksum", &self.request_checksum),
            ("raft_entry_checksum", &self.raft_entry_checksum),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        fields.insert(
            format!("{field_prefix}target_voter_count"),
            self.target_voter_node_ids.len().to_string(),
        );
        for (index, node_id) in self.target_voter_node_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}target_voter.{index}"),
                node_id.clone(),
            );
        }
        for (name, value) in [
            ("target_healthy_peer_count", self.target_healthy_peer_count),
            (
                "expected_previous_commit_index",
                self.expected_previous_commit_index,
            ),
            ("prepared_commit_index", self.prepared_commit_index),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.extend(self.plan.to_control_fields(&format!("{field_prefix}plan.")));
        fields
    }
}

impl RaftMembershipRollout {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        fields.insert(
            format!("{field_prefix}mutation_id_prefix"),
            self.mutation_id_prefix.clone(),
        );
        fields.insert(
            format!("{field_prefix}final_target_voter_count"),
            self.final_target_voter_node_ids.len().to_string(),
        );
        for (index, node_id) in self.final_target_voter_node_ids.iter().enumerate() {
            fields.insert(
                format!("{field_prefix}final_target_voter.{index}"),
                node_id.clone(),
            );
        }
        fields.insert(
            format!("{field_prefix}final_target_healthy_peer_count"),
            self.final_target_healthy_peer_count.to_string(),
        );
        fields.insert(
            format!("{field_prefix}prepared_phase_count"),
            self.prepared_phases.len().to_string(),
        );
        fields.insert(
            format!("{field_prefix}applied_phase_count"),
            self.applied_phases.len().to_string(),
        );
        for (name, value) in [
            ("completed", self.completed),
            ("phase_budget_limited", self.phase_budget_limited),
            ("needs_read_index_refresh", self.needs_read_index_refresh),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.extend(
            self.initial_plan
                .to_control_fields(&format!("{field_prefix}initial_plan.")),
        );
        fields.extend(
            self.final_plan
                .to_control_fields(&format!("{field_prefix}final_plan.")),
        );
        for (index, prepared) in self.prepared_phases.iter().enumerate() {
            fields.extend(
                prepared.to_control_fields(&format!("{field_prefix}prepared_phase.{index}.")),
            );
        }
        for (index, metadata) in self.applied_phases.iter().enumerate() {
            fields.extend(
                metadata.to_control_fields(&format!("{field_prefix}applied_phase.{index}.")),
            );
        }
        fields
    }
}

impl RaftMembershipReceiptPage {
    pub fn proof_format(&self) -> &'static str {
        "MXOS-RAFT-MEMBERSHIP-RECEIPT-PAGE-PROOF1"
    }

    pub fn proof_checksum(&self) -> String {
        raft_membership_receipt_page_proof_checksum(self)
    }

    pub fn verify_expected_proof_checksum(
        &self,
        expected_raft_membership_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        let expected_raft_membership_receipt_page_proof_checksum =
            expected_raft_membership_receipt_page_proof_checksum.as_ref();
        if expected_raft_membership_receipt_page_proof_checksum.is_empty() {
            return Ok(());
        }
        let actual = self.proof_checksum();
        if actual != expected_raft_membership_receipt_page_proof_checksum {
            return Err(ObjectError::PreconditionFailed(format!(
                "raft membership receipt page proof checksum mismatch: expected {} got {}",
                expected_raft_membership_receipt_page_proof_checksum, actual
            )));
        }
        Ok(())
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_optional_string_field(&mut fields, field_prefix, "next_marker", &self.next_marker);
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "target_voter_node_id",
            &self.target_voter_node_id,
        );
        append_optional_string_field(
            &mut fields,
            field_prefix,
            "target_voter_checksum",
            &self.target_voter_checksum,
        );
        if let Some(quorum_preserved) = self.quorum_preserved {
            fields.insert(
                format!("{field_prefix}quorum_preserved"),
                quorum_preserved.to_string(),
            );
        }
        if let Some(membership_plan_safe) = self.membership_plan_safe {
            fields.insert(
                format!("{field_prefix}membership_plan_safe"),
                membership_plan_safe.to_string(),
            );
        }
        for (name, value) in [
            ("receipt_count", self.receipt_count),
            ("total_receipt_count", self.total_receipt_count),
            ("scanned_count", self.scanned_count),
            ("limit", self.limit),
            (
                "skipped_target_voter_count",
                self.skipped_target_voter_count,
            ),
            (
                "skipped_target_checksum_count",
                self.skipped_target_checksum_count,
            ),
            (
                "skipped_quorum_preserved_count",
                self.skipped_quorum_preserved_count,
            ),
            ("skipped_plan_safe_count", self.skipped_plan_safe_count),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields.insert(
            format!("{field_prefix}truncated"),
            self.truncated.to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_membership_receipt_page_proof_format"),
            self.proof_format().to_string(),
        );
        fields.insert(
            format!("{field_prefix}raft_membership_receipt_page_proof_checksum"),
            self.proof_checksum(),
        );
        for (index, receipt) in self.receipts.iter().enumerate() {
            fields.extend(receipt.to_control_fields(&format!("{field_prefix}receipt.{index}.")));
        }
        fields
    }
}

impl ProvenReceiptRead<RaftMembershipReceiptPage> {
    pub fn verify_expected_raft_membership_receipt_page_proof_checksum(
        &self,
        expected_raft_membership_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<(), ObjectError> {
        self.value
            .verify_expected_proof_checksum(expected_raft_membership_receipt_page_proof_checksum)
    }

    pub fn require_raft_membership_receipt_page_proof_checksum(
        self,
        expected_raft_membership_receipt_page_proof_checksum: impl AsRef<str>,
    ) -> Result<Self, ObjectError> {
        self.verify_expected_raft_membership_receipt_page_proof_checksum(
            expected_raft_membership_receipt_page_proof_checksum,
        )?;
        Ok(self)
    }
}
