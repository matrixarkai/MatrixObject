// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub command: ObjectCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftApplyReceipt {
    pub index: u64,
    pub term: u64,
    pub command_checksum: String,
    pub result: ObjectMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMembershipPlan {
    pub current_voter_node_ids: Vec<String>,
    pub target_voter_node_ids: Vec<String>,
    pub final_target_voter_node_ids: Vec<String>,
    pub added_voter_node_ids: Vec<String>,
    pub removed_voter_node_ids: Vec<String>,
    pub changed_voter_count: u64,
    pub staged_changed_voter_count: u64,
    pub current_voter_checksum: String,
    pub target_voter_checksum: String,
    pub final_target_voter_checksum: String,
    pub current_quorum: u64,
    pub target_quorum: u64,
    pub final_target_quorum: u64,
    pub target_healthy_peer_count: u64,
    pub final_target_healthy_peer_count: u64,
    pub membership_change_type: String,
    pub staged_membership_change_type: String,
    pub requires_joint_consensus: bool,
    pub staged_membership_required: bool,
    pub membership_plan_safe: bool,
    pub membership_plan_reject_reason: String,
}

impl RaftMembershipPlan {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        for (name, values) in [
            ("current_voter", &self.current_voter_node_ids),
            ("target_voter", &self.target_voter_node_ids),
            ("final_target_voter", &self.final_target_voter_node_ids),
            ("added_voter", &self.added_voter_node_ids),
            ("removed_voter", &self.removed_voter_node_ids),
        ] {
            fields.insert(
                format!("{field_prefix}{name}_count"),
                values.len().to_string(),
            );
            for (index, node_id) in values.iter().enumerate() {
                fields.insert(format!("{field_prefix}{name}.{index}"), node_id.clone());
            }
        }
        for (name, value) in [
            ("changed_voter_count", self.changed_voter_count),
            (
                "staged_changed_voter_count",
                self.staged_changed_voter_count,
            ),
            ("current_quorum", self.current_quorum),
            ("target_quorum", self.target_quorum),
            ("final_target_quorum", self.final_target_quorum),
            ("target_healthy_peer_count", self.target_healthy_peer_count),
            (
                "final_target_healthy_peer_count",
                self.final_target_healthy_peer_count,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        for (name, value) in [
            ("current_voter_checksum", &self.current_voter_checksum),
            ("target_voter_checksum", &self.target_voter_checksum),
            (
                "final_target_voter_checksum",
                &self.final_target_voter_checksum,
            ),
            ("membership_change_type", &self.membership_change_type),
            (
                "staged_membership_change_type",
                &self.staged_membership_change_type,
            ),
            (
                "membership_plan_reject_reason",
                &self.membership_plan_reject_reason,
            ),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.clone());
        }
        for (name, value) in [
            ("requires_joint_consensus", self.requires_joint_consensus),
            (
                "staged_membership_required",
                self.staged_membership_required,
            ),
            ("membership_plan_safe", self.membership_plan_safe),
        ] {
            fields.insert(format!("{field_prefix}{name}"), value.to_string());
        }
        fields
    }
}
