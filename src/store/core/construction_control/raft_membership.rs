impl MatrixObjectStore {
    pub fn plan_raft_membership_change(
        &self,
        voter_node_ids: Vec<String>,
        target_healthy_peer_count: u64,
    ) -> Result<RaftMembershipPlan, ObjectError> {
        plan_raft_membership_change(
            &self.raft_voter_node_ids,
            voter_node_ids,
            target_healthy_peer_count,
        )
    }

    pub fn apply_raft_membership_change(
        &mut self,
        mutation_id: String,
        voter_node_ids: Vec<String>,
        target_healthy_peer_count: u64,
        expected_previous_commit_index: u64,
    ) -> Result<RaftMembershipReceipt, ObjectError> {
        validate_idempotency_key(&mutation_id)?;
        let plan = self.plan_raft_membership_change(voter_node_ids, target_healthy_peer_count)?;
        if !plan.membership_plan_safe {
            return Err(ObjectError::PreconditionFailed(format!(
                "raft membership change is unsafe: {}",
                plan.membership_plan_reject_reason
            )));
        }
        if expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "raft membership previous commit fence mismatch".to_string(),
            ));
        }
        let command_checksum = raft_membership_command_checksum(
            &plan.target_voter_node_ids,
            plan.target_healthy_peer_count,
            expected_previous_commit_index,
        );
        if let Some(existing) = self.raft_membership_receipts.get(&mutation_id) {
            if existing.command_checksum != command_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "raft membership mutation id reused with different command".to_string(),
                ));
            }
            return Ok(existing.clone());
        }
        self.raft_voter_node_ids = plan.target_voter_node_ids.clone();
        self.raft_healthy_peer_count = plan
            .target_healthy_peer_count
            .min(self.raft_voter_node_ids.len() as u64);
        self.raft_membership_epoch = self.raft_membership_epoch.saturating_add(1);
        let receipt = RaftMembershipReceipt {
            mutation_id: mutation_id.clone(),
            target_voter_node_ids: self.raft_voter_node_ids.clone(),
            target_voter_checksum: plan.target_voter_checksum,
            target_healthy_peer_count: self.raft_healthy_peer_count,
            expected_previous_commit_index,
            membership_epoch: self.raft_membership_epoch,
            command_checksum,
        };
        self.raft_membership_receipts
            .insert(mutation_id, receipt.clone());
        Ok(receipt)
    }

    pub fn prepare_raft_membership_change_raft_entry(
        &self,
        mutation_id: String,
        voter_node_ids: Vec<String>,
        target_healthy_peer_count: u64,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedRaftMembershipChange, ObjectError> {
        validate_idempotency_key(&mutation_id)?;
        if expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "raft membership previous commit fence mismatch".to_string(),
            ));
        }
        let plan = self.plan_raft_membership_change(voter_node_ids, target_healthy_peer_count)?;
        if !plan.membership_plan_safe {
            return Err(ObjectError::PreconditionFailed(format!(
                "raft membership change is unsafe: {}",
                plan.membership_plan_reject_reason
            )));
        }
        let request_checksum = raft_membership_command_checksum(
            &plan.target_voter_node_ids,
            plan.target_healthy_peer_count,
            expected_previous_commit_index,
        );
        if let Some(existing) = self.raft_membership_receipts.get(&mutation_id) {
            if existing.command_checksum != request_checksum {
                return Err(ObjectError::PreconditionFailed(
                    "raft membership mutation id reused with different command".to_string(),
                ));
            }
        }
        let mut prepared = PreparedRaftMembershipChange {
            mutation_id,
            target_voter_node_ids: plan.target_voter_node_ids.clone(),
            target_healthy_peer_count: plan.target_healthy_peer_count,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            before_state_fingerprint: raft_membership_state_fingerprint(
                &self.raft_voter_node_ids,
                self.raft_healthy_peer_count,
                self.raft_membership_epoch,
            ),
            request_checksum,
            plan,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_raft_membership_change_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_raft_membership_change(
        &mut self,
        prepared: PreparedRaftMembershipChange,
    ) -> Result<RaftMembershipReceipt, ObjectError> {
        validate_idempotency_key(&prepared.mutation_id)?;
        let expected_checksum =
            prepared_raft_membership_change_checksum(&PreparedRaftMembershipChange {
                raft_entry_checksum: String::new(),
                ..prepared.clone()
            });
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership checksum mismatch".to_string(),
            ));
        }
        let request_checksum = raft_membership_command_checksum(
            &prepared.target_voter_node_ids,
            prepared.target_healthy_peer_count,
            prepared.expected_previous_commit_index,
        );
        if prepared.request_checksum != request_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership request checksum mismatch".to_string(),
            ));
        }
        if prepared.prepared_commit_index
            != prepared.expected_previous_commit_index.saturating_add(1)
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership commit index is not contiguous".to_string(),
            ));
        }
        if prepared.expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint
            != raft_membership_state_fingerprint(
                &self.raft_voter_node_ids,
                self.raft_healthy_peer_count,
                self.raft_membership_epoch,
            )
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership source state changed".to_string(),
            ));
        }
        let plan_target_voters = if prepared.plan.staged_membership_required {
            prepared.plan.final_target_voter_node_ids.clone()
        } else {
            prepared.target_voter_node_ids.clone()
        };
        let plan_target_healthy_peer_count = if prepared.plan.staged_membership_required {
            prepared.plan.final_target_healthy_peer_count
        } else {
            prepared.target_healthy_peer_count
        };
        let plan =
            self.plan_raft_membership_change(plan_target_voters, plan_target_healthy_peer_count)?;
        if plan != prepared.plan {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership plan no longer matches current state".to_string(),
            ));
        }
        self.apply_raft_membership_change(
            prepared.mutation_id,
            prepared.target_voter_node_ids,
            prepared.target_healthy_peer_count,
            prepared.expected_previous_commit_index,
        )
    }

    pub fn update_raft_healthy_peer_count(
        &mut self,
        healthy_peer_count: u64,
    ) -> Result<u64, ObjectError> {
        if healthy_peer_count > self.raft_voter_node_ids.len() as u64 {
            return Err(ObjectError::InvalidArgument(
                "healthy peer count exceeds raft voter count".to_string(),
            ));
        }
        self.raft_healthy_peer_count = healthy_peer_count;
        Ok(self.raft_healthy_peer_count)
    }

}
