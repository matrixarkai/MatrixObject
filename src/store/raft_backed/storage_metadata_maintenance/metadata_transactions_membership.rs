impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn propose_begin_meta_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::BeginMetaTransaction {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            ttl_ms,
            mutations,
            now_ms,
        })
    }

    pub fn propose_begin_meta_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        mutations: Vec<MetadataMutation>,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::BeginMetaTransactionIdempotent {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            ttl_ms,
            mutations,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_commit_meta_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitMetaTransaction {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            now_ms,
        })
    }

    pub fn propose_renew_meta_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewMetaTransaction {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            ttl_ms,
            now_ms,
        })
    }

    pub fn propose_renew_meta_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        ttl_ms: u64,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::RenewMetaTransactionIdempotent {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            ttl_ms,
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_commit_meta_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::CommitMetaTransactionIdempotent {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_abort_meta_transaction(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::AbortMetaTransaction {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            now_ms,
        })
    }

    pub fn propose_abort_meta_transaction_idempotent(
        &mut self,
        transaction_id: &str,
        owner: &str,
        now_ms: u64,
        idempotency_key: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::AbortMetaTransactionIdempotent {
            transaction_id: transaction_id.to_string(),
            owner: owner.to_string(),
            now_ms,
            idempotency_key: idempotency_key.to_string(),
        })
    }

    pub fn propose_expire_meta_transactions(
        &mut self,
        now_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ExpireMetaTransactions { now_ms })
    }

    pub fn propose_apply_raft_membership(
        &mut self,
        mutation_id: &str,
        voter_node_ids: Vec<String>,
        target_healthy_peer_count: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::ApplyRaftMembership {
            mutation_id: mutation_id.to_string(),
            voter_node_ids,
            target_healthy_peer_count,
            expected_previous_commit_index: self.store.last_applied_index(),
        })
    }

    pub fn prepare_apply_raft_membership_entry(
        &self,
        mutation_id: &str,
        voter_node_ids: Vec<String>,
        target_healthy_peer_count: u64,
    ) -> Result<PreparedRaftMembershipChange, ObjectError> {
        self.store.prepare_raft_membership_change_raft_entry(
            mutation_id.to_string(),
            voter_node_ids,
            target_healthy_peer_count,
            self.store.last_applied_index(),
        )
    }

    pub fn propose_prepared_raft_membership(
        &mut self,
        prepared: PreparedRaftMembershipChange,
    ) -> Result<ObjectMetadata, ObjectError> {
        if self.raft.commit_index() != self.store.last_applied_index() {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership requires raft/store commit alignment".to_string(),
            ));
        }
        self.store
            .clone()
            .apply_command(ObjectCommand::ApplyPreparedRaftMembership {
                prepared: prepared.clone(),
            })?;
        let entry = self
            .raft
            .append(ObjectCommand::ApplyPreparedRaftMembership { prepared })?;
        if entry.index != self.store.last_applied_index().saturating_add(1) {
            return Err(ObjectError::PreconditionFailed(
                "prepared raft membership index is not contiguous".to_string(),
            ));
        }
        self.store.apply_raft_entry(&entry)
    }

    pub fn propose_apply_raft_membership_prepared(
        &mut self,
        mutation_id: &str,
        voter_node_ids: Vec<String>,
        target_healthy_peer_count: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        let prepared = self.prepare_apply_raft_membership_entry(
            mutation_id,
            voter_node_ids,
            target_healthy_peer_count,
        )?;
        self.propose_prepared_raft_membership(prepared)
    }

    pub fn propose_staged_raft_membership_rollout(
        &mut self,
        mutation_id_prefix: &str,
        final_voter_node_ids: Vec<String>,
        final_target_healthy_peer_count: u64,
        max_phases: usize,
    ) -> Result<RaftMembershipRollout, ObjectError> {
        validate_idempotency_key(mutation_id_prefix)?;
        if max_phases == 0 {
            return Err(ObjectError::InvalidArgument(
                "raft membership rollout max_phases must be positive".to_string(),
            ));
        }
        let final_target_voter_node_ids = canonical_raft_voters(final_voter_node_ids)?;
        let initial_plan = self.store.plan_raft_membership_change(
            final_target_voter_node_ids.clone(),
            final_target_healthy_peer_count,
        )?;
        if !initial_plan.membership_plan_safe {
            return Err(ObjectError::PreconditionFailed(format!(
                "raft membership rollout is unsafe: {}",
                initial_plan.membership_plan_reject_reason
            )));
        }
        let mut rollout = RaftMembershipRollout {
            mutation_id_prefix: mutation_id_prefix.to_string(),
            final_target_voter_node_ids: final_target_voter_node_ids.clone(),
            final_target_healthy_peer_count,
            initial_plan: initial_plan.clone(),
            final_plan: initial_plan,
            prepared_phases: Vec::new(),
            applied_phases: Vec::new(),
            completed: false,
            phase_budget_limited: false,
            needs_read_index_refresh: false,
        };
        for phase in 0..max_phases {
            let plan = self.store.plan_raft_membership_change(
                final_target_voter_node_ids.clone(),
                final_target_healthy_peer_count,
            )?;
            if !plan.membership_plan_safe {
                return Err(ObjectError::PreconditionFailed(format!(
                    "raft membership rollout phase {} is unsafe: {}",
                    phase + 1,
                    plan.membership_plan_reject_reason
                )));
            }
            rollout.final_plan = plan.clone();
            if plan.current_voter_node_ids == final_target_voter_node_ids {
                rollout.completed = true;
                return Ok(rollout);
            }
            let mutation_id = format!("{mutation_id_prefix}-phase-{phase:04}", phase = phase + 1);
            validate_idempotency_key(&mutation_id)?;
            let prepared = self.prepare_apply_raft_membership_entry(
                &mutation_id,
                final_target_voter_node_ids.clone(),
                final_target_healthy_peer_count,
            )?;
            let applied = self.propose_prepared_raft_membership(prepared.clone())?;
            rollout.prepared_phases.push(prepared);
            rollout.applied_phases.push(applied);
            rollout.needs_read_index_refresh = true;
        }
        let final_plan = self.store.plan_raft_membership_change(
            final_target_voter_node_ids.clone(),
            final_target_healthy_peer_count,
        )?;
        rollout.completed = final_plan.current_voter_node_ids == final_target_voter_node_ids;
        rollout.phase_budget_limited = !rollout.completed;
        rollout.final_plan = final_plan;
        Ok(rollout)
    }

    pub fn propose_update_raft_peer_health(
        &mut self,
        healthy_peer_count: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::UpdateRaftPeerHealth { healthy_peer_count })
    }

    pub fn propose_gc_meta_transactions(
        &mut self,
        now_ms: u64,
        max_transactions: usize,
        min_age_ms: u64,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.propose(ObjectCommand::GcMetaTransactions {
            now_ms,
            max_transactions,
            min_age_ms,
        })
    }

}
