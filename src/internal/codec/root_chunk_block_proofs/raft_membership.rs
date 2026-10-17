pub(crate) fn canonical_raft_voters(mut voters: Vec<String>) -> Result<Vec<String>, ObjectError> {
    if voters.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "raft membership must contain at least one voter".to_string(),
        ));
    }
    for voter in &voters {
        validate_node_id(voter)?;
    }
    voters.sort();
    if voters.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ObjectError::InvalidArgument(
            "raft membership voter ids contain duplicates".to_string(),
        ));
    }
    if voters.is_empty() {
        return Err(ObjectError::InvalidArgument(
            "raft membership must contain at least one voter".to_string(),
        ));
    }
    Ok(voters)
}

pub(crate) fn raft_quorum(voter_count: u64) -> u64 {
    voter_count / 2 + 1
}

pub(crate) fn raft_voter_checksum(voters: &[String]) -> String {
    etag(voters.join("\n").as_bytes())
}

pub(crate) fn raft_membership_command_checksum(
    target_voters: &[String],
    target_healthy_peer_count: u64,
    expected_previous_commit_index: u64,
) -> String {
    etag(
        format!(
            "{}\n{}\n{}",
            target_voters.join("\n"),
            target_healthy_peer_count,
            expected_previous_commit_index
        )
        .as_bytes(),
    )
}

pub(crate) fn raft_membership_state_fingerprint(
    voters: &[String],
    healthy_peer_count: u64,
    membership_epoch: u64,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-RAFT-MEMBERSHIP-STATE1");
    append_u64(&mut material, membership_epoch);
    append_u64(&mut material, healthy_peer_count);
    append_u64(&mut material, voters.len() as u64);
    for voter in voters {
        append_string(&mut material, voter);
    }
    etag(&material)
}

pub(crate) fn append_raft_membership_plan(out: &mut Vec<u8>, plan: &RaftMembershipPlan) {
    append_u64(out, plan.current_voter_node_ids.len() as u64);
    for voter in &plan.current_voter_node_ids {
        append_string(out, voter);
    }
    append_u64(out, plan.target_voter_node_ids.len() as u64);
    for voter in &plan.target_voter_node_ids {
        append_string(out, voter);
    }
    append_u64(out, plan.final_target_voter_node_ids.len() as u64);
    for voter in &plan.final_target_voter_node_ids {
        append_string(out, voter);
    }
    append_u64(out, plan.added_voter_node_ids.len() as u64);
    for voter in &plan.added_voter_node_ids {
        append_string(out, voter);
    }
    append_u64(out, plan.removed_voter_node_ids.len() as u64);
    for voter in &plan.removed_voter_node_ids {
        append_string(out, voter);
    }
    append_u64(out, plan.changed_voter_count);
    append_u64(out, plan.staged_changed_voter_count);
    append_string(out, &plan.current_voter_checksum);
    append_string(out, &plan.target_voter_checksum);
    append_string(out, &plan.final_target_voter_checksum);
    append_u64(out, plan.current_quorum);
    append_u64(out, plan.target_quorum);
    append_u64(out, plan.final_target_quorum);
    append_u64(out, plan.target_healthy_peer_count);
    append_u64(out, plan.final_target_healthy_peer_count);
    append_string(out, &plan.membership_change_type);
    append_string(out, &plan.staged_membership_change_type);
    append_u64(out, plan.requires_joint_consensus as u64);
    append_u64(out, plan.staged_membership_required as u64);
    append_u64(out, plan.membership_plan_safe as u64);
    append_string(out, &plan.membership_plan_reject_reason);
}

pub(crate) fn prepared_raft_membership_change_checksum(
    prepared: &PreparedRaftMembershipChange,
) -> String {
    let mut material = Vec::new();
    append_string(&mut material, "MOS-RUST-PREPARED-RAFT-MEMBERSHIP1");
    append_string(&mut material, &prepared.mutation_id);
    append_u64(&mut material, prepared.target_voter_node_ids.len() as u64);
    for voter in &prepared.target_voter_node_ids {
        append_string(&mut material, voter);
    }
    append_u64(&mut material, prepared.target_healthy_peer_count);
    append_u64(&mut material, prepared.expected_previous_commit_index);
    append_u64(&mut material, prepared.prepared_commit_index);
    append_string(&mut material, &prepared.before_state_fingerprint);
    append_string(&mut material, &prepared.request_checksum);
    append_raft_membership_plan(&mut material, &prepared.plan);
    etag(&material)
}

pub(crate) fn validate_raft_membership_state(
    voters: &[String],
    healthy_peer_count: u64,
    membership_epoch: u64,
) -> Result<(), ObjectError> {
    if membership_epoch == 0 {
        return Err(ObjectError::Corruption(
            "raft membership epoch must be positive".to_string(),
        ));
    }
    let canonical = canonical_raft_voters(voters.to_vec()).map_err(|err| {
        ObjectError::Corruption(format!("raft membership voters are invalid: {err}"))
    })?;
    if canonical != voters {
        return Err(ObjectError::Corruption(
            "raft membership voters must be canonical".to_string(),
        ));
    }
    if healthy_peer_count > voters.len() as u64 {
        return Err(ObjectError::Corruption(
            "raft healthy peer count is invalid".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn plan_raft_membership_change(
    current_voters: &[String],
    target_voters: Vec<String>,
    target_healthy_peer_count: u64,
) -> Result<RaftMembershipPlan, ObjectError> {
    let current = canonical_raft_voters(current_voters.to_vec())?;
    let final_target = canonical_raft_voters(target_voters)?;
    if target_healthy_peer_count == 0 || target_healthy_peer_count > final_target.len() as u64 {
        return Err(ObjectError::InvalidArgument(
            "target healthy peer count must be 1..target voter count".to_string(),
        ));
    }
    let current_set: BTreeSet<_> = current.iter().cloned().collect();
    let final_target_set: BTreeSet<_> = final_target.iter().cloned().collect();
    let final_added = final_target_set
        .difference(&current_set)
        .cloned()
        .collect::<Vec<_>>();
    let final_removed = current_set
        .difference(&final_target_set)
        .cloned()
        .collect::<Vec<_>>();
    let final_changed_voter_count = final_added.len().saturating_add(final_removed.len()) as u64;
    let staged_membership_required = final_changed_voter_count > 1;
    let mut target = final_target.clone();
    if staged_membership_required {
        if let Some(first_added) = final_added.first() {
            target = current.clone();
            target.push(first_added.clone());
            target = canonical_raft_voters(target)?;
        } else if let Some(first_removed) = final_removed.first() {
            target = current
                .iter()
                .filter(|node| *node != first_removed)
                .cloned()
                .collect::<Vec<_>>();
            target = canonical_raft_voters(target)?;
        }
    }
    let target_healthy_for_stage = if staged_membership_required {
        target.len() as u64
    } else {
        target_healthy_peer_count
    };
    let target_set: BTreeSet<_> = target.iter().cloned().collect();
    let added = target_set
        .difference(&current_set)
        .cloned()
        .collect::<Vec<_>>();
    let removed = current_set
        .difference(&target_set)
        .cloned()
        .collect::<Vec<_>>();
    let current_quorum = raft_quorum(current.len() as u64);
    let target_quorum = raft_quorum(target.len() as u64);
    let final_target_quorum = raft_quorum(final_target.len() as u64);
    let membership_change_type = match (added.is_empty(), removed.is_empty()) {
        (true, true) => "no_change",
        (false, true) => "add_voter",
        (true, false) => "remove_voter",
        (false, false) => "replace_voter",
    }
    .to_string();
    let mut membership_plan_safe = true;
    let mut membership_plan_reject_reason = String::new();
    let staged_changed_voter_count = added.len().saturating_add(removed.len()) as u64;
    let requires_joint_consensus = staged_membership_required;
    if target_healthy_for_stage < target_quorum {
        membership_plan_safe = false;
        membership_plan_reject_reason = "target_quorum_unavailable".to_string();
    } else if current.len() > 1
        && (target_set.intersection(&current_set).count() as u64).saturating_add(added.len() as u64)
            < target_quorum
    {
        membership_plan_safe = false;
        membership_plan_reject_reason = "target_voters_do_not_preserve_quorum".to_string();
    }
    Ok(RaftMembershipPlan {
        current_voter_node_ids: current.clone(),
        target_voter_node_ids: target.clone(),
        final_target_voter_node_ids: final_target.clone(),
        added_voter_node_ids: added,
        removed_voter_node_ids: removed,
        changed_voter_count: final_changed_voter_count,
        staged_changed_voter_count,
        current_voter_checksum: raft_voter_checksum(&current),
        target_voter_checksum: raft_voter_checksum(&target),
        final_target_voter_checksum: raft_voter_checksum(&final_target),
        current_quorum,
        target_quorum,
        final_target_quorum,
        target_healthy_peer_count: target_healthy_for_stage,
        final_target_healthy_peer_count: target_healthy_peer_count,
        membership_change_type: if staged_membership_required {
            match (final_added.is_empty(), final_removed.is_empty()) {
                (true, true) => "no_change",
                (false, true) => "add_voter",
                (true, false) => "remove_voter",
                (false, false) => "replace_voter",
            }
            .to_string()
        } else {
            membership_change_type.clone()
        },
        staged_membership_change_type: membership_change_type,
        requires_joint_consensus,
        staged_membership_required,
        membership_plan_safe,
        membership_plan_reject_reason,
    })
}
