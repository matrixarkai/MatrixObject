// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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

pub(crate) fn raft_voter_checksum(voters: &[String]) -> String {
    etag(voters.join("\n").as_bytes())
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
