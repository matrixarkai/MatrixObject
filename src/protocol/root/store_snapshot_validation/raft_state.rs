impl StoreSnapshot {
    fn validate_raft_state(&self) -> Result<(), ObjectError> {
        if self.last_applied_index > 0 && self.last_applied_term == 0 {
            return Err(ObjectError::Corruption(
                "snapshot applied index has no raft term".to_string(),
            ));
        }
        if self.applied_raft_receipts.len() > k_max_raft_apply_receipts() {
            return Err(ObjectError::Corruption(
                "snapshot retained raft apply receipt count exceeds limit".to_string(),
            ));
        }
        validate_raft_membership_state(
            &self.raft_voter_node_ids,
            self.raft_healthy_peer_count,
            self.raft_membership_epoch,
        )?;
        for (mutation_id, receipt) in &self.raft_membership_receipts {
            validate_idempotency_key(mutation_id).map_err(|err| {
                ObjectError::Corruption(format!(
                    "snapshot raft membership mutation id is invalid: {err}"
                ))
            })?;
            if mutation_id != &receipt.mutation_id
                || receipt.target_voter_node_ids.is_empty()
                || receipt.target_voter_checksum
                    != raft_voter_checksum(&receipt.target_voter_node_ids)
                || receipt.command_checksum.is_empty()
                || receipt.membership_epoch > self.raft_membership_epoch
            {
                return Err(ObjectError::Corruption(
                    "snapshot raft membership receipt is invalid".to_string(),
                ));
            }
        }
        for (index, receipt) in &self.applied_raft_receipts {
            if *index == 0 || receipt.index != *index || receipt.term == 0 {
                return Err(ObjectError::Corruption(
                    "snapshot raft apply receipt identity is invalid".to_string(),
                ));
            }
            if receipt.index > self.last_applied_index {
                return Err(ObjectError::Corruption(
                    "snapshot raft apply receipt is ahead of applied index".to_string(),
                ));
            }
            if receipt.index == self.last_applied_index && receipt.term != self.last_applied_term {
                return Err(ObjectError::Corruption(
                    "snapshot latest raft apply receipt term does not match applied term"
                        .to_string(),
                ));
            }
            if receipt.command_checksum.is_empty() {
                return Err(ObjectError::Corruption(
                    "snapshot raft apply receipt has empty command checksum".to_string(),
                ));
            }
        }
        Ok(())
    }
}
