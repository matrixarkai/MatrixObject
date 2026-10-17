impl MatrixObjectStore {
    pub fn raft_voter_node_ids(&self) -> &[String] {
        &self.raft_voter_node_ids
    }

    pub fn raft_healthy_peer_count(&self) -> u64 {
        self.raft_healthy_peer_count
    }

    pub fn raft_membership_epoch(&self) -> u64 {
        self.raft_membership_epoch
    }

    pub fn raft_membership_receipt_count(&self) -> u64 {
        self.raft_membership_receipts.len() as u64
    }

    pub fn list_raft_membership_receipts(
        &self,
        marker: Option<&str>,
        limit: usize,
        target_voter_node_id: Option<&str>,
        target_voter_checksum: Option<&str>,
        quorum_preserved: Option<bool>,
        membership_plan_safe: Option<bool>,
    ) -> Result<RaftMembershipReceiptPage, ObjectError> {
        if let Some(marker) = marker {
            validate_idempotency_key(marker)?;
        }
        if let Some(target_voter_node_id) = target_voter_node_id {
            validate_idempotency_key(target_voter_node_id)?;
        }
        let limit = limit.clamp(1, k_max_raft_membership_receipt_list_limit());
        let mut page = RaftMembershipReceiptPage {
            total_receipt_count: self.raft_membership_receipts.len() as u64,
            limit: limit as u64,
            target_voter_node_id: target_voter_node_id.map(str::to_string),
            target_voter_checksum: target_voter_checksum.map(str::to_string),
            quorum_preserved,
            membership_plan_safe,
            ..RaftMembershipReceiptPage::default()
        };
        let iter: Box<dyn Iterator<Item = (&String, &RaftMembershipReceipt)> + '_> =
            if let Some(marker) = marker {
                Box::new(self.raft_membership_receipts.range((
                    std::ops::Bound::Excluded(marker.to_string()),
                    std::ops::Bound::Unbounded,
                )))
            } else {
                Box::new(self.raft_membership_receipts.iter())
            };
        for (mutation_id, receipt) in iter {
            page.scanned_count = page.scanned_count.saturating_add(1);
            if target_voter_node_id.is_some_and(|node_id| {
                !receipt.target_voter_node_ids.iter().any(|id| id == node_id)
            }) {
                page.skipped_target_voter_count = page.skipped_target_voter_count.saturating_add(1);
                continue;
            }
            if target_voter_checksum
                .is_some_and(|checksum| receipt.target_voter_checksum != checksum)
            {
                page.skipped_target_checksum_count =
                    page.skipped_target_checksum_count.saturating_add(1);
                continue;
            }
            let receipt_quorum_preserved = receipt.target_healthy_peer_count
                >= raft_quorum(receipt.target_voter_node_ids.len() as u64);
            if quorum_preserved.is_some_and(|expected| receipt_quorum_preserved != expected) {
                page.skipped_quorum_preserved_count =
                    page.skipped_quorum_preserved_count.saturating_add(1);
                continue;
            }
            const COMMITTED_MEMBERSHIP_RECEIPT_PLAN_SAFE: bool = true;
            if membership_plan_safe
                .is_some_and(|expected| COMMITTED_MEMBERSHIP_RECEIPT_PLAN_SAFE != expected)
            {
                page.skipped_plan_safe_count = page.skipped_plan_safe_count.saturating_add(1);
                continue;
            }
            if page.receipts.len() >= limit {
                page.truncated = true;
                page.next_marker = Some(mutation_id.clone());
                break;
            }
            page.receipts.push(receipt.clone());
        }
        page.receipt_count = page.receipts.len() as u64;
        Ok(page)
    }

}
