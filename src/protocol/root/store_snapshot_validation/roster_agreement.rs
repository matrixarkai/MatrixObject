// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl StoreSnapshot {
    /// The two copies of the control-plane roster have to say the same thing.
    ///
    /// # WHY THERE ARE TWO COPIES AT ALL
    ///
    /// The roster is moving out of its own snapshot and into the metadata snapshot. For this
    /// one step both carry it, which is deliberate: the new copy is produced by a different
    /// export function from the old one, both reading the same service fields, so comparing
    /// them compares two independently produced copies rather than comparing a value with
    /// itself. When the old copy goes, this rule goes with it and the comparison is no longer
    /// available -- which is the argument for having it now, while it still is.
    ///
    /// This runs on the write path as well as the read path, because `to_bytes` validates. On
    /// the write path an empty new copy beside a populated old one is a FAILURE: that is the
    /// shape a broken export would take, and it is the one case the loader's conversion below
    /// would otherwise paper over.
    fn validate_roster_agreement(&self) -> Result<(), ObjectError> {
        let meta = &self.meta_snapshot;
        let placement = &self.root_snapshot;
        let disagreement = if meta.placement_nodes != placement.nodes {
            Some("nodes")
        } else if meta.placements != placement.placements {
            Some("placements")
        } else if meta.placement_leader_lease != placement.leader_lease {
            Some("leader lease")
        } else if meta.placement_leader_lease_receipts != placement.leader_lease_receipts {
            Some("leader lease receipts")
        } else if meta.placement_coordinator_config != placement.coordinator_config {
            Some("coordinator config")
        } else if meta.placement_commit_index != placement.next_commit_index {
            Some("commit index")
        } else {
            None
        };
        // NAMED, not a bare boolean: a rule that can fail six ways and says only "disagree"
        // leaves the reader to find out which, and the one that failed is the whole message.
        if let Some(part) = disagreement {
            return Err(ObjectError::Corruption(format!(
                "the two copies of the control-plane roster disagree about its {part}, so one \
                 of them is not what this store believes"
            )));
        }
        Ok(())
    }
}
