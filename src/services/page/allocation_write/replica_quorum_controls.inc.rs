// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn read_quorum(&self) -> usize {
        self.read_quorum
    }

    pub fn configure_write_quorum(&mut self, write_quorum: usize) -> Result<(), ObjectError> {
        self.validate_runtime_quorum(write_quorum, self.read_quorum)?;
        self.write_quorum = write_quorum;
        Ok(())
    }

    pub fn configure_read_quorum(&mut self, read_quorum: usize) -> Result<(), ObjectError> {
        self.validate_runtime_quorum(self.write_quorum, read_quorum)?;
        self.read_quorum = read_quorum;
        Ok(())
    }

    pub fn configure_quorums(
        &mut self,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<(), ObjectError> {
        self.validate_runtime_quorum(write_quorum, read_quorum)?;
        self.write_quorum = write_quorum;
        self.read_quorum = read_quorum;
        Ok(())
    }

    fn validate_runtime_quorum(
        &self,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<(), ObjectError> {
        if write_quorum == 0 || write_quorum > self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "write_quorum must be 1..replica_count".to_string(),
            ));
        }
        if read_quorum == 0 || read_quorum > self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "read_quorum must be 1..replica_count".to_string(),
            ));
        }
        let active_replicas = self
            .replicas
            .len()
            .saturating_sub(self.quarantined_replicas.len())
            .saturating_sub(self.disabled_chunk_endpoints.len());
        let writable_replicas = self
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, _)| self.replica_writable(*index))
            .count();
        if write_quorum > writable_replicas {
            return Err(ObjectError::PreconditionFailed(
                "page write quorum exceeds writable replica count".to_string(),
            ));
        }
        if read_quorum > active_replicas {
            return Err(ObjectError::PreconditionFailed(
                "page quorum exceeds active replica count".to_string(),
            ));
        }
        Ok(())
    }

    pub fn quarantined_replica_indices(&self) -> Vec<usize> {
        self.quarantined_replicas.iter().copied().collect()
    }

    pub fn frozen_replica_indices(&self) -> Vec<usize> {
        self.replicas
            .iter()
            .enumerate()
            .filter_map(|(index, replica)| replica.is_frozen().then_some(index))
            .collect()
    }

    pub fn set_replica_frozen(
        &mut self,
        replica_index: usize,
        frozen: bool,
    ) -> Result<(), ObjectError> {
        if replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "replica index is out of range".to_string(),
            ));
        }
        if frozen && !self.replicas[replica_index].is_frozen() {
            let writable_after = self
                .replicas
                .iter()
                .enumerate()
                .filter(|(index, replica)| {
                    *index != replica_index && self.replica_active(*index) && !replica.is_frozen()
                })
                .count();
            if writable_after < self.write_quorum {
                return Err(ObjectError::PreconditionFailed(
                    "replica freeze would violate page write quorum".to_string(),
                ));
            }
        }
        // A FROZEN REPLICA STOPS RECEIVING WRITES, so from here on it may be behind. The
        // epoch moves and this replica is left at the old one; thawing does not bring it
        // back, only a check-in does.
        if frozen && !self.replicas[replica_index].is_frozen() {
            self.record_replica_membership_change(&BTreeSet::from([replica_index]));
        }
        self.replicas[replica_index].set_frozen(frozen);
        Ok(())
    }

    /// Record that these replicas have stopped receiving writes, so nothing may be
    /// concluded from what they hold until each is checked back in.
    ///
    /// Only the leaving replicas are touched. A replica that stayed is believable because
    /// it stayed, and one that was ALREADY stale must not be cleared by somebody else's
    /// membership change -- both of which fall out of adding rather than of comparing,
    /// which is the second reason the epoch arithmetic went: its first version advanced
    /// nobody, and quarantining one replica of three made all three unbelievable.
    pub(crate) fn record_replica_membership_change(&mut self, leaving: &BTreeSet<usize>) {
        for index in leaving {
            if *index < self.replicas.len() {
                self.stale_replicas.insert(*index);
            }
        }
    }

    /// The replicas the authority will not believe, so they do not count in a read.
    pub fn stale_replica_indices(&self) -> Vec<usize> {
        self.stale_replicas.iter().copied().collect()
    }

    /// Declare a replica current again -- but only once its pages say so.
    ///
    /// # WHY THIS VERIFIES INSTEAD OF BEING TOLD
    ///
    /// The epoch exists so that a returning replica cannot be mistaken for current. An
    /// entry point that simply set it would hand that mistake straight back to whichever
    /// caller felt confident. So this reads the evidence: for every page any believable
    /// replica holds, the candidate must match the highest observed write sequence and
    /// the complete page head: length, payload checksum, state checksum, and discards.
    /// Current sources at that sequence must agree. Unverified extra pages, newer local
    /// sequences, and an absence of current sources all keep the candidate stale.
    ///
    /// # WHAT IT DOES NOT DO
    ///
    /// It repairs nothing and deletes nothing. A refused check-in leaves the replica
    /// exactly as it was, still holding its bytes -- which is the one case the design this
    /// follows is explicit about: a stale copy is kept, never dropped, because it may be
    /// the last copy of something.
    pub fn check_in_replica(&mut self, replica_index: usize) -> Result<(), ObjectError> {
        if replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "replica index is out of range".to_string(),
            ));
        }
        if !self.replica_writable(replica_index) || !self.replicas[replica_index].accepting_writes() {
            return Err(ObjectError::PreconditionFailed(
                "a quarantined, disabled, frozen, draining, or write-disabled replica cannot be checked in"
                    .to_string(),
            ));
        }
        if !self.stale_replicas.contains(&replica_index) {
            return Ok(());
        }
        let current: Vec<usize> = (0..self.replicas.len())
            .filter(|index| *index != replica_index && self.replica_current(*index)
                && self.replica_writable(*index) && self.replicas[*index].accepting_writes())
            .collect();
        if current.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "replica check-in requires a current source".to_string(),
            ));
        }
        // Keep a source index alongside each maximum sequence. Inventory stays small;
        // full heads are compared one page at a time after collecting all maxima.
        let mut best: BTreeMap<PageId, (u64, usize)> = BTreeMap::new();
        for &index in &current {
            for (page_id, sequence) in self.replicas[index].page_write_sequences() {
                let entry = best.entry(page_id).or_insert((sequence, index));
                if sequence > entry.0 { *entry = (sequence, index); }
            }
        }
        let candidate = &self.replicas[replica_index];
        for (page_id, (required, source)) in &best {
            let held = candidate.page_write_sequence(page_id).unwrap_or_default();
            if held != *required {
                return Err(ObjectError::PreconditionFailed(format!(
                    "replica {replica_index} cannot be checked in: page {page_id:?} is at write sequence {held} and a current replica holds {required}",
                )));
            }
            let expected = self.replicas[*source].head_chunk(page_id)?;
            for &index in &current {
                if self.replicas[index].page_write_sequence(page_id) == Some(*required)
                    && self.replicas[index].head_chunk(page_id)? != expected
                {
                    return Err(ObjectError::PreconditionFailed(format!(
                        "replica check-in has conflicting current states for page {page_id:?} at write sequence {required}",
                    )));
                }
            }
            if candidate.head_chunk(page_id).ok().as_ref() != Some(&expected) {
                return Err(ObjectError::PreconditionFailed(format!(
                    "replica {replica_index} cannot be checked in: page {page_id:?} does not match current page state",
                )));
            }
        }
        if candidate.page_count() != best.len() {
            return Err(ObjectError::PreconditionFailed(
                "replica check-in found unverified extra pages".to_string(),
            ));
        }
        self.stale_replicas.remove(&replica_index);
        Ok(())
    }

    pub fn set_replica_quarantine(
        &mut self,
        replica_index: usize,
        quarantined: bool,
    ) -> Result<(), ObjectError> {
        if replica_index >= self.replicas.len() {
            return Err(ObjectError::InvalidArgument(
                "replica index is out of range".to_string(),
            ));
        }
        if quarantined {
            let active_after = self
                .replicas
                .len()
                .saturating_sub(self.quarantined_replicas.len())
                .saturating_sub(self.disabled_chunk_endpoints.len())
                .saturating_sub(if self.quarantined_replicas.contains(&replica_index) {
                    0
                } else {
                    1
                });
            if active_after < self.write_quorum || active_after < self.read_quorum {
                return Err(ObjectError::PreconditionFailed(
                    "replica quarantine would violate page quorum".to_string(),
                ));
            }
            let writable_after = self
                .replicas
                .iter()
                .enumerate()
                .filter(|(index, replica)| {
                    *index != replica_index && self.replica_active(*index) && !replica.is_frozen()
                })
                .count();
            if writable_after < self.write_quorum {
                return Err(ObjectError::PreconditionFailed(
                    "replica quarantine would violate page write quorum".to_string(),
                ));
            }
            // AFTER THE PRECONDITIONS, NOT BEFORE. Quarantine stops both reads and
            // writes to this replica, so from here it is behind whatever happens next --
            // but a quarantine the quorum checks above REFUSED did not happen, and the
            // first version of this marked the replica stale anyway. An existing test
            // caught that, by failing a read it had always been able to serve.
            self.record_replica_membership_change(&BTreeSet::from([replica_index]));
            self.quarantined_replicas.insert(replica_index);
        } else {
            // Coming back does NOT clear the record: that is the moment this whole
            // mechanism exists for. `check_in_replica` is the way out.
            self.quarantined_replicas.remove(&replica_index);
        }
        Ok(())
    }
}
