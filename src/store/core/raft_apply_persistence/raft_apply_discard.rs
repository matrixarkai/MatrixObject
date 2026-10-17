impl MatrixObjectStore {
    pub fn apply_raft_entry(
        &mut self,
        entry: &RaftLogEntry,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.validate_raft_entry_header(entry.index, entry.term)?;
        if entry.index <= self.last_applied_index {
            let command_checksum = object_command_checksum(&entry.command);
            return self.apply_stale_raft_entry_receipt(
                entry.index,
                entry.term,
                &command_checksum,
            );
        }
        let command_checksum = object_command_checksum(&entry.command);
        let result =
            self.apply_command_with_raft_index(entry.command.clone(), Some(entry.index))?;
        self.last_applied_index = entry.index;
        self.last_applied_term = entry.term;
        self.remember_raft_apply_receipt(
            entry.index,
            entry.term,
            command_checksum,
            result.clone(),
        );
        Ok(result)
    }

    pub fn apply_raft_entry_owned(
        &mut self,
        entry: RaftLogEntry,
    ) -> Result<ObjectMetadata, ObjectError> {
        self.validate_raft_entry_header(entry.index, entry.term)?;
        let command_checksum = object_command_checksum(&entry.command);
        if entry.index <= self.last_applied_index {
            return self.apply_stale_raft_entry_receipt(
                entry.index,
                entry.term,
                &command_checksum,
            );
        }
        let index = entry.index;
        let term = entry.term;
        let result = self.apply_command_with_raft_index(entry.command, Some(index))?;
        self.last_applied_index = index;
        self.last_applied_term = term;
        self.remember_raft_apply_receipt(index, term, command_checksum, result.clone());
        Ok(result)
    }

    fn validate_raft_entry_header(&self, index: u64, term: u64) -> Result<(), ObjectError> {
        if index == 0 || term == 0 {
            return Err(ObjectError::InvalidArgument(
                "raft entry index and term must be positive".to_string(),
            ));
        }
        Ok(())
    }

    fn apply_stale_raft_entry_receipt(
        &self,
        index: u64,
        term: u64,
        command_checksum: &str,
    ) -> Result<ObjectMetadata, ObjectError> {
        if let Some(receipt) = self.applied_raft_receipts.get(&index) {
            if receipt.term == term && receipt.command_checksum == command_checksum {
                return Ok(receipt.result.clone());
            }
        }
        Err(ObjectError::InvalidArgument(format!(
            "raft entry is stale or conflicts with retained apply receipt; index={} last_applied={}",
            index, self.last_applied_index
        )))
    }

    fn remember_raft_apply_receipt(
        &mut self,
        index: u64,
        term: u64,
        command_checksum: String,
        result: ObjectMetadata,
    ) {
        self.applied_raft_receipts.insert(
            index,
            RaftApplyReceipt {
                index,
                term,
                command_checksum,
                result,
            },
        );
        while self.applied_raft_receipts.len() > k_max_raft_apply_receipts() {
            if let Some(oldest) = self.applied_raft_receipts.keys().next().copied() {
                self.applied_raft_receipts.remove(&oldest);
            } else {
                break;
            }
        }
    }

    pub fn plan_discard(
        &self,
        min_stale_bytes: u64,
        max_ranges_per_slab: usize,
    ) -> Vec<DiscardPlan> {
        let mut plans = Vec::new();
        for slab in self.slabs.values() {
            if slab.stale_bytes < min_stale_bytes {
                continue;
            }
            let ranges = merge_range_slice(&slab.stale_ranges);
            let discard_bytes = ranges.iter().map(|range| range.length).sum();
            let fragmented = ranges.len() > self.options.compaction_fragment_threshold;
            let compact_instead =
                fragmented || (max_ranges_per_slab > 0 && ranges.len() > max_ranges_per_slab);
            plans.push(DiscardPlan {
                slab_id: slab.slab_id.clone(),
                ranges,
                discard_bytes,
                fragmented,
                compact_instead,
            });
        }
        plans.sort_by(|a, b| b.discard_bytes.cmp(&a.discard_bytes));
        plans
    }

    pub fn apply_discard(
        &mut self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
    ) -> Result<(), ObjectError> {
        self.apply_discard_borrowed(slab_id, &ranges)
    }

    pub fn apply_discard_borrowed(
        &mut self,
        slab_id: &SlabId,
        ranges: &[DataRange],
    ) -> Result<(), ObjectError> {
        let merged = merge_range_slice(ranges);
        let slab = self
            .slabs
            .get_mut(slab_id)
            .ok_or_else(|| ObjectError::NotFound(format!("segment {:?}", slab_id)))?;
        for range in merged {
            range.end()?;
            slab.discarded_bytes = saturating_add(slab.discarded_bytes, range.length);
            slab.discarded_ranges.push(range);
        }
        slab.discarded_ranges = merge_ranges(std::mem::take(&mut slab.discarded_ranges));
        Ok(())
    }

    pub fn prepare_slab_discard_raft_entry(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_previous_commit_index: u64,
    ) -> Result<PreparedSlabDiscard, ObjectError> {
        let admitted_route_checksum = self.page_service.route_control_proof().state_checksum;
        self.prepare_slab_discard_raft_entry_fenced(
            slab_id,
            ranges,
            expected_previous_commit_index,
            Some(&admitted_route_checksum),
        )
    }

    pub fn prepare_slab_discard_raft_entry_fenced(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedSlabDiscard, ObjectError> {
        self.prepare_slab_discard_raft_entry_fenced_borrowed(
            slab_id,
            &ranges,
            expected_previous_commit_index,
            expected_route_control_checksum,
        )
    }

    pub fn prepare_slab_discard_raft_entry_fenced_borrowed(
        &self,
        slab_id: &SlabId,
        ranges: &[DataRange],
        expected_previous_commit_index: u64,
        expected_route_control_checksum: Option<&str>,
    ) -> Result<PreparedSlabDiscard, ObjectError> {
        self.page_service
            .check_expected_route_control_checksum(expected_route_control_checksum)?;
        if expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(format!(
                "segment discard expected previous commit {} does not match applied {}",
                expected_previous_commit_index, self.last_applied_index
            )));
        }
        let ranges = merge_range_slice(ranges);
        if ranges.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "segment discard requires at least one range".to_string(),
            ));
        }
        for range in &ranges {
            range.end()?;
        }
        let slab = self
            .slabs
            .get(slab_id)
            .ok_or_else(|| ObjectError::NotFound(format!("segment {:?}", slab_id)))?;
        let source_stale_ranges = merge_range_slice(&slab.stale_ranges);
        if !ranges_covered_by(&ranges, &source_stale_ranges) {
            return Err(ObjectError::PreconditionFailed(
                "segment discard ranges must be covered by stale ranges".to_string(),
            ));
        }
        let source_discarded_ranges = merge_range_slice(&slab.discarded_ranges);
        let before_state_fingerprint = self.state_fingerprint_without_payload_clones();
        let mut projected = self.clone();
        projected.apply_discard_borrowed(slab_id, &ranges)?;
        let after_state_fingerprint = projected.state_fingerprint_without_payload_clones();
        let request_checksum = slab_discard_request_checksum(slab_id, &ranges);
        let route_control_proof = self.page_service.route_control_proof();
        let expected_route_control_checksum =
            expected_route_control_checksum.unwrap_or("").to_string();
        let mut prepared = PreparedSlabDiscard {
            slab_id: slab_id.clone(),
            discard_bytes: ranges.iter().map(|range| range.length).sum(),
            ranges,
            source_stale_ranges,
            source_discarded_ranges,
            before_state_fingerprint,
            after_state_fingerprint,
            request_checksum,
            expected_previous_commit_index,
            prepared_commit_index: expected_previous_commit_index.saturating_add(1),
            route_control_proof,
            expected_route_control_checksum,
            raft_entry_checksum: String::new(),
        };
        prepared.raft_entry_checksum = prepared_slab_discard_checksum(&prepared);
        Ok(prepared)
    }

    pub fn apply_prepared_slab_discard(
        &mut self,
        prepared: PreparedSlabDiscard,
    ) -> Result<(), ObjectError> {
        let expected_checksum = prepared_slab_discard_checksum(&prepared);
        if prepared.raft_entry_checksum != expected_checksum {
            return Err(ObjectError::Corruption(
                "prepared segment discard checksum mismatch".to_string(),
            ));
        }
        if prepared.request_checksum
            != slab_discard_request_checksum(&prepared.slab_id, &prepared.ranges)
        {
            return Err(ObjectError::Corruption(
                "prepared segment discard request checksum mismatch".to_string(),
            ));
        }
        if prepared.expected_previous_commit_index != self.last_applied_index {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard previous commit fence mismatch".to_string(),
            ));
        }
        if prepared.before_state_fingerprint != self.state_fingerprint_without_payload_clones() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard source state changed".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard route fence is required".to_string(),
            ));
        }
        if prepared.expected_route_control_checksum != prepared.route_control_proof.state_checksum {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard route proof checksum mismatch".to_string(),
            ));
        }
        self.page_service
            .check_prepared_route_control_proof_cursor(
                &prepared.route_control_proof,
                "prepared segment discard",
            )?;
        let current = self.prepare_slab_discard_raft_entry_fenced_borrowed(
            &prepared.slab_id,
            &prepared.ranges,
            prepared.expected_previous_commit_index,
            Some(prepared.route_control_proof.state_checksum.as_str()),
        )?;
        if current.source_stale_ranges != prepared.source_stale_ranges
            || current.source_discarded_ranges != prepared.source_discarded_ranges
            || current.after_state_fingerprint != prepared.after_state_fingerprint
            || current.route_control_proof != prepared.route_control_proof
            || current.discard_bytes != prepared.discard_bytes
        {
            return Err(ObjectError::PreconditionFailed(
                "prepared segment discard no longer matches current state".to_string(),
            ));
        }
        self.apply_discard_borrowed(&prepared.slab_id, &prepared.ranges)?;
        if self.state_fingerprint_without_payload_clones() != prepared.after_state_fingerprint {
            return Err(ObjectError::Corruption(
                "prepared segment discard post-state checksum mismatch".to_string(),
            ));
        }
        Ok(())
    }

}
