impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_slab_discard_raft_entry(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
    ) -> Result<PreparedSlabDiscard, ObjectError> {
        self.store.prepare_slab_discard_raft_entry(
            slab_id,
            ranges,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_slab_discard_raft_entry_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedSlabDiscard, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "segment discard",
            expected_page_route_control_checksum,
        )?;
        self.store.prepare_slab_discard_raft_entry_fenced(
            slab_id,
            ranges,
            self.store.last_applied_index(),
            Some(expected_page_route_control_checksum),
        )
    }

    pub fn prepare_slab_discard_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedSlabDiscard>, ObjectError> {
        self.prepare_slab_discard_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            slab_id,
            ranges,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_slab_discard_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedSlabDiscard>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "segment discard",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_slab_discard_raft_entry_linearizable_fenced(
            slab_id,
            ranges,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_slab_discard_envelope_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedSlabDiscardEnvelope, ObjectError> {
        let prepared = self.prepare_slab_discard_raft_entry_linearizable_fenced(
            slab_id,
            ranges,
            expected_page_route_control_checksum,
        )?;
        Ok(make_prepared_slab_discard_envelope(&prepared))
    }

    pub fn prepare_slab_discard_envelope_with_read_proof_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedSlabDiscardEnvelope>, ObjectError> {
        self.prepare_slab_discard_envelope_with_read_proof_linearizable_fenced_committed_at(
            slab_id,
            ranges,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_slab_discard_envelope_with_read_proof_linearizable_fenced_committed_at(
        &self,
        slab_id: &SlabId,
        ranges: Vec<DataRange>,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedSlabDiscardEnvelope>, ObjectError> {
        let proven_prepared = self
            .prepare_slab_discard_raft_entry_with_read_proof_linearizable_fenced_committed_at(
                slab_id,
                ranges,
                expected_page_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenStorageRead {
            value: make_prepared_slab_discard_envelope(&proven_prepared.value),
            proof: proven_prepared.proof,
        })
    }

    pub fn plan_slab_discards_with_read_proof_linearizable_fenced(
        &self,
        min_stale_bytes: u64,
        max_ranges_per_slab: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<Vec<DiscardPlan>>, ObjectError> {
        self.plan_slab_discards_with_read_proof_linearizable_fenced_committed_at(
            min_stale_bytes,
            max_ranges_per_slab,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn plan_slab_discards_with_read_proof_linearizable_fenced_committed_at(
        &self,
        min_stale_bytes: u64,
        max_ranges_per_slab: usize,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<Vec<DiscardPlan>>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "segment discard plan",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: self
                .store
                .plan_discard(min_stale_bytes, max_ranges_per_slab),
            proof,
        })
    }

}
