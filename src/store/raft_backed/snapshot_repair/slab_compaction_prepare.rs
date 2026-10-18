impl<R: RustRaftLog> RaftBackedObjectStore<R> {
    pub fn prepare_compact_slab_raft_entry(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
    ) -> Result<PreparedSlabCompaction, ObjectError> {
        self.store.prepare_slab_compaction_raft_entry(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            self.store.last_applied_index(),
        )
    }

    pub fn prepare_compact_slab_raft_entry_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedSlabCompaction, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "segment compaction",
            expected_page_route_control_checksum,
        )?;
        self.store.prepare_slab_compaction_raft_entry_fenced(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            self.store.last_applied_index(),
            Some(expected_page_route_control_checksum),
            None,
        )
    }

    pub fn prepare_compact_slab_raft_entry_linearizable_gc_proof_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<PreparedSlabCompaction, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let expected_object_gc_report_proof_checksum =
            expected_object_gc_report_proof_checksum.as_ref();
        self.ensure_page_route_control_ready(
            "segment compaction",
            expected_page_route_control_checksum,
        )?;
        if expected_object_gc_report_proof_checksum.is_empty() {
            return Err(ObjectError::PreconditionFailed(
                "segment compaction requires expected object GC report proof checksum".to_string(),
            ));
        }
        self.store.prepare_slab_compaction_raft_entry_fenced(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            self.store.last_applied_index(),
            Some(expected_page_route_control_checksum),
            Some(expected_object_gc_report_proof_checksum),
        )
    }

    pub fn prepare_compact_slab_raft_entry_with_read_proof_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedSlabCompaction>, ObjectError> {
        self.prepare_compact_slab_raft_entry_with_read_proof_linearizable_fenced_committed_at(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_compact_slab_raft_entry_with_read_proof_linearizable_fenced_committed_at(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedSlabCompaction>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "segment compaction",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_compact_slab_raft_entry_linearizable_fenced(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_compact_slab_raft_entry_with_read_proof_linearizable_gc_proof_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedSlabCompaction>, ObjectError> {
        self.prepare_compact_slab_raft_entry_with_read_proof_linearizable_gc_proof_fenced_committed_at(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_compact_slab_raft_entry_with_read_proof_linearizable_gc_proof_fenced_committed_at(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedSlabCompaction>, ObjectError> {
        let expected_page_route_control_checksum = expected_page_route_control_checksum.as_ref();
        let requested_read_index = self
            .raft
            .commit_index()
            .max(min_read_index)
            .max(min_commit_index);
        self.ensure_page_route_control_ready_at(
            "segment compaction",
            expected_page_route_control_checksum,
            min_read_index,
            min_commit_index,
        )?;
        let prepared = self.prepare_compact_slab_raft_entry_linearizable_gc_proof_fenced(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )?;
        let proof = self.page_storage_read_proof_committed(requested_read_index, min_commit_index);
        Ok(ProvenStorageRead {
            value: prepared,
            proof,
        })
    }

    pub fn prepare_compact_slab_envelope_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<PreparedSlabCompactionEnvelope, ObjectError> {
        let prepared = self.prepare_compact_slab_raft_entry_linearizable_fenced(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
        )?;
        Ok(make_prepared_slab_compaction_envelope(&prepared))
    }

    pub fn prepare_compact_slab_envelope_linearizable_gc_proof_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<PreparedSlabCompactionEnvelope, ObjectError> {
        let prepared = self.prepare_compact_slab_raft_entry_linearizable_gc_proof_fenced(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
        )?;
        Ok(make_prepared_slab_compaction_envelope(&prepared))
    }

    pub fn prepare_compact_slab_envelope_with_read_proof_linearizable_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedSlabCompactionEnvelope>, ObjectError> {
        self.prepare_compact_slab_envelope_with_read_proof_linearizable_fenced_committed_at(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_compact_slab_envelope_with_read_proof_linearizable_fenced_committed_at(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedSlabCompactionEnvelope>, ObjectError> {
        let proven_prepared = self
            .prepare_compact_slab_raft_entry_with_read_proof_linearizable_fenced_committed_at(
                slab_id,
                min_stale_bytes,
                update_time_ms,
                expected_page_route_control_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenStorageRead {
            value: make_prepared_slab_compaction_envelope(&proven_prepared.value),
            proof: proven_prepared.proof,
        })
    }

    pub fn prepare_compact_slab_envelope_with_read_proof_linearizable_gc_proof_fenced(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
    ) -> Result<ProvenStorageRead<PreparedSlabCompactionEnvelope>, ObjectError> {
        self.prepare_compact_slab_envelope_with_read_proof_linearizable_gc_proof_fenced_committed_at(
            slab_id,
            min_stale_bytes,
            update_time_ms,
            expected_page_route_control_checksum,
            expected_object_gc_report_proof_checksum,
            self.raft.commit_index(),
            0,
        )
    }

    pub fn prepare_compact_slab_envelope_with_read_proof_linearizable_gc_proof_fenced_committed_at(
        &self,
        slab_id: &SlabId,
        min_stale_bytes: u64,
        update_time_ms: u64,
        expected_page_route_control_checksum: impl AsRef<str>,
        expected_object_gc_report_proof_checksum: impl AsRef<str>,
        min_read_index: u64,
        min_commit_index: u64,
    ) -> Result<ProvenStorageRead<PreparedSlabCompactionEnvelope>, ObjectError> {
        let proven_prepared = self
            .prepare_compact_slab_raft_entry_with_read_proof_linearizable_gc_proof_fenced_committed_at(
                slab_id,
                min_stale_bytes,
                update_time_ms,
                expected_page_route_control_checksum,
                expected_object_gc_report_proof_checksum,
                min_read_index,
                min_commit_index,
            )?;
        Ok(ProvenStorageRead {
            value: make_prepared_slab_compaction_envelope(&proven_prepared.value),
            proof: proven_prepared.proof,
        })
    }

}
