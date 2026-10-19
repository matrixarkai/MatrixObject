impl MatrixReplicaService {
    pub fn configure_service_mode(
        &mut self,
        accepting_writes: Option<bool>,
        draining: Option<bool>,
        frozen: Option<bool>,
    ) {
        if let Some(accepting_writes) = accepting_writes {
            self.accepting_writes = accepting_writes;
        }
        if let Some(draining) = draining {
            self.draining = draining;
        }
        if let Some(frozen) = frozen {
            self.frozen = frozen;
        }
    }

    /// Opens a batch that owns its undo.
    pub fn begin_mutation_batch(
        &mut self,
        batch_id: impl Into<String>,
    ) -> Result<ReplicaMutationBatchStatus, ObjectError> {
        self.begin_mutation_batch_inner(batch_id, true)
    }

    /// Opens a batch whose undo the caller holds.
    ///
    /// Use this when the caller already captured enough to restore this service.
    /// It skips the snapshot, which copies every chunk this replica holds, so on
    /// a put it is the difference between one copy of the chunk state and two.
    pub fn begin_mutation_batch_without_undo(
        &mut self,
        batch_id: impl Into<String>,
    ) -> Result<ReplicaMutationBatchStatus, ObjectError> {
        self.begin_mutation_batch_inner(batch_id, false)
    }

    fn begin_mutation_batch_inner(
        &mut self,
        batch_id: impl Into<String>,
        capture_undo: bool,
    ) -> Result<ReplicaMutationBatchStatus, ObjectError> {
        self.ensure_writable()?;
        let batch_id = batch_id.into();
        if !batch_id.is_empty() {
            validate_idempotency_key(&batch_id)?;
        }
        if let Some(active) = &self.active_mutation_batch {
            if active.batch_id == batch_id {
                return Ok(self.mutation_batch_status());
            }
            return Err(ObjectError::PreconditionFailed(
                "chunk mutation batch already active".to_string(),
            ));
        }
        let snapshot = if capture_undo {
            Some(Box::new(self.export_snapshot_unsealed()))
        } else {
            None
        };
        self.active_mutation_batch = Some(ReplicaMutationBatch {
            batch_id,
            started_control_commit_index: self.chunk_mutation_control_commit_index,
            snapshot,
        });
        Ok(self.mutation_batch_status())
    }

    pub fn end_mutation_batch(
        &mut self,
        batch_id: impl AsRef<str>,
    ) -> Result<ReplicaMutationBatchStatus, ObjectError> {
        let active = self.active_mutation_batch.take().ok_or_else(|| {
            ObjectError::PreconditionFailed("chunk mutation batch is not active".to_string())
        })?;
        let expected = batch_id.as_ref();
        if !expected.is_empty() && active.batch_id != expected {
            self.active_mutation_batch = Some(active);
            return Err(ObjectError::PreconditionFailed(
                "chunk mutation batch id mismatch".to_string(),
            ));
        }
        let mut status = self.mutation_batch_status();
        status.batch_id = active.batch_id;
        status.started_control_commit_index = active.started_control_commit_index;
        status.committed = true;
        Ok(status)
    }

    pub fn abort_mutation_batch(
        &mut self,
        batch_id: impl AsRef<str>,
    ) -> Result<ReplicaMutationBatchStatus, ObjectError> {
        let active = self.active_mutation_batch.take().ok_or_else(|| {
            ObjectError::PreconditionFailed("chunk mutation batch is not active".to_string())
        })?;
        let expected = batch_id.as_ref();
        if !expected.is_empty() && active.batch_id != expected {
            self.active_mutation_batch = Some(active);
            return Err(ObjectError::PreconditionFailed(
                "chunk mutation batch id mismatch".to_string(),
            ));
        }
        let batch_id = active.batch_id;
        let started_control_commit_index = active.started_control_commit_index;
        // A marker batch has no undo: whoever opened it holds one, and
        // restoring is theirs to do.
        if let Some(snapshot) = active.snapshot {
            let mut rollback = *snapshot;
            rollback.seal_checksum();
            self.install_snapshot(rollback)?;
        }
        let mut status = self.mutation_batch_status();
        status.batch_id = batch_id;
        status.started_control_commit_index = started_control_commit_index;
        status.aborted = true;
        Ok(status)
    }

    pub fn mutation_batch_status(&self) -> ReplicaMutationBatchStatus {
        let active = self.active_mutation_batch.as_ref();
        ReplicaMutationBatchStatus {
            active: active.is_some(),
            batch_id: active.map_or_else(String::new, |batch| batch.batch_id.clone()),
            started_control_commit_index: active
                .map_or(0, |batch| batch.started_control_commit_index),
            current_control_commit_index: self.chunk_mutation_control_commit_index,
            page_count: self.pages.len() as u64,
            physical_bytes: self.physical_bytes(),
            discarded_bytes: self.discarded_bytes(),
            write_receipt_count: self.write_request_receipts.len() as u64,
            discard_receipt_count: self.discard_request_receipts.len() as u64,
            batch_write_receipt_count: self.batch_write_request_receipts.len() as u64,
            batch_discard_receipt_count: self.batch_discard_request_receipts.len() as u64,
            committed: false,
            aborted: false,
        }
    }

    pub fn accepting_writes(&self) -> bool {
        self.accepting_writes
    }

    pub fn is_draining(&self) -> bool {
        self.draining
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

}
