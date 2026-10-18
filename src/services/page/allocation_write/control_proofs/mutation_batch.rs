impl MatrixPageService {
    pub fn begin_mutation_batch(
        &mut self,
        batch_id: impl Into<String>,
    ) -> Result<PageMutationBatchStatus, ObjectError> {
        let batch_id = batch_id.into();
        if !batch_id.is_empty() {
            validate_idempotency_key(&batch_id)?;
        }
        if let Some(active) = &self.active_mutation_batch {
            if active.batch_id == batch_id {
                return Ok(self.mutation_batch_status());
            }
            return Err(ObjectError::PreconditionFailed(
                "block mutation batch already active".to_string(),
            ));
        }
        let snapshot = self.export_snapshot_unsealed();
        let started_route_commit_index = self.page_route_control_commit_index;
        let mut replica_indices = Vec::with_capacity(self.replicas.len());
        for index in 0..self.replicas.len() {
            if self.replica_writable(index) {
                replica_indices.push(index);
            }
        }
        if replica_indices.len() < self.write_quorum {
            return Err(ObjectError::PreconditionFailed(
                "block mutation batch requires writable replica quorum".to_string(),
            ));
        }
        for index in &replica_indices {
            let replica_batch_id = page_chunk_mutation_batch_id(&batch_id, *index);
            // `snapshot` above already carries every replica, and the abort below
            // rebuilds them from it, so the replica needs no second copy.
            if let Err(err) =
                self.replicas[*index].begin_mutation_batch_without_undo(replica_batch_id)
            {
                // `install_snapshot` clears `active_mutation_batch` itself, and
                // this batch has not been registered yet, so the snapshot is
                // still in hand and needs no copy.
                let mut rollback = snapshot;
                rollback.seal_checksum();
                self.install_snapshot(rollback)?;
                return Err(err);
            }
        }
        self.active_mutation_batch = Some(PageMutationBatch {
            batch_id,
            started_route_commit_index,
            replica_indices,
            snapshot: Box::new(snapshot),
        });
        Ok(self.mutation_batch_status())
    }

    pub fn end_mutation_batch(
        &mut self,
        batch_id: impl AsRef<str>,
    ) -> Result<PageMutationBatchStatus, ObjectError> {
        let Some(active) = self.active_mutation_batch.as_ref() else {
            return Err(ObjectError::PreconditionFailed(
                "block mutation batch is not active".to_string(),
            ));
        };
        let expected = batch_id.as_ref();
        if !expected.is_empty() && active.batch_id != expected {
            return Err(ObjectError::PreconditionFailed(
                "block mutation batch id mismatch".to_string(),
            ));
        }
        let batch_id = active.batch_id.clone();
        let started_route_commit_index = active.started_route_commit_index;
        let replica_indices = active.replica_indices.clone();
        for index in replica_indices {
            self.replicas[index]
                .end_mutation_batch(page_chunk_mutation_batch_id(&batch_id, index))?;
        }
        self.active_mutation_batch = None;
        let mut status = self.mutation_batch_status();
        status.batch_id = batch_id;
        status.started_route_commit_index = started_route_commit_index;
        status.committed = true;
        Ok(status)
    }

    pub fn abort_mutation_batch(
        &mut self,
        batch_id: impl AsRef<str>,
    ) -> Result<PageMutationBatchStatus, ObjectError> {
        let active = self.active_mutation_batch.take().ok_or_else(|| {
            ObjectError::PreconditionFailed("block mutation batch is not active".to_string())
        })?;
        let expected = batch_id.as_ref();
        if !expected.is_empty() && active.batch_id != expected {
            self.active_mutation_batch = Some(active);
            return Err(ObjectError::PreconditionFailed(
                "block mutation batch id mismatch".to_string(),
            ));
        }
        let batch_id = active.batch_id;
        let started_route_commit_index = active.started_route_commit_index;
        let mut rollback = *active.snapshot;
        rollback.seal_checksum();
        self.install_snapshot(rollback)?;
        let mut status = self.mutation_batch_status();
        status.batch_id = batch_id;
        status.started_route_commit_index = started_route_commit_index;
        status.aborted = true;
        Ok(status)
    }

    pub fn mutation_batch_status(&self) -> PageMutationBatchStatus {
        let active = self.active_mutation_batch.as_ref();
        let active_replica_count = (0..self.replicas.len())
            .filter(|index| self.replica_active(*index))
            .count() as u64;
        let chunk_batch_active_replica_count = self
            .replicas
            .iter()
            .filter(|replica| replica.mutation_batch_status().active)
            .count() as u64;
        PageMutationBatchStatus {
            active: active.is_some(),
            batch_id: active.map_or_else(String::new, |batch| batch.batch_id.clone()),
            started_route_commit_index: active.map_or(0, |batch| batch.started_route_commit_index),
            current_route_commit_index: self.page_route_control_commit_index,
            active_replica_count,
            opened_replica_batch_count: active
                .map_or(0, |batch| batch.replica_indices.len() as u64),
            chunk_batch_active_replica_count,
            slab_allocation_receipt_count: self.allocation_request_receipts.len() as u64,
            batch_allocation_receipt_count: self.batch_allocation_request_receipts.len() as u64,
            release_allocation_receipt_count: self.release_allocation_request_receipts.len() as u64,
            write_receipt_count: self.write_request_receipts.len() as u64,
            batch_write_receipt_count: self.batch_write_request_receipts.len() as u64,
            discard_receipt_count: self.discard_request_receipts.len() as u64,
            batch_discard_receipt_count: self.batch_discard_request_receipts.len() as u64,
            committed: false,
            aborted: false,
        }
    }


}
