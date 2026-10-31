// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixChunkService {
    pub fn begin_mutation_batch(
        &mut self,
        batch_id: impl Into<String>,
    ) -> Result<ChunkMutationBatchStatus, ObjectError> {
        let batch_id = batch_id.into();
        if !batch_id.is_empty() {
            validate_idempotency_key(&batch_id)?;
        }
        if let Some(active) = &self.active_mutation_batch {
            if active.batch_id == batch_id {
                return Ok(self.mutation_batch_status());
            }
            return Err(ObjectError::PreconditionFailed(
                "page mutation batch already active".to_string(),
            ));
        }
        let started_route_commit_index = self.chunk_route_control_commit_index;
        let mut replica_indices = Vec::with_capacity(self.replicas.len());
        for index in 0..self.replicas.len() {
            if self.replica_writable(index) {
                replica_indices.push(index);
            }
        }
        if replica_indices.len() < self.write_quorum {
            return Err(ObjectError::PreconditionFailed(
                "page mutation batch requires writable replica quorum".to_string(),
            ));
        }
        // THE REPLICAS FIRST, THE SNAPSHOT AFTER, so that a failure here needs no
        // snapshot to recover from. Nothing of this service's own state has changed
        // until the batch is registered below, and the replicas that did open roll
        // themselves back -- which also means this no longer has to hold a snapshot
        // carrying every replica's pages just for an error path.
        let mut opened: Vec<usize> = Vec::with_capacity(replica_indices.len());
        for index in &replica_indices {
            let replica_batch_id = replica_mutation_batch_id(&batch_id, *index);
            // EACH REPLICA OWNS ITS PAGE UNDO, and this service's own copy of its
            // pages is left out below. See the note on `undo`.
            if let Err(err) = self.replicas[*index].begin_mutation_batch(replica_batch_id) {
                for already_open in opened {
                    let _ = self.replicas[already_open]
                        .abort_mutation_batch(replica_mutation_batch_id(&batch_id, already_open));
                }
                return Err(err);
            }
            opened.push(*index);
        }
        // ---------------------------------------------------------------------------
        // THE UNDO DOES NOT HOLD THE REPLICAS' PAGES, AND THAT IS THE WHOLE POINT
        // ---------------------------------------------------------------------------
        //
        // A snapshot of this service clones each replica's page map, which SHARES every
        // page's handle. Cheap to take -- and then the next write to any of those pages
        // goes through `Arc::make_mut` and copies the WHOLE PAGE, payload included, so
        // that this undo keeps the old bytes. A put is an append, so filling one page
        // with many puts copied the page once per put: quadratic in the page size, and
        // 303,599,229 instructions of `memcpy` in a 400-put callgrind run, 38.8% of it.
        //
        // The copy was never needed HERE. Each replica can hold its own undo, and a
        // replica knows what its writes replaced -- so its undo is a length for an
        // append and the replaced bytes for a write inside the payload, never a copy of
        // the page. That is why the loop above opens each replica's batch WITH its undo,
        // where it used to skip it on the grounds that this snapshot already carried the
        // pages.
        //
        // So this snapshot is built without them, for exactly the replicas that opened a
        // batch above: ASKED FOR RATHER THAN EMPTIED AFTERWARDS, because emptying meant
        // cloning every page map per append and throwing it away, which measured as a
        // 10-25% append regression on TemporalStore's shared store -- 1 KiB pages, where
        // there is no page copy for the undo to be saving in exchange.
        //
        // It keeps everything else, including each replica's receipts and counters, and
        // `abort_mutation_batch` fills the page maps back in from the replicas AFTER
        // they have rolled themselves back -- which leaves what gets installed byte for
        // byte what a full snapshot would have carried. A replica that was not writable
        // gets no batch, so it will not roll itself back, and its pages stay here.
        let undo = self.export_snapshot_unsealed_without_pages_for(&replica_indices);
        self.active_mutation_batch = Some(ChunkMutationBatch {
            batch_id,
            started_route_commit_index,
            replica_indices,
            snapshot: Box::new(undo),
        });
        Ok(self.mutation_batch_status())
    }

    pub fn end_mutation_batch(
        &mut self,
        batch_id: impl AsRef<str>,
    ) -> Result<ChunkMutationBatchStatus, ObjectError> {
        let Some(active) = self.active_mutation_batch.as_ref() else {
            return Err(ObjectError::PreconditionFailed(
                "page mutation batch is not active".to_string(),
            ));
        };
        let expected = batch_id.as_ref();
        if !expected.is_empty() && active.batch_id != expected {
            return Err(ObjectError::PreconditionFailed(
                "page mutation batch id mismatch".to_string(),
            ));
        }
        let batch_id = active.batch_id.clone();
        let started_route_commit_index = active.started_route_commit_index;
        let replica_indices = active.replica_indices.clone();
        for index in replica_indices {
            self.replicas[index]
                .end_mutation_batch(replica_mutation_batch_id(&batch_id, index))?;
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
    ) -> Result<ChunkMutationBatchStatus, ObjectError> {
        let active = self.active_mutation_batch.take().ok_or_else(|| {
            ObjectError::PreconditionFailed("page mutation batch is not active".to_string())
        })?;
        let expected = batch_id.as_ref();
        if !expected.is_empty() && active.batch_id != expected {
            self.active_mutation_batch = Some(active);
            return Err(ObjectError::PreconditionFailed(
                "page mutation batch id mismatch".to_string(),
            ));
        }
        let batch_id = active.batch_id;
        let started_route_commit_index = active.started_route_commit_index;
        let mut rollback = *active.snapshot;
        // THE REPLICAS ROLL THEMSELVES BACK FIRST, because this snapshot gave up their
        // pages when the batch opened -- see the note in `begin_mutation_batch`. Each
        // replica restores its own page map from its own undo, which for an appended
        // page is a truncation and moves no bytes, and the restored map is then what
        // goes into the snapshot this service installs.
        for index in &active.replica_indices {
            let replica_batch_id = replica_mutation_batch_id(&batch_id, *index);
            self.replicas[*index].abort_mutation_batch(replica_batch_id)?;
            if let Some(taken) = rollback.replicas.get_mut(*index) {
                taken.pages = self.replicas[*index].pages_local().clone();
            }
        }
        rollback.seal_checksum();
        self.install_snapshot(rollback)?;
        let mut status = self.mutation_batch_status();
        status.batch_id = batch_id;
        status.started_route_commit_index = started_route_commit_index;
        status.aborted = true;
        Ok(status)
    }

    pub fn mutation_batch_status(&self) -> ChunkMutationBatchStatus {
        let active = self.active_mutation_batch.as_ref();
        let active_replica_count = (0..self.replicas.len())
            .filter(|index| self.replica_active(*index))
            .count() as u64;
        let chunk_batch_active_replica_count = self
            .replicas
            .iter()
            .filter(|replica| replica.mutation_batch_status().active)
            .count() as u64;
        ChunkMutationBatchStatus {
            active: active.is_some(),
            batch_id: active.map_or_else(String::new, |batch| batch.batch_id.clone()),
            started_route_commit_index: active.map_or(0, |batch| batch.started_route_commit_index),
            current_route_commit_index: self.chunk_route_control_commit_index,
            active_replica_count,
            opened_replica_batch_count: active
                .map_or(0, |batch| batch.replica_indices.len() as u64),
            chunk_batch_active_replica_count,
            chunk_allocation_receipt_count: self.allocation_request_receipts.len() as u64,
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
