// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
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
            page_append_undo: BTreeMap::new(),
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
        let page_append_undo = active.page_append_undo;
        // A marker batch has no undo: whoever opened it holds one, and
        // restoring is theirs to do.
        if let Some(snapshot) = active.snapshot {
            let mut rollback = *snapshot;
            // THE PAGES WHOSE UNDO IS A LENGTH. Each is absent from the rollback
            // because its append was allowed to land in the live payload; its old
            // state is that payload truncated, which moves no bytes, plus the
            // scalars the write advanced.
            for (page_id, undo) in page_append_undo {
                let Some(live) = self.pages.remove(&page_id) else {
                    // Unreachable: every path that can drop a page calls
                    // `materialise_page_undo` first, which takes the record with it.
                    debug_assert!(false, "an append undo outlived the page it describes");
                    continue;
                };
                let mut state = match std::sync::Arc::try_unwrap(live) {
                    Ok(state) => state,
                    Err(shared) => (*shared).clone(),
                };
                let payload = std::sync::Arc::make_mut(&mut state.data);
                payload.truncate(undo.payload_len);
                // The allocation too: see `payload_capacity`.
                payload.shrink_to(undo.payload_capacity);
                state.logical_length = undo.logical_length;
                state.write_sequence = undo.write_sequence;
                state.checksum_fnv1a64 = undo.checksum_fnv1a64;
                state.discarded_ranges = undo.discarded_ranges;
                state.ec_parity_fragment = undo.ec_parity_fragment;
                state.ec_fragment_digests = undo.ec_fragment_digests;
                rollback.pages.insert(page_id, std::sync::Arc::new(state));
            }
            rollback.seal_checksum();
            self.install_snapshot(rollback)?;
        }
        let mut status = self.mutation_batch_status();
        status.batch_id = batch_id;
        status.started_control_commit_index = started_control_commit_index;
        status.aborted = true;
        Ok(status)
    }

    // ---------------------------------------------------------------------------
    // AN APPEND'S UNDO IS A LENGTH, NOT A COPY
    // ---------------------------------------------------------------------------
    //
    // `begin_mutation_batch` captures its undo by cloning the page map, which SHARES
    // every page's handle rather than copying it. Taking it is cheap, and that was
    // measured. The cost is paid later: the next write to a shared page goes through
    // `Arc::make_mut`, which copies the whole page -- payload included -- so that the
    // undo keeps the old bytes. Filling one page with many small appends therefore
    // copies the page once per append, which is quadratic in the page size, and was
    // 294,202,726 instructions, 24.0% of a 400-put callgrind run.
    //
    // The copy exists only because the undo is expressed as A COPY OF THE OLD STATE.
    // For an append it does not have to be: nothing below the old end changes, so the
    // undo is "truncate back to this length" plus the few scalars the write advances.
    // That costs nothing to record and nothing to apply, and it leaves the live page
    // with one holder, so the append lands in place.
    //
    // WHAT MAKES THE RECORD COMPLETE, rather than nearly complete. `write_chunk`
    // changes exactly seven fields of a page: `data`, `logical_length`,
    // `write_sequence`, `checksum_fnv1a64`, `discarded_ranges`, and the two EC
    // sidecars. All seven are in the record, and the payload is the only one that is
    // not stored outright -- it is the length to truncate to. The sidecars are stored
    // by HANDLE, which is free because `refresh_chunk_ec_sidecars` replaces them
    // rather than writing through them. A page state that grows an eighth field a
    // write touches has to be added here, and
    // `an_append_undo_restores_every_field_a_write_changes` is what fails if it is not.
    //
    // ONE OF THE SEVEN CANNOT BE OBSERVED TO MATTER, and it is worth saying which
    // rather than implying all seven are load-bearing. `discarded_ranges` is carried
    // because the write runs `subtract_range` over it -- but a discarded range is
    // refused above `logical_length`, and an append starts at or after the payload
    // length, so no append reaches one. Dropping this leg from the restore was
    // planted and no test failed. It stays because it is what `write_chunk` touches,
    // and a restore that covers what the write touches needs no case analysis to
    // stay right when either side moves.
    //
    // WHAT MAKES IT SAFE WHEN SOMETHING ELSE HAPPENS TO THE PAGE. A record describes
    // an append and nothing else. Any other mutation of that page inside the same
    // batch -- a write reaching below the recorded length, a discard, a reclaim, a
    // state install, a removal -- calls `materialise_page_undo` first, which rebuilds
    // the full pre-image and hands it back to the undo. The copy is then paid exactly
    // once, at the point where it is the only correct answer.

    /// How many payload bytes this batch's undo would truncate this page back to,
    /// if this page's undo is a length.
    fn recorded_append_undo_len(&self, page_id: &PageId) -> Option<usize> {
        self.active_mutation_batch
            .as_ref()?
            .page_append_undo
            .get(page_id)
            .map(|undo| undo.payload_len)
    }

    /// Keep this page's undo cheap if this write is an append, and pay for the
    /// page's full pre-image now if it is not.
    ///
    /// `start_index` is the LOWEST payload index the write will touch. Call this
    /// before taking the page mutably: whether it runs decides how many holders the
    /// page has, and so whether taking it copies.
    pub(crate) fn keep_page_undo_cheap_for_write(&mut self, page_id: &PageId, start_index: usize) {
        if let Some(recorded) = self.recorded_append_undo_len(page_id) {
            if start_index < recorded {
                // This write reaches under the bytes the record would truncate away,
                // so truncating would not put them back.
                self.materialise_page_undo(page_id);
            }
            return;
        }
        let Self {
            active_mutation_batch,
            pages,
            ..
        } = self;
        let Some(batch) = active_mutation_batch.as_mut() else {
            return;
        };
        let Some(snapshot) = batch.snapshot.as_mut() else {
            return;
        };
        let Some(live) = pages.get(page_id) else {
            return;
        };
        if start_index < live.data.len() {
            return;
        }
        let Some(taken) = snapshot.pages.get(page_id) else {
            return;
        };
        // THE UNDO MUST BE THE ONLY OTHER HOLDER, and the counts say so rather than
        // being assumed: two holders of the state (live and undo, which are the same
        // allocation) and one of the payload.
        //
        // WHAT THESE COUNTS ARE AND ARE NOT. They are NOT what keeps another holder's
        // bytes -- `Arc::make_mut` is, at both levels, and it is why loosening these
        // to admit a third holder breaks nothing. Planted that way, every test in the
        // suite still passed, which is the honest state of it. What they do is keep
        // the record to the case where it BUYS something: with a third holder the
        // append copies anyway, so taking the record would give up the undo's handle
        // for nothing. Refusing is the conservative answer and it keeps this path's
        // precondition small enough to read.
        if !std::sync::Arc::ptr_eq(live, taken)
            || std::sync::Arc::strong_count(live) != 2
            || std::sync::Arc::strong_count(&live.data) != 1
        {
            return;
        }
        batch.page_append_undo.insert(
            page_id.clone(),
            PageAppendUndo {
                payload_len: live.data.len(),
                payload_capacity: live.data.capacity(),
                logical_length: live.logical_length,
                write_sequence: live.write_sequence,
                checksum_fnv1a64: live.checksum_fnv1a64,
                discarded_ranges: live.discarded_ranges.clone(),
                ec_parity_fragment: live.ec_parity_fragment.clone(),
                ec_fragment_digests: live.ec_fragment_digests.clone(),
            },
        );
        snapshot.pages.remove(page_id);
    }

    /// Put the page's whole pre-image back in this batch's undo, because what is
    /// about to happen to it is not an append.
    ///
    /// A no-op unless this page's undo is currently a length, so every site that
    /// mutates a page can call it unconditionally.
    pub(crate) fn materialise_page_undo(&mut self, page_id: &PageId) {
        let Self {
            active_mutation_batch,
            pages,
            ..
        } = self;
        let Some(batch) = active_mutation_batch.as_mut() else {
            return;
        };
        if !batch.page_append_undo.contains_key(page_id) {
            return;
        }
        let Some(snapshot) = batch.snapshot.as_mut() else {
            return;
        };
        let Some(live) = pages.get(page_id) else {
            debug_assert!(false, "an append undo outlived the page it describes");
            return;
        };
        let Some(undo) = batch.page_append_undo.remove(page_id) else {
            return;
        };
        let mut state = (**live).clone();
        let payload = std::sync::Arc::make_mut(&mut state.data);
        payload.truncate(undo.payload_len);
        payload.shrink_to(undo.payload_capacity);
        state.logical_length = undo.logical_length;
        state.write_sequence = undo.write_sequence;
        state.checksum_fnv1a64 = undo.checksum_fnv1a64;
        state.discarded_ranges = undo.discarded_ranges;
        state.ec_parity_fragment = undo.ec_parity_fragment;
        state.ec_fragment_digests = undo.ec_fragment_digests;
        snapshot.pages.insert(page_id.clone(), std::sync::Arc::new(state));
    }

    /// The undo an open batch on THIS replica holds, for the footprint audit.
    ///
    /// The page service's batch used to carry every replica's pages, and the audit
    /// read them there. Now the page service gives them up and each replica holds
    /// its own, so the audit has to come here or it reports no pages held at all
    /// while a mutation is in flight -- which is the one moment it was added to be
    /// able to see.
    pub(crate) fn open_mutation_batch_undo(&self) -> Option<&ReplicaServiceSnapshot> {
        self.active_mutation_batch
            .as_ref()?
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.as_ref())
    }

    /// The pages whose undo is a LENGTH, and what those records cost.
    ///
    /// Reported apart from the pages the undo still HOLDS, because they are
    /// different quantities: a record is a handful of scalars and the page it
    /// describes is not duplicated at all. Adding them together would read as if
    /// the undo still held every page, which is exactly what stopped being true.
    pub(crate) fn open_mutation_batch_append_undo(&self) -> (u64, u64) {
        let Some(batch) = self.active_mutation_batch.as_ref() else {
            return (0, 0);
        };
        let mut bytes = 0u64;
        for undo in batch.page_append_undo.values() {
            bytes = bytes
                .saturating_add(
                    (std::mem::size_of::<PageId>() + std::mem::size_of::<PageAppendUndo>()) as u64,
                )
                .saturating_add(
                    (undo.discarded_ranges.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<DataRange>() as u64),
                );
        }
        (batch.page_append_undo.len() as u64, bytes)
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
