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
        // THE PAGES ARE NOT COPIED HERE. See `capture_page_pre_image`: the undo
        // collects pre-images as pages are touched, so opening a batch costs the
        // scalars and nothing per page held.
        let snapshot = if capture_undo {
            Some(Box::new(self.export_snapshot_unsealed_without_pages()))
        } else {
            None
        };
        self.active_mutation_batch = Some(ReplicaMutationBatch {
            batch_id,
            started_control_commit_index: self.chunk_mutation_control_commit_index,
            snapshot,
            page_write_undo: BTreeMap::new(),
            created_pages: std::collections::BTreeSet::new(),
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
        let page_write_undo = active.page_write_undo;
        let created_pages = active.created_pages;
        // A marker batch has no undo: whoever opened it holds one, and
        // restoring is theirs to do.
        if let Some(snapshot) = active.snapshot {
            let mut rollback = *snapshot;
            // THE ROLLBACK STARTS FROM THE LIVE MAP, MOVED. The snapshot holds only
            // the pages this batch touched, so installing its map would drop every
            // page the batch never looked at. Taking the live map costs nothing --
            // it is a move -- and the three loops below put the touched pages back.
            let mut pages = std::mem::take(&mut self.pages);
            // PAGES WHOSE PRE-IMAGE WAS KEPT OUTRIGHT, which is every touch that a
            // record cannot describe: an install, a repair, a discard, a removal.
            for (page_id, state) in std::mem::take(&mut rollback.pages) {
                pages.insert(page_id, state);
            }
            // THE PAGES WHOSE UNDO IS WHAT THE WRITES REPLACED. Each is absent from
            // the pre-images because its writes were allowed to land in the live
            // payload; its old state is that payload truncated, with the replaced
            // bytes put back, plus the scalars the writes advanced.
            for (page_id, undo) in page_write_undo {
                debug_assert!(
                    !created_pages.contains(&page_id),
                    "a page created inside the batch cannot also have a pre-image record"
                );
                let Some(live) = pages.remove(&page_id) else {
                    // Unreachable: every path that can drop a page calls
                    // `materialise_page_undo` first, which takes the record with it.
                    debug_assert!(false, "a write undo outlived the page it describes");
                    continue;
                };
                let mut state = match std::sync::Arc::try_unwrap(live) {
                    Ok(state) => state,
                    Err(shared) => (*shared).clone(),
                };
                Self::restore_page_from_write_undo(&mut state, undo);
                pages.insert(page_id, std::sync::Arc::new(state));
            }
            // AND THE PAGES THAT WERE NOT THERE. Undoing a creation is a removal.
            for page_id in created_pages {
                pages.remove(&page_id);
            }
            rollback.pages = pages;
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

    /// Keep this page's undo to what this write replaces, rather than the page.
    ///
    /// `start_index` and `end_index` are the payload range the write will cover.
    /// Call this BEFORE taking the page mutably: whether it runs decides how many
    /// holders the page has, and so whether taking it copies.
    /// Keep this page as the batch found it, the first time anything touches it.
    ///
    /// # WHY THE UNDO IS FILLED LAZILY AND NOT AT `begin`
    ///
    /// `begin_mutation_batch` used to capture its undo by cloning the whole page
    /// map. Each VALUE in that clone is a handle, so it copies no payload -- that
    /// was measured, and it is why the clone looked free. What it is not free in is
    /// the MAP: one `BTreeMap` node allocation per page held, per batch, and a
    /// matching deallocation when the batch ends. A batch touches a handful of
    /// pages and a replica holds thousands, so nearly all of that work was for
    /// pages the batch never looked at, and on the commit path -- which is every
    /// path that is not an abort -- the whole structure was dropped unread.
    ///
    /// MEASURED on TemporalStore's five-node shared-store sweep, where the page
    /// count is large and pages are small: `clone_subtree` of
    /// `BTreeMap<PageId, Arc<PageState>>` at **31.82% inclusive** of a
    /// 17,632,228,672-instruction run, its matching `drop` at 19.36%, and
    /// `IntoIter::dying_next` under that drop at 17.03%.
    ///
    /// So the pre-image is collected one page at a time instead, as pages are
    /// touched. Every site that mutates a page already calls either this or
    /// `materialise_page_undo` before mutating -- `keep_page_undo_cheap_for_write`
    /// calls this, and `materialise_page_undo` calls it too -- and
    /// `every_page_mutation_pays_its_undo_before_mutating` is the guard that keeps
    /// that true of sites added later.
    ///
    /// # WHAT ABSENCE MEANS NOW
    ///
    /// A page absent from `snapshot.pages` used to mean "created inside this
    /// batch", because the map started complete. It now means "created inside this
    /// batch OR never touched", and those two need opposite undos, so the first is
    /// recorded in `created_pages` rather than inferred.
    pub(crate) fn capture_page_pre_image(&mut self, page_id: &PageId) {
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
        // Already accounted for: kept outright, re-encoded as a record, or known new.
        if snapshot.pages.contains_key(page_id)
            || batch.page_write_undo.contains_key(page_id)
            || batch.created_pages.contains(page_id)
        {
            return;
        }
        match pages.get(page_id) {
            // A HANDLE, NOT A COPY -- exactly what the whole-map clone gave every
            // page. This is also why the strong counts that
            // `keep_page_undo_cheap_for_write` checks need no adjusting: the capture
            // runs immediately before the check, so the page it reads has the same
            // two state holders and one payload holder it had when the map was
            // cloned at `begin`.
            Some(live) => {
                snapshot
                    .pages
                    .insert(page_id.clone(), std::sync::Arc::clone(live));
            }
            // NOT THERE, so the pre-image IS its absence.
            None => {
                batch.created_pages.insert(page_id.clone());
            }
        }
    }

    pub(crate) fn keep_page_undo_cheap_for_write(
        &mut self,
        page_id: &PageId,
        start_index: usize,
        end_index: usize,
    ) {
        // FIRST TOUCH KEEPS THE PRE-IMAGE. A no-op on every later write to the
        // same page, and the reason the lookup below finds anything at all.
        self.capture_page_pre_image(page_id);
        {
            let Self {
                active_mutation_batch,
                pages,
                ..
            } = self;
            if let Some(batch) = active_mutation_batch.as_mut() {
                if let Some(undo) = batch.page_write_undo.get_mut(page_id) {
                    // A RECORD IS ALREADY STANDING for this page, so this write only
                    // has to add what IT replaces. Clipped to the payload as it was
                    // when the batch opened, because anything above that is truncated
                    // away rather than restored.
                    let covered_end = end_index.min(undo.payload_len);
                    if start_index < covered_end {
                        if let Some(live) = pages.get(page_id) {
                            if covered_end <= live.data.len() {
                                undo.replaced.push((
                                    start_index,
                                    live.data[start_index..covered_end].to_vec(),
                                ));
                            } else {
                                // Cannot happen: `payload_len` is the length when the
                                // batch opened and a payload only grows inside one.
                                debug_assert!(false, "a page shrank inside its own batch");
                            }
                        }
                    }
                    return;
                }
            }
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
        let Some(taken) = snapshot.pages.get(page_id) else {
            return;
        };
        // THE CONDITION IS ABOUT THE PAYLOAD, NOT THE STATE, and that distinction was
        // measured rather than reasoned.
        //
        // The obvious condition is "the undo names the same page state": live and
        // undo are one allocation, two holders of it, one holder of the payload. That
        // is what an append arrives in, and the first version of this required it --
        // and it missed every in-place write on the put path. An overwrite DISCARDS
        // the range it is replacing first, and `discard_chunk` takes the page through
        // `Arc::make_mut`, which clones the state and leaves the payload shared. So by
        // the time the write arrives the live state is a clone: `ptr_eq` is false, the
        // state has one holder, and the PAYLOAD has two. Measured on the benchmark,
        // all 60 in-place writes landed in exactly that shape and the record was
        // refused for all 60.
        //
        // What the record actually needs is that the undo's only claim on the live
        // page is the payload allocation, and that nothing else claims it. Then
        // giving the entry up leaves the writer alone with the bytes, and the record
        // -- built from the UNDO's fields, which are the ones the batch started from,
        // not the live clone's post-discard ones -- can put everything back.
        //
        // WHAT THE COUNTS ARE AND ARE NOT. They are NOT what keeps another holder's
        // bytes; `Arc::make_mut` is, at both levels, and it is why loosening them to
        // admit a third holder breaks nothing. Planted that way, every test in the
        // suite still passed, which is the honest state of it. What they do is keep
        // the record to the case where it BUYS something, and keep this precondition
        // small enough to read.
        let same_state = std::sync::Arc::ptr_eq(live, taken);
        let payload_is_the_only_tie = std::sync::Arc::ptr_eq(&live.data, &taken.data)
            && if same_state {
                // One state, held by the live map and by the undo; one payload.
                std::sync::Arc::strong_count(live) == 2
                    && std::sync::Arc::strong_count(&live.data) == 1
            } else {
                // Two states, each held once; one payload, held by both of them.
                std::sync::Arc::strong_count(live) == 1
                    && std::sync::Arc::strong_count(taken) == 1
                    && std::sync::Arc::strong_count(&live.data) == 2
            };
        if !payload_is_the_only_tie {
            return;
        }
        // THE PAYLOAD IS SHARED, so reading the replaced bytes off the live page reads
        // the bytes the batch started from: no write in this batch can have changed
        // them, because a write with the payload shared copies it instead of writing
        // through it, and that copy would have left no second holder to find here.
        let covered_end = end_index.min(live.data.len());
        let replaced = if start_index < covered_end {
            vec![(start_index, live.data[start_index..covered_end].to_vec())]
        } else {
            // An append replaces nothing.
            Vec::new()
        };
        // EVERY FIELD FROM `taken`, which is the page as the batch found it. The live
        // state can already have diverged -- a discard before this write moved its
        // `discarded_ranges` and `write_sequence` -- and restoring the live clone's
        // values would undo the write and keep the discard.
        batch.page_write_undo.insert(
            page_id.clone(),
            PageWriteUndo {
                payload_len: taken.data.len(),
                payload_capacity: taken.data.capacity(),
                logical_length: taken.logical_length,
                write_sequence: taken.write_sequence,
                checksum_fnv1a64: taken.checksum_fnv1a64,
                discarded_ranges: taken.discarded_ranges.clone(),
                ec_parity_fragment: taken.ec_parity_fragment.clone(),
                ec_fragment_digests: taken.ec_fragment_digests.clone(),
                replaced,
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
        // BEFORE THE EARLY RETURN BELOW, because this is the call every site that
        // mutates a page in a way a record cannot describe already makes, and the
        // pre-image has to be kept whether or not a record happens to exist. A
        // page with no record and no pre-image yet is captured here; one that was
        // never there is marked as created.
        self.capture_page_pre_image(page_id);
        let Self {
            active_mutation_batch,
            pages,
            ..
        } = self;
        let Some(batch) = active_mutation_batch.as_mut() else {
            return;
        };
        if !batch.page_write_undo.contains_key(page_id) {
            return;
        }
        let Some(snapshot) = batch.snapshot.as_mut() else {
            return;
        };
        let Some(live) = pages.get(page_id) else {
            debug_assert!(false, "an append undo outlived the page it describes");
            return;
        };
        let Some(undo) = batch.page_write_undo.remove(page_id) else {
            return;
        };
        let mut state = (**live).clone();
        Self::restore_page_from_write_undo(&mut state, undo);
        snapshot.pages.insert(page_id.clone(), std::sync::Arc::new(state));
    }

    /// Put a page back the way this batch's write undo says it was.
    ///
    /// Shared by the abort and by `materialise_page_undo`, because the two have to
    /// agree byte for byte: one of them restoring a field the other does not is a
    /// rollback whose result depends on which path reached it.
    fn restore_page_from_write_undo(state: &mut PageState, undo: PageWriteUndo) {
        let payload = std::sync::Arc::make_mut(&mut state.data);
        payload.truncate(undo.payload_len);
        // The allocation too: see `payload_capacity`.
        payload.shrink_to(undo.payload_capacity);
        // IN REVERSE, so the earliest record covering a byte is applied last and
        // wins. See `replaced`.
        for (at, bytes) in undo.replaced.into_iter().rev() {
            let end = at.saturating_add(bytes.len());
            if end <= payload.len() {
                payload[at..end].copy_from_slice(&bytes);
            } else {
                // Cannot happen: every record is clipped to `payload_len`, which the
                // truncate above restored.
                debug_assert!(false, "a write undo range outran the page it restores");
            }
        }
        state.logical_length = undo.logical_length;
        state.write_sequence = undo.write_sequence;
        state.checksum_fnv1a64 = undo.checksum_fnv1a64;
        state.discarded_ranges = undo.discarded_ranges;
        state.ec_parity_fragment = undo.ec_parity_fragment;
        state.ec_fragment_digests = undo.ec_fragment_digests;
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

    /// The pages whose undo is WHAT THE WRITES REPLACED, and what those records
    /// cost -- the replaced bytes included, since they are real memory.
    ///
    /// Reported apart from the pages the undo still HOLDS, because they are
    /// different quantities: a record is a handful of scalars plus the bytes this
    /// batch's writes covered, and the page it describes is not duplicated at all.
    /// Adding them together would read as if the undo still held every page, which
    /// is exactly what stopped being true.
    pub(crate) fn open_mutation_batch_write_undo(&self) -> (u64, u64) {
        let Some(batch) = self.active_mutation_batch.as_ref() else {
            return (0, 0);
        };
        let mut bytes = 0u64;
        for undo in batch.page_write_undo.values() {
            bytes = bytes
                .saturating_add(
                    (std::mem::size_of::<PageId>() + std::mem::size_of::<PageWriteUndo>()) as u64,
                )
                .saturating_add(
                    (undo.discarded_ranges.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<DataRange>() as u64),
                );
            for (_, bytes_replaced) in &undo.replaced {
                bytes = bytes
                    .saturating_add(std::mem::size_of::<(usize, Vec<u8>)>() as u64)
                    .saturating_add(bytes_replaced.capacity() as u64);
            }
        }
        (batch.page_write_undo.len() as u64, bytes)
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
