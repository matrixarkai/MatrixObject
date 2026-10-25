// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Reaching a replica's state through calls instead of through its fields.
//
// # WHY THIS FILE EXISTS
//
// A replica of this store is meant to be able to live in its own process. The obstacle to
// that is not the four operations a page write needs -- those already cross a socket -- it
// is that code above the replica reaches **into** it: `replica.pages`, `replica.capacity_bytes`,
// `replica.physical_write_count` and thirteen more fields, read and written directly.
//
// A field read cannot cross a process boundary. A method call can. So every field the layer
// above touched is closed here behind a call, which is the step that has to happen before any
// of those call sites can be pointed at a socket, and which the compiler can drive: the
// fields become private and it names every site.
//
// # WHAT THE SITES ACTUALLY WANTED, MEASURED RATHER THAN GUESSED
//
// The sites were enumerated by making the fields private and reading the errors -- not by
// searching for names, because `MatrixChunkService` has fields called `write_request_receipts`,
// `batch_write_request_receipts`, `discard_request_receipts`,
// `batch_discard_request_receipts` and `receipt_retention` too, and a search would have
// rewritten the wrong struct's.
//
// What the compiler found: **16 fields, 112 sites, 11 files** -- 44 sites in production code
// across 10 files, and 68 in the library test suite.
//
// Most of those 44 needed nothing new. **A getter already existed, with the same name, for
// nine of the sixteen fields** -- `capacity_bytes()`, `capacity_rejected_writes()`,
// `service_mode_rejected_mutations()`, the four `physical_*()` counters and
// `receipt_retention()` -- so those sites were reading around an accessor that was already
// there. They now call it.
//
// What was missing is in this file, and it is the three shapes worth naming:
//
// | shape | what it is for | the calls |
// |---|---|---|
// | **narrow read** | a site that wanted one fact about one page | [`MatrixReplicaService::page_write_sequence`], [`MatrixReplicaService::page_logical_length`], [`MatrixReplicaService::page_physical_len`], [`MatrixReplicaService::holds_any_page`], [`MatrixReplicaService::page_count`] |
// | **batched read** | a site that wanted one fact about *every* page | [`MatrixReplicaService::page_write_sequences`] |
// | **stays local** | a site that genuinely needs the whole page map | [`MatrixReplicaService::pages_local`] |
//
// # THE ONE FIELD THAT IS A REDESIGN, AND WHY IT IS NOT DONE HERE
//
// `pages` is read at 13 production sites, and eleven of them turned into a narrow or batched
// call. Two did not, and they are the honest residue:
//
// * the footprint audit walks every page's state to sum payload lengths, capacities, fragment
//   bytes and discarded-range rows;
// * the two batch-mutation fingerprints append every page id and every page state into hash
//   material.
//
// Neither is a query. Both are whole-state traversals, and answering one over a socket means
// shipping the replica's entire state to its caller -- which is a snapshot transfer with a
// different name, not a remote method. **Their remote form is a different call: ask the node
// for its own footprint, and ask the node for its own fingerprint, then combine the answers.**
// That is a change to what those two features compute and where, so it is not smuggled in
// here. Until it happens they call [`MatrixReplicaService::pages_local`], whose name is the
// marker: a caller of `pages_local` cannot be moved to another process, and `pages_local` is
// greppable in a way `pages` was not.
//
// So the count that matters after this file: **of 13 production reads of `pages`, 11 can
// cross a process boundary and 2 cannot**, and the 2 are named.

impl MatrixReplicaService {
    // -----------------------------------------------------------------------------------
    // Pages, narrowly
    // -----------------------------------------------------------------------------------

    /// How many pages this replica holds.
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Whether this replica holds any page at all.
    ///
    /// Separate from [`MatrixReplicaService::page_count`] because the caller that wants it --
    /// the stripe layout, refusing to change once anything is written -- wants a bit, and a
    /// count is a larger promise to keep honest over a wire than a bit is.
    pub fn holds_any_page(&self) -> bool {
        !self.pages.is_empty()
    }

    /// The write sequence of one page, if this replica holds it.
    pub fn page_write_sequence(&self, page_id: &PageId) -> Option<u64> {
        self.pages.get(page_id).map(|state| state.write_sequence)
    }

    /// Every page's write sequence, in one call.
    ///
    /// # WHY THIS IS A BATCHED READ AND NOT A LOOP OVER [`MatrixReplicaService::page_write_sequence`]
    ///
    /// Its caller is `check_in_replica`, which decides whether a replica that was out of the
    /// set may be believed again. It compares, for every page any current replica holds, the
    /// best sequence seen against the returning replica's own -- so it needs the whole set of
    /// pages, not a page it already knows about. Asking page by page would be one round trip
    /// per page, which at a million pages is not a different constant, it is a different
    /// design.
    ///
    /// What makes this affordable is what it leaves out: a `(PageId, u64)` pair per page and
    /// **no payload**. That is the difference between a page index and a page map, and it is
    /// why this call can stand behind a socket while [`MatrixReplicaService::pages_local`]
    /// cannot.
    pub fn page_write_sequences(&self) -> BTreeMap<PageId, u64> {
        self.pages
            .iter()
            .map(|(page_id, state)| (page_id.clone(), state.write_sequence))
            .collect()
    }

    /// The logical length of one page, if this replica holds it.
    pub fn page_logical_length(&self, page_id: &PageId) -> Option<u64> {
        self.pages.get(page_id).map(|state| state.logical_length)
    }

    /// How many bytes one page's payload occupies, or zero if the page is absent.
    ///
    /// Zero for an absent page rather than `Option`, because the one caller -- projecting what
    /// a write would cost against this replica's capacity -- treats "not here yet" as "costs
    /// nothing yet", and an `Option` would make every call site restate that.
    pub fn page_physical_len(&self, page_id: &PageId) -> u64 {
        self.pages
            .get(page_id)
            .map_or(0u64, |state| state.data.len() as u64)
    }

    /// Put a page state in place WITHOUT re-checking that this replica accepts writes.
    ///
    /// # WHY THIS IS NOT `install_chunk_state`
    ///
    /// `install_chunk_state` begins with `ensure_writable`, which is right for a caller
    /// arriving from outside. The two callers here are the apply halves of a prepared batch
    /// mutation, and they have already fenced the write in a stronger way than a writability
    /// check: they compared the replica's whole control proof -- state checksum, commit index
    /// and next log index -- against the proof the entry was prepared against, and refused
    /// unless all three matched.
    ///
    /// Re-running `ensure_writable` there would let a service-mode change between prepare and
    /// apply abandon an entry the log has already committed, which is how a replica ends up
    /// disagreeing with the log it is replaying. So the fence is named in the method rather
    /// than duplicated: `unfenced` says the caller owes one, and the two callers pay it.
    pub(crate) fn install_page_unfenced(&mut self, page_id: PageId, state: PageState) {
        self.pages.insert(page_id, std::sync::Arc::new(state));
    }

    /// The whole page map, by reference. **THIS CALL CANNOT CROSS A PROCESS BOUNDARY.**
    ///
    /// Named `_local` on purpose. Every other accessor in this file answers a question; this
    /// one hands over the replica's state and lets the caller ask its own. Over a socket that
    /// is a snapshot transfer, so a caller of this method is a caller that still has to be
    /// redesigned before the replica it reads can live anywhere else.
    ///
    /// Two callers, both whole-state traversals rather than queries: the footprint audit, and
    /// the batch-mutation image fingerprints. See the file note for what their remote form is.
    pub(crate) fn pages_local(&self) -> &BTreeMap<PageId, std::sync::Arc<PageState>> {
        &self.pages
    }

    // -----------------------------------------------------------------------------------
    // Counters the layer above advances
    // -----------------------------------------------------------------------------------
    //
    // Each of these had a getter already and no setter, so the sites that advanced them did
    // it by reading the field, adding, and writing it back. Saturating addition is kept
    // inside the replica rather than restated at four call sites: a counter that wrapped
    // because one site used `+` is a metric that reads as a success.

    /// Count writes this replica refused for want of capacity.
    pub(crate) fn note_capacity_rejected_writes(&mut self, count: u64) {
        self.capacity_rejected_writes = self.capacity_rejected_writes.saturating_add(count);
    }

    /// Count mutations this replica refused because of its service mode.
    pub(crate) fn note_service_mode_rejected_mutations(&mut self, count: u64) {
        self.service_mode_rejected_mutations =
            self.service_mode_rejected_mutations.saturating_add(count);
    }

    /// Count a physical write and the bytes it moved.
    ///
    /// Both terms in one call because they are one event, and because two calls is two places
    /// a caller can advance one and forget the other -- which reads afterwards as a write of
    /// zero bytes, or bytes written by no write.
    pub(crate) fn note_physical_write(&mut self, writes: u64, bytes: u64) {
        self.physical_write_count = self.physical_write_count.saturating_add(writes);
        self.physical_write_bytes = self.physical_write_bytes.saturating_add(bytes);
    }

    /// Count physical discarded ranges and the bytes they covered.
    pub(crate) fn note_physical_discard(&mut self, ranges: u64, bytes: u64) {
        self.physical_discard_ranges = self.physical_discard_ranges.saturating_add(ranges);
        self.physical_discard_bytes = self.physical_discard_bytes.saturating_add(bytes);
    }

    // -----------------------------------------------------------------------------------
    // Configuration and cursors
    // -----------------------------------------------------------------------------------

    // DELIBERATELY ABSENT: a `keeps_ec_sidecar` getter to pair with the setter below. It was
    // written and removed before this landed, because nothing reads it -- the field's own
    // readers are all inside the replica. A getter nothing calls is not symmetry, it is a
    // promise about a future caller, and it rots silently because no test can tell it from a
    // working one.

    /// Decide whether this replica keeps the XOR sidecar.
    ///
    /// Must be uniform across a replica set: quorum agreement compares whole page states, so
    /// replicas disagreeing about whether a sidecar exists would never agree. The two callers
    /// are construction and snapshot restore, which is why the rule lives next to the two
    /// places that can break it rather than only in a comment on the field.
    pub(crate) fn set_keep_ec_sidecar(&mut self, keep: bool) {
        self.keep_ec_sidecar = keep;
    }

    /// Where this replica's mutation control log will write next.
    ///
    /// Available on `control_proof()` already, which builds a whole proof structure including
    /// a state checksum over every page. The caller here wants one number to pick a commit
    /// index with, so it gets one number.
    pub fn chunk_mutation_control_next_log_index(&self) -> u64 {
        self.chunk_mutation_control_next_log_index
    }

    // -----------------------------------------------------------------------------------
    // Receipts
    // -----------------------------------------------------------------------------------

    /// The batch write receipt filed under this idempotency key, if there is one.
    pub(crate) fn batch_write_receipt(&self, key: &str) -> Option<&ReplicaBatchWriteReceipt> {
        self.batch_write_request_receipts.get(key)
    }

    /// File a batch write receipt under its idempotency key.
    pub(crate) fn record_batch_write_receipt(
        &mut self,
        key: String,
        receipt: ReplicaBatchWriteReceipt,
    ) {
        std::sync::Arc::make_mut(&mut self.batch_write_request_receipts).insert(key, receipt);
    }

    /// The batch discard receipt filed under this idempotency key, if there is one.
    pub(crate) fn batch_discard_receipt(&self, key: &str) -> Option<&ReplicaBatchDiscardReceipt> {
        self.batch_discard_request_receipts.get(key)
    }

    /// File a batch discard receipt under its idempotency key.
    pub(crate) fn record_batch_discard_receipt(
        &mut self,
        key: String,
        receipt: ReplicaBatchDiscardReceipt,
    ) {
        std::sync::Arc::make_mut(&mut self.batch_discard_request_receipts).insert(key, receipt);
    }

    /// How many receipts this replica is retaining, over the four maps the audit charges for.
    ///
    /// ALL FOUR TERMS, NAMED. The caller was adding four `len()` calls together, and a helper
    /// that quietly dropped one of them would move the published figure while still looking
    /// like the sum of a list. The four are written out here so the omission would be visible
    /// at the one place it could happen.
    pub(crate) fn retained_receipt_count(&self) -> u64 {
        (self.write_request_receipts.len() as u64)
            .saturating_add(self.discard_request_receipts.len() as u64)
            .saturating_add(self.batch_write_request_receipts.len() as u64)
            .saturating_add(self.batch_discard_request_receipts.len() as u64)
    }

    // -----------------------------------------------------------------------------------
    // The rest of the whole-state reach, named `_local`, and reachable only from the suite
    // -----------------------------------------------------------------------------------
    //
    // EVERY ACCESSOR BELOW IS `#[cfg(test)]`, AND THAT IS THE POINT RATHER THAN TIDINESS.
    // Their only callers are in `src/tests.rs`. Left ungated they would be a production
    // surface whose readers are all `cfg(test)` -- which this repository has been bitten by
    // before: a field whose only readers were test code could not be seen to be dead by
    // `cargo test --lib`, because the suite is exactly the build in which it is alive.
    // Gating them means `cargo build` compiles the production surface and nothing else, so a
    // production caller that reaches for one of these fails to compile instead of quietly
    // becoming the twelfth site that cannot cross a process boundary.
    //
    // WHAT THESE ARE FOR, SAID PLAINLY: the library test suite reads this replica's state
    // directly at 68 sites, and a fault-injection test in particular has to corrupt a page,
    // drop every page, or compare two receipt maps key for key. Those are legitimate things
    // for a test to do and none of them is a query a socket could answer.
    //
    // They are accessors rather than public fields anyway, because the point of this file is
    // that NOTHING outside the replica reaches into it: a field is invisible to the compiler
    // once it is `pub(crate)`, while a call named `_local` is a thing that can be counted,
    // and the count is what says how much of the reach is left.

    /// The whole page map, mutably. **THIS CALL CANNOT CROSS A PROCESS BOUNDARY.**
    ///
    /// Separate from [`MatrixReplicaService::pages_local`] so that the read-only callers and
    /// the ones that change pages under the service's feet are told apart by name: a test
    /// that clears every page is doing something no production path may do, and it should not
    /// be reachable through the accessor the footprint audit uses.
    #[cfg(test)]
    pub(crate) fn pages_local_mut(&mut self) -> &mut BTreeMap<PageId, std::sync::Arc<PageState>> {
        &mut self.pages
    }

    /// This replica's write receipts. **LOCAL ONLY** -- see [`MatrixReplicaService::pages_local`].
    #[cfg(test)]
    pub(crate) fn write_receipts_local(&self) -> &BTreeMap<String, ReplicaWriteReceipt> {
        &self.write_request_receipts
    }

    /// This replica's discard receipts. **LOCAL ONLY.**
    #[cfg(test)]
    pub(crate) fn discard_receipts_local(&self) -> &BTreeMap<String, ReplicaDiscardReceipt> {
        &self.discard_request_receipts
    }

    /// This replica's batch write receipts. **LOCAL ONLY.**
    #[cfg(test)]
    pub(crate) fn batch_write_receipts_local(
        &self,
    ) -> &BTreeMap<String, ReplicaBatchWriteReceipt> {
        &self.batch_write_request_receipts
    }

    /// This replica's batch discard receipts. **LOCAL ONLY.**
    #[cfg(test)]
    pub(crate) fn batch_discard_receipts_local(
        &self,
    ) -> &BTreeMap<String, ReplicaBatchDiscardReceipt> {
        &self.batch_discard_request_receipts
    }

    /// This replica's read cache. **LOCAL ONLY**, and a `RefCell`, so this is enough to
    /// change it -- which is why only a test reaches for it.
    #[cfg(test)]
    pub(crate) fn read_cache_local(
        &self,
    ) -> &RefCell<HashMap<ReplicaReadCacheKey, ReplicaReadCacheEntry>> {
        &self.read_cache
    }

    // -----------------------------------------------------------------------------------
    // The shared handles, which are NOT the same accessor as the maps
    // -----------------------------------------------------------------------------------
    //
    // THE DISTINCTION IS LOAD-BEARING AND WAS FOUND BY THE COMPILER. The four accessors
    // above hand back `&BTreeMap`, dereferencing through the `Arc`. Four sites do not want
    // the map: they want the `Arc` itself, to put through `Arc::ptr_eq` and prove that a
    // snapshot of this service NAMES the same allocation rather than copying it.
    //
    // Returning the dereferenced map to those sites would have compiled if the comparison
    // had been `==` instead of `ptr_eq`, and it would have turned a test about SHARING into
    // a test about CONTENTS -- which passes just as happily when the snapshot has copied
    // every receipt. A pointer comparison is the only honest test that sharing happened, so
    // the handle gets its own accessor rather than being reachable by accident through the
    // other one.

    /// The shared handle holding the write receipts, for proving a snapshot shares it.
    #[cfg(test)]
    pub(crate) fn write_receipts_handle(
        &self,
    ) -> &std::sync::Arc<BTreeMap<String, ReplicaWriteReceipt>> {
        &self.write_request_receipts
    }

    /// The shared handle holding the discard receipts.
    #[cfg(test)]
    pub(crate) fn discard_receipts_handle(
        &self,
    ) -> &std::sync::Arc<BTreeMap<String, ReplicaDiscardReceipt>> {
        &self.discard_request_receipts
    }

    /// The shared handle holding the batch write receipts.
    #[cfg(test)]
    pub(crate) fn batch_write_receipts_handle(
        &self,
    ) -> &std::sync::Arc<BTreeMap<String, ReplicaBatchWriteReceipt>> {
        &self.batch_write_request_receipts
    }

    /// The shared handle holding the batch discard receipts.
    #[cfg(test)]
    pub(crate) fn batch_discard_receipts_handle(
        &self,
    ) -> &std::sync::Arc<BTreeMap<String, ReplicaBatchDiscardReceipt>> {
        &self.batch_discard_request_receipts
    }
}
