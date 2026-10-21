// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//
// The striped write and read, which is what makes the layout move the payload floor
// rather than describe a figure.
//
// # THE ADDRESSING, WHICH IS THE WHOLE TRICK
//
// Every replica holds EXACTLY ONE unit of each stripe group -- that is what
// `EcStripeLayout::replica_for` arranges -- so replica `r` can store group `g`'s unit at
// its own offset `g * unit_bytes`. Replica `r`'s page is then dense: one unit per group,
// laid out by group number, with no holes.
//
// Dense is the whole point. The obvious alternative is to give every replica the page's
// own offsets and write only its own ranges, which leaves the rest of the page as zeros
// -- and zeros that are stored are not a saving. With this addressing the store holds
// `replicas * groups * unit_bytes` against `data_units * groups * unit_bytes` of logical
// data, which is the `(k + m) / k` the layout promises and nothing more.
//
// Which unit of group `g` does replica `r` hold? `replica_for(g, i) == r` means
// `i == (r - g) mod replicas`. If that index is below `data_units` it is a data unit; if
// it equals `data_units` it is the redundancy unit.
//
// # WHAT A REPLICA'S PAGE MEANS NOW, AND WHAT STILL HOLDS
//
// A replica's page holds units rather than the page. Everything the replica itself does
// is unaffected: its checksum is over its own bytes, its validator compares its own
// logical length against its own payload, its read cache caches its own ranges, and its
// repair rebuilds its own page. Those were the assumptions worth checking and they are
// about a page's SELF-consistency, which striping does not disturb.
//
// What does change is the chunk service's read: `read_chunk_quorum` votes a majority over
// `(write_sequence, checksum, etag)` across replicas, and that is meaningless when every
// replica holds different bytes. So the striped read gathers and reconstructs instead.
// The CHOICE of algorithm is one line at the top of `read_chunk_quorum`, because all four
// of its callers pass the same three arguments and want the same bytes back -- dispatching
// there wires every read path at once. The algorithm itself is here.

impl MatrixChunkService {
    /// Store pages as stripe units across the replicas instead of copying them to each.
    ///
    /// Refused once anything has been written, because the layout decides where a byte
    /// lives: switching it would leave every existing page addressed one way and every
    /// new one addressed another, with nothing able to tell which.
    pub fn configure_stripe_layout(
        &mut self,
        layout: Option<EcStripeLayout>,
    ) -> Result<(), ObjectError> {
        if let Some(layout) = layout {
            if layout.group_units() != self.replicas.len() {
                return Err(ObjectError::InvalidArgument(format!(
                    "a layout of {} units needs {} replicas and this service has {}",
                    layout.group_units(),
                    layout.group_units(),
                    self.replicas.len()
                )));
            }
            if self.write_quorum < self.replicas.len() {
                // EVERY REPLICA, because a unit has no peer. A quorum short of all of them
                // would let a write succeed with one unit of a group missing, and that
                // group then has no redundancy left for the next failure -- the write
                // would have consumed the one loss the layout is allowed.
                return Err(ObjectError::InvalidArgument(format!(
                    "a striped write needs every replica to accept it, so write_quorum must \
                     be {} and it is {}",
                    self.replicas.len(),
                    self.write_quorum
                )));
            }
        }
        if self.replicas.iter().any(|replica| !replica.pages.is_empty()) {
            return Err(ObjectError::PreconditionFailed(
                "the stripe layout decides where a byte lives, so it cannot change once \
                 pages have been written"
                    .to_string(),
            ));
        }
        self.ec_layout = layout;
        Ok(())
    }

    /// The layout in force, if the service is striping.
    pub fn stripe_layout(&self) -> Option<EcStripeLayout> {
        self.ec_layout
    }

    /// Write ranges into one page, striped.
    ///
    /// `receipts` carries the replica-receipt prefix and the caller's idempotency key when
    /// the write is an idempotent one, so a retried striped write is deduplicated at each
    /// replica exactly as a retried replicated write is. Without it a striped service would
    /// quietly have no idempotent write while a replicated one did, which is the kind of
    /// asymmetry that is only ever found by the retry that needed it.
    pub(crate) fn write_striped_page_ranges(
        &mut self,
        page_id: &PageId,
        writes: &[(u64, &[u8])],
        receipts: Option<(&str, &str)>,
    ) -> Result<ReplicaWriteBatchStats, ObjectError> {
        let layout = self
            .ec_layout
            .ok_or_else(|| ObjectError::InvalidArgument("the service is not striping".to_string()))?;
        let mut stats = ReplicaWriteBatchStats::default();
        let mut touched_groups: BTreeSet<u64> = BTreeSet::new();

        for (offset, data) in writes.iter().copied() {
            if data.is_empty() {
                continue;
            }
            validate_pipeline_range("striped chunk write", offset, data.len() as u64)?;
            stats.input_ranges = stats.input_ranges.saturating_add(1);
            stats.input_bytes = stats.input_bytes.saturating_add(data.len() as u64);

            // Walk the range unit by unit: a range may start mid-unit and span several.
            let mut at = offset;
            let mut consumed = 0usize;
            while consumed < data.len() {
                let (group, unit_index) = layout.locate(at);
                let within = (at % layout.unit_bytes as u64) as usize;
                let take = (layout.unit_bytes - within).min(data.len() - consumed);
                let replica = layout.replica_for(group, unit_index);
                let replica_offset = group
                    .checked_mul(layout.unit_bytes as u64)
                    .and_then(|base| base.checked_add(within as u64))
                    .ok_or_else(|| {
                        ObjectError::InvalidArgument(
                            "striped write offset overflows the replica's page".to_string(),
                        )
                    })?;
                let unit_write = (replica_offset, &data[consumed..consumed + take]);
                let chunk_stats = match receipts {
                    Some((prefix, base_key)) => {
                        // KEYED BY THE BYTES GOING TO THE REPLICA, because each replica
                        // receives a different range of a striped write and one replica can
                        // receive several. A key naming only the page and the replica would
                        // make the second of those look like the first retried, and that
                        // replica would silently never receive its second unit.
                        //
                        // The replica index is in the key because this helper is shared with
                        // the replicated path, where every replica receives the SAME bytes
                        // and the index is the only thing telling their receipts apart. Here
                        // it is redundant -- the receipt map is per replica -- and no input
                        // can make it matter, so no test covers it.
                        let page_key = replica_receipt_key(
                            prefix,
                            base_key,
                            page_id,
                            replica,
                            &unit_write,
                        );
                        self.replicas[replica]
                            .write_chunk_ranges_idempotent_borrowed(
                                page_id.clone(),
                                &[unit_write],
                                &page_key,
                            )
                            .map(|(stats, _)| stats)?
                    }
                    // NO CALLER KEY, SO NO RECEIPT. A receipt here could only be keyed
                    // on the request's own checksum, and nothing looks that up -- a retry
                    // arrives with the caller's key or with nothing. The replicated
                    // single-range path writes through `write_chunk`, which records none
                    // either.
                    None => self.replicas[replica].write_chunk_ranges_borrowed_without_receipt(
                        page_id.clone(),
                        &[unit_write],
                    )?,
                };
                stats.physical_write_count = stats
                    .physical_write_count
                    .saturating_add(chunk_stats.physical_write_count);
                stats.physical_write_bytes = stats
                    .physical_write_bytes
                    .saturating_add(chunk_stats.physical_write_bytes);
                stats.coalesced_ranges = stats.coalesced_ranges.saturating_add(1);
                stats.coalesced_bytes = stats.coalesced_bytes.saturating_add(take as u64);
                touched_groups.insert(group);
                at += take as u64;
                consumed += take;
            }
        }

        // THE REDUNDANCY UNIT IS REWRITTEN PER TOUCHED GROUP, AFTER the data. Computing it
        // before would XOR the bytes the write is replacing, so the group would not
        // reconstruct -- and nothing would notice until a replica was lost.
        for group in touched_groups {
            self.rewrite_striped_group_redundancy(page_id, &layout, group)?;
        }
        Ok(stats)
    }

    /// Recompute one group's redundancy unit from whatever its data units now hold.
    fn rewrite_striped_group_redundancy(
        &mut self,
        page_id: &PageId,
        layout: &EcStripeLayout,
        group: u64,
    ) -> Result<(), ObjectError> {
        let base = group * layout.unit_bytes as u64;
        let mut units: Vec<Vec<u8>> = Vec::with_capacity(layout.data_units);
        for unit_index in 0..layout.data_units {
            let replica = layout.replica_for(group, unit_index);
            // Nothing there is padding HERE, which is the one place it legitimately is: a
            // group being filled has units that do not exist yet.
            units.push(
                // BYPASSING, and this is the whole of the saving: these bytes go into an
                // XOR on the next line and are never asked for again. Caching them filled
                // a bounded cache out of the write path, which the footprint audit counts
                // as resident -- 212 MB of it against 8 MB of logical data.
                self.read_striped_unit_bytes(
                    page_id,
                    layout,
                    replica,
                    base,
                    ReadCacheIntent::Bypass,
                )?
                .unwrap_or_default(),
            );
        }
        let borrowed: Vec<&[u8]> = units.iter().map(|unit| unit.as_slice()).collect();
        let redundancy = stripe_parity(layout, &borrowed);
        let replica = layout.replica_for(group, layout.parity_unit_index());
        // NOT GIVEN THE CALLER'S KEY, deliberately. This is a recompute, not a write being
        // applied: it reads whatever the group's data units now hold and writes the result,
        // so running it twice writes the same bytes. Keying it on the caller's request would
        // let a retry SKIP it, which is the one thing a redundancy rewrite must never do
        // after the data has moved.
        //
        // AND IT IS RECEIPT-FREE, which follows from the same fact. A receipt here could
        // only be keyed on this rewrite's own request checksum, and nothing ever looks such
        // a key up: the one receipt a retry can match is the caller's, recorded by the unit
        // write above. One per touched group was being written and could not be consulted,
        // which at 1 KiB objects was most of 636 bytes an object.
        self.replicas[replica].write_chunk_ranges_borrowed_without_receipt(
            page_id.clone(),
            &[(base, &redundancy)],
        )?;
        Ok(())
    }

    /// One unit's bytes as this replica holds them, or `None` if it holds nothing there.
    ///
    /// # WHY THIS IS AN `Option` AND NOT AN EMPTY VECTOR
    ///
    /// The two states have to be told apart and an empty vector cannot do it. A unit that
    /// was never written is not an error -- a group part-way through being filled has units
    /// that do not exist yet, and the redundancy over a zero-padded unit is the same
    /// arithmetic as over a short one. A unit whose replica LOST it is a loss, and offering
    /// it to reconstruction as a run of zeros makes the rebuild produce bytes nobody wrote.
    ///
    /// It did exactly that: a group with two units gone rebuilt silently wrong data instead
    /// of being refused, because the second absent unit arrived looking like zeros.
    ///
    /// A replica that HAS the page and holds bytes at the group's base has that unit,
    /// however short. One with no page, or whose page stops before the group, does not.
    ///
    /// `intent` is the caller's, because this one function serves both kinds of read: the
    /// unit a client asked for, and the sibling units the store reads to XOR.
    fn read_striped_unit_bytes(
        &self,
        page_id: &PageId,
        layout: &EcStripeLayout,
        replica: usize,
        base: u64,
        intent: ReadCacheIntent,
    ) -> Result<Option<Vec<u8>>, ObjectError> {
        let Some(state) = self.replicas[replica].pages.get(page_id) else {
            return Ok(None);
        };
        if base >= state.logical_length {
            return Ok(None);
        }
        let available = (state.logical_length - base).min(layout.unit_bytes as u64) as usize;
        let (read, _) = self.replicas[replica].read_chunk_with_cache_status(
            page_id,
            base,
            available as u64,
            intent,
        )?;
        Ok(Some(read.data))
    }

    /// Read a range of one page, striped, reconstructing any unit whose replica is gone.
    pub(crate) fn read_striped_page_range(
        &self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, ChunkReadStats), ObjectError> {
        let layout = self
            .ec_layout
            .ok_or_else(|| ObjectError::InvalidArgument("the service is not striping".to_string()))?;
        validate_pipeline_range("striped chunk read", offset, length)?;
        let mut out = Vec::with_capacity(length as usize);
        let mut stats = ChunkReadStats::default();
        let mut at = offset;

        while out.len() < length as usize {
            let (group, unit_index) = layout.locate(at);
            let within = (at % layout.unit_bytes as u64) as usize;
            let take = (layout.unit_bytes - within).min(length as usize - out.len());
            let replica = layout.replica_for(group, unit_index);
            let base = group * layout.unit_bytes as u64;

            let unit = if self.replica_active(replica) {
                match self.read_striped_unit_bytes(
                    page_id,
                    &layout,
                    replica,
                    base,
                    // THE BYTES THE CLIENT ASKED FOR, so this read fills as it always did.
                    ReadCacheIntent::Fill,
                ) {
                    Ok(Some(unit)) if unit.len() >= within + take => {
                        stats.physical_chunk_read_count =
                            stats.physical_chunk_read_count.saturating_add(1);
                        unit
                    }
                    // Absent, too short for what was asked, or a failed read: the group may
                    // be able to rebuild it, and serving a partial unit would hand back
                    // bytes nobody wrote.
                    _ => self.reconstruct_striped_unit(page_id, &layout, group, unit_index, &mut stats)?,
                }
            } else {
                self.reconstruct_striped_unit(page_id, &layout, group, unit_index, &mut stats)?
            };

            if unit.len() < within + take {
                return Err(ObjectError::InvalidArgument(
                    "striped read reaches past what this page holds".to_string(),
                ));
            }
            out.extend_from_slice(&unit[within..within + take]);
            at += take as u64;
        }
        Ok((out, stats))
    }

    /// Rebuild one data unit from its group's other units and the redundancy unit.
    fn reconstruct_striped_unit(
        &self,
        page_id: &PageId,
        layout: &EcStripeLayout,
        group: u64,
        unit_index: usize,
        stats: &mut ChunkReadStats,
    ) -> Result<Vec<u8>, ObjectError> {
        let base = group * layout.unit_bytes as u64;
        let mut offered: Vec<Option<Vec<u8>>> = Vec::with_capacity(layout.data_units);
        for index in 0..layout.data_units {
            if index == unit_index {
                offered.push(None);
                continue;
            }
            let replica = layout.replica_for(group, index);
            if !self.replica_active(replica) {
                offered.push(None);
                continue;
            }
            // A SIBLING IS NOT WHAT THE CLIENT ASKED FOR. The client asked for one unit;
            // these are read to XOR it back, exactly as on the write path, so they are the
            // same kind of read. Only the write path's effect is measured -- this is the
            // same reasoning applied to the same shape of read, not a second measurement.
            match self.read_striped_unit_bytes(
                page_id,
                layout,
                replica,
                base,
                ReadCacheIntent::Bypass,
            )? {
                // Zero-EXTENDED, not zero-invented: a short unit is a unit whose tail
                // nobody wrote, and the redundancy was computed over exactly that.
                Some(mut unit) => {
                    unit.resize(layout.unit_bytes, 0);
                    offered.push(Some(unit));
                }
                // NOT THERE IS MISSING. Offering zeros here is what made a group with two
                // losses rebuild silently wrong bytes; the codec refuses two and this is
                // what lets it see the second one.
                None => offered.push(None),
            }
        }
        let redundancy_replica = layout.replica_for(group, layout.parity_unit_index());
        if !self.replica_active(redundancy_replica) {
            return Err(ObjectError::Corruption(format!(
                "group {group} has lost a data unit and its redundancy unit, and one unit of \
                 redundancy recovers one"
            )));
        }
        // The redundancy unit goes into the same XOR as the siblings above.
        let Some(mut redundancy) = self.read_striped_unit_bytes(
            page_id,
            layout,
            redundancy_replica,
            base,
            ReadCacheIntent::Bypass,
        )?
        else {
            return Err(ObjectError::Corruption(format!(
                "group {group} has lost a data unit and its redundancy unit is not there"
            )));
        };
        redundancy.resize(layout.unit_bytes, 0);
        let borrowed: Vec<Option<&[u8]>> = offered
            .iter()
            .map(|unit| unit.as_ref().map(|unit| unit.as_slice()))
            .collect();
        let rebuilt = reconstruct_stripe_unit(layout, &borrowed, &redundancy)?;
        stats.replica_read_repairs = stats.replica_read_repairs.saturating_add(1);
        Ok(rebuilt)
    }
}
