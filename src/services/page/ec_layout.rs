// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Placing a page's bytes across replicas instead of copying them to each.
//
// # WHY THE PAYLOAD SIDE IS STUCK AT 3.0000x WITHOUT THIS
//
// `measure_what_sits_above_the_replication_floor_on_each_side` reports `replica.data.capacity` at
// exactly 3.0000x of logical bytes, and everything above it is 16 bytes an object of payload index
// that exists to catch a payload disagreeing with its extent. There is no slack to recover: three
// replicas hold three copies, each exactly the size of the data.
//
// The existing redundancy fragment does not help, and
// `measure_what_the_redundancy_fragment_buys_at_each_replica_count` says why: it is computed from
// one page's own bytes and kept on the SAME replica, so it is redundancy *within* a copy. It is
// also switched off entirely once a page has peers.
//
// What moves the floor is placing DIFFERENT bytes on different replicas: `data_units` units of
// data and `parity_units` of redundancy, so the store holds
// `(data_units + parity_units) / data_units` of the logical size instead of one copy per replica.
// At three replicas with two data units and one parity unit that is **1.5x instead of 3.0x**, and
// it survives losing any one replica where three-way replication survives two.
//
// # WHY FIXED-SIZE UNITS AND NOT "SPLIT THE PAGE N WAYS"
//
// Splitting a page into `k` equal pieces makes the piece size a function of the page's current
// length, so a page that GROWS changes every boundary and every replica has to be rewritten. A
// write at one offset would re-stripe the whole page.
//
// Fixed units do not move. Byte `p` of a page lives in unit `p / unit_bytes` for the whole life of
// the page, so a write touches the one unit holding it plus the one parity unit of its group, and
// nothing else is read or written. That is the property that makes the layout usable for a store
// whose pages are appended to rather than written once.
//
// # WHAT IS HERE AND WHAT IS NOT
//
// Here: the layout arithmetic, the placement, the parity, and reconstruction of one lost unit --
// with the amplification it produces measured against the replicated figure.
//
// NOT here: the storage path. Wiring this in replaces what a replica's page HOLDS, and the read
// path currently votes on identical copies from each replica (`read_chunk_quorum`), which is
// meaningless when each one holds a different unit. That is a change to a durable layout and to
// the recovery story, so it is a decision to take deliberately rather than a consequence of
// landing a codec. The arithmetic is here so the decision has numbers.

/// How a page's bytes are spread over a replica set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EcStripeLayout {
    /// Units of real data in one stripe group.
    pub data_units: usize,
    /// Units of redundancy in one stripe group. One unit survives one loss.
    pub parity_units: usize,
    /// Bytes in one unit. Fixed, so a page's unit boundaries never move.
    pub unit_bytes: usize,
}

impl EcStripeLayout {
    /// The layout for a replica set of `replicas`, keeping `parity_units` of redundancy.
    ///
    /// Every replica holds one unit of each stripe group, so `data_units` is whatever is left.
    /// Refused rather than clamped when the numbers cannot describe a layout: a store that asked
    /// for more redundancy than it has replicas would otherwise get silently less than it asked
    /// for, which is the one mistake a durability setting must not make.
    pub fn for_replicas(
        replicas: usize,
        parity_units: usize,
        unit_bytes: usize,
    ) -> Result<Self, ObjectError> {
        if unit_bytes == 0 {
            return Err(ObjectError::InvalidArgument(
                "stripe unit must hold bytes".to_string(),
            ));
        }
        if parity_units == 0 {
            return Err(ObjectError::InvalidArgument(
                "a layout with no redundancy is not a layout: use replication".to_string(),
            ));
        }
        if replicas <= parity_units {
            return Err(ObjectError::InvalidArgument(format!(
                "{replicas} replicas cannot hold {parity_units} units of redundancy and any data"
            )));
        }
        Ok(Self {
            data_units: replicas - parity_units,
            parity_units,
            unit_bytes,
        })
    }

    /// Units in one stripe group, which is also the replica count.
    pub fn group_units(&self) -> usize {
        self.data_units + self.parity_units
    }

    /// Bytes of real data in one stripe group.
    pub fn group_data_bytes(&self) -> usize {
        self.data_units * self.unit_bytes
    }

    /// What the store holds per byte of logical data, in thousandths.
    ///
    /// In thousandths rather than as a float so a test can assert it exactly: 1500 is 1.5x, where
    /// three-way replication is 3000.
    pub fn amplification_permille(&self) -> u64 {
        (self.group_units() as u64 * 1000) / self.data_units as u64
    }

    /// Which stripe group and which unit within it hold byte `offset` of a page.
    pub fn locate(&self, offset: u64) -> (u64, usize) {
        let unit = offset / self.unit_bytes as u64;
        (unit / self.data_units as u64, (unit % self.data_units as u64) as usize)
    }

    /// Which replica holds unit `unit_index` of group `group`.
    ///
    /// ROTATED BY THE GROUP, so the parity unit is not always on the same replica. Pinning parity
    /// to one replica makes that replica carry every write in the store -- each write to any unit
    /// updates its group's parity -- and turns a single node into the write bottleneck for the
    /// whole layout.
    pub fn replica_for(&self, group: u64, unit_index: usize) -> usize {
        ((group as usize) + unit_index) % self.group_units()
    }

    /// Which unit index of `group` the parity is, so a reader knows which replica to skip.
    pub fn parity_unit_index(&self) -> usize {
        self.data_units
    }
}

/// The redundancy unit for one stripe group.
///
/// XOR across the data units, which survives exactly one loss. Units shorter than `unit_bytes` are
/// treated as zero-padded, which is what lets a partly-written group be coded without waiting for
/// it to fill.
pub fn stripe_parity(layout: &EcStripeLayout, units: &[&[u8]]) -> Vec<u8> {
    let mut parity = vec![0u8; layout.unit_bytes];
    for unit in units {
        for (at, byte) in unit.iter().enumerate() {
            if at < parity.len() {
                parity[at] ^= byte;
            }
        }
    }
    parity
}

/// Rebuild the one missing unit of a stripe group.
///
/// `units` holds every data unit in order, with `None` for the one that is gone. Returns the bytes
/// of the missing unit, zero-padded to `unit_bytes` -- the caller knows the page's real length from
/// its extents and truncates.
///
/// Refuses two losses rather than returning something: XOR redundancy of one unit recovers one
/// unit, and a function that answered anyway would be inventing bytes.
pub fn reconstruct_stripe_unit(
    layout: &EcStripeLayout,
    units: &[Option<&[u8]>],
    parity: &[u8],
) -> Result<Vec<u8>, ObjectError> {
    if units.len() != layout.data_units {
        return Err(ObjectError::InvalidArgument(format!(
            "a group holds {} data units and {} were offered",
            layout.data_units,
            units.len()
        )));
    }
    if parity.len() != layout.unit_bytes {
        return Err(ObjectError::Corruption(format!(
            "a redundancy unit is {} bytes and the layout is {}",
            parity.len(),
            layout.unit_bytes
        )));
    }
    let missing = units.iter().filter(|unit| unit.is_none()).count();
    if missing == 0 {
        return Err(ObjectError::InvalidArgument(
            "nothing is missing from this group, so there is nothing to rebuild".to_string(),
        ));
    }
    if missing > 1 {
        return Err(ObjectError::Corruption(format!(
            "{missing} units of this group are gone and one unit of redundancy recovers one"
        )));
    }
    let mut rebuilt = parity.to_vec();
    for unit in units.iter().flatten() {
        for (at, byte) in unit.iter().enumerate() {
            if at < rebuilt.len() {
                rebuilt[at] ^= byte;
            }
        }
    }
    Ok(rebuilt)
}

/// Split a page's bytes into the data units of each stripe group, in order.
///
/// The last unit of the last group is SHORT rather than padded, so the caller can see how many
/// real bytes it holds. `stripe_parity` treats a short unit as zero-padded, which is the same
/// arithmetic without storing the padding.
pub fn stripe_data_units<'a>(layout: &EcStripeLayout, data: &'a [u8]) -> Vec<Vec<&'a [u8]>> {
    let mut groups = Vec::new();
    let mut at = 0usize;
    while at < data.len() {
        let mut group = Vec::with_capacity(layout.data_units);
        for _ in 0..layout.data_units {
            if at >= data.len() {
                break;
            }
            let end = (at + layout.unit_bytes).min(data.len());
            group.push(&data[at..end]);
            at = end;
        }
        groups.push(group);
    }
    groups
}

/// What the store would hold for `logical_bytes` under this layout, against replication.
///
/// Both figures include only the data, which is the leg that is at 3.0000x today -- the payload
/// index and the handles sit above the floor and are the same either way.
pub fn stored_bytes(layout: &EcStripeLayout, logical_bytes: u64) -> u64 {
    let group_data = layout.group_data_bytes() as u64;
    let full_groups = logical_bytes / group_data;
    let remainder = logical_bytes % group_data;
    // A PARTIAL GROUP STILL PAYS A WHOLE REDUNDANCY UNIT, because the redundancy covers the group
    // and not the bytes in it. Counted, because a layout costed as if it did not would understate
    // what a store holding many small pages actually pays.
    let partial = if remainder == 0 {
        0
    } else {
        remainder + (layout.parity_units * layout.unit_bytes) as u64
    };
    full_groups * (group_data + (layout.parity_units * layout.unit_bytes) as u64) + partial
}
