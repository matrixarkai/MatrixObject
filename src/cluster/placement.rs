// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Where this store's replicas are, and the one place that chooses.
//!
//! # THE DEFAULT IS WHAT THE STORE DID BEFORE
//!
//! [`ReplicaPlacement::default`] is [`ReplicaPlacement::InProcess`] with three replicas and
//! a write quorum of two: replicas in this address space, reached by a function call. Nothing
//! that exists today passes through this type, so turning it on changes nothing, and reading
//! the default tells you what the store has always been.
//!
//! # WHY THIS IS AN ENUM AND NOT A FLAG
//!
//! A boolean `remote: bool` would need the endpoints somewhere else, and then a build with
//! `remote = true` and no endpoints is representable. Here it is not: the remote arm carries
//! its endpoints, the in-process arm carries its replica count, and neither can be
//! configured into a state that has to be checked for later.
//!
//! # BOTH ARMS APPLY THE SAME QUORUM RULE, AND THAT IS ASSERTED
//!
//! A write counts acknowledgements against a write quorum; a read takes a majority over
//! `(write_sequence, checksum, bytes)` against a read quorum. That rule is written once per
//! arm -- once over `MatrixReplicaService` method calls and once over sockets -- which is two
//! places it could drift. So it is not left to a comment: the suite runs one corpus through
//! both arms and requires the bytes read back to be equal, which is what makes "the remote
//! path behaves like the local one" a thing that fails when it stops being true.

use crate::cluster::client::ReplicaClientOptions;
use crate::cluster::replica_set::RemoteReplicaSet;
use crate::{MatrixReplicaService, ObjectError, PageId, ReplicaHead, ReplicaReadResult};

use std::collections::BTreeMap;

/// Where the replicas of this store live.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaPlacement {
    /// Replicas in this process, reached by a function call. The historic arrangement.
    InProcess {
        replica_count: usize,
        write_quorum: usize,
        read_quorum: usize,
    },
    /// Replicas in other processes, reached over a socket.
    Remote {
        endpoints: Vec<String>,
        write_quorum: usize,
        read_quorum: usize,
    },
}

impl Default for ReplicaPlacement {
    fn default() -> Self {
        ReplicaPlacement::InProcess {
            replica_count: 3,
            write_quorum: 2,
            read_quorum: 2,
        }
    }
}

/// The smallest majority of `replica_count` replicas.
///
/// `replica_count / 2 + 1`: 1 of 1, 2 of 2, 2 of 3, 3 of 4, 3 of 5, 4 of 7. Two majorities of
/// the same set always share a member, which is the property every quorum rule below is
/// built on.
///
/// # WHY THIS IS A FUNCTION AND NOT A NUMBER AT EACH CALL SITE
///
/// A majority written as a literal is a majority for one width. The write quorum of 2 that
/// this store shipped is a majority of three and `all but one` of three at the same time, so
/// the two readings were indistinguishable at the only width that existed. They part company
/// at five, where a majority is 3 and all-but-one is 4 -- and the arrangement a quorum rule
/// is for is the one where two of the five are gone. Written once, it cannot be a majority in
/// one arm and a head count in the other.
pub fn majority(replica_count: usize) -> usize {
    replica_count / 2 + 1
}

/// Check a quorum pair against a replica count, REFUSING a pair that cannot read its writes.
///
/// Three clauses, each refused by its own name:
///
/// 1. a write quorum of zero, or one larger than the set;
/// 2. a read quorum of zero, or one larger than the set;
/// 3. `write_quorum + read_quorum <= replica_count` -- the two quorums need not share a
///    replica, so a read quorum can be assembled entirely out of replicas that the write
///    quorum never reached.
///
/// # THE THIRD CLAUSE IS THE ONE THAT WAS MISSING, AND WHY IT COULD NOT BE SEEN AT THREE
///
/// The first two are arithmetic about whether a quorum is reachable at all. The third is
/// about whether meeting both of them means anything, and it is the one a wider set needs.
/// At three replicas the shipped pair 2/2 sums to 4, which is already greater than 3, so
/// every pair this store had ever been configured with satisfied the third clause by
/// accident. At five replicas 2/2 still passes the first two clauses and sums to 4, which is
/// NOT greater than 5: replicas 1 and 2 can acknowledge a write while replicas 3 and 4
/// answer the read, and the read returns the previous value having met its quorum honestly.
/// Nothing in the fan-out can detect that afterwards, because every count involved is
/// correct. So it is refused here, at the one point where a set is constructed, rather than
/// reported later by a read that has no way to know it happened.
///
/// # ONE FUNCTION, BOTH ARMS, ON PURPOSE
///
/// [`InProcessReplicaSet::new`] and [`crate::cluster::replica_set::RemoteReplicaSet::connect`]
/// both call this. The quorum rule is already written twice -- once over method calls and
/// once over sockets -- and that is two places it can drift; the admission rule is not going
/// to be a third and a fourth.
pub fn check_quorums(
    replica_count: usize,
    write_quorum: usize,
    read_quorum: usize,
) -> Result<(), ObjectError> {
    if write_quorum == 0 || write_quorum > replica_count {
        return Err(ObjectError::InvalidArgument(format!(
            "a write quorum of {write_quorum} cannot be met by {replica_count} replica(s)"
        )));
    }
    if read_quorum == 0 || read_quorum > replica_count {
        return Err(ObjectError::InvalidArgument(format!(
            "a read quorum of {read_quorum} cannot be met by {replica_count} replica(s)"
        )));
    }
    // `checked_add` rather than `+`: both are caller-supplied `usize`, and a sum that wrapped
    // would pass this clause by being small.
    let overlap = write_quorum.checked_add(read_quorum).ok_or_else(|| {
        ObjectError::InvalidArgument(format!(
            "a write quorum of {write_quorum} and a read quorum of {read_quorum} overflow \
             when added, so their overlap over {replica_count} replica(s) cannot be checked"
        ))
    })?;
    if overlap <= replica_count {
        return Err(ObjectError::InvalidArgument(format!(
            "a write quorum of {write_quorum} and a read quorum of {read_quorum} sum to \
             {overlap} over {replica_count} replica(s), so the two quorums need not share a \
             replica and a read can miss an acknowledged write; a sum greater than \
             {replica_count} is needed"
        )));
    }
    Ok(())
}

impl ReplicaPlacement {
    // DELIBERATELY ABSENT: a `from_env` reading `MATRIXOBJECT_REPLICA_ENDPOINTS` and a
    // `describe` for a startup line. Both were written and then removed before this landed,
    // because nothing in this slice reads either one: the fan-out is a library type, and the
    // process that would parse an endpoint list and log the arrangement it chose is the
    // coordinator binary that does not exist yet. A setting nothing reads is not
    // configuration, it is a promise about a future caller, and it rots silently because no
    // test can tell it from a working one. They belong in the change that adds that binary.

    /// Replicas in other processes, with a majority for both quorums.
    ///
    /// The pair this produces always satisfies the overlap clause in [`check_quorums`]:
    /// `2 * majority(n) > n` for every `n`. That is the point of the constructor -- a caller
    /// that wants "a majority of however many nodes I was given" cannot express it as two
    /// numbers without restating the arithmetic, and restating it is where a five-node set
    /// acquires a three-node set's quorum.
    pub fn remote_with_majority(endpoints: Vec<String>) -> Self {
        let quorum = majority(endpoints.len());
        ReplicaPlacement::Remote {
            endpoints,
            write_quorum: quorum,
            read_quorum: quorum,
        }
    }

    /// Replicas in this process, with a majority for both quorums.
    pub fn in_process_with_majority(replica_count: usize) -> Self {
        let quorum = majority(replica_count);
        ReplicaPlacement::InProcess {
            replica_count,
            write_quorum: quorum,
            read_quorum: quorum,
        }
    }

    /// How many replicas this placement addresses.
    pub fn replica_count(&self) -> usize {
        match self {
            ReplicaPlacement::InProcess { replica_count, .. } => *replica_count,
            ReplicaPlacement::Remote { endpoints, .. } => endpoints.len(),
        }
    }

    /// The write quorum and then the read quorum, whichever arm this is.
    pub fn quorums(&self) -> (usize, usize) {
        match self {
            ReplicaPlacement::InProcess {
                write_quorum,
                read_quorum,
                ..
            }
            | ReplicaPlacement::Remote {
                write_quorum,
                read_quorum,
                ..
            } => (*write_quorum, *read_quorum),
        }
    }

    /// True when both quorums are the smallest majority of this placement's width.
    ///
    /// Distinguishes a majority from `all but one`, which are the same number at three
    /// replicas and different numbers at four or more.
    pub fn is_majority_quorum(&self) -> bool {
        let quorum = majority(self.replica_count());
        self.quorums() == (quorum, quorum)
    }

    /// True when this placement puts replicas in other processes.
    pub fn is_remote(&self) -> bool {
        matches!(self, ReplicaPlacement::Remote { .. })
    }

    /// Build the fan-out this placement describes.
    pub fn open(&self, options: &ReplicaClientOptions) -> Result<ReplicaFanout, ObjectError> {
        match self {
            ReplicaPlacement::InProcess {
                replica_count,
                write_quorum,
                read_quorum,
            } => Ok(ReplicaFanout::InProcess(InProcessReplicaSet::new(
                *replica_count,
                *write_quorum,
                *read_quorum,
            )?)),
            ReplicaPlacement::Remote {
                endpoints,
                write_quorum,
                read_quorum,
            } => Ok(ReplicaFanout::Remote(RemoteReplicaSet::connect(
                endpoints,
                *write_quorum,
                *read_quorum,
                options,
            )?)),
        }
    }
}

/// Replicas in this process, with a quorum over them.
///
/// Holds the same [`MatrixReplicaService`] the store has always used; what this adds is the
/// quorum loop around a `Vec` of them, so that the two placements are addressed the same way.
#[derive(Debug)]
pub struct InProcessReplicaSet {
    replicas: Vec<MatrixReplicaService>,
    write_quorum: usize,
    read_quorum: usize,
}

impl InProcessReplicaSet {
    pub fn new(
        replica_count: usize,
        write_quorum: usize,
        read_quorum: usize,
    ) -> Result<Self, ObjectError> {
        if replica_count == 0 {
            return Err(ObjectError::InvalidArgument(
                "an in-process replica set needs at least one replica".to_string(),
            ));
        }
        check_quorums(replica_count, write_quorum, read_quorum)?;
        Ok(Self {
            replicas: (0..replica_count)
                .map(|_| MatrixReplicaService::new())
                .collect(),
            write_quorum,
            read_quorum,
        })
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn write_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, ObjectError> {
        let mut acknowledged = 0usize;
        let mut first_error: Option<ObjectError> = None;
        for replica in self.replicas.iter_mut() {
            match replica.write_chunk(page_id.clone(), offset, data) {
                Ok(()) => acknowledged += 1,
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }
        if acknowledged < self.write_quorum {
            return Err(first_error.unwrap_or_else(|| {
                ObjectError::Corruption(format!(
                    "a page write was acknowledged by {acknowledged} of {} replica(s), \
                     short of a write quorum of {}",
                    self.replicas.len(),
                    self.write_quorum
                ))
            }));
        }
        Ok(acknowledged)
    }

    pub fn read_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<ReplicaReadResult, ObjectError> {
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut answered = Vec::with_capacity(replica_count);
        let mut not_found = 0usize;
        let mut refused = 0usize;
        for replica in self.replicas.iter() {
            match replica.read_chunk(page_id, offset, length) {
                Ok(read) => answered.push(read),
                Err(ObjectError::NotFound(_)) => not_found += 1,
                Err(_) => refused += 1,
            }
        }
        if answered.len() < read_quorum {
            if not_found >= read_quorum {
                return Err(ObjectError::NotFound(format!(
                    "no replica holds this page: {not_found} of {replica_count} answered \
                     that it is absent, meeting a read quorum of {read_quorum}"
                )));
            }
            return Err(ObjectError::Corruption(format!(
                "a page read was answered by {} of {replica_count} replica(s), short of a \
                 read quorum of {read_quorum} ({refused} refused the read, {not_found} \
                 reported the page absent)",
                answered.len()
            )));
        }
        let mut votes: BTreeMap<(u64, String, Vec<u8>), usize> = BTreeMap::new();
        for read in &answered {
            let key = (read.write_sequence, read.checksum.clone(), read.data.clone());
            *votes.entry(key).or_insert(0) += 1;
        }
        let (winning_key, winning_count) = votes
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "a page read reached quorum with no answers to vote over".to_string(),
                )
            })?;
        if winning_count < read_quorum {
            return Err(ObjectError::Corruption(format!(
                "the largest group of replicas agreeing on this page was {winning_count}, \
                 short of a read quorum of {read_quorum}"
            )));
        }
        let (write_sequence, checksum, data) = winning_key;
        Ok(ReplicaReadResult {
            data,
            write_sequence,
            checksum,
        })
    }

    /// Discard a byte range on every replica, requiring a write quorum.
    pub fn discard_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<usize, ObjectError> {
        let mut acknowledged = 0usize;
        let mut first_error: Option<ObjectError> = None;
        for replica in self.replicas.iter_mut() {
            match replica.discard_chunk(page_id, offset, length) {
                Ok(()) => acknowledged += 1,
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
        }
        if acknowledged < self.write_quorum {
            return Err(first_error.unwrap_or_else(|| {
                ObjectError::Corruption(format!(
                    "a page discard was acknowledged by {acknowledged} of {} replica(s), \
                     short of a write quorum of {}",
                    self.replicas.len(),
                    self.write_quorum
                ))
            }));
        }
        Ok(acknowledged)
    }

    pub fn head_chunk(&mut self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut answered = Vec::with_capacity(replica_count);
        let mut not_found = 0usize;
        let mut refused = 0usize;
        for replica in self.replicas.iter() {
            match replica.head_chunk(page_id) {
                Ok(head) => answered.push(head),
                Err(ObjectError::NotFound(_)) => not_found += 1,
                Err(_) => refused += 1,
            }
        }
        if answered.len() < read_quorum {
            if not_found >= read_quorum {
                return Err(ObjectError::NotFound(format!(
                    "no replica holds this page: {not_found} of {replica_count} answered \
                     that it is absent, meeting a read quorum of {read_quorum}"
                )));
            }
            return Err(ObjectError::Corruption(format!(
                "a page head was answered by {} of {replica_count} replica(s), short of a \
                 read quorum of {read_quorum} ({refused} refused the read, {not_found} \
                 reported the page absent)",
                answered.len()
            )));
        }
        vote_heads(answered, read_quorum)
    }
}

/// A fan-out over replicas, wherever the placement put them.
///
/// Two arms, one set of calls. A caller written against this does not know whether it is
/// addressing three `BTreeMap`s on this heap or three processes on three machines -- which is
/// the property that makes the remote arrangement adoptable rather than a second store.
#[derive(Debug)]
pub enum ReplicaFanout {
    InProcess(InProcessReplicaSet),
    Remote(RemoteReplicaSet),
}

impl ReplicaFanout {
    pub fn is_remote(&self) -> bool {
        matches!(self, ReplicaFanout::Remote(_))
    }

    pub fn replica_count(&self) -> usize {
        match self {
            ReplicaFanout::InProcess(set) => set.replica_count(),
            ReplicaFanout::Remote(set) => set.replica_count(),
        }
    }

    pub fn write_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, ObjectError> {
        match self {
            ReplicaFanout::InProcess(set) => set.write_chunk(page_id, offset, data),
            ReplicaFanout::Remote(set) => set.write_chunk(page_id, offset, data),
        }
    }

    pub fn read_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<ReplicaReadResult, ObjectError> {
        match self {
            ReplicaFanout::InProcess(set) => set.read_chunk(page_id, offset, length),
            ReplicaFanout::Remote(set) => set.read_chunk(page_id, offset, length),
        }
    }

    pub fn head_chunk(&mut self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        match self {
            ReplicaFanout::InProcess(set) => set.head_chunk(page_id),
            ReplicaFanout::Remote(set) => set.head_chunk(page_id),
        }
    }

    pub fn discard_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<usize, ObjectError> {
        match self {
            ReplicaFanout::InProcess(set) => set.discard_chunk(page_id, offset, length),
            ReplicaFanout::Remote(set) => set.discard_chunk(page_id, offset, length),
        }
    }
}

// A digest is one field of the reply, not proof that the other fields agree.
pub(super) fn vote_heads(
    answered: Vec<ReplicaHead>,
    read_quorum: usize,
) -> Result<ReplicaHead, ObjectError> {
    let mut votes = BTreeMap::new();
    for head in answered {
        let key = (
            head.logical_length,
            head.write_sequence,
            head.checksum.clone(),
            head.state_checksum.clone(),
            head.discarded_ranges
                .iter()
                .map(|range| (range.offset, range.length))
                .collect::<Vec<_>>(),
        );
        let entry = votes.entry(key).or_insert((0usize, head));
        entry.0 += 1;
    }
    let (winning_count, winning_head) = votes
        .into_values()
        .max_by_key(|(count, _)| *count)
        .ok_or_else(|| {
            ObjectError::Corruption(
                "a page head reached quorum with no answers to vote over".into(),
            )
        })?;
    if winning_count < read_quorum {
        return Err(ObjectError::Corruption(format!(
            "the largest group of replicas agreeing on this page's complete head was {winning_count}, short of a read quorum of {read_quorum}"
        )));
    }
    Ok(winning_head)
}
