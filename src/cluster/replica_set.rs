// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The fan-out, when the replicas are in other processes.
//!
//! # THE SHAPE IS THE EXISTING ONE, DELIBERATELY
//!
//! The write loop here counts acknowledgements against a write quorum and keeps the first
//! error; the read loop takes a majority over `(write_sequence, checksum, bytes)` and
//! refuses a set that cannot agree. That is the same rule the in-process fan-out applies,
//! kept the same on purpose: if the remote path used a different quorum rule, then a kill
//! test passing here would say nothing about the rule the store actually ships.
//!
//! # WHAT A WRITE QUORUM MEANS NOW
//!
//! In the in-process fan-out, a write quorum of 3 was three `BTreeMap::insert` calls on one
//! heap returning `Ok`, and the number was a loop count. Here it is three acknowledgements
//! from three processes, each of which put the bytes in its own address space before
//! answering. The arithmetic is identical and the guarantee is not: these three can be killed
//! one at a time.
//!
//! # WHY A FAILED READ SAYS WHICH KIND OF FAILURE IT WAS
//!
//! A read that finds nothing and a read that could not reach anybody are different events
//! that a single "quorum was not met" would merge. They are told apart here: if every
//! replica that answered said [`ObjectError::NotFound`], the set reports `NotFound`, and
//! anything else reports `Corruption` naming how many answered and how many were needed.
//! Without that, a test that writes a corpus, kills a node and then reads a key that was
//! never written cannot tell the absence it is checking for from the outage it just caused.

use crate::cluster::client::{ReplicaClientOptions, RemoteReplicaClient};
use crate::{ObjectError, PageId, ReplicaHead, ReplicaReadResult};

use std::collections::BTreeMap;

/// A set of replica nodes in other processes, and the quorums over them.
#[derive(Debug)]
pub struct RemoteReplicaSet {
    replicas: Vec<RemoteReplicaClient>,
    write_quorum: usize,
    read_quorum: usize,
}

impl RemoteReplicaSet {
    /// Connect to every endpoint, REFUSING to return a partly connected set.
    ///
    /// An endpoint that cannot be reached at startup fails the whole call. The alternative --
    /// carrying on with the ones that answered -- produces a set whose `write_quorum` is
    /// counted against a membership nobody chose, so a write that met quorum over two of an
    /// intended three would report success at the durability of two.
    pub fn connect(
        endpoints: &[String],
        write_quorum: usize,
        read_quorum: usize,
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
        if endpoints.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "a remote replica set needs at least one endpoint".to_string(),
            ));
        }
        if write_quorum == 0 || write_quorum > endpoints.len() {
            return Err(ObjectError::InvalidArgument(format!(
                "a write quorum of {write_quorum} cannot be met by {} replica(s)",
                endpoints.len()
            )));
        }
        if read_quorum == 0 || read_quorum > endpoints.len() {
            return Err(ObjectError::InvalidArgument(format!(
                "a read quorum of {read_quorum} cannot be met by {} replica(s)",
                endpoints.len()
            )));
        }
        let mut replicas = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            replicas.push(RemoteReplicaClient::connect(endpoint, options)?);
        }
        Ok(Self {
            replicas,
            write_quorum,
            read_quorum,
        })
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn read_quorum(&self) -> usize {
        self.read_quorum
    }

    /// Every connection's local and peer address, in endpoint order.
    ///
    /// This is what makes "the replicas are in other processes" checkable from outside
    /// rather than asserted in a comment: the pairs returned here are the four-tuples that
    /// appear in the operating system's own socket table, so a caller can match them against
    /// it and against the PID on each end.
    pub fn connections(&self) -> Result<Vec<(String, String)>, ObjectError> {
        let mut pairs = Vec::with_capacity(self.replicas.len());
        for replica in &self.replicas {
            pairs.push((
                replica.local_addr()?.to_string(),
                replica.peer_addr()?.to_string(),
            ));
        }
        Ok(pairs)
    }

    /// Write `data` at `offset` in `page_id` on every replica, and require a write quorum.
    ///
    /// Returns how many replicas acknowledged, which is always at least `write_quorum` when
    /// this returns `Ok`. The count is returned rather than discarded because "3 of 3" and
    /// "2 of 3" are both successes under a quorum of two and mean different things about what
    /// a later kill can take away.
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

    /// Read a byte range of `page_id`, taking a majority over the replicas that answer.
    ///
    /// Every replica is asked, not just enough of them to reach quorum. Stopping at the
    /// first `read_quorum` agreeing answers is the faster rule and is what the in-process
    /// path does; here the whole set is polled so that the count of replicas that answered
    /// is a real count, which is what a kill test needs to assert against.
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
        let mut unreachable = 0usize;
        let mut first_error: Option<ObjectError> = None;
        for replica in self.replicas.iter_mut() {
            match replica.read_chunk(page_id, offset, length) {
                Ok(read) => answered.push(read),
                Err(err) => {
                    if matches!(err, ObjectError::NotFound(_)) {
                        not_found += 1;
                    } else {
                        unreachable += 1;
                    }
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        if answered.len() < read_quorum {
            // THE ABSENT CASE IS NAMED, AND THE TEST IS A QUORUM OF ITS OWN. A read quorum
            // of replicas affirmatively said the page is not there, so this is absence and
            // not an outage -- even with other replicas unreachable at the same moment.
            //
            // The first version of this asked for `unreachable == 0` instead, which is the
            // rule that fails in the one case it matters: after a replica process is killed,
            // a key that was never written draws `NotFound` from each survivor and a dead
            // socket from the corpse, and that condition is false. A caller checking that an
            // unwritten key is absent would have been handed `Corruption` and been unable to
            // tell its control from the fault it had just induced.
            if not_found >= read_quorum {
                return Err(ObjectError::NotFound(format!(
                    "no replica holds this page: {not_found} of {replica_count} answered \
                     that it is absent, meeting a read quorum of {read_quorum}"
                )));
            }
            return Err(ObjectError::Corruption(format!(
                "a page read was answered by {} of {replica_count} replica(s), short of a \
                 read quorum of {read_quorum} ({unreachable} did not answer, {not_found} \
                 reported the page absent); the first failure was: {}",
                answered.len(),
                first_error
                    .map(|err| err.to_string())
                    .unwrap_or_else(|| "none recorded".to_string())
            )));
        }
        // The vote is over the three things a replica can disagree about: how many writes it
        // has seen, the checksum it keeps for the page, and the bytes it just handed back.
        // The bytes are in the key rather than trusted to follow from the checksum, because
        // two replicas agreeing on a checksum while returning different bytes is exactly the
        // case a vote exists to catch.
        //
        // WHAT THIS COSTS, SAID RATHER THAN HIDDEN: the key holds a COPY of the payload, so a
        // quorum read of an `n`-byte range over `r` replicas holds about `2rn` bytes at its
        // peak -- the answers plus the vote keys -- against `rn` for a vote over a digest. At
        // a 1 MiB page and three replicas that is roughly 6 MiB transient instead of 3 MiB.
        // The in-process path votes over `(write_sequence, checksum, etag)` and pays the
        // smaller figure. This one is deliberately the stricter comparison while the
        // arrangement is new: a digest collision here would hand back bytes a quorum never
        // agreed on, and the first thing worth being sure of about a remote replica is that
        // what arrived is what the others have. Replacing the payload in this key with a
        // digest is a change worth making once that is established, and it is a change to
        // this line.
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
                "a page read got {} answer(s) from {replica_count} replica(s) and the \
                 largest group agreeing was {winning_count}, short of a read quorum of \
                 {read_quorum}: the replicas hold divergent copies of this page",
                answered.len()
            )));
        }
        let (write_sequence, checksum, data) = winning_key;
        Ok(ReplicaReadResult {
            data,
            write_sequence,
            checksum,
        })
    }

    /// Read a page's metadata from a read quorum, voting on the whole head.
    pub fn head_chunk(&mut self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut answered = Vec::with_capacity(replica_count);
        let mut not_found = 0usize;
        let mut unreachable = 0usize;
        for replica in self.replicas.iter_mut() {
            match replica.head_chunk(page_id) {
                Ok(head) => answered.push(head),
                Err(ObjectError::NotFound(_)) => not_found += 1,
                Err(_) => unreachable += 1,
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
                 read quorum of {read_quorum} ({unreachable} did not answer, {not_found} \
                 reported the page absent)",
                answered.len()
            )));
        }
        let mut votes: BTreeMap<String, (usize, ReplicaHead)> = BTreeMap::new();
        for head in answered {
            let entry = votes
                .entry(head.state_checksum.clone())
                .or_insert((0, head));
            entry.0 += 1;
        }
        let (winning_count, winning_head) = votes
            .into_values()
            .max_by_key(|(count, _)| *count)
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "a page head reached quorum with no answers to vote over".to_string(),
                )
            })?;
        if winning_count < read_quorum {
            return Err(ObjectError::Corruption(format!(
                "the largest group of replicas agreeing on this page's state was \
                 {winning_count}, short of a read quorum of {read_quorum}"
            )));
        }
        Ok(winning_head)
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

    /// How many replicas answer a ping right now, and how many pages each says it holds.
    ///
    /// `None` for a replica that did not answer. Used to establish the regime before a test
    /// asserts anything about it: three entries that are all `Some` is the observable form
    /// of three live replica processes.
    pub fn ping_all(&mut self) -> Vec<Option<u64>> {
        self.replicas
            .iter_mut()
            .map(|replica| replica.ping().ok())
            .collect()
    }

    /// Every endpoint, in the order writes are sent to them.
    pub fn endpoints(&self) -> Vec<String> {
        self.replicas
            .iter()
            .map(|replica| replica.endpoint().to_string())
            .collect()
    }
}
