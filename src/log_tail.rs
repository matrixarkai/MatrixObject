// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Opening a log chunk for append: the writer lease and the tail-length rule.
//!
//! A log chunk's length is not recorded per write, so a writer that opens the chunk (a new
//! gateway, or the same one after a crash) must decide where the chunk ends from what the
//! replicas hold, then fence the previous writer and cut the replicas that hold more.
//!
//! * `WriterLeases` is the table the metadata plane keeps: one live lease per object, each grant
//!   carries an epoch that only goes up. The chunk replicas refuse any write below the highest
//!   epoch they have seen, which is what moves the single-writer and offset checks off the
//!   metadata compare-and-swap and onto the chunk owner.
//! * `decide_tail` is the rule that picks the length from the replica reports.
use std::collections::BTreeMap;

/// How a write is acknowledged to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Every replica holds a write before it is acknowledged.
    Strong,
    /// A majority holds a write before it is acknowledged.
    Quorum,
}

/// What one replica says about the chunk. `crc` is the CRC-32C of all `length` bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaTail {
    pub length: u64,
    pub crc: u32,
    pub epoch: u64,
    /// Sealed at exactly `length`.
    pub sealed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TailError {
    /// Fewer replicas answered than the rule needs to be sure no acknowledged byte is dropped.
    TooFewReplicas { reachable: usize, needed: usize },
    /// Two replicas hold the same length with different bytes, or sealed replicas disagree.
    Divergent,
    /// The metadata plane recorded a length no reachable replica holds.
    ShorterThanRecorded { recorded: u64, longest: u64 },
}

/// The decision: the chunk ends at `length`. Replicas listed in `cut` hold more and must be
/// truncated to it; replicas in `behind` hold less and must catch up (or be replaced) before they
/// serve a write again. Indexes are positions in the slice passed to `decide_tail`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailDecision {
    pub length: u64,
    pub sealed: bool,
    pub cut: Vec<usize>,
    pub behind: Vec<usize>,
}

/// Decide the length of a chunk being opened for append.
///
/// `replicas[i]` is `None` for a replica that did not answer. `recorded` is the length the
/// metadata plane holds from a chunk switch or check-in, when it holds one; a recorded length is
/// final and wins, since the chunk was closed at it.
///
/// Without a recorded length:
/// * `Strong`: every acknowledged write is on every replica, so the shortest answer keeps all of
///   them. One answer is enough to be safe; any number is accepted.
/// * `Quorum`: every acknowledged write is on at least two of three, so with all three answering
///   the length is the second longest (the longest length at least two replicas hold); with two
///   answering the longest of the two is taken, since the acknowledged bytes may be on only one
///   of them; with one answering the rule refuses.
/// A sealed replica fixes the chunk: it was closed at that length.
pub fn decide_tail(
    mode: WriteMode,
    replicas: &[Option<ReplicaTail>],
    recorded: Option<u64>,
) -> Result<TailDecision, TailError> {
    let seen: Vec<(usize, ReplicaTail)> =
        replicas.iter().enumerate().filter_map(|(i, r)| r.map(|r| (i, r))).collect();
    if seen.is_empty() {
        return Err(TailError::TooFewReplicas { reachable: 0, needed: 1 });
    }
    // replicas holding the same length must hold the same bytes
    for (i, a) in &seen {
        for (j, b) in &seen {
            if i < j && a.length == b.length && a.crc != b.crc {
                return Err(TailError::Divergent);
            }
        }
    }
    let sealed: Vec<&ReplicaTail> = seen.iter().map(|(_, r)| r).filter(|r| r.sealed).collect();
    let length = if let Some(first) = sealed.first() {
        if sealed.iter().any(|r| r.length != first.length || r.crc != first.crc) {
            return Err(TailError::Divergent);
        }
        if recorded.is_some_and(|recorded| recorded != first.length) {
            return Err(TailError::Divergent);
        }
        first.length
    } else if let Some(recorded) = recorded {
        let longest = seen.iter().map(|(_, r)| r.length).max().unwrap();
        if longest < recorded {
            return Err(TailError::ShorterThanRecorded { recorded, longest });
        }
        recorded
    } else {
        let mut lengths: Vec<u64> = seen.iter().map(|(_, r)| r.length).collect();
        lengths.sort_unstable();
        match mode {
            WriteMode::Strong => lengths[0],
            WriteMode::Quorum => match (replicas.len(), lengths.len()) {
                (_, 1) if replicas.len() > 1 => {
                    return Err(TailError::TooFewReplicas { reachable: 1, needed: 2 })
                }
                (_, 1) => lengths[0],
                (n, k) if k == n => lengths[n - 2],
                _ => *lengths.last().unwrap(),
            },
        }
    };
    let mut cut = Vec::new();
    let mut behind = Vec::new();
    for (i, r) in &seen {
        if r.length > length {
            cut.push(*i);
        } else if r.length < length {
            behind.push(*i);
        }
    }
    // a recorded or sealed length that some answering replica does not hold at all is a
    // decision only if at least one answering replica holds it
    if !seen.iter().any(|(_, r)| r.length >= length) {
        let longest = seen.iter().map(|(_, r)| r.length).max().unwrap();
        return Err(TailError::ShorterThanRecorded { recorded: length, longest });
    }
    Ok(TailDecision { length, sealed: !sealed.is_empty(), cut, behind })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseError {
    /// Another holder has a live lease.
    Held { holder: String, expires_at_ms: u64 },
    /// The lease was never granted to this holder at this epoch, or has expired.
    NotHeld,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Lease {
    holder: String,
    epoch: u64,
    expires_at_ms: u64,
}

/// One live writer lease per object. The epoch of a grant is supplied by the caller and must be
/// larger than every epoch granted before (the metadata plane uses the index of the command that
/// makes the grant, which only goes up), so an epoch names exactly one grant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriterLeases {
    leases: BTreeMap<String, Lease>,
    highest_epoch: u64,
}

impl WriterLeases {
    /// Grant `object` to `holder` until `now + ttl`. A live lease held by someone else refuses; the
    /// same holder re-opening gets a NEW epoch (its previous writer, if any, is fenced). An expired
    /// lease may be taken over.
    pub fn grant(
        &mut self,
        object: &str,
        holder: &str,
        now_ms: u64,
        ttl_ms: u64,
        epoch: u64,
    ) -> Result<u64, LeaseError> {
        if epoch <= self.highest_epoch {
            // an epoch that does not go up would let two grants share a fence
            return Err(LeaseError::NotHeld);
        }
        if let Some(lease) = self.leases.get(object) {
            if lease.holder != holder && lease.expires_at_ms > now_ms {
                return Err(LeaseError::Held { holder: lease.holder.clone(), expires_at_ms: lease.expires_at_ms });
            }
        }
        self.highest_epoch = epoch;
        self.leases.insert(
            object.to_string(),
            Lease { holder: holder.to_string(), epoch, expires_at_ms: now_ms.saturating_add(ttl_ms) },
        );
        Ok(epoch)
    }

    /// Extend a live lease (the heartbeat). Refused if it expired or was taken over.
    pub fn renew(&mut self, object: &str, holder: &str, epoch: u64, now_ms: u64, ttl_ms: u64) -> Result<(), LeaseError> {
        match self.leases.get_mut(object) {
            Some(lease) if lease.holder == holder && lease.epoch == epoch && lease.expires_at_ms > now_ms => {
                lease.expires_at_ms = now_ms.saturating_add(ttl_ms);
                Ok(())
            }
            _ => Err(LeaseError::NotHeld),
        }
    }

    /// Give the lease up so a standby may take the object over at once.
    pub fn release(&mut self, object: &str, holder: &str, epoch: u64) -> Result<(), LeaseError> {
        match self.leases.get(object) {
            Some(lease) if lease.holder == holder && lease.epoch == epoch => {
                self.leases.remove(object);
                Ok(())
            }
            _ => Err(LeaseError::NotHeld),
        }
    }

    /// The live epoch of `object`, if a lease is held at `now`.
    pub fn live_epoch(&self, object: &str, now_ms: u64) -> Option<u64> {
        self.leases.get(object).filter(|l| l.expires_at_ms > now_ms).map(|l| l.epoch)
    }
    pub fn highest_epoch(&self) -> u64 {
        self.highest_epoch
    }
}

use crate::log_chunk::{LogChunk, LogError};

/// What a replica says about its chunk.
pub fn tail_of(chunk: &LogChunk) -> ReplicaTail {
    ReplicaTail {
        length: chunk.length(),
        crc: chunk.footer_crc(),
        epoch: chunk.epoch(),
        sealed: chunk.sealed().is_some(),
    }
}

#[derive(Debug)]
pub enum OpenError {
    Tail(TailError),
    Log(LogError),
}
impl From<TailError> for OpenError {
    fn from(error: TailError) -> Self {
        Self::Tail(error)
    }
}
impl From<LogError> for OpenError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

/// Open a chunk for append at `epoch` (a fresh grant from `WriterLeases`): decide the length from
/// the replicas that answer, cut the ones that hold more, and raise the epoch on every answering
/// replica so the previous writer is refused from here on. Everything is made durable before this
/// returns. Replicas that did not answer (`None`) are not touched; they are fenced when they are
/// reached, and a replica below the decided length is reported in `behind`.
pub fn open_for_append(
    mode: WriteMode,
    replicas: &mut [Option<&mut LogChunk>],
    recorded: Option<u64>,
    epoch: u64,
) -> Result<TailDecision, OpenError> {
    let tails: Vec<Option<ReplicaTail>> = replicas.iter().map(|r| r.as_ref().map(|c| tail_of(c))).collect();
    let decision = decide_tail(mode, &tails, recorded)?;
    for (index, slot) in replicas.iter_mut().enumerate() {
        let Some(chunk) = slot else { continue };
        if chunk.sealed().is_some() {
            continue;
        }
        let target = if decision.cut.contains(&index) { decision.length } else { chunk.length() };
        chunk.truncate(epoch, target)?;
        chunk.commit()?;
    }
    Ok(decision)
}
