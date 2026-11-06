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

use crate::cluster::client::{RemoteReplicaClient, ReplicaClientOptions};
use crate::cluster::message::{ConditionalChunkWrite, ReplicaReply, ReplicaRequest};
use crate::{ObjectError, PageId, ReplicaHead, ReplicaReadResult};

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

#[cfg(unix)]
fn wait_for_pending(pending: &[Option<super::client::PendingReplicaReply<'_>>]) {
    use std::os::fd::AsRawFd;

    let mut deadline = None;
    let mut descriptors = Vec::with_capacity(pending.len());
    for reply in pending.iter().flatten() {
        let (socket, events) = reply.readiness();
        descriptors.push(libc::pollfd {
            fd: socket.as_raw_fd(),
            events,
            revents: 0,
        });
        deadline = Some(
            deadline.map_or(reply.deadline(), |current: std::time::Instant| {
                current.min(reply.deadline())
            }),
        );
    }
    let Some(deadline) = deadline else {
        return;
    };
    let timeout = deadline
        .saturating_duration_since(std::time::Instant::now())
        .min(std::time::Duration::from_millis(50));
    let milliseconds = timeout
        .as_millis()
        .saturating_add(u128::from(timeout.subsec_nanos() % 1_000_000 != 0))
        .min(50) as libc::c_int;
    // SAFETY: descriptors owns this initialized contiguous array for the
    // duration of poll, and every socket stays borrowed by `pending`.
    let result = unsafe {
        libc::poll(
            descriptors.as_mut_ptr(),
            descriptors.len() as libc::nfds_t,
            milliseconds,
        )
    };
    if result < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(not(unix))]
fn wait_for_pending(_: &[Option<super::client::PendingReplicaReply<'_>>]) {
    std::thread::sleep(std::time::Duration::from_millis(1));
}

/// A set of replica nodes in other processes, and the quorums over them.
#[derive(Debug)]
pub struct RemoteReplicaSet {
    replicas: Vec<RemoteReplicaClient>,
    write_quorum: usize,
    read_quorum: usize,
    options: ReplicaClientOptions,
}

/// Result for one configured member, in endpoint order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PageRepairStatus {
    /// Exact validated bytes already matched the selected source state.
    Current,
    /// The node acknowledged durable installation (or atomic in-memory installation).
    Installed,
    /// No commit was sent. A best-effort abort may leave staging until expiry.
    Deferred(String),
    /// A commit was sent without a confirmed result. Inspect before retrying repair.
    Unconfirmed(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRepairReport {
    pub source: ReplicaHead,
    pub members: Vec<PageRepairStatus>,
}

/// One attempted page in a bounded inventory repair traversal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRepairEntry {
    pub page: PageId,
    pub result: Result<PageRepairReport, String>,
}

/// Live inventory progress; this is not a frozen snapshot or readmission certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRepairBatch {
    pub pages: Vec<PageRepairEntry>,
    /// Last page whose members all returned Current or Installed. Exclusive on resume.
    pub resume_after: Option<PageId>,
    /// More inventory was observed, or an unresolved page stopped traversal.
    pub has_more: bool,
    /// Inspect the last page before resuming, especially after an uncertain commit.
    pub blocked: bool,
}


fn applied(reply: Result<ReplicaReply, ObjectError>) -> Result<(), ObjectError> {
    match reply? {
        ReplicaReply::Applied => Ok(()),
        _ => Err(ObjectError::Corruption(
            "unexpected replica mutation response".into(),
        )),
    }
}

impl RemoteReplicaSet {
    /// Connect to every endpoint, refusing incomplete or duplicate membership.
    ///
    /// Each connected socket must name a distinct peer address. Multiple connections
    /// to the same peer cannot supply independent quorum votes. Address aliases are
    /// checked after connection, using the actual peer rather than endpoint text.
    /// This does not establish separate machines or failure domains behind proxies.
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
        crate::cluster::placement::check_quorums(endpoints.len(), write_quorum, read_quorum)?;
        let mut replicas = Vec::with_capacity(endpoints.len());
        let mut peers = BTreeSet::new();
        for endpoint in endpoints {
            let replica = RemoteReplicaClient::connect(endpoint, options)?;
            let peer = replica.peer_addr()?;
            // An IPv4-mapped IPv6 address still names the same IPv4 peer.
            let identity = match peer {
                SocketAddr::V6(address) => address
                    .ip()
                    .to_ipv4_mapped()
                    .map(|ip| SocketAddr::new(ip.into(), address.port()))
                    .unwrap_or(peer),
                SocketAddr::V4(_) => peer,
            };
            if !peers.insert(identity) {
                return Err(ObjectError::InvalidArgument(format!(
                    "remote replica endpoint {endpoint} repeats peer {identity}; each quorum member must have a distinct peer"
                )));
            }
            replicas.push(replica);
        }
        Ok(Self {
            replicas,
            write_quorum,
            read_quorum,
            options: options.clone(),
        })
    }

    fn fan_out(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
    ) -> Vec<Result<ReplicaReply, ObjectError>> {
        // One shared encoded frame and one pending exchange per selected member.
        // No worker threads or mutation retries; finish every exchange even after quorum.
        let frame: std::sync::Arc<[u8]> = super::message::encode_request(request).into();
        let pending: Vec<_> = self
            .replicas
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| selected.map_or(true, |members| members[*i]))
            .map(|(_, replica)| replica.begin_encoded(request.code(), frame.clone()))
            .collect();
        let mut replies: Vec<_> = (0..pending.len()).map(|_| None).collect();
        let mut pending: Vec<_> = pending.into_iter().enumerate().map(|(index, result)| {
            match result {
                Ok(reply) => Some(reply),
                Err(error) => {
                    replies[index] = Some(Err(error));
                    None
                }
            }
        }).collect();
        while pending.iter().any(Option::is_some) {
            let mut progressed = false;
            for (index, slot) in pending.iter_mut().enumerate() {
                let result = slot.as_mut().and_then(|reply| {
                    let before = reply.transferred_bytes();
                    let result = reply.poll();
                    progressed |= result.is_some() || reply.transferred_bytes() != before;
                    result
                });
                if let Some(result) = result {
                    replies[index] = Some(result);
                    // Drop retires an incomplete frame before the connection can be reused.
                    *slot = None;
                }
            }
            if !progressed && pending.iter().any(Option::is_some) {
                wait_for_pending(&pending);
            }
        }
        replies.into_iter().map(Option::unwrap).collect()
    }

    fn reconnect_failed(&mut self) {
        for replica in &mut self.replicas {
            let _ = replica.reconnect_standalone_if_failed(&self.options);
        }
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

    /// Extend a quorum-agreed page state on matching replicas and require a write quorum.
    /// A conditional sequence check refuses concurrent changes after the head read.
    /// Divergent or unreachable replicas receive no mutation; this does not repair them.
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
        let (expected, observed) = self.mutation_state(page_id)?;
        let sequence = expected.as_ref().map_or(0, |head| head.write_sequence);
        let mut acknowledged = 0usize;
        let mut first_error: Option<ObjectError> = None;
        let request = ReplicaRequest::WriteChunkIfSequence {
            page_id: page_id.clone(),
            offset,
            data: data.to_vec(),
            expected_sequence: sequence,
        };
        for result in self.fan_out(&request, Some(&observed)) {
            match applied(result) {
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

    /// Apply up to eight independent writes through the standalone star path.
    ///
    /// Every page is fenced against a quorum-agreed head before any mutation is
    /// sent. Pages with the same matching member set share one encoded conditional
    /// write batch, reducing mutation frames without letting a divergent member
    /// vote for a write. Frames carry at most four MiB of page data. An error can
    /// follow an acknowledged earlier frame; callers must probe candidate pages
    /// instead of replaying an uncertain batch.
    pub fn write_chunks(
        &mut self,
        writes: &[(PageId, u64, &[u8])],
    ) -> Result<Vec<usize>, ObjectError> {
        if writes.len() > 8 {
            return Err(ObjectError::InvalidArgument(
                "star write batch exceeds eight pages".into(),
            ));
        }
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        if writes.iter().any(|(_, _, data)| data.len() > 1024 * 1024) {
            return Err(ObjectError::InvalidArgument(
                "star write page exceeds one MiB".into(),
            ));
        }
        let mut pages = BTreeSet::new();
        if writes
            .iter()
            .any(|(page, _, _)| !pages.insert(page.clone()))
        {
            return Err(ObjectError::InvalidArgument(
                "star write batch repeats a page".into(),
            ));
        }

        let page_ids: Vec<_> = writes.iter().map(|(page, _, _)| page.clone()).collect();
        let states = self.mutation_states(&page_ids)?;
        let mut groups: BTreeMap<Vec<bool>, Vec<(usize, ConditionalChunkWrite)>> = BTreeMap::new();
        for (index, ((page_id, offset, data), (expected, matching))) in
            writes.iter().zip(states).enumerate()
        {
            groups.entry(matching).or_default().push((
                index,
                ConditionalChunkWrite {
                    page_id: page_id.clone(),
                    offset: *offset,
                    data: data.to_vec(),
                    expected_sequence: expected.as_ref().map_or(0, |head| head.write_sequence),
                },
            ));
        }

        let mut acknowledgements = vec![0; writes.len()];
        for (matching, group) in groups {
            let mut remaining = group.into_iter().peekable();
            while remaining.peek().is_some() {
                let mut frame = Vec::new();
                let mut frame_bytes = 0usize;
                while frame.len() < 8 {
                    let Some((_, write)) = remaining.peek() else {
                        break;
                    };
                    let Some(next_bytes) = frame_bytes.checked_add(write.data.len()) else {
                        break;
                    };
                    if !frame.is_empty() && next_bytes > 4 * 1024 * 1024 {
                        break;
                    }
                    let (index, write) = remaining.next().unwrap();
                    frame_bytes = next_bytes;
                    frame.push((index, write));
                }
                let indexes: Vec<_> = frame.iter().map(|(index, _)| *index).collect();
                let request = ReplicaRequest::WriteChunkBatchIfSequence {
                    writes: frame.into_iter().map(|(_, write)| write).collect(),
                };
                let mut acknowledged = 0usize;
                let mut first_error = None;
                for result in self.fan_out(&request, Some(&matching)) {
                    match applied(result) {
                        Ok(()) => acknowledged += 1,
                        Err(error) if first_error.is_none() => first_error = Some(error),
                        Err(_) => {}
                    }
                }
                if acknowledged < self.write_quorum {
                    return Err(first_error.unwrap_or_else(|| {
                        ObjectError::Corruption(format!(
                            "a star write batch was acknowledged by {acknowledged} of {} replica(s), \
                             short of a write quorum of {}",
                            self.replicas.len(),
                            self.write_quorum
                        ))
                    }));
                }
                for index in indexes {
                    acknowledgements[index] = acknowledged;
                }
            }
        }
        Ok(acknowledgements)
    }

    fn mutation_state(
        &mut self,
        page_id: &PageId,
    ) -> Result<(Option<ReplicaHead>, Vec<bool>), ObjectError> {
        self.reconnect_failed();
        // Establish the state being extended before any mutation is sent. A
        // returning or divergent replica cannot contribute a fresh write vote.
        let observed: Vec<_> = self
            .fan_out(
                &ReplicaRequest::HeadChunk {
                    page_id: page_id.clone(),
                },
                None,
            )
            .into_iter()
            .map(|reply| match reply {
                Ok(ReplicaReply::Head(head)) => Some(Some(head)),
                Err(ObjectError::NotFound(_)) => Some(None),
                _ => None,
            })
            .collect();
        self.select_mutation_state(observed)
    }

    fn mutation_states(
        &mut self,
        pages: &[PageId],
    ) -> Result<Vec<(Option<ReplicaHead>, Vec<bool>)>, ObjectError> {
        self.reconnect_failed();
        let replies = self.fan_out(
            &ReplicaRequest::HeadChunkBatch {
                pages: pages.to_vec(),
            },
            None,
        );
        let unsupported = replies.iter().any(|reply| {
            matches!(
                reply,
                Err(ObjectError::PreconditionFailed(message))
                    if message.starts_with("the replica does not implement this request:")
            )
        });
        if unsupported {
            return pages
                .iter()
                .map(|page| self.mutation_state(page))
                .collect();
        }
        let mut observed = vec![Vec::with_capacity(self.replicas.len()); pages.len()];
        for reply in replies {
            match reply {
                Ok(ReplicaReply::Heads(heads)) if heads.len() == pages.len() => {
                    for (page, head) in observed.iter_mut().zip(heads) {
                        page.push(Some(head));
                    }
                }
                _ => {
                    for page in &mut observed {
                        page.push(None);
                    }
                }
            }
        }
        observed
            .into_iter()
            .map(|page| self.select_mutation_state(page))
            .collect()
    }

    fn select_mutation_state(
        &self,
        observed: Vec<Option<Option<ReplicaHead>>>,
    ) -> Result<(Option<ReplicaHead>, Vec<bool>), ObjectError> {
        let absent = observed
            .iter()
            .filter(|head| matches!(head, Some(None)))
            .count();
        let expected = if absent >= self.read_quorum {
            None
        } else {
            let heads = observed
                .iter()
                .filter_map(|head| head.as_ref().and_then(|value| value.clone()))
                .collect();
            Some(crate::cluster::placement::vote_heads(
                heads,
                self.read_quorum,
            )?)
        };
        if observed
            .iter()
            .filter(|head| head.as_ref() == Some(&expected))
            .count()
            < self.write_quorum
        {
            return Err(ObjectError::PreconditionFailed(
                "matching page state cannot satisfy write quorum".into(),
            ));
        }
        let matching = observed
            .iter()
            .map(|head| head.as_ref() == Some(&expected))
            .collect();
        Ok((expected, matching))
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
        self.reconnect_failed();
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut answered = Vec::with_capacity(replica_count);
        let mut not_found = 0usize;
        let mut unreachable = 0usize;
        let mut first_error: Option<ObjectError> = None;
        let request = ReplicaRequest::ReadChunk {
            page_id: page_id.clone(),
            offset,
            length,
        };
        for result in self.fan_out(&request, None) {
            let result = result.and_then(|reply| match reply {
                ReplicaReply::Read(read) => Ok(read),
                _ => Err(ObjectError::Corruption(
                    "unexpected replica read response".into(),
                )),
            });
            match result {
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
        vote_reads(answered, replica_count, read_quorum)
    }

    /// Read up to eight independent ranges through quorum-voted star fan-out.
    /// Default one-MiB ranges share four-range frames while every member is
    /// still polled and every range is voted independently.
    pub fn read_chunks(
        &mut self,
        reads: &[(PageId, u64, u64)],
    ) -> Result<Vec<ReplicaReadResult>, ObjectError> {
        if reads.len() > 8 {
            return Err(ObjectError::InvalidArgument(
                "star read batch exceeds eight pages".into(),
            ));
        }
        if reads.iter().any(|(_, _, length)| *length > 1024 * 1024) {
            return Err(ObjectError::InvalidArgument(
                "star read range exceeds one MiB".into(),
            ));
        }
        if reads.is_empty() {
            return Ok(Vec::new());
        }

        self.reconnect_failed();
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut completed = 0usize;
        let mut output = Vec::with_capacity(reads.len());
        while completed < reads.len() {
            let mut end = completed;
            let mut frame_bytes = 0u64;
            while end < reads.len() && end - completed < 8 {
                let Some(next_bytes) = frame_bytes.checked_add(reads[end].2) else {
                    break;
                };
                if end > completed && next_bytes > 4 * 1024 * 1024 {
                    break;
                }
                frame_bytes = next_bytes;
                end += 1;
            }
            let frame = &reads[completed..end];
            let request = ReplicaRequest::ReadChunkBatch {
                reads: frame.to_vec(),
            };
            let replies = self.fan_out(&request, None);
            let scalar_fallback = replies.iter().any(|reply| {
                matches!(
                    reply,
                    Err(ObjectError::PreconditionFailed(_))
                        | Err(ObjectError::InvalidArgument(_))
                        | Err(ObjectError::NotFound(_))
                )
            });
            if scalar_fallback {
                for (page_id, offset, length) in frame {
                    output.push(self.read_chunk(page_id, *offset, *length)?);
                }
                completed = end;
                continue;
            }

            let mut votes: Vec<Vec<ReplicaReadResult>> = (0..frame.len())
                .map(|_| Vec::with_capacity(replica_count))
                .collect();
            let mut answered = 0usize;
            let mut not_found = 0usize;
            let mut unreachable = 0usize;
            let mut first_error = None;
            for reply in replies {
                match reply {
                    Ok(ReplicaReply::Reads(reads)) if reads.len() == frame.len() => {
                        for (range_votes, read) in votes.iter_mut().zip(reads) {
                            range_votes.push(read);
                        }
                        answered += 1;
                    }
                    Ok(_) => {
                        unreachable += 1;
                        first_error.get_or_insert_with(|| ObjectError::Corruption(
                            "unexpected replica read batch response".into(),
                        ));
                    }
                    Err(error) => {
                        if matches!(error, ObjectError::NotFound(_)) {
                            not_found += 1;
                        } else {
                            unreachable += 1;
                        }
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            if answered < read_quorum {
                if not_found >= read_quorum {
                    return Err(ObjectError::NotFound(format!(
                        "one or more requested pages are absent on {not_found} of {replica_count} replicas"
                    )));
                }
                return Err(ObjectError::Corruption(format!(
                    "a star read batch was answered by {answered} of {replica_count} replicas, \
                     short of a read quorum of {read_quorum} ({unreachable} did not answer, \
                     {not_found} reported an absent page); the first failure was: {}",
                    first_error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "none recorded".into())
                )));
            }
            for range_votes in votes {
                output.push(vote_reads(range_votes, replica_count, read_quorum)?);
            }
            completed = end;
        }
        Ok(output)
    }

    /// Read complete page bytes and discard metadata from a matching read quorum.
    /// Each source is checksum-fenced; exact bytes must also agree. At most two
    /// assembled payloads are retained, independent of the replica count.
    pub fn read_chunk_state(&mut self, page_id: &PageId) -> Result<crate::PageState, ObjectError> {
        self.read_chunk_state_with_quorum(page_id, self.read_quorum)
    }

    fn read_chunk_state_with_quorum(
        &mut self,
        page_id: &PageId,
        quorum: usize,
    ) -> Result<crate::PageState, ObjectError> {
        let expected = self.head_chunk_with_quorum(page_id, quorum)?;
        let mut candidate: Option<crate::PageState> = None;
        let mut agreeing = 0;
        for replica in &mut self.replicas {
            if let Ok(state) = replica.read_chunk_state_at(page_id, &expected) {
                if let Some(candidate) = &candidate {
                    if candidate.data != state.data {
                        return Err(ObjectError::Corruption(
                            "page state bytes disagree despite matching heads".into(),
                        ));
                    }
                } else {
                    candidate = Some(state);
                }
                agreeing += 1;
                if agreeing == quorum {
                    break;
                }
            }
        }
        if agreeing < quorum {
            return Err(ObjectError::PreconditionFailed(
                "complete page state did not reach read quorum".into(),
            ));
        }
        if self.head_chunk_with_quorum(page_id, quorum)? != expected {
            return Err(ObjectError::PreconditionFailed(
                "page quorum changed during state transfer".into(),
            ));
        }
        candidate.ok_or_else(|| ObjectError::Corruption("page state quorum has no payload".into()))
    }

    /// Read a page's metadata from a read quorum, voting on the whole head.
    pub fn head_chunk(&mut self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        self.head_chunk_with_quorum(page_id, self.read_quorum)
    }

    fn head_chunk_with_quorum(
        &mut self,
        page_id: &PageId,
        read_quorum: usize,
    ) -> Result<ReplicaHead, ObjectError> {
        self.reconnect_failed();
        let replica_count = self.replicas.len();
        let mut answered = Vec::with_capacity(replica_count);
        let mut not_found = 0usize;
        let mut unreachable = 0usize;
        let request = ReplicaRequest::HeadChunk {
            page_id: page_id.clone(),
        };
        for reply in self.fan_out(&request, None) {
            match reply {
                Ok(ReplicaReply::Head(head)) => answered.push(head),
                Err(ObjectError::NotFound(_)) => not_found += 1,
                _ => unreachable += 1,
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
        crate::cluster::placement::vote_heads(answered, read_quorum)
    }

    /// Repair one existing page from max(read_quorum, write_quorum) validated copies.
    /// Does not delete pages, retry uncertain commits, or run in the background.
    /// The report describes the selected state, not a cross-node transaction.
    pub fn repair_chunk(&mut self, page_id: &PageId) -> Result<PageRepairReport, ObjectError> {
        use super::state_install::StateInstall;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_REPAIR: AtomicU64 = AtomicU64::new(0);
        let quorum = self.read_quorum.max(self.write_quorum);
        let state = self.read_chunk_state_with_quorum(page_id, quorum)?;
        let checksum = state.checksum();
        let source = ReplicaHead {
            logical_length: state.logical_length,
            write_sequence: state.write_sequence,
            checksum: checksum.clone(),
            state_checksum: crate::chunk_state_handoff_checksum(page_id, &state, &checksum),
            discarded_ranges: state.discarded_ranges.clone(),
        };
        let prefix = format!(
            "repair-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| ObjectError::InvalidArgument("system clock precedes epoch".into()))?
                .as_nanos(),
            NEXT_REPAIR.fetch_add(1, Ordering::Relaxed)
        );
        let size = self
            .options
            .max_payload_bytes
            .saturating_sub(512)
            .min(super::state_transfer::MAX_STATE_RANGE_BYTES);
        if size == 0 {
            return Err(ObjectError::InvalidArgument(
                "frame limit cannot hold repair data".into(),
            ));
        }
        let mut report = PageRepairReport {
            source: source.clone(),
            members: Vec::with_capacity(self.replicas.len()),
        };
        for index in 0..self.replicas.len() {
            let observed = match self.replicas[index].head_chunk(page_id) {
                Ok(head) => Some(head),
                Err(ObjectError::NotFound(_)) => None,
                Err(error) => {
                    report
                        .members
                        .push(PageRepairStatus::Deferred(error.to_string()));
                    continue;
                }
            };
            if observed.as_ref() == Some(&source) {
                if let Ok(current) = self.replicas[index].read_chunk_state_at(page_id, &source) {
                    if current.data == state.data {
                        report.members.push(PageRepairStatus::Current);
                        continue;
                    }
                }
            }
            if observed
                .as_ref()
                .is_some_and(|head| head.write_sequence > source.write_sequence)
            {
                report.members.push(PageRepairStatus::Deferred(
                    "destination is newer than source quorum".into(),
                ));
                continue;
            }
            // Check authority immediately before staging and again before commit.
            if self.head_chunk_with_quorum(page_id, quorum).ok().as_ref() != Some(&source) {
                report.members.resize(
                    self.replicas.len(),
                    PageRepairStatus::Deferred("source quorum changed before staging".into()),
                );
                break;
            }
            let token = format!("{prefix}-{index}");
            let staged = (|| -> Result<(), ObjectError> {
                let target = &mut self.replicas[index];
                target.stage_chunk_state(StateInstall::begin(
                    token.clone(),
                    page_id,
                    observed.as_ref().map(|head| head.state_checksum.as_str()),
                    &source,
                    state.data.len() as u64,
                ))?;
                for (part, data) in state.data.chunks(size).enumerate() {
                    let mut append = StateInstall::command(2, token.clone());
                    append.offset = (part * size) as u64;
                    append.data = data.to_vec();
                    target.stage_chunk_state(append)?;
                }
                Ok(())
            })();
            if let Err(error) = staged {
                let _ = self.replicas[index].stage_chunk_state(StateInstall::command(4, token));
                report
                    .members
                    .push(PageRepairStatus::Deferred(error.to_string()));
                continue;
            }
            if self.head_chunk_with_quorum(page_id, quorum).ok().as_ref() != Some(&source) {
                let _ = self.replicas[index].stage_chunk_state(StateInstall::command(4, token));
                report.members.resize(
                    self.replicas.len(),
                    PageRepairStatus::Deferred("source quorum changed before commit".into()),
                );
                break;
            }
            match self.replicas[index].stage_chunk_state(StateInstall::command(3, token)) {
                Ok(()) => report.members.push(PageRepairStatus::Installed),
                Err(error) => report
                    .members
                    .push(PageRepairStatus::Unconfirmed(error.to_string())),
            }
        }
        Ok(report)
    }

    /// Repair at most 64 distinct pages from a bounded union of member inventories.
    /// Every inventory must answer before mutations begin. Stop at the first page
    /// with an error, deferred member or uncertain commit, without advancing past it.
    /// Fence writers before using a completed live traversal for readmission.
    pub fn repair_inventory_batch(
        &mut self,
        after: Option<&PageId>,
        limit: usize,
    ) -> Result<PageRepairBatch, ObjectError> {
        if !(1..=64).contains(&limit) {
            return Err(ObjectError::InvalidArgument(
                "repair batch limit must be between 1 and 64".into(),
            ));
        }
        self.reconnect_failed();
        let mut selected = BTreeSet::new();
        let mut has_more = false;
        for replica in &mut self.replicas {
            let inventory = replica.page_inventory(after, limit as u64)?;
            has_more |= inventory.has_more;
            for (page, _) in inventory.entries {
                selected.insert(page);
                if selected.len() > limit {
                    selected.pop_last();
                    has_more = true;
                }
            }
        }
        let mut batch = PageRepairBatch {
            pages: Vec::with_capacity(selected.len()),
            resume_after: after.cloned(),
            has_more,
            blocked: false,
        };
        for page in selected {
            let result = self.repair_chunk(&page).map_err(|error| error.to_string());
            let resolved = result.as_ref().is_ok_and(|report| {
                report.members.iter().all(|status| {
                    matches!(
                        status,
                        PageRepairStatus::Current | PageRepairStatus::Installed
                    )
                })
            });
            batch.pages.push(PageRepairEntry {
                page: page.clone(),
                result,
            });
            if !resolved {
                batch.blocked = true;
                batch.has_more = true;
                break;
            }
            batch.resume_after = Some(page);
        }
        Ok(batch)
    }

    /// Conditionally discard on replicas matching a quorum-agreed existing page.
    pub fn discard_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<usize, ObjectError> {
        let (expected, observed) = self.mutation_state(page_id)?;
        let expected =
            expected.ok_or_else(|| ObjectError::NotFound("no quorum page to discard".into()))?;
        let mut acknowledged = 0usize;
        let mut first_error: Option<ObjectError> = None;
        let request = ReplicaRequest::DiscardChunkIfSequence {
            page_id: page_id.clone(),
            offset,
            length,
            expected_sequence: expected.write_sequence,
        };
        for result in self.fan_out(&request, Some(&observed)) {
            match applied(result) {
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
        self.reconnect_failed();
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

// Keep exact sequence/checksum/byte voting while moving each received payload
// into the vote table. No second payload allocation is needed for a vote.
fn vote_reads(
    answered: Vec<ReplicaReadResult>,
    replica_count: usize,
    read_quorum: usize,
) -> Result<ReplicaReadResult, ObjectError> {
    let answered_count = answered.len();
    let mut votes: BTreeMap<(u64, String, Vec<u8>), usize> = BTreeMap::new();
    for read in answered {
        let key = (read.write_sequence, read.checksum, read.data);
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
            answered_count
        )));
    }
    let (write_sequence, checksum, data) = winning_key;
    Ok(ReplicaReadResult {
        data,
        write_sequence,
        checksum,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_vote_returns_received_payload_and_rejects_conflicting_bytes() {
        let reply = |data: Vec<u8>| ReplicaReadResult {
            data,
            write_sequence: 7,
            checksum: "same-checksum".into(),
        };
        let first = reply(vec![3; 1024 * 1024]);
        let second = first.clone();
        let original = first.data.as_ptr();
        let other = second.data.as_ptr();
        let mut conflicting = first.clone();
        conflicting.data[0] = 4;
        let result = vote_reads(vec![conflicting, first, second], 3, 2).unwrap();
        assert!(
            result.data.as_ptr() == original || result.data.as_ptr() == other,
            "the winning bytes must retain a received allocation"
        );
        assert_eq!(result.data, vec![3; 1024 * 1024]);
        assert_eq!(result.write_sequence, 7);
        assert_eq!(result.checksum, "same-checksum");
        assert!(matches!(
            vote_reads(vec![reply(vec![1]), reply(vec![2])], 2, 2),
            Err(ObjectError::Corruption(_))
        ));
        let mut newer = reply(vec![1]);
        newer.write_sequence = 8;
        assert!(vote_reads(vec![reply(vec![1]), newer], 2, 2).is_err());
        let mut changed = reply(vec![1]);
        changed.checksum.push('x');
        assert!(vote_reads(vec![reply(vec![1]), changed], 2, 2).is_err());
    }
}


#[cfg(all(test, target_os = "linux"))]
mod send_progress_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::os::fd::AsRawFd;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn star_fanout_wait_wakes_for_a_replica_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let mut client = RemoteReplicaClient::connect(
            &endpoint,
            &ReplicaClientOptions {
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .unwrap();
        let (mut peer, _) = listener.accept().unwrap();
        let worker = std::thread::spawn(move || {
            let mut header = [0; 6];
            peer.read_exact(&mut header).unwrap();
            let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
            let mut payload = vec![0; length];
            peer.read_exact(&mut payload).unwrap();
            std::thread::sleep(Duration::from_millis(20));
            peer.write_all(&super::super::message::encode_reply(&ReplicaReply::Applied))
                .unwrap();
        });
        let request = ReplicaRequest::WriteChunkIfSequence {
            page_id: PageId {
                chunk_id: crate::ChunkId::new(1, 1).unwrap(),
                page_index: 0,
            },
            offset: 0,
            data: vec![7; 1024],
            expected_sequence: 0,
        };
        let mut pending = Some(
            client
                .begin_encoded(
                    request.code(),
                    super::super::message::encode_request(&request).into(),
                )
                .unwrap(),
        );
        assert!(pending.as_mut().unwrap().poll().is_none());
        wait_for_pending(std::slice::from_ref(&pending));
        assert!(matches!(
            pending.as_mut().unwrap().poll(),
            Some(Ok(ReplicaReply::Applied))
        ));
        drop(pending);
        worker.join().unwrap();
    }

    #[test]
    fn star_blocked_sender_does_not_delay_healthy_members_or_replay() {
        let listeners: Vec<_> = (0..3).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
        // Bound the first receive window before the handshake to force backpressure.
        let size: libc::c_int = 4096;
        assert_eq!(unsafe {
            libc::setsockopt(listeners[0].as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF,
                &size as *const _ as *const libc::c_void, std::mem::size_of_val(&size) as libc::socklen_t)
        }, 0);
        let endpoints: Vec<_> = listeners.iter().map(|listener| listener.local_addr().unwrap().to_string()).collect();
        let mut set = RemoteReplicaSet::connect(&endpoints, 2, 2, &ReplicaClientOptions {
            write_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(3),
            ..Default::default()
        }).unwrap();
        let sockets: Vec<_> = listeners.iter().map(|listener| listener.accept().unwrap().0).collect();
        let (ready, received) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let mut receivers = Vec::new();
        let mut released = Some(released);
        for (index, mut socket) in sockets.into_iter().enumerate() {
            socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let ready = ready.clone();
            let released = if index == 0 { released.take() } else { None };
            receivers.push(std::thread::spawn(move || {
                if let Some(released) = released {
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    let mut bytes = Vec::new();
                    socket.read_to_end(&mut bytes).unwrap();
                    assert!(!bytes.is_empty());
                    assert!(bytes.len() < 8 * 1024 * 1024, "stalled member must receive only a partial frame");
                    return;
                }
                let mut header = [0; 6];
                socket.read_exact(&mut header).unwrap();
                let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                assert!(length > 8 * 1024 * 1024 && length < 9 * 1024 * 1024);
                let mut payload = vec![0; length];
                socket.read_exact(&mut payload).unwrap();
                ready.send(index).unwrap();
                socket.write_all(&super::super::message::encode_reply(&ReplicaReply::Applied)).unwrap();
                let mut byte = [0];
                assert_eq!(socket.read(&mut byte).unwrap(), 0, "mutation must not replay");
            }));
        }
        let writer = std::thread::spawn(move || {
            let replies = set.fan_out(&ReplicaRequest::WriteChunkIfSequence {
                page_id: PageId { chunk_id: crate::ChunkId::new(1, 1).unwrap(), page_index: 0 },
                offset: 0, data: vec![7; 8 * 1024 * 1024], expected_sequence: 0,
            }, None);
            assert!(replies[0].is_err());
            assert!(matches!(replies[1], Ok(ReplicaReply::Applied)));
            assert!(matches!(replies[2], Ok(ReplicaReply::Applied)));
        });
        let first = received.recv_timeout(Duration::from_secs(1));
        let second = received.recv_timeout(Duration::from_secs(1));
        writer.join().unwrap();
        release.send(()).unwrap();
        for worker in receivers { worker.join().unwrap(); }
        assert!(first.is_ok() && second.is_ok(), "healthy members waited behind the stalled sender");
    }
}
