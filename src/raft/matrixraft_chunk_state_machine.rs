// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// Replicating a chunk write through a MatrixRaft log, instead of from the coordinator.
//
// # THE WRITE TOPOLOGY THIS IS AN ALTERNATIVE TO
//
// A replicated page is written one way today: the coordinator builds the admitted-replica list and
// applies the prepared write to each replica ITSELF, counting successes against the write quorum.
// One hop to every replica, the coordinator pays the bandwidth, and no replica is ever asked to
// pass a write to another. `a_write_reaches_every_replica_from_the_coordinator_in_one_hop` asserts
// it, because the alternative is a reasonable thing for someone to introduce.
//
// This is that alternative, and it is a different shape rather than a cheaper one. The write is
// proposed ONCE, as a log entry. Every replica applies it from its own copy of the log, through
// this state machine, and the coordinator never touches a replica. What that buys is not bandwidth
// -- the bytes still reach every replica -- it is AGREEMENT: a quorum of replicas that accepted a
// log entry agree on the order of every write they applied, where a star write establishes only
// that a quorum accepted this one.
//
// Both are kept. A single-writer coordinator with a quorum is the cheaper answer when there is one
// coordinator, and ordering through a log is the answer when there is more than one.
//
// # WHY `apply` CAN BE WIRED HERE AND COULD NOT BE FOR THE METADATA SERVICE
//
// [`MetadataStateMachine::apply`] reports a refusal instead of applying, and the reason is in its
// doc comment: a MatrixRaft payload is `Vec<u8>`, `ObjectCommand` has 212 variants and no byte
// codec, and writing one would mean choosing the encoding of the raft log for the whole store's
// command language -- a durable format decision far larger than the trait method that provoked it.
//
// A chunk write is not that. What has to cross the log is a page identity -- three integers -- and
// a list of (offset, bytes) ranges. There is no enum to encode, the payload is already opaque
// bytes, and the format below is 18 bytes of header and a length-prefixed run per range. It is
// still a durable format, so it is versioned, its spelling is pinned byte-for-byte by a test, and
// a decoder that meets anything it does not recognise REFUSES rather than guessing -- including
// trailing bytes, because a laxer decoder on one replica and a stricter one on another is how two
// replicas come to disagree about what an entry said.

// Everything else this file names -- ApplyRequest, ApplyResponse, LogId, RaftError,
// SnapshotChunk, SnapshotMetadata, StateMachine -- is already in scope: both facade files are
// include!d into the same module, so importing them again is a redefinition rather than a
// convenience.
use matrixraft::GroupId;

/// Marks a payload as this crate's chunk-write command, so an entry from another state machine is
/// refused rather than read as a page identity.
pub const CHUNK_RAFT_COMMAND_MAGIC: [u8; 4] = *b"MXCW";

/// The command encoding's version. A decoder refuses a version it does not know, which is the
/// difference between a rejected entry and a replica that silently applies a different write.
pub const CHUNK_RAFT_COMMAND_VERSION: u8 = 1;

/// The one command kind this state machine applies: write these ranges into this page.
pub const CHUNK_RAFT_COMMAND_WRITE_RANGES: u8 = 1;

/// What one chunk-write log entry says.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkWriteCommand {
    pub page_id: PageId,
    /// `(offset, bytes)`, in the order the proposer wrote them.
    ///
    /// The order is carried on the wire and is NOT what decides overlapping bytes. The replica's
    /// own write path sorts by offset whenever the ranges are not already ascending and then
    /// coalesces, so two entries holding the same ranges in different order reach the same bytes.
    /// That is the useful direction: within one entry the order cannot cause two replicas to
    /// diverge, so what the log has to fix is the order ACROSS entries, which is exactly what it
    /// does fix. Asserted both ways --
    /// `the_order_the_log_fixes_is_what_makes_the_replicas_agree` for across, and
    /// `the_range_order_inside_one_entry_cannot_make_two_replicas_diverge` for within.
    pub ranges: Vec<(u64, Vec<u8>)>,
}

impl ChunkWriteCommand {
    /// The bytes that travel in a raft entry.
    ///
    /// Big-endian, so a hexdump of a captured entry reads in the order the fields are written.
    pub fn to_payload(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.payload_size_hint());
        out.extend_from_slice(&CHUNK_RAFT_COMMAND_MAGIC);
        out.push(CHUNK_RAFT_COMMAND_VERSION);
        out.push(CHUNK_RAFT_COMMAND_WRITE_RANGES);
        out.extend_from_slice(&self.page_id.chunk_id.volume_id().to_be_bytes());
        out.extend_from_slice(&self.page_id.chunk_id.id().to_be_bytes());
        out.extend_from_slice(&self.page_id.page_index.to_be_bytes());
        out.extend_from_slice(&(self.ranges.len() as u32).to_be_bytes());
        for (offset, data) in &self.ranges {
            out.extend_from_slice(&offset.to_be_bytes());
            out.extend_from_slice(&(data.len() as u32).to_be_bytes());
            out.extend_from_slice(data);
        }
        out
    }

    /// Exactly the size [`Self::to_payload`] produces, so the buffer is allocated once.
    pub fn payload_size_hint(&self) -> usize {
        let mut size = 4 + 1 + 1 + 8 + 8 + 8 + 4;
        for (_, data) in &self.ranges {
            size += 8 + 4 + data.len();
        }
        size
    }

    /// Read one back.
    ///
    /// Every refusal names what was wrong and where. A raft entry that cannot be decoded stalls
    /// the apply loop on every replica at once, so the message is the only thing an operator has.
    pub fn from_payload(payload: &[u8]) -> Result<Self, ObjectError> {
        const HEADER: usize = 4 + 1 + 1 + 8 + 8 + 8 + 4;
        if payload.len() < HEADER {
            return Err(ObjectError::InvalidArgument(format!(
                "chunk write command is {} bytes, shorter than its {HEADER}-byte header",
                payload.len()
            )));
        }
        if payload[..4] != CHUNK_RAFT_COMMAND_MAGIC {
            return Err(ObjectError::InvalidArgument(
                "chunk write command does not carry this crate's command marker, so it was \
                 produced by something else and is not a page write"
                    .to_string(),
            ));
        }
        let version = payload[4];
        if version != CHUNK_RAFT_COMMAND_VERSION {
            return Err(ObjectError::InvalidArgument(format!(
                "chunk write command is version {version} and this node understands version \
                 {CHUNK_RAFT_COMMAND_VERSION}"
            )));
        }
        let kind = payload[5];
        if kind != CHUNK_RAFT_COMMAND_WRITE_RANGES {
            return Err(ObjectError::InvalidArgument(format!(
                "chunk write command kind {kind} is not one this node applies"
            )));
        }
        let volume_id = read_be_u64(payload, 6);
        let chunk_id = read_be_u64(payload, 14);
        let page_index = read_be_u64(payload, 22);
        let range_count = read_be_u32(payload, 30) as usize;
        let mut ranges = Vec::with_capacity(range_count.min(1_024));
        let mut at = HEADER;
        for index in 0..range_count {
            if at + 12 > payload.len() {
                return Err(ObjectError::InvalidArgument(format!(
                    "chunk write command claims {range_count} ranges and ran out of bytes at \
                     range {index}"
                )));
            }
            let offset = read_be_u64(payload, at);
            let length = read_be_u32(payload, at + 8) as usize;
            at += 12;
            let end = at.checked_add(length).ok_or_else(|| {
                ObjectError::InvalidArgument(format!(
                    "chunk write command range {index} has a length that overflows its buffer"
                ))
            })?;
            if end > payload.len() {
                return Err(ObjectError::InvalidArgument(format!(
                    "chunk write command range {index} wants {length} bytes and only {} remain",
                    payload.len() - at
                )));
            }
            ranges.push((offset, payload[at..end].to_vec()));
            at = end;
        }
        // TRAILING BYTES ARE A REFUSAL, NOT SLACK. A decoder that ignores them accepts an entry a
        // stricter decoder rejects, and two replicas disagreeing about whether an entry is valid
        // is the one failure a replicated log exists to rule out.
        if at != payload.len() {
            return Err(ObjectError::InvalidArgument(format!(
                "chunk write command has {} bytes after its last range",
                payload.len() - at
            )));
        }
        Ok(Self {
            page_id: PageId {
                chunk_id: ChunkId::new(volume_id, chunk_id)?,
                page_index,
            },
            ranges,
        })
    }
}

fn read_be_u64(bytes: &[u8], at: usize) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[at..at + 8]);
    u64::from_be_bytes(buf)
}

fn read_be_u32(bytes: &[u8], at: usize) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[at..at + 4]);
    u32::from_be_bytes(buf)
}

/// One chunk replica, driven by a MatrixRaft log rather than by a coordinator.
pub struct ChunkStateMachine {
    /// The group whose entries this machine applies. An entry for another group is a routing
    /// mistake, and applying it would put one group's writes into another group's pages.
    group_id: GroupId,
    replica: MatrixReplicaService,
    /// The highest index applied. Raft may redeliver an entry -- a state machine's applied index
    /// is not committed with the log, so a node that restarts replays from the last index its
    /// SNAPSHOT reached -- and a page write is not idempotent in its effect on
    /// `write_sequence` or on the replica's physical-write counters even though it is idempotent
    /// in the bytes it leaves. A coordinator-driven star write never faces this, because the
    /// coordinator issues each write exactly once.
    last_applied_index: u64,
    /// Bytes accumulated across the pages of one incoming snapshot.
    installing: Vec<u8>,
}

impl ChunkStateMachine {
    pub fn new(group_id: GroupId, replica: MatrixReplicaService) -> Self {
        Self {
            group_id,
            replica,
            last_applied_index: 0,
            installing: Vec::new(),
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn last_applied_index(&self) -> u64 {
        self.last_applied_index
    }

    pub fn replica(&self) -> &MatrixReplicaService {
        &self.replica
    }

    pub fn replica_mut(&mut self) -> &mut MatrixReplicaService {
        &mut self.replica
    }

    pub fn into_replica(self) -> MatrixReplicaService {
        self.replica
    }

    /// The snapshot bytes a peer would send, without the raft envelope.
    ///
    /// Goes through [`StateMachine::snapshot`] rather than calling the replica directly, so the
    /// bytes a catching-up node installs locally are the same bytes it would receive over a
    /// group -- there is one export path, not two that could drift.
    pub fn snapshot_payload(&self) -> Result<Vec<u8>, ObjectError> {
        StateMachine::snapshot(self)
            .map(|chunk| chunk.data)
            .map_err(|err| ObjectError::Corruption(format!("chunk snapshot export failed: {err}")))
    }

    /// Install snapshot bytes as a catching-up node would, through the same path.
    ///
    /// `through_index` is the index the snapshot is complete to, and it is carried rather than
    /// derived: the bytes themselves record the replica's pages and no raft index, so a caller
    /// that does not pass it would leave this node replaying entries the snapshot contains.
    pub fn install_snapshot_payload(
        &mut self,
        bytes: &[u8],
        through_index: u64,
    ) -> Result<(), ObjectError> {
        StateMachine::install_snapshot(
            self,
            SnapshotChunk {
                meta: SnapshotMetadata {
                    snapshot_id: format!("chunk-{}-{through_index}", self.group_id),
                    last_log_id: LogId {
                        term: 0,
                        index: through_index,
                    },
                    membership: Vec::new(),
                    members: Vec::new(),
                },
                offset: 0,
                data: bytes.to_vec(),
                done: true,
            },
        )
        .map_err(|err| ObjectError::Corruption(format!("chunk snapshot install failed: {err}")))
    }

    /// Propose one write as the payload of a raft entry.
    ///
    /// Here rather than at the call site so a proposer and an applier cannot disagree about the
    /// encoding: there is one spelling of a chunk write and both halves go through it.
    pub fn encode_write(page_id: PageId, ranges: Vec<(u64, Vec<u8>)>) -> Vec<u8> {
        ChunkWriteCommand { page_id, ranges }.to_payload()
    }
}

impl StateMachine for ChunkStateMachine {
    fn apply(&mut self, request: ApplyRequest) -> Result<ApplyResponse, RaftError> {
        if request.group_id != self.group_id {
            return Err(RaftError::InvalidRequest(format!(
                "entry at index {} belongs to group {} and this replica serves group {}",
                request.log_id.index, request.group_id, self.group_id
            )));
        }
        // A REDELIVERY IS A SUCCESS, NOT A FAILURE. Refusing it would stall the apply loop on an
        // entry raft has every right to send again; applying it twice would advance
        // `write_sequence` and the physical-write counters for a write that happened once, and
        // those counters are what the page service's degraded-write accounting reads. So the
        // already-applied index is acknowledged and nothing is written.
        if request.log_id.index <= self.last_applied_index {
            return Ok(ApplyResponse {
                applied_index: self.last_applied_index,
                response: Vec::new(),
            });
        }
        // A GAP IS REFUSED, NOT APPLIED. Raft delivers a group's entries contiguously, so this
        // should never fire against a correct sender -- which is exactly why it is checked rather
        // than assumed. Applying index n+2 onto a replica that has applied n leaves a page that
        // NO sequence of writes produces: it holds the later write and not the earlier one, it
        // will never be told about the earlier one again, and nothing downstream can see the
        // difference between that and a page that was written correctly.
        //
        // Found by writing the group route rather than by reading this function: a proposal that
        // leaves a replica out -- a partition, expressed -- means the next entry that replica
        // does receive arrives with a hole behind it. The replica has to be caught up from a
        // snapshot instead, and refusing the gap is what makes that mandatory rather than
        // optional.
        if request.log_id.index > self.last_applied_index.saturating_add(1) {
            return Err(RaftError::InvalidRequest(format!(
                "entry at index {} would skip indexes {}..{}: this replica has applied through \
                 {} and must be caught up from a snapshot rather than applying over the gap",
                request.log_id.index,
                self.last_applied_index.saturating_add(1),
                request.log_id.index.saturating_sub(1),
                self.last_applied_index
            )));
        }
        let command = ChunkWriteCommand::from_payload(&request.payload)
            .map_err(|err| RaftError::InvalidRequest(err.to_string()))?;
        self.replica
            .write_chunk_ranges(command.page_id, command.ranges)
            .map_err(|err| RaftError::Storage(format!("chunk write apply failed: {err}")))?;
        self.last_applied_index = request.log_id.index;
        Ok(ApplyResponse {
            applied_index: self.last_applied_index,
            response: Vec::new(),
        })
    }

    /// The replica's own snapshot encoding, carried opaquely.
    ///
    /// NO NEW FORMAT. MatrixRaft carries opaque bytes in `SnapshotChunk::data`, and what travels
    /// in it is the replica snapshot encoding this crate already writes and already checksums on
    /// install -- so a snapshot that crosses a raft group is the same bytes a local install would
    /// accept, and the validation inside `install_snapshot_bytes_in_place` runs unchanged.
    fn snapshot(&self) -> Result<SnapshotChunk, RaftError> {
        let data = self
            .replica
            .export_snapshot()
            .to_bytes()
            .map_err(|err| RaftError::Storage(format!("chunk snapshot export failed: {err}")))?;
        Ok(SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: format!("chunk-{}-{}", self.group_id, self.last_applied_index),
                // THE INDEX IS CARRIED AND THE TERM IS NOT INVENTED. A replica snapshot records no
                // term, and a receiver checks the index: a snapshot is what tells a restarting
                // node which entries it no longer needs to replay.
                last_log_id: LogId {
                    term: 0,
                    index: self.last_applied_index,
                },
                membership: Vec::new(),
                members: Vec::new(),
            },
            offset: 0,
            data,
            done: true,
        })
    }

    fn install_snapshot(&mut self, chunk: SnapshotChunk) -> Result<(), RaftError> {
        // A sender may restart a snapshot, so an offset of zero begins a fresh one rather than
        // appending to whatever a previous attempt left.
        if chunk.offset == 0 {
            self.installing.clear();
        } else if chunk.offset as usize != self.installing.len() {
            return Err(RaftError::InvalidRequest(format!(
                "chunk snapshot chunk starts at {} but {} bytes have arrived",
                chunk.offset,
                self.installing.len()
            )));
        }
        self.installing.extend_from_slice(&chunk.data);
        if !chunk.done {
            return Ok(());
        }
        let index = chunk.meta.last_log_id.index;
        let bytes = std::mem::take(&mut self.installing);
        self.replica
            .install_snapshot_bytes_in_place(&bytes)
            .map_err(|err| RaftError::Storage(format!("chunk snapshot install failed: {err}")))?;
        // THE APPLIED INDEX MOVES WITH THE DATA. Left where it was, this node would replay every
        // entry the snapshot already contains -- and the redelivery guard above would then be the
        // only thing standing between it and double-applying all of them.
        self.last_applied_index = index;
        Ok(())
    }
}
