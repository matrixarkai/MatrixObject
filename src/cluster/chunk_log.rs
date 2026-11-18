// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The client side of a log chunk: append to the three replicas that hold it, read it back.
//!
//! A log chunk lives on three nodes. A writer holding the chunk's epoch sends every record to
//! all three; a record is acknowledged to the caller once TWO nodes report it durable. The node
//! reports where the bytes landed (its applied and durable lengths), so the writer never guesses:
//! a node that lost an offset, or answered late, or came back after a restart, is brought up from
//! the bytes the writer still holds, starting at the length that node itself reports.
//!
//! The quorum-durable length of a chunk is the second largest durable length among its three
//! nodes. Every acknowledged byte lies below it, and a reader never goes above it: a byte held by
//! one node only has not been acknowledged and may yet be discarded.
use super::client::{RemoteReplicaClient, ReplicaClientOptions};
use super::log_chunk_store::{LOG_CHUNK_LIMIT, SEALED_MESSAGE, STALE_EPOCH_PREFIX};
use super::log_op::{LogOpReply, LogOpRequest, MAX_OP_DATA};
use crate::ObjectError;
use std::sync::mpsc;
use std::time::Duration;

/// Nodes that hold one log chunk.
pub const LOG_REPLICAS: usize = 3;
/// Nodes that must report a byte durable before it is acknowledged.
pub const LOG_QUORUM: usize = 2;
/// The largest record the writer sends: bigger appends are split, and records pipeline.
pub const LOG_RECORD_BYTES: usize = 1 << 20;
/// The writer keeps the bytes a node may still need, up to this many. A node further behind
/// than this cannot be caught up from the writer and is given up on (repair is another path).
pub const LOG_WINDOW_BYTES: usize = 32 << 20;

const UNKNOWN_VERB_PREFIX: &str = "the replica does not implement this request:";
/// A node sends its error as text, and `ObjectError`'s display puts this in front of a
/// precondition failure, so that is what a client receives.
const WIRE_PRECONDITION_PREFIX: &str = "precondition failed: ";

/// Why one call to one node did not apply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogCallError {
    /// The node does not know the log chunk verb. Nothing was applied.
    NotSpoken,
    /// The chunk is held at a newer writer epoch.
    StaleEpoch { current: u64 },
    /// The chunk is sealed.
    Sealed,
    /// The node refused for another reason; the connection is still good.
    Refused(ObjectError),
    /// The connection failed. Whether the request landed is not known.
    Transport(ObjectError),
}

impl LogCallError {
    pub(crate) fn from_object(error: ObjectError, transport: bool) -> Self {
        if transport {
            return Self::Transport(error);
        }
        if let ObjectError::PreconditionFailed(message) = &error {
            let message = message.strip_prefix(WIRE_PRECONDITION_PREFIX).unwrap_or(message);
            if message.starts_with(UNKNOWN_VERB_PREFIX) {
                return Self::NotSpoken;
            }
            if let Some(rest) = message.strip_prefix(STALE_EPOCH_PREFIX) {
                if let Ok(current) = rest.trim().parse::<u64>() {
                    return Self::StaleEpoch { current };
                }
            }
            if message == SEALED_MESSAGE {
                return Self::Sealed;
            }
        }
        Self::Refused(error)
    }
}

/// Why a chunk operation did not complete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChunkLogError {
    /// The nodes do not speak log chunks.
    NotSpoken,
    /// Another writer holds a newer epoch: this writer must stop.
    StaleEpoch { current: u64 },
    Sealed,
    /// Fewer than two nodes reached the length asked for. The bytes are held by the writer and
    /// `settle` retries them.
    NoQuorum { wanted: u64, durable: u64 },
    Refused(ObjectError),
}

impl From<LogCallError> for ChunkLogError {
    fn from(error: LogCallError) -> Self {
        match error {
            LogCallError::NotSpoken => Self::NotSpoken,
            LogCallError::StaleEpoch { current } => Self::StaleEpoch { current },
            LogCallError::Sealed => Self::Sealed,
            LogCallError::Refused(error) | LogCallError::Transport(error) => Self::Refused(error),
        }
    }
}

impl std::fmt::Display for ChunkLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSpoken => write!(f, "the nodes do not speak log chunks"),
            Self::StaleEpoch { current } => write!(f, "the chunk is at writer epoch {current}"),
            Self::Sealed => write!(f, "the chunk is sealed"),
            Self::NoQuorum { wanted, durable } => {
                write!(f, "only {durable} of {wanted} bytes are durable on two nodes")
            }
            Self::Refused(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ChunkLogError {}

/// The second largest of the three durable lengths.
fn quorum_length(durables: &[u64]) -> u64 {
    let mut sorted: Vec<u64> = durables.to_vec();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    sorted.get(LOG_QUORUM - 1).copied().unwrap_or(0)
}

struct Member {
    endpoint: String,
    /// `None` while the node is down or a request is in flight on its connection.
    client: Option<RemoteReplicaClient>,
    busy: bool,
    /// Bytes the node reported applied, and durable. Both only grow while the epoch holds.
    applied: u64,
    durable: u64,
    /// Whether the node has answered a tail since it last failed.
    known: bool,
    /// Further behind than the writer's window, or refused: no longer sent anything.
    given_up: bool,
}

/// What one node's batch came back with.
struct Done {
    index: usize,
    client: Option<RemoteReplicaClient>,
    applied: u64,
    durable: u64,
    fatal: Option<LogCallError>,
    refused: bool,
}

/// Appends to one log chunk across its three nodes.
pub struct ChunkLogWriter {
    members: Vec<Member>,
    options: ReplicaClientOptions,
    volume: u64,
    chunk: u64,
    generation: u32,
    epoch: u64,
    /// The bytes `[window_start, next)`: what a node behind the quorum may still need.
    window: Vec<u8>,
    window_start: u64,
    next: u64,
    done_tx: mpsc::Sender<Done>,
    done_rx: mpsc::Receiver<Done>,
    fatal: Option<ChunkLogError>,
}

fn connect_and_tail(
    endpoint: &str,
    options: &ReplicaClientOptions,
    volume: u64,
    chunk: u64,
    generation: u32,
) -> Result<(RemoteReplicaClient, LogOpReply), LogCallError> {
    let mut client = RemoteReplicaClient::connect(endpoint, options).map_err(|error| LogCallError::Transport(error))?;
    let reply = client.log_tail(volume, chunk, generation)?;
    Ok((client, reply))
}

impl ChunkLogWriter {
    /// Take the chunk at `epoch`. The writer's length is the quorum-durable length of the
    /// nodes; bytes any node holds beyond it were never acknowledged and are cut off, so every
    /// node starts from the same bytes.
    pub fn open(
        endpoints: &[String],
        volume: u64,
        chunk: u64,
        generation: u32,
        epoch: u64,
        options: ReplicaClientOptions,
    ) -> Result<Self, ChunkLogError> {
        if endpoints.len() != LOG_REPLICAS || epoch == 0 {
            return Err(ChunkLogError::Refused(ObjectError::InvalidArgument(
                "a log chunk is written to three nodes at a nonzero epoch".into(),
            )));
        }
        let (done_tx, done_rx) = mpsc::channel();
        let mut members = Vec::new();
        let mut spoken = 0;
        let mut silent = 0;
        for endpoint in endpoints {
            let mut member = Member {
                endpoint: endpoint.clone(),
                client: None,
                busy: false,
                applied: 0,
                durable: 0,
                known: false,
                given_up: false,
            };
            match connect_and_tail(endpoint, &options, volume, chunk, generation) {
                Ok((client, reply)) => {
                    if reply.sealed {
                        return Err(ChunkLogError::Sealed);
                    }
                    member.client = Some(client);
                    member.applied = reply.length;
                    member.durable = reply.durable_length;
                    member.known = true;
                    spoken += 1;
                }
                Err(LogCallError::NotSpoken) => silent += 1,
                Err(LogCallError::Transport(_)) => {}
                Err(other) => return Err(other.into()),
            }
            members.push(member);
        }
        if silent == LOG_REPLICAS || (spoken == 0 && silent > 0) {
            return Err(ChunkLogError::NotSpoken);
        }
        if spoken < LOG_QUORUM {
            return Err(ChunkLogError::NoQuorum { wanted: 1, durable: 0 });
        }
        let durables: Vec<u64> = members.iter().map(|m| m.durable).collect();
        let next = quorum_length(&durables);
        for member in &mut members {
            if !member.known {
                continue;
            }
            // Every node that holds the quorum length or more is cut back to it AT THIS EPOCH,
            // also when it already holds exactly that: the truncate is what fences an older
            // writer on that node. Without it a late append from the writer this one replaced
            // would still be accepted until the new writer happened to write.
            if member.applied >= next {
                let answer = member
                    .client
                    .as_mut()
                    .expect("a node that answered a tail has a connection")
                    .log_truncate(volume, chunk, generation, epoch, next);
                match answer {
                    Ok(reply) => {
                        member.applied = reply.length;
                        member.durable = reply.durable_length;
                    }
                    // a node that went away between its tail and its truncate is behind, as far as
                    // this writer can tell: repair's job
                    Err(LogCallError::Transport(_)) => {
                        member.client = None;
                        member.given_up = true;
                        continue;
                    }
                    Err(other) => return Err(other.into()),
                }
            }
            // Behind the quorum and nothing to catch it up from: repair's job, not the writer's.
            if member.applied < next {
                member.given_up = true;
            }
        }
        Ok(Self {
            members,
            options,
            volume,
            chunk,
            generation,
            epoch,
            window: Vec::new(),
            window_start: next,
            next,
            done_tx,
            done_rx,
            fatal: None,
        })
    }

    /// Bytes the chunk holds, acknowledged or not.
    pub fn length(&self) -> u64 {
        self.next
    }

    /// The length two nodes have made durable: everything below it is acknowledged.
    pub fn durable_length(&self) -> u64 {
        quorum_length(&self.members.iter().map(|m| m.durable).collect::<Vec<_>>())
    }

    /// Append `data` and return once two nodes hold it durably. On `NoQuorum` the bytes are
    /// kept and still count toward the chunk's length: call `settle` to wait for them again.
    pub fn append(&mut self, data: &[u8]) -> Result<u64, ChunkLogError> {
        if let Some(error) = &self.fatal {
            return Err(error.clone());
        }
        if data.is_empty() {
            return Ok(self.durable_length());
        }
        if self.next + data.len() as u64 > LOG_CHUNK_LIMIT {
            return Err(ChunkLogError::Refused(ObjectError::InvalidArgument(
                "the log chunk is full: switch to the next chunk".into(),
            )));
        }
        self.window.extend_from_slice(data);
        self.next += data.len() as u64;
        self.drive()
    }

    /// Send whatever a node still lacks and wait until two nodes hold everything appended.
    pub fn settle(&mut self) -> Result<u64, ChunkLogError> {
        if let Some(error) = &self.fatal {
            return Err(error.clone());
        }
        self.drive()
    }

    fn apply(&mut self, done: Done) -> Result<(), ChunkLogError> {
        let member = &mut self.members[done.index];
        member.busy = false;
        member.client = done.client;
        if member.client.is_none() {
            // The connection died mid-batch: the next call reconnects and asks for the tail.
            member.known = false;
        } else {
            member.applied = member.applied.max(done.applied);
            member.durable = member.durable.max(done.durable);
        }
        if done.refused {
            member.given_up = true;
        }
        if let Some(error) = done.fatal {
            let error = ChunkLogError::from(error);
            self.fatal = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }

    fn collect_finished(&mut self) -> Result<(), ChunkLogError> {
        while let Ok(done) = self.done_rx.try_recv() {
            self.apply(done)?;
        }
        Ok(())
    }

    /// Bring a node that was down back: a new connection, and ask it where it is.
    fn reconnect(&mut self, index: usize) {
        let member = &mut self.members[index];
        let Ok((client, reply)) = connect_and_tail(&member.endpoint, &self.options, self.volume, self.chunk, self.generation)
        else {
            return;
        };
        member.client = Some(client);
        member.applied = reply.length;
        member.durable = reply.durable_length;
        member.known = true;
    }

    fn start(&mut self, index: usize) {
        let (volume, chunk, generation, epoch) = (self.volume, self.chunk, self.generation, self.epoch);
        let member = &mut self.members[index];
        let from = member.applied;
        if from < self.window_start {
            member.given_up = true;
            return;
        }
        let bytes = &self.window[(from - self.window_start) as usize..];
        let mut ops = Vec::new();
        let mut offset = from;
        for piece in bytes.chunks(LOG_RECORD_BYTES.min(MAX_OP_DATA)) {
            ops.push(LogOpRequest::append(volume, chunk, generation, epoch, offset, piece.to_vec()));
            offset += piece.len() as u64;
        }
        let Some(mut client) = member.client.take() else { return };
        member.busy = true;
        let tx = self.done_tx.clone();
        std::thread::spawn(move || {
            let mut done = Done { index, client: None, applied: 0, durable: 0, fatal: None, refused: false };
            let mut alive = true;
            for batch in ops.chunks(super::client::LOG_PIPELINE_DEPTH) {
                for result in client.log_append_pipelined(batch) {
                    match result {
                        Ok(reply) => {
                            done.applied = done.applied.max(reply.length);
                            done.durable = done.durable.max(reply.durable_length);
                        }
                        Err(LogCallError::Transport(_)) => alive = false,
                        Err(LogCallError::Refused(_)) => done.refused = true,
                        Err(other) => {
                            if done.fatal.is_none() {
                                done.fatal = Some(other);
                            }
                        }
                    }
                }
                if !alive || done.refused || done.fatal.is_some() {
                    break;
                }
            }
            if alive {
                done.client = Some(client);
            }
            let _ = tx.send(done);
        });
    }

    fn drive(&mut self) -> Result<u64, ChunkLogError> {
        self.collect_finished()?;
        let end = self.next;
        for index in 0..self.members.len() {
            let member = &self.members[index];
            if member.busy || member.given_up {
                continue;
            }
            if member.client.is_none() || !member.known {
                self.reconnect(index);
            }
            let member = &self.members[index];
            if member.client.is_none() || member.applied >= end {
                continue;
            }
            self.start(index);
        }
        loop {
            let durable = self.durable_length();
            if durable >= end {
                self.trim();
                return Ok(durable);
            }
            if !self.members.iter().any(|m| m.busy) {
                self.trim();
                return Err(ChunkLogError::NoQuorum { wanted: end, durable });
            }
            match self.done_rx.recv_timeout(Duration::from_secs(60)) {
                Ok(done) => self.apply(done)?,
                Err(_) => {
                    return Err(ChunkLogError::NoQuorum { wanted: end, durable: self.durable_length() });
                }
            }
        }
    }

    /// Forget the bytes every node that is still being sent to already holds.
    fn trim(&mut self) {
        if self.window.len() > LOG_WINDOW_BYTES {
            let oldest = self.next - LOG_WINDOW_BYTES as u64;
            for member in &mut self.members {
                if member.applied < oldest {
                    member.given_up = true;
                }
            }
        }
        let floor = self
            .members
            .iter()
            .filter(|m| !m.given_up)
            .map(|m| m.applied)
            .min()
            .unwrap_or(self.next)
            .max(self.window_start);
        if floor > self.window_start {
            self.window.drain(..(floor - self.window_start) as usize);
            self.window_start = floor;
        }
    }

    /// Seal the chunk on every node that is reachable. Two must seal for the chunk to count as
    /// sealed; the rest are repaired later.
    pub fn seal(&mut self, with_sha256: bool) -> Result<u64, ChunkLogError> {
        Ok(self.seal_footer(with_sha256)?.length)
    }

    /// Seal the chunk and return its footer: the length, the CRC-32C and (when asked for) the
    /// SHA-256 of the sealed bytes, as the nodes computed them. The footer is what the metadata
    /// plane records when the object moves to its next chunk. Every node that sealed holds the
    /// same bytes, so a node that answers with a different footer is refused.
    pub fn seal_footer(&mut self, with_sha256: bool) -> Result<SealedFooter, ChunkLogError> {
        let end = self.settle()?;
        let mut sealed = 0;
        let mut footer: Option<SealedFooter> = None;
        for index in 0..self.members.len() {
            self.collect_finished()?;
            let member = &mut self.members[index];
            let Some(client) = member.client.as_mut() else { continue };
            match client.log_seal(self.volume, self.chunk, self.generation, self.epoch, with_sha256) {
                Ok(reply) if reply.length == end => {
                    let seen = SealedFooter { length: reply.length, crc: reply.crc, sha256: reply.sha256 };
                    match &footer {
                        None => footer = Some(seen),
                        Some(first) if *first == seen => {}
                        Some(_) => {
                            return Err(ChunkLogError::Refused(ObjectError::Corruption(
                                "the nodes sealed the chunk with different footers".into(),
                            )))
                        }
                    }
                    sealed += 1;
                }
                Ok(_) => {}
                Err(LogCallError::Transport(_)) => member.client = None,
                Err(LogCallError::Refused(_)) => {}
                Err(other) => return Err(other.into()),
            }
        }
        match footer {
            Some(footer) if sealed >= LOG_QUORUM => Ok(footer),
            _ => Err(ChunkLogError::NoQuorum { wanted: end, durable: sealed as u64 }),
        }
    }
}

/// The footer of a sealed chunk: its length, CRC-32C and, if it was sealed with a digest, SHA-256.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedFooter {
    pub length: u64,
    pub crc: u32,
    pub sha256: Vec<u8>,
}

/// Reads a log chunk from its nodes, never above the quorum-durable length.
pub struct ChunkLogReader {
    endpoints: Vec<String>,
    clients: Vec<Option<RemoteReplicaClient>>,
    options: ReplicaClientOptions,
    volume: u64,
    chunk: u64,
    generation: u32,
}

impl ChunkLogReader {
    pub fn new(endpoints: &[String], volume: u64, chunk: u64, generation: u32, options: ReplicaClientOptions) -> Self {
        Self {
            endpoints: endpoints.to_vec(),
            clients: endpoints.iter().map(|_| None).collect(),
            options,
            volume,
            chunk,
            generation,
        }
    }

    fn client(&mut self, index: usize) -> Result<&mut RemoteReplicaClient, LogCallError> {
        if self.clients[index].is_none() {
            let client = RemoteReplicaClient::connect(&self.endpoints[index], &self.options).map_err(LogCallError::Transport)?;
            self.clients[index] = Some(client);
        }
        Ok(self.clients[index].as_mut().expect("just connected"))
    }

    /// Each node's durable length, `None` for one that did not answer, and the quorum length.
    fn lengths(&mut self) -> Result<(Vec<Option<u64>>, u64), ChunkLogError> {
        let mut durables = Vec::new();
        let mut silent = 0;
        for index in 0..self.endpoints.len() {
            let (volume, chunk, generation) = (self.volume, self.chunk, self.generation);
            match self.client(index).and_then(|client| client.log_tail(volume, chunk, generation)) {
                Ok(reply) => durables.push(Some(reply.durable_length)),
                Err(LogCallError::NotSpoken) => {
                    silent += 1;
                    durables.push(None);
                }
                Err(_) => {
                    self.clients[index] = None;
                    durables.push(None);
                }
            }
        }
        if silent == self.endpoints.len() {
            return Err(ChunkLogError::NotSpoken);
        }
        let answered: Vec<u64> = durables.iter().flatten().copied().collect();
        if answered.len() < LOG_QUORUM {
            return Err(ChunkLogError::NoQuorum { wanted: 1, durable: 0 });
        }
        let quorum = quorum_length(&answered);
        Ok((durables, quorum))
    }

    /// The length two nodes hold durably.
    pub fn length(&mut self) -> Result<u64, ChunkLogError> {
        Ok(self.lengths()?.1)
    }

    /// The bytes `[offset, offset + length)`, cut short at the quorum-durable length.
    pub fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, ChunkLogError> {
        let (durables, quorum) = self.lengths()?;
        let end = offset.saturating_add(length as u64).min(quorum);
        let mut out = Vec::new();
        let mut at = offset;
        while at < end {
            let piece = ((end - at) as usize).min(LOG_RECORD_BYTES);
            let piece_end = at + piece as u64;
            let mut got = None;
            for index in 0..self.endpoints.len() {
                if durables[index].map_or(true, |durable| durable < piece_end) {
                    continue;
                }
                let (volume, chunk, generation) = (self.volume, self.chunk, self.generation);
                match self.client(index).and_then(|client| client.log_read(volume, chunk, generation, at, piece as u32)) {
                    Ok(reply) if reply.data.len() == piece => {
                        got = Some(reply.data);
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => self.clients[index] = None,
                }
            }
            let Some(data) = got else {
                return Err(ChunkLogError::NoQuorum { wanted: piece_end, durable: at });
            };
            out.extend_from_slice(&data);
            at = piece_end;
        }
        Ok(out)
    }
}
