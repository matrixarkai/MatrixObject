// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The client side of a log chunk: append to the three replicas that hold it, read it back.
//!
//! A log chunk lives on three nodes. A writer holding the chunk's epoch sends every record to
//! all three; a record is acknowledged only after all three report it durable. The node
//! reports where the bytes landed (its applied and durable lengths), so the writer never guesses:
//! a node that lost an offset, or answered late, or came back after a restart, is brought up from
//! the bytes the writer still holds, starting at the length that node itself reports.
//!
//! The quorum-durable length of a chunk is the second largest durable length among its three
//! nodes. Recovery and reads retain this rule for data written by older clients. New writes
//! wait for all three durable lengths. A reader never goes above the recovery prefix: a byte held by
//! one node only has not been acknowledged and may yet be discarded.
use super::client::{RemoteReplicaClient, ReplicaClientOptions};
use super::log_chunk_store::{LOG_CHUNK_LIMIT, SEALED_MESSAGE, STALE_EPOCH_PREFIX};
use super::log_op::{LogOpReply, LogOpRequest, MAX_OP_DATA};
use crate::ObjectError;
use std::collections::VecDeque;
use std::sync::mpsc;
#[path = "identified_log_writer.rs"]
mod identified_writer;
#[path = "published_tail_reader.rs"]
mod published_tail_reader;
pub use identified_writer::AppendIdentity;
use identified_writer::{PendingAttempt, PendingRecord};
use std::time::{Duration, Instant};

/// Nodes that hold one log chunk.
pub const LOG_REPLICAS: usize = 3;
/// Nodes needed to recover existing data and resolve readable lengths.
/// New writes and seals require all [`LOG_REPLICAS`] nodes.
pub const LOG_QUORUM: usize = 2;
/// The largest record the writer sends: bigger appends are split, and records pipeline.
pub const LOG_RECORD_BYTES: usize = 1 << 20;

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
    /// The required durable replicas did not reach the target. The bytes are held by the writer and
    /// `settle` retries them.
    NoQuorum { wanted: u64, durable: u64 },
    /// A returning replica holds a record belonging to another attempt or another boundary.
    /// This writer cannot establish its pending outcome and must stop.
    AttemptMismatch { offset: u64 },
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
                write!(f, "the durability target was not reached: wanted {wanted}, reached {durable}")
            }
            Self::AttemptMismatch { offset } => write!(f, "the durable record at {offset} does not match the pending append attempt"),
            Self::Refused(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ChunkLogError {}

/// The second largest of the three durable lengths.
fn quorum_length(durables: &[u64]) -> u64 {
    let (mut largest, mut second) = (0, 0);
    for &length in durables {
        if length >= largest { second = largest; largest = length; }
        else if length > second { second = length; }
    }
    second
}

struct Member {
    endpoint: String,
    /// `None` while the node is down or a request is in flight on its connection.
    client: Option<RemoteReplicaClient>,
    busy: bool,
    /// Bytes the node reported applied, and durable. Both only grow while the epoch holds.
    applied: u64,
    durable: u64,
    /// Pending identified records confirmed by correlated replies or durable identity queries.
    confirmed: u64,
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
    confirmed: u64,
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
    /// The recovered prefix, retained even if a third replica still needs repair.
    recovery_length: u64,
    next: u64,
    done_tx: mpsc::Sender<Done>,
    done_rx: mpsc::Receiver<Done>,
    fatal: Option<ChunkLogError>,
    identified: bool,
    pending_attempt: Option<PendingAttempt>,
    pending_records: VecDeque<PendingRecord>,
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
                confirmed: 0,
            };
            match connect_and_tail(endpoint, &options, volume, chunk, generation) {
                Ok((client, reply)) => {
                    if reply.epoch > epoch {
                        return Err(ChunkLogError::StaleEpoch { current: reply.epoch });
                    }
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
                        member.known = false;
                        member.applied = 0;
                        member.durable = 0;
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
            recovery_length: next,
            next,
            done_tx,
            done_rx,
            fatal: None,
            identified: false,
            pending_attempt: None,
            pending_records: VecDeque::new(),
        })
    }

    /// Bytes the chunk holds, acknowledged or not.
    pub fn length(&self) -> u64 {
        self.next
    }

    /// The prefix all three nodes have made durable. This can be below the recovered
    /// length while a replica containing older data still needs repair.
    pub fn durable_length(&self) -> u64 {
        self.members.iter().map(|m| if self.identified { m.durable.min(m.confirmed) } else { m.durable }).min().unwrap_or(0)
    }

    /// Append `data` and return once all three nodes hold it durably. On `NoQuorum` the bytes are
    /// kept and still count toward the chunk's length: call `settle` to wait for them again.
    pub fn append(&mut self, data: &[u8]) -> Result<u64, ChunkLogError> {
        if self.identified {
            return Err(ChunkLogError::Refused(ObjectError::InvalidArgument("an identified writer requires an append identity".into())));
        }
        if let Some(error) = &self.fatal {
            return Err(error.clone());
        }
        if data.is_empty() {
            return self.drive();
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

    /// Send whatever a node still lacks and wait until all three hold everything appended.
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
        member.confirmed = member.confirmed.max(done.confirmed);
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
    fn reconnect(&mut self, index: usize) -> Result<(), ChunkLogError> {
        let (mut client, mut reply) = match connect_and_tail(
            &self.members[index].endpoint, &self.options, self.volume, self.chunk, self.generation,
        ) {
            Ok(answer) => answer,
            Err(LogCallError::Transport(_)) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if reply.epoch > self.epoch {
            let error = ChunkLogError::StaleEpoch { current: reply.epoch };
            self.fatal = Some(error.clone());
            return Err(error);
        }
        if reply.sealed && (reply.epoch != self.epoch || reply.length != self.next) {
            self.fatal = Some(ChunkLogError::Sealed);
            return Err(ChunkLogError::Sealed);
        }
        // An older epoch may still hold a suffix this writer replaced. Fence and
        // discard that suffix before any of its lengths can count toward success.
        if reply.epoch < self.epoch && reply.length >= self.recovery_length {
            reply = match client.log_truncate(
                self.volume, self.chunk, self.generation, self.epoch, self.recovery_length,
            ) {
                Ok(reply) => reply,
                Err(LogCallError::Transport(_)) => return Ok(()),
                Err(error) => {
                    let error = ChunkLogError::from(error);
                    if matches!(error, ChunkLogError::StaleEpoch { .. } | ChunkLogError::Sealed) {
                        self.fatal = Some(error.clone());
                    }
                    return Err(error);
                }
            };
        }
        let member = &mut self.members[index];
        member.client = Some(client);
        member.applied = reply.length;
        member.durable = reply.durable_length;
        member.known = true;
        if self.identified { member.confirmed = reply.durable_length.min(self.window_start); }
        member.given_up = reply.length < self.window_start;
        Ok(())
    }

    fn start(&mut self, index: usize) {
        let (volume, chunk, generation, epoch) = (self.volume, self.chunk, self.generation, self.epoch);
        let member = &mut self.members[index];
        let from = member.applied;
        if from < self.window_start {
            member.given_up = true;
            return;
        }
        let mut ops = Vec::new();
        if self.identified {
            for record in self.pending_records.iter().filter(|r| r.offset >= from) {
                let begin = (record.offset - self.window_start) as usize;
                let bytes = &self.window[begin..begin + record.length as usize];
                ops.push(LogOpRequest::append_identified(volume, chunk, generation, epoch, record.offset, bytes.to_vec(), record.tag));
            }
        } else {
            let bytes = &self.window[(from - self.window_start) as usize..];
            let mut offset = from;
            for piece in bytes.chunks(LOG_RECORD_BYTES.min(MAX_OP_DATA)) {
                ops.push(LogOpRequest::append(volume, chunk, generation, epoch, offset, piece.to_vec()));
                offset += piece.len() as u64;
            }
        }
        let confirmed = member.confirmed;
        let Some(mut client) = member.client.take() else { return };
        member.busy = true;
        let tx = self.done_tx.clone();
        std::thread::spawn(move || {
            let mut done = Done { index, client: None, applied: 0, durable: 0, confirmed, fatal: None, refused: false };
            let mut alive = true;
            for batch in ops.chunks(super::client::LOG_PIPELINE_DEPTH) {
                for (op, result) in batch.iter().zip(client.log_append_pipelined(batch)) {
                    match result {
                        Ok(reply) => {
                            done.applied = done.applied.max(reply.length);
                            done.durable = done.durable.max(reply.durable_length);
                            if op.version == 2 && op.offset == done.confirmed {
                                let end = op.offset + op.data.len() as u64;
                                if reply.durable_length >= end { done.confirmed = end; }
                            }
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

    /// Start work on every idle node that is still short of `end` bytes.
    fn start_lacking(&mut self, end: u64) -> Result<(), ChunkLogError> {
        for index in 0..self.members.len() {
            let member = &self.members[index];
            if member.busy {
                continue;
            }
            if member.given_up || member.client.is_none() || !member.known {
                self.reconnect(index)?;
            }
            if self.identified { self.confirm_pending(index)?; }
            let member = &self.members[index];
            if member.given_up || member.client.is_none() || member.applied >= end {
                continue;
            }
            self.start(index);
        }
        Ok(())
    }

    /// The common one-record append shares one encoded frame across the three sockets.
    /// Reuse the existing nonblocking replica fan-out instead of creating three threads
    /// and cloning the record into three independently encoded requests.
    fn drive_single_identified(&mut self) -> Option<Result<u64, ChunkLogError>> {
        if !self.identified || self.pending_records.len()!=1 || self.members.len()!=LOG_REPLICAS
            || self.members.iter().any(|m|m.busy||!m.known||m.given_up||m.client.is_none()
                ||m.applied!=self.window_start||m.durable<self.window_start||m.confirmed<self.window_start) {
            return None;
        }
        let record=self.pending_records.front().unwrap();
        if record.offset!=self.window_start || record.offset.checked_add(u64::from(record.length))!=Some(self.next) || record.length as usize!=self.window.len() {return None;}
        let mut op=LogOpRequest::append_identified(self.volume,self.chunk,self.generation,self.epoch,record.offset,Vec::new(),record.tag);
        op.data=std::mem::take(&mut self.window);
        if prost::Message::encoded_len(&op)>self.options.max_payload_bytes {
            self.window=op.data;return None;
        }
        let request=super::message::ReplicaRequest::LogOp(op);
        let frame=std::sync::Arc::new(super::message::encode_request(&request));
        let super::message::ReplicaRequest::LogOp(op)=request else {unreachable!()};
        // Retain the original attempt and payload before any request can reach storage.
        self.window=op.data;
        let mut results:[Option<Result<super::message::ReplicaReply,ObjectError>>;LOG_REPLICAS]=std::array::from_fn(|_|None);
        {
            let mut pending:[Option<super::client::PendingReplicaReply<'_>>;LOG_REPLICAS]=std::array::from_fn(|_|None);
            for (i,member) in self.members.iter_mut().enumerate() {
                member.busy=true;
                match member.client.as_mut().unwrap().begin_encoded_before(crate::cluster::wire::RequestCode::LogOp,frame.clone(),self.options.deadline) {
                    Ok(reply)=>pending[i]=Some(reply),Err(error)=>results[i]=Some(Err(error)),
                }
            }
            while pending.iter().any(Option::is_some) {
                let mut progress=false;let mut fatal=false;
                for (i,slot) in pending.iter_mut().enumerate() {
                    let result=slot.as_mut().and_then(|reply| {let before=reply.transferred_bytes();let result=reply.poll();progress|=result.is_some()||reply.transferred_bytes()!=before;result});
                    if let Some(result)=result {
                        if let Err(error)=&result {
                            fatal|=matches!(LogCallError::from_object(error.clone(),false),LogCallError::StaleEpoch{..}|LogCallError::Sealed|LogCallError::NotSpoken);
                        }
                        results[i]=Some(result);*slot=None;
                    }
                }
                if fatal {break;}
                if !progress {super::replica_set::wait_for_pending(&pending);}
            }
        }
        let mut first_error=None;
        for (i,result) in results.into_iter().enumerate() {
            let member=&mut self.members[i];
            let usable=!member.client.as_ref().unwrap().needs_standalone_reconnect();
            let mut done=Done{index:i,client:if usable {member.client.take()} else {None},applied:0,durable:0,confirmed:member.confirmed,fatal:None,refused:false};
            if !usable {member.client=None;}
            match result {
                Some(Ok(super::message::ReplicaReply::LogOp(reply)))=>{
                    done.applied=reply.length;done.durable=reply.durable_length;
                    if reply.epoch>self.epoch {done.fatal=Some(LogCallError::StaleEpoch{current:reply.epoch});}
                    else if reply.epoch!=self.epoch {done.refused=true;}
                    else if done.durable>=self.next {done.confirmed=self.next;}
                }
                Some(Ok(_))=>done.refused=true,
                Some(Err(error))=>match LogCallError::from_object(error,!usable) {
                    LogCallError::Transport(_)=>{},LogCallError::Refused(_)=>done.refused=true,other=>done.fatal=Some(other),
                },
                None=>{},
            }
            if let Err(error)=self.apply(done) {if first_error.is_none(){first_error=Some(error);}}
        }
        if let Some(error)=first_error {self.fatal=Some(error.clone());return Some(Err(error));}
        let durable=self.durable_length();
        if durable>=self.next {self.trim();Some(Ok(durable))}
        else {Some(Err(ChunkLogError::NoQuorum{wanted:self.next,durable}))}
    }

    fn drive(&mut self) -> Result<u64, ChunkLogError> {
        self.collect_finished()?;
        if let Some(result)=self.drive_single_identified() {return result;}
        let end = self.next;
        self.start_lacking(end)?;
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

    /// Retain pending bytes until every required replica reports them durable.
    /// The chunk limit bounds one writer's retained data, including retries.
    fn trim(&mut self) {
        let floor = self.durable_length().max(self.window_start);
        // Keep the complete unsettled attempt for exact retry comparison, even when a common
        // fragment prefix is already durable. The chunk limit bounds its retained bytes.
        if self.identified && floor < self.next { return; }
        if self.identified {
            self.pending_records.clear();
            self.pending_attempt = None;
        }
        if floor > self.window_start {
            self.window.drain(..(floor - self.window_start) as usize);
            self.window_start = floor;
        }
    }

    /// Wait until every node that can still be caught up holds all `end` bytes. A node that is
    /// still short when the wait ends (it never answered, or its connection failed and would not
    /// come back) is left as it is: the caller does not seal it. The wait is bounded by the
    /// connection's read timeout.
    fn drain(&mut self, end: u64) -> Result<(), ChunkLogError> {
        let deadline = Instant::now() + self.options.read_timeout;
        loop {
            self.collect_finished()?;
            self.start_lacking(end)?;
            if !self.members.iter().any(|m| m.busy) {
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(());
            }
            match self.done_rx.recv_timeout(left) {
                Ok(done) => self.apply(done)?,
                Err(_) => return Ok(()),
            }
        }
    }

    /// Seal the chunk on every node that holds all of its bytes. All three must seal for it to
    /// count as sealed; a node that is still short is left open (see `seal_footer`).
    pub fn seal(&mut self, with_sha256: bool) -> Result<u64, ChunkLogError> {
        Ok(self.seal_footer(with_sha256)?.length)
    }

    /// Seal the chunk and return its footer: the length, the CRC-32C and (when asked for) the
    /// SHA-256 of the sealed bytes, as the nodes computed them. The footer is what the metadata
    /// plane records when the object moves to its next chunk. Every node that sealed holds the
    /// same bytes, so a node that answers with a different footer is refused.
    ///
    /// A seal is final, so a node is sealed only once it holds every byte the writer has: first
    /// the nodes that are behind are given the bytes they lack and waited for (a node whose
    /// appends are still in flight is not sealed at the shorter length it happens to hold), and
    /// then only a node at exactly the chunk's length is sealed. A node that is down, or that does
    /// not catch up within the read timeout, is NOT sealed: it is left open and is under-replicated
    /// until a repair copies the chunk to it, and a reader falls back to the nodes that sealed.
    /// A node that answers a seal with a length other than the one it was asked to hold has sealed
    /// something wrong and cannot be undone: that is an error, and no footer is returned.
    pub fn seal_footer(&mut self, with_sha256: bool) -> Result<SealedFooter, ChunkLogError> {
        let end = self.settle()?;
        self.drain(end)?;
        let mut sealed = 0;
        let mut footer: Option<SealedFooter> = None;
        for index in 0..self.members.len() {
            let member = &mut self.members[index];
            if member.busy || member.given_up || member.applied != end {
                continue;
            }
            let Some(client) = member.client.as_mut() else { continue };
            let answer = match client.log_seal(self.volume, self.chunk, self.generation, self.epoch, with_sha256) {
                // A previous seal can have persisted even when its reply was lost.
                // Inspect the immutable footer rather than writing another record.
                Err(LogCallError::Sealed) => client.log_tail(self.volume, self.chunk, self.generation),
                answer => answer,
            };
            match answer {
                Ok(reply) if reply.sealed && reply.length == end && (!with_sha256 || reply.sha256.len() == 32) => {
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
                Ok(reply) => {
                    // some node holds a sealed chunk of another length: that cannot be undone, so
                    // this writer is finished, no footer is returned, and the chunk must not be
                    // recorded as sealed anywhere
                    member.given_up = true;
                    let error = ChunkLogError::Refused(ObjectError::Corruption(format!(
                        "a node sealed the chunk at {} bytes, not the {} the writer holds",
                        reply.length, end
                    )));
                    self.fatal = Some(error.clone());
                    return Err(error);
                }
                Err(LogCallError::Transport(_)) => member.client = None,
                Err(LogCallError::Refused(_)) => {}
                Err(other) => return Err(other.into()),
            }
        }
        match footer {
            Some(footer) if sealed == LOG_REPLICAS => Ok(footer),
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

static NEXT_READ_REPLICA: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Reads open chunks at their recovery prefix, or immutable bytes through a verified footer.
pub struct ChunkLogReader {
    endpoints: Vec<String>,
    clients: Vec<Option<RemoteReplicaClient>>,
    options: ReplicaClientOptions,
    volume: u64,
    chunk: u64,
    generation: u32,
    preferred: usize,
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
            preferred: NEXT_READ_REPLICA.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % endpoints.len().max(1),
        }
    }

    fn client(&mut self, index: usize) -> Result<&mut RemoteReplicaClient, LogCallError> {
        if self.clients[index].is_none() {
            let client = RemoteReplicaClient::connect(&self.endpoints[index], &self.options).map_err(LogCallError::Transport)?;
            self.clients[index] = Some(client);
        }
        Ok(self.clients[index].as_mut().expect("just connected"))
    }

    /// One read-only call to node `index`. A connection kept from an earlier call can have been
    /// closed by the node while it sat idle (a node closes a connection that sends nothing for
    /// its read timeout); the first request on it then fails, which says nothing about the node.
    /// So a transport failure on a connection that was already in use is repeated once on a
    /// fresh one. Only for calls that change nothing (a tail query, a read): a mutation whose
    /// outcome is unknown must never be sent twice.
    fn read_call<T>(&mut self, index: usize, call: impl Fn(&mut RemoteReplicaClient) -> Result<T, LogCallError>) -> Result<T, LogCallError> {
        let reused = self.clients[index].is_some();
        match self.client(index).and_then(&call) {
            Err(LogCallError::Transport(_)) if reused => {
                self.clients[index] = None;
                self.client(index).and_then(&call)
            }
            other => other,
        }
    }

    /// Each node's durable length and the quorum length. A node that did not answer, or that is
    /// still at an older writer epoch than another node reports, is `None`: it takes no part in
    /// the quorum length and no read is served from it. A node left at an older epoch missed the
    /// writer change (it was down, or behind the quorum when the new writer opened), so whatever
    /// it holds past the point the new writer started from may be bytes that were never
    /// acknowledged and that the new writer's bytes replaced elsewhere.
    fn lengths(&mut self) -> Result<(Vec<Option<u64>>, u64), ChunkLogError> {
        let mut answers: Vec<Option<LogOpReply>> = Vec::new();
        let mut silent = 0;
        for index in 0..self.endpoints.len() {
            let (volume, chunk, generation) = (self.volume, self.chunk, self.generation);
            match self.read_call(index, |client| client.log_tail(volume, chunk, generation)) {
                Ok(reply) => answers.push(Some(reply)),
                Err(LogCallError::NotSpoken) => {
                    silent += 1;
                    answers.push(None);
                }
                Err(_) => {
                    self.clients[index] = None;
                    answers.push(None);
                }
            }
        }
        if silent == self.endpoints.len() {
            return Err(ChunkLogError::NotSpoken);
        }
        let highest = answers.iter().flatten().map(|reply| reply.epoch).max().unwrap_or(0);
        let immutable = answers.len() == LOG_REPLICAS && answers.iter().all(|answer| {
            let (Some(first), Some(reply)) = (answers[0].as_ref(), answer.as_ref()) else { return false };
            first.sealed && reply.sealed && first.length == reply.length
                && first.durable_length == first.length && reply.durable_length == reply.length
                && first.crc == reply.crc && first.sha256 == reply.sha256
        });
        let durables: Vec<Option<u64>> = answers.iter().map(|answer| {
            answer.as_ref().filter(|reply| immutable || reply.epoch == highest)
                .map(|reply| reply.durable_length)
        }).collect();
        let eligible: Vec<u64> = durables.iter().flatten().copied().collect();
        if eligible.len() < LOG_QUORUM {
            return Err(ChunkLogError::NoQuorum { wanted: 1, durable: 0 });
        }
        let quorum = quorum_length(&eligible);
        Ok((durables, quorum))
    }

    /// The length two nodes hold durably.
    pub fn length(&mut self) -> Result<u64, ChunkLogError> {
        Ok(self.lengths()?.1)
    }

    /// Read immutable bytes using the footer supplied by committed metadata or
    /// a completed seal. Each piece needs one matching replica, without tail probes.
    /// A failed or mismatched replica is skipped; successful reads reuse that peer.
    pub fn read_sealed(&mut self, offset: u64, length: usize, footer: &SealedFooter) -> Result<Vec<u8>, ChunkLogError> {
        if footer.length > LOG_CHUNK_LIMIT || (!footer.sha256.is_empty() && footer.sha256.len() != 32) {
            return Err(ChunkLogError::Refused(ObjectError::InvalidArgument("invalid sealed log footer".into())));
        }
        let end = offset.checked_add(length as u64).ok_or_else(||
            ChunkLogError::Refused(ObjectError::InvalidArgument("range overflow".into())))?.min(footer.length);
        if offset >= end { return Ok(Vec::new()); }
        let mut out = Vec::with_capacity(((end - offset) as usize).min(LOG_RECORD_BYTES));
        let mut at = offset;
        while at < end {
            let piece = ((end - at) as usize).min(LOG_RECORD_BYTES);
            let mut got = None;
            for attempt in 0..self.endpoints.len() {
                let index = (self.preferred + attempt) % self.endpoints.len();
                let (volume, chunk, generation) = (self.volume, self.chunk, self.generation);
                match self.read_call(index, |client| client.log_read(volume, chunk, generation, at, piece as u32)) {
                    Ok(reply) if reply.sealed && reply.length == footer.length
                        && reply.durable_length == footer.length && reply.crc == footer.crc
                        && (footer.sha256.is_empty() || reply.sha256 == footer.sha256)
                        && reply.data.len() == piece => {
                            self.preferred = index;
                            got = Some(reply.data);
                            break;
                        }
                    Ok(_) => {}
                    Err(_) => self.clients[index] = None,
                }
            }
            let Some(data) = got else { return Err(ChunkLogError::NoQuorum { wanted: at + piece as u64, durable: at }); };
            out.extend_from_slice(&data);
            at += piece as u64;
        }
        Ok(out)
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
                match self.read_call(index, |client| client.log_read(volume, chunk, generation, at, piece as u32)) {
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
