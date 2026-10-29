// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Calling a replica that is in another process.
//!
//! # THE SIGNATURES, AND THE ONE THAT COULD NOT MATCH
//!
//! Three of the four calls here have the same name, the same arguments and the same return
//! type as the method of the same name on [`crate::MatrixReplicaService`]:
//!
//! | this client | the in-process replica |
//! |---|---|
//! | `write_chunk(&mut self, PageId, u64, &[u8]) -> Result<(), ObjectError>` | identical |
//! | `head_chunk(&mut self, &PageId) -> Result<ReplicaHead, ObjectError>` | takes `&self` |
//! | `discard_chunk(&mut self, &PageId, u64, u64) -> Result<(), ObjectError>` | identical |
//! | `read_chunk(&mut self, &PageId, u64, u64) -> Result<ReplicaReadResult, ObjectError>` | takes `&self` |
//!
//! **The reads take `&mut self` here and `&self` there, and that is not a detail that can be
//! polished away.** A read over a socket WRITES: it puts a request frame on the connection
//! before it can read an answer back. The local read borrows immutably because it only looks
//! at a `BTreeMap`. Any attempt to put both behind one trait has to pick one, and picking
//! `&self` means the connection goes behind a lock or a cell, which turns a read into a
//! contended one for no gain at this seam. So this difference is recorded rather than hidden,
//! and [`crate::cluster`] gives the measured size of the surface that would have to cross
//! the same question.
//!
//! # ERRORS ARRIVE AS THE STORE'S OWN
//!
//! A refusal comes back as the [`ObjectError`] variant the replica produced, carrying the
//! replica's message. A caller cannot tell a remote `NotFound` from a local one by its type,
//! which is what lets a quorum read count "this replica does not hold the page" separately
//! from "this replica is broken" without knowing where the replica is.
//!
//! # A TRANSPORT FAILURE IS NOT A STORAGE FAILURE, AND IS NOT CALLED ONE
//!
//! A dead peer, a refused connection and a frame cut in half are reported as
//! [`ObjectError::Corruption`] with the endpoint in the message. That variant is the only
//! one of the four that is not a claim about the store's contents, and saying `NotFound`
//! when a socket died would tell a quorum read that a page is absent when nobody asked
//! anyone.

use std::io::{BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use crate::cluster::message::{
    check_stamp, decode_failure, encode_request, ReplicaReply, ReplicaRequest,
};
use crate::cluster::wire::{StatusCode, HEADER_BYTES, MAX_PAYLOAD_BYTES};
use crate::{ObjectError, PageId, ReplicaHead, ReplicaReadResult};

/// How a client connects and how long it waits.
#[derive(Clone, Debug)]
pub struct ReplicaClientOptions {
    pub connect_timeout: Duration,
    /// Deadline for the complete response header and payload.
    pub read_timeout: Duration,
    /// Deadline for writing the complete request frame.
    pub write_timeout: Duration,
    pub max_payload_bytes: usize,
}

impl Default for ReplicaClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        }
    }
}

/// One active connection, optionally selected from a fixed chunk catalog.
/// A transport or framing failure closes its connection. A single-endpoint client
/// must be replaced before another call; a catalog can establish another connection.
/// A failed write may have reached the node and is never replayed on uncertainty.
#[derive(Debug)]
pub struct RemoteReplicaClient {
    endpoint: String,
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    max_payload_bytes: usize,
    connection_usable: bool,
    last_exchange: Instant,
    read_timeout: Duration,
    write_timeout: Duration,
    catalog: Option<(Vec<String>, ReplicaClientOptions)>,
    group_identity: Option<crate::cluster::ChunkGroupIdentity>,
}

impl RemoteReplicaClient {
    /// Open a connection to `endpoint` and keep it.
    ///
    /// The connection is held for the life of the client rather than opened per call. That
    /// is partly cost -- a handshake per replica per write is the dominant term in a fan-out
    /// of small writes -- and partly so that a cluster has something an operator can look at:
    /// an established socket with a PID on each end is the observable form of "these
    /// processes are talking to each other".
    pub fn connect(endpoint: &str, options: &ReplicaClientOptions) -> Result<Self, ObjectError> {
        let address = resolve(endpoint)?;
        let stream = TcpStream::connect_timeout(&address, options.connect_timeout)
            .map_err(|err| transport_error(endpoint, "connecting", &err.to_string()))?;
        stream
            .set_read_timeout(Some(options.read_timeout))
            .map_err(|err| transport_error(endpoint, "setting a read timeout", &err.to_string()))?;
        stream
            .set_write_timeout(Some(options.write_timeout))
            .map_err(|err| {
                transport_error(endpoint, "setting a write timeout", &err.to_string())
            })?;
        let _ = stream.set_nodelay(true);
        let writer = stream
            .try_clone()
            .map_err(|err| transport_error(endpoint, "duplicating the socket", &err.to_string()))?;
        Ok(Self {
            endpoint: endpoint.to_string(),
            reader: BufReader::new(stream),
            writer,
            max_payload_bytes: options.max_payload_bytes,
            connection_usable: true,
            last_exchange: Instant::now(),
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
            catalog: None,
            group_identity: None,
        })
    }

    /// Connect to a fixed catalog of one through seven chunk API IP sockets.
    /// Only typed pre-admission refusals allow a mutation to move between voters.
    /// Reads may also move after transport failure. Each call visits the catalog once.
    /// Every endpoint must belong to the same configured Raft group.
    /// Connections idle for the smaller of five seconds and the response timeout
    /// are replaced before sending a new request, with voter identity rechecked.
    pub fn connect_catalog(
        endpoints: &[String],
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
        if endpoints.is_empty() || endpoints.len() > 7 {
            return Err(ObjectError::InvalidArgument(
                "chunk catalog requires one through seven endpoints".into(),
            ));
        }
        let mut unique = std::collections::BTreeSet::new();
        let mut canonical = Vec::with_capacity(endpoints.len());
        for value in endpoints {
            if value.len() > 128 {
                return Err(ObjectError::InvalidArgument(
                    "chunk endpoint exceeds size bound".into(),
                ));
            }
            let address = value.parse::<SocketAddr>().map_err(|_| {
                ObjectError::InvalidArgument("chunk catalog endpoints must be IP sockets".into())
            })?;
            let address = match address {
                SocketAddr::V6(v) => v
                    .ip()
                    .to_ipv4_mapped()
                    .map(|ip| SocketAddr::new(ip.into(), v.port()))
                    .unwrap_or(address),
                _ => address,
            };
            if address.port() == 0
                || address.ip().is_unspecified()
                || address.ip().is_multicast()
                || !unique.insert(address)
            {
                return Err(ObjectError::InvalidArgument(
                    "chunk catalog endpoints must be valid and distinct".into(),
                ));
            }
            canonical.push(address.to_string());
        }
        let mut last = None;
        for endpoint in &canonical {
            match Self::connect(endpoint, options) {
                Ok(mut client) => {
                    client.catalog = Some((canonical, options.clone()));
                    return Ok(client);
                }
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap())
    }

    /// Connect to a Raft group, checking its ID before any data request.
    /// Every subsequent connection must match the initial voter's full configuration.
    /// This guards configuration errors, not malicious servers; transport authentication is separate.
    pub fn connect_raft_catalog(
        endpoints: &[String],
        group_id: u64,
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
        if group_id == 0 {
            return Err(ObjectError::InvalidArgument(
                "chunk group ID must be nonzero".into(),
            ));
        }
        let mut client = Self::connect_catalog(endpoints, options)?;
        let catalog = client.catalog.as_ref().unwrap().0.clone();
        let identity = loop {
            match client.chunk_group_identity() {
                Ok(identity) => break identity,
                Err(error) if !client.connection_usable => {
                    // No application request has been sent on this connection.
                    // Try only the remaining catalog endpoints, never replay a write.
                    let next = catalog.iter().position(|e| e == client.endpoint()).unwrap() + 1;
                    if next == catalog.len() {
                        return Err(error);
                    }
                    client = Self::connect_catalog(&catalog[next..], options)?;
                    client.catalog = Some((catalog.clone(), options.clone()));
                }
                Err(error) => return Err(error),
            }
        };
        if identity.group_id != group_id {
            return Err(ObjectError::PreconditionFailed(
                "chunk group ID does not match catalog".into(),
            ));
        }
        client.group_identity = Some(identity);
        Ok(client)
    }

    /// Connect against an operator- or metadata-supplied complete configuration.
    /// Unlike discovery by numeric ID, this also checks the very first connection's membership.
    pub fn connect_verified_raft_catalog(
        endpoints: &[String],
        expected: &crate::cluster::ChunkGroupIdentity,
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
        expected.validate()?;
        let client = Self::connect_raft_catalog(endpoints, expected.group_id, options)?;
        if !expected.same_group(
            client
                .group_identity
                .as_ref()
                .expect("Raft catalog has an identity"),
        ) {
            return Err(ObjectError::PreconditionFailed(
                "initial chunk voter configuration does not match catalog".into(),
            ));
        }
        Ok(client)
    }

    /// Describe the currently connected voter without a read barrier or leader redirection.
    pub fn chunk_group_identity(
        &mut self,
    ) -> Result<crate::cluster::ChunkGroupIdentity, ObjectError> {
        match self.call_one(&ReplicaRequest::GroupIdentity)? {
            ReplicaReply::GroupIdentity(identity) => Ok(identity),
            _ => Err(ObjectError::Corruption(
                "missing chunk group identity".into(),
            )),
        }
    }

    /// The currently selected endpoint; catalogs use canonical IP sockets.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// This end of the connection.
    pub fn local_addr(&self) -> Result<SocketAddr, ObjectError> {
        self.writer.local_addr().map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading the local address",
                &err.to_string(),
            )
        })
    }

    /// The replica's end of the connection.
    pub fn peer_addr(&self) -> Result<SocketAddr, ObjectError> {
        self.writer.peer_addr().map_err(|err| {
            transport_error(&self.endpoint, "reading the peer address", &err.to_string())
        })
    }

    /// Atomically require the current page sequence, or absence when it is zero.
    /// An uncertain response must not be automatically replayed.
    pub fn write_chunk_if_sequence(
        &mut self,
        page_id: PageId,
        offset: u64,
        data: &[u8],
        expected_sequence: u64,
    ) -> Result<(), ObjectError> {
        match self.call(ReplicaRequest::WriteChunkIfSequence {
            page_id,
            offset,
            data: data.to_vec(),
            expected_sequence,
        })? {
            ReplicaReply::Applied => Ok(()),
            other => Err(unexpected_reply("a conditional chunk write", &other)),
        }
    }

    pub fn write_chunk(
        &mut self,
        page_id: PageId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), ObjectError> {
        match self.call(ReplicaRequest::WriteChunk {
            page_id,
            offset,
            data: data.to_vec(),
        })? {
            ReplicaReply::Applied => Ok(()),
            other => Err(unexpected_reply("a chunk write", &other)),
        }
    }

    pub fn read_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<ReplicaReadResult, ObjectError> {
        match self.call(ReplicaRequest::ReadChunk {
            page_id: page_id.clone(),
            offset,
            length,
        })? {
            ReplicaReply::Read(read) => Ok(read),
            other => Err(unexpected_reply("a chunk read", &other)),
        }
    }

    pub fn head_chunk(&mut self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        match self.call(ReplicaRequest::HeadChunk {
            page_id: page_id.clone(),
        })? {
            ReplicaReply::Head(head) => Ok(head),
            other => Err(unexpected_reply("a chunk head", &other)),
        }
    }

    /// Discard only at the expected existing page sequence. Uncertain replies are not retried.
    pub fn discard_chunk_if_sequence(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
        expected_sequence: u64,
    ) -> Result<(), ObjectError> {
        if expected_sequence == 0 || expected_sequence == u64::MAX {
            return Err(ObjectError::PreconditionFailed(
                "conditional discard requires a nonexhausted existing sequence".into(),
            ));
        }
        match self.call(ReplicaRequest::DiscardChunkIfSequence {
            page_id: page_id.clone(),
            offset,
            length,
            expected_sequence,
        })? {
            ReplicaReply::Applied => Ok(()),
            other => Err(unexpected_reply("a conditional chunk discard", &other)),
        }
    }

    pub fn discard_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<(), ObjectError> {
        match self.call(ReplicaRequest::DiscardChunk {
            page_id: page_id.clone(),
            offset,
            length,
        })? {
            ReplicaReply::Applied => Ok(()),
            other => Err(unexpected_reply("a chunk discard", &other)),
        }
    }

    /// Read at most `limit` page identities and sequences after an exclusive cursor.
    /// Old servers refuse this new verb; no older frame layout has changed.
    /// Multiple batches are a live traversal, not a snapshot: fence writers before readmission.
    pub fn page_inventory(
        &mut self,
        after: Option<&PageId>,
        limit: u64,
    ) -> Result<crate::cluster::message::PageInventory, ObjectError> {
        crate::cluster::message::check_inventory_limit(limit)?;
        match self.call(ReplicaRequest::PageInventory {
            after: after.cloned(),
            limit,
        })? {
            ReplicaReply::PageInventory(batch) => {
                if batch.entries.len() as u64 > limit
                    || (batch.has_more && batch.entries.len() as u64 != limit)
                    || after.is_some_and(|cursor| {
                        batch.entries.first().is_some_and(|(id, _)| id <= cursor)
                    })
                {
                    return Err(ObjectError::Corruption(
                        "page inventory does not respect the requested cursor or limit".into(),
                    ));
                }
                Ok(batch)
            }
            other => Err(unexpected_reply("a page inventory", &other)),
        }
    }

    /// Ask whether the replica is answering, and how many pages it holds.
    pub fn ping(&mut self) -> Result<u64, ObjectError> {
        match self.call(ReplicaRequest::Ping)? {
            ReplicaReply::Pong { page_count } => Ok(page_count),
            other => Err(unexpected_reply("a ping", &other)),
        }
    }

    fn call(&mut self, request: ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        let Some((endpoints, options)) = self.catalog.clone() else {
            return match self.call_one(&request) {
                Ok(ReplicaReply::NotLeader) => Err(not_leader()),
                other => other,
            };
        };
        // Retire an idle socket before any bytes of the next request are sent.
        // This is connection maintenance, never a retry of an uncertain mutation.
        if self.last_exchange.elapsed() >= options.read_timeout.min(Duration::from_secs(5)) {
            self.connection_usable = false;
            let _ = self.writer.shutdown(Shutdown::Both);
        }
        let start = endpoints
            .iter()
            .position(|e| e == &self.endpoint)
            .unwrap_or(0);
        let start = (start + usize::from(!self.connection_usable)) % endpoints.len();
        let read_only = matches!(
            request,
            ReplicaRequest::Ping
                | ReplicaRequest::ReadChunk { .. }
                | ReplicaRequest::HeadChunk { .. }
                | ReplicaRequest::PageInventory { .. }
        );
        let mut last = not_leader();
        for offset in 0..endpoints.len() {
            let endpoint = &endpoints[(start + offset) % endpoints.len()];
            if !self.connection_usable || self.endpoint != *endpoint {
                match Self::connect(endpoint, &options) {
                    Ok(mut next) => {
                        if let Some(expected) = &self.group_identity {
                            let actual = match next.chunk_group_identity() {
                                Ok(identity) => identity,
                                Err(error) if !next.connection_usable => {
                                    last = error;
                                    continue;
                                }
                                Err(error) => return Err(error),
                            };
                            if !expected.same_group(&actual) {
                                return Err(ObjectError::PreconditionFailed(
                                    "chunk voter configuration does not match catalog".into(),
                                ));
                            }
                            next.group_identity = Some(expected.clone());
                        }
                        next.catalog = Some((endpoints.clone(), options.clone()));
                        *self = next;
                    }
                    Err(error) => {
                        last = error;
                        continue;
                    }
                }
            }
            match self.call_one(&request) {
                Ok(ReplicaReply::NotLeader) => last = not_leader(),
                Ok(reply) => return Ok(reply),
                Err(error) => {
                    // A decoded server refusal is authoritative. A transport failure
                    // after sending a mutation is uncertain, so neither is replayed.
                    if self.connection_usable || !read_only {
                        return Err(error);
                    }
                    last = error;
                }
            }
        }
        Err(last)
    }

    fn call_one(&mut self, request: &ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        if !self.connection_usable {
            return Err(transport_error(
                &self.endpoint,
                "reusing a failed connection",
                "reconnect before sending another request",
            ));
        }
        // A failed exchange can leave an unknown number of bytes in either direction.
        // Only a completely decoded reply makes this connection reusable.
        self.connection_usable = false;
        let result = self.exchange(request);
        self.last_exchange = Instant::now();
        if !self.connection_usable {
            let _ = self.writer.shutdown(Shutdown::Both);
        }
        result
    }

    fn exchange(&mut self, request: &ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        let code = request.code();
        let frame = encode_request(&request);
        write_before(
            &mut self.writer,
            &frame,
            exchange_deadline(self.write_timeout)?,
        )
        .map_err(|err| transport_error(&self.endpoint, "sending a request", &err.to_string()))?;
        let deadline = exchange_deadline(self.read_timeout)?;
        let mut header = [0u8; HEADER_BYTES];
        read_before(&mut self.reader, &mut header, deadline).map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading a response header",
                &err.to_string(),
            )
        })?;
        check_stamp(header[0])?;
        let status = StatusCode::from_u8(header[1]).ok_or_else(|| {
            ObjectError::Corruption(format!(
                "the replica at {} answered with status {}, which this build does not know",
                self.endpoint, header[1]
            ))
        })?;
        let payload_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
        if payload_len > self.max_payload_bytes {
            return Err(ObjectError::Corruption(format!(
                "the replica at {} declared a {payload_len}-byte response, past the \
                 {}-byte bound",
                self.endpoint, self.max_payload_bytes
            )));
        }
        let mut payload = vec![0u8; payload_len];
        read_before(&mut self.reader, &mut payload, deadline).map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading a response payload",
                &err.to_string(),
            )
        })?;
        if status == StatusCode::NotLeader {
            if !payload.is_empty() {
                return Err(ObjectError::Corruption(
                    "not-leader response must have an empty payload".into(),
                ));
            }
            self.connection_usable = true;
            return Ok(ReplicaReply::NotLeader);
        }
        if status == StatusCode::Ok {
            let reply = ReplicaReply::decode(code, &payload)?;
            self.connection_usable = true;
            return Ok(reply);
        }
        let message = decode_failure(&payload)?;
        self.connection_usable = true;
        Err(status.into_error(message))
    }
}

fn resolve(endpoint: &str) -> Result<SocketAddr, ObjectError> {
    let mut addresses = endpoint.to_socket_addrs().map_err(|err| {
        ObjectError::InvalidArgument(format!(
            "the replica endpoint {endpoint:?} could not be resolved: {err}"
        ))
    })?;
    addresses.next().ok_or_else(|| {
        ObjectError::InvalidArgument(format!(
            "the replica endpoint {endpoint:?} resolved to no address at all"
        ))
    })
}

/// A failure of the connection rather than of the store.
///
/// Always `Corruption`, and the endpoint and the step are in the message. A quorum read
/// counts `NotFound` as "the page is not here" and everything else as "this replica did not
/// answer", so putting a transport failure under any other variant would be a claim about
/// data that nobody looked at.
fn transport_error(endpoint: &str, doing: &str, detail: &str) -> ObjectError {
    ObjectError::Corruption(format!(
        "the replica at {endpoint} failed while {doing}: {detail}"
    ))
}

fn unexpected_reply(asked: &str, got: &ReplicaReply) -> ObjectError {
    ObjectError::Corruption(format!(
        "{asked} was answered with {got:?}, which does not belong to that request"
    ))
}

fn exchange_deadline(timeout: Duration) -> Result<Instant, ObjectError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| ObjectError::InvalidArgument("replica timeout exceeds clock range".into()))
}
fn time_left(deadline: Instant) -> std::io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "replica frame deadline"))
}
fn read_before(
    reader: &mut BufReader<TcpStream>,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        reader
            .get_ref()
            .set_read_timeout(Some(time_left(deadline)?))?;
        let count = reader.read(bytes)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete replica frame",
            ));
        }
        bytes = &mut bytes[count..];
    }
    Ok(())
}
fn write_before(
    writer: &mut TcpStream,
    mut bytes: &[u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        writer.set_write_timeout(Some(time_left(deadline)?))?;
        let count = writer.write(bytes)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "incomplete replica frame",
            ));
        }
        bytes = &bytes[count..];
    }
    Ok(())
}

fn not_leader() -> ObjectError {
    ObjectError::PreconditionFailed("chunk request was not admitted by a leader".into())
}
