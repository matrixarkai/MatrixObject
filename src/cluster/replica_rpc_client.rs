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
    /// Optional preloaded mutual-TLS identity for API connections.
    pub tls: Option<super::ApiTlsConfig>,
    /// Optional absolute deadline shared by connection attempts and wire frames.
    /// Deadline-bound connections require numeric IP endpoints to avoid blocking DNS.
    pub deadline: Option<Instant>,
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
            tls: None,
            deadline: None,
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
    deadline: Option<Instant>,
    endpoint: String,
    peer: SocketAddr,
    reader: BufReader<super::api_tls::ApiSocket>,
    writer: super::api_tls::ApiSocket,
    max_payload_bytes: usize,
    connection_usable: bool,
    last_exchange: Instant,
    read_timeout: Duration,
    write_timeout: Duration,
    catalog: Option<(Vec<String>, ReplicaClientOptions)>,
    group_identity: Option<crate::cluster::ChunkGroupIdentity>,
}

// Owns an exclusive client borrow until its response is consumed or abandoned.
// Abandoning an incomplete response closes the socket before it can be reused.
pub(super) struct PendingReplicaReply<'a> {
    client: &'a mut RemoteReplicaClient,
    code: crate::cluster::wire::RequestCode,
    deadline: Instant,
    request: Option<std::sync::Arc<[u8]>>,
    request_written: usize,
    header: [u8; HEADER_BYTES],
    header_read: usize,
    payload: Option<(StatusCode, Vec<u8>)>,
    payload_read: usize,
}
impl PendingReplicaReply<'_> {
    pub(super) fn transferred_bytes(&self) -> usize {
        self.request_written + self.header_read + self.payload_read
    }

    fn send_pending(&mut self) -> Result<bool, ObjectError> {
        let Some(frame) = &self.request else { return Ok(true) };
        time_left(self.deadline).map_err(|error| {
            transport_error(&self.client.endpoint, "sending a fan-out request", &error.to_string())
        })?;
        if self.request_written < frame.len() {
            let end = frame.len().min(self.request_written + 64 * 1024);
            match self.client.writer.write(&frame[self.request_written..end]) {
                Ok(0) => return Err(transport_error(&self.client.endpoint, "sending a fan-out request", "zero-byte write")),
                Ok(count) => self.request_written += count,
                Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted) => return Ok(false),
                Err(error) => return Err(transport_error(&self.client.endpoint, "sending a fan-out request", &error.to_string())),
            }
        }
        if self.request_written != frame.len() {
            return Ok(false);
        }
        match self.client.writer.flush() {
            Ok(()) => {},
            Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted) => return Ok(false),
            Err(error) => return Err(transport_error(&self.client.endpoint, "flushing a fan-out request", &error.to_string())),
        }
        self.request = None;
        self.deadline = exchange_deadline(self.client.read_timeout, self.client.deadline)?;
        Ok(true)
    }
    pub(super) fn poll(&mut self) -> Option<Result<ReplicaReply, ObjectError>> {
        match self.poll_inner() {
            Ok(None) => None,
            Ok(Some(ReplicaReply::NotLeader)) => Some(Err(not_leader())),
            Ok(Some(reply)) => Some(Ok(reply)),
            Err(error) => Some(Err(error)),
        }
    }

    fn poll_inner(&mut self) -> Result<Option<ReplicaReply>, ObjectError> {
        if !self.send_pending()? {
            return Ok(None);
        }
        // Bound each member's work so fragmented or large replies cannot monopolize a sweep.
        let mut budget = 64 * 1024;
        loop {
            time_left(self.deadline).map_err(|error| {
                transport_error(&self.client.endpoint, "collecting a fan-out response", &error.to_string())
            })?;
            if self.header_read == HEADER_BYTES && self.payload.is_none() {
                let (status, length) = self.client.reply_header(&self.header)?;
                self.payload = Some((status, vec![0; length]));
            }
            if let Some((status, payload)) = &self.payload {
                if self.payload_read == payload.len() {
                    self.client.writer.set_nonblocking(false).map_err(|error| {
                        transport_error(&self.client.endpoint, "restoring response socket", &error.to_string())
                    })?;
                    return self.client.decode_reply(self.code, *status, payload).map(Some);
                }
            }
            if budget == 0 {
                return Ok(None);
            }
            let target = if self.header_read < HEADER_BYTES {
                &mut self.header[self.header_read..]
            } else {
                &mut self.payload.as_mut().unwrap().1[self.payload_read..]
            };
            let limit = target.len().min(budget);
            match self.client.reader.read(&mut target[..limit]) {
                Ok(0) => return Err(transport_error(&self.client.endpoint, "collecting a fan-out response", "incomplete frame")),
                Ok(count) => {
                    if self.header_read < HEADER_BYTES {
                        self.header_read += count;
                    } else {
                        self.payload_read += count;
                    }
                    budget -= count;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => return Ok(None),
                Err(error) => return Err(transport_error(&self.client.endpoint, "collecting a fan-out response", &error.to_string())),
            }
        }
    }

    #[cfg(test)]
    pub(super) fn finish(mut self) -> Result<ReplicaReply, ObjectError> {
        loop {
            if let Some(result) = self.poll() {
                return result;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
impl Drop for PendingReplicaReply<'_> {
    fn drop(&mut self) {
        self.client.last_exchange = Instant::now();
        if !self.client.connection_usable {
            let _ = self.client.writer.shutdown(Shutdown::Both);
        }
    }
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
        let address = if options.deadline.is_some() || options.tls.is_some() {
            endpoint.parse::<SocketAddr>().map_err(|_| ObjectError::InvalidArgument(
                "deadline-bound connections require a numeric IP endpoint".into()))?
        } else {
            resolve(endpoint)?
        };
        let connect_timeout = match options.deadline {
            Some(deadline) => options.connect_timeout.min(time_left(deadline)
                .map_err(|error| transport_error(endpoint, "connecting", &error.to_string()))?),
            None => options.connect_timeout,
        };
        let handshake_deadline = Instant::now().checked_add(connect_timeout)
            .ok_or_else(|| ObjectError::InvalidArgument("connection timeout exceeds clock range".into()))?;
        let stream = TcpStream::connect_timeout(&address, connect_timeout)
            .map_err(|err| transport_error(endpoint, "connecting", &err.to_string()))?;
        let stream = super::api_tls::ApiSocket::client(stream, address, options.tls.as_ref(), handshake_deadline)
            .map_err(|err| transport_error(endpoint, "authenticating API connection", &err.to_string()))?;
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
            deadline: options.deadline,
            endpoint: endpoint.to_string(),
            peer: address,
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

    // Star membership stays bound to its initially resolved socket. This runs
    // only before a later operation, never as a retry of the failed request.
    pub(super) fn reconnect_standalone_if_failed(
        &mut self,
        options: &ReplicaClientOptions,
    ) -> Result<(), ObjectError> {
        if self.connection_usable {
            return Ok(());
        }
        if self.catalog.is_some() || self.group_identity.is_some() {
            return Err(ObjectError::InvalidArgument(
                "standalone reconnect cannot replace a verified catalog".into(),
            ));
        }
        let mut next = Self::connect(&self.peer.to_string(), options)?;
        next.endpoint = self.endpoint.clone();
        *self = next;
        Ok(())
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

    /// Read bounded raw bytes at a complete page-state checksum, including discarded bytes.
    pub fn read_chunk_state_range(
        &mut self,
        page_id: &PageId,
        expected_state_checksum: &str,
        offset: u64,
        length: u64,
    ) -> Result<super::state_transfer::ChunkStateRange, ObjectError> {
        super::state_transfer::validate_request(expected_state_checksum, offset, length)?;
        match self.call(ReplicaRequest::ReadChunkStateRange {
            page_id: page_id.clone(),
            expected_state_checksum: expected_state_checksum.into(),
            offset,
            length,
        })? {
            ReplicaReply::StateRange(value) => {
                if value.offset != offset
                    || value.state_checksum != expected_state_checksum
                    || value.data.len() as u64
                        != length.min(value.data_length.saturating_sub(offset))
                {
                    return Err(ObjectError::Corruption(
                        "page state range differs from request".into(),
                    ));
                }
                Ok(value)
            }
            other => Err(unexpected_reply("a page state range", &other)),
        }
    }

    /// Assemble one validated page state through bounded, checksum-fenced ranges.
    /// Derived redundancy is omitted and may be rebuilt when installing the state.
    /// Concurrent page changes refuse the transfer rather than mixing versions.
    pub fn read_chunk_state(&mut self, page_id: &PageId) -> Result<crate::PageState, ObjectError> {
        let head = self.head_chunk(page_id)?;
        self.read_chunk_state_at(page_id, &head)
    }

    pub(super) fn read_chunk_state_at(
        &mut self,
        page_id: &PageId,
        head: &ReplicaHead,
    ) -> Result<crate::PageState, ObjectError> {
        let length = self
            .max_payload_bytes
            .saturating_sub(super::state_transfer::RANGE_OVERHEAD)
            .min(super::state_transfer::MAX_STATE_RANGE_BYTES) as u64;
        if length == 0 {
            return Err(ObjectError::InvalidArgument(
                "frame limit cannot hold a state range".into(),
            ));
        }
        let mut data = Vec::new();
        let mut total = None;
        loop {
            let value = self.read_chunk_state_range(
                page_id,
                &head.state_checksum,
                data.len() as u64,
                length,
            )?;
            if let Some(total) = total {
                if total != value.data_length {
                    return Err(ObjectError::Corruption(
                        "page state length changed during transfer".into(),
                    ));
                }
            } else {
                data.try_reserve_exact(value.data_length as usize)
                    .map_err(|_| {
                        ObjectError::PreconditionFailed("cannot reserve page state buffer".into())
                    })?;
                total = Some(value.data_length);
            }
            data.extend_from_slice(&value.data);
            if data.len() as u64 == value.data_length {
                break;
            }
        }
        let state = crate::PageState {
            logical_length: head.logical_length,
            write_sequence: head.write_sequence,
            checksum_fnv1a64: crate::fnv1a64(&data),
            data: std::sync::Arc::new(data),
            ec_parity_fragment: None,
            ec_fragment_digests: None,
            discarded_ranges: head.discarded_ranges.clone(),
        };
        crate::validate_chunk_state(&state)?;
        if state.checksum() != head.checksum
            || crate::chunk_state_handoff_checksum(page_id, &state, &state.checksum())
                != head.state_checksum
        {
            return Err(ObjectError::Corruption(
                "transferred page state checksum mismatch".into(),
            ));
        }
        Ok(state)
    }

    /// Execute one staged installation command without replaying uncertain writes.
    pub fn stage_chunk_state(
        &mut self,
        request: super::state_install::StateInstall,
    ) -> Result<(), ObjectError> {
        request.validate()?;
        match self.call(ReplicaRequest::StateInstall(request))? {
            ReplicaReply::Applied => Ok(()),
            other => Err(unexpected_reply(
                "a state installation acknowledgement",
                &other,
            )),
        }
    }

    /// Install validated page state on a standalone node using a caller-unique token.
    /// Empty destination means absence. An error may leave a stage or a committed
    /// installation; inspect the destination before deciding how to recover.
    /// Never automatically retries commands or falls back to unconditional writes.
    pub fn install_chunk_state(
        &mut self,
        page: &PageId,
        state: &crate::PageState,
        expected: Option<&str>,
        token: &str,
    ) -> Result<(), ObjectError> {
        crate::validate_chunk_state(state)?;
        let checksum = state.checksum();
        let head = ReplicaHead {
            logical_length: state.logical_length,
            write_sequence: state.write_sequence,
            checksum: checksum.clone(),
            state_checksum: crate::chunk_state_handoff_checksum(page, state, &checksum),
            discarded_ranges: state.discarded_ranges.clone(),
        };
        let size = self
            .max_payload_bytes
            .saturating_sub(512)
            .min(super::state_transfer::MAX_STATE_RANGE_BYTES);
        if size == 0 {
            return Err(ObjectError::InvalidArgument(
                "frame limit cannot hold install data".into(),
            ));
        }
        self.stage_chunk_state(super::state_install::StateInstall::begin(
            token.into(),
            page,
            expected,
            &head,
            state.data.len() as u64,
        ))?;
        for (index, data) in state.data.chunks(size).enumerate() {
            let mut request = super::state_install::StateInstall::command(2, token.into());
            request.offset = (index * size) as u64;
            request.data = data.to_vec();
            self.stage_chunk_state(request)?;
        }
        self.stage_chunk_state(super::state_install::StateInstall::command(3, token.into()))
    }

    pub fn chunk_membership(
        &mut self,
        request: super::MembershipRequest,
    ) -> Result<super::MembershipStatus, ObjectError> {
        request.validate()?;
        match self.call(ReplicaRequest::Membership(request))? {
            ReplicaReply::Membership(status) => Ok(status),
            reply => Err(unexpected_reply("chunk membership", &reply)),
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
                | ReplicaRequest::ReadChunkStateRange { .. }
                | ReplicaRequest::HeadChunk { .. }
                | ReplicaRequest::PageInventory { .. }
        ) || matches!(&request, ReplicaRequest::Membership(command) if command.is_read());
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

    #[cfg(test)]
    fn begin_standalone(
        &mut self,
        request: &ReplicaRequest,
    ) -> Result<PendingReplicaReply<'_>, ObjectError> {
        let mut pending = self.begin_encoded(request.code(), encode_request(request).into())?;
        pending.send_pending()?;
        Ok(pending)
    }

    // Every member shares one immutable frame and advances without blocking siblings.
    pub(super) fn begin_encoded(
        &mut self,
        code: crate::cluster::wire::RequestCode,
        frame: std::sync::Arc<[u8]>,
    ) -> Result<PendingReplicaReply<'_>, ObjectError> {
        if self.catalog.is_some() || !self.connection_usable {
            return Err(transport_error(
                &self.endpoint,
                "starting fan-out",
                "connection is unavailable or uses a catalog",
            ));
        }
        let deadline = exchange_deadline(self.write_timeout, self.deadline)?;
        self.connection_usable = false;
        if let Err(error) = self.writer.set_nonblocking(true) {
            let _ = self.writer.shutdown(Shutdown::Both);
            return Err(transport_error(&self.endpoint, "starting fan-out", &error.to_string()));
        }
        Ok(PendingReplicaReply {
            client: self,
            code,
            deadline,
            request: Some(frame),
            request_written: 0,
            header: [0; HEADER_BYTES],
            header_read: 0,
            payload: None,
            payload_read: 0,
        })
    }

    fn exchange(&mut self, request: &ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        self.send_request(request)?;
        self.read_reply(request.code(), exchange_deadline(self.read_timeout, self.deadline)?)
    }

    fn send_request(&mut self, request: &ReplicaRequest) -> Result<(), ObjectError> {
        let frame = encode_request(request);
        write_before(
            &mut self.writer,
            &frame,
            exchange_deadline(self.write_timeout, self.deadline)?,
        )
        .map_err(|err| transport_error(&self.endpoint, "sending a request", &err.to_string()))?;
        Ok(())
    }

    fn read_reply(
        &mut self,
        code: crate::cluster::wire::RequestCode,
        deadline: Instant,
    ) -> Result<ReplicaReply, ObjectError> {
        let mut header = [0u8; HEADER_BYTES];
        read_before(&mut self.reader, &mut header, deadline).map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading a response header",
                &err.to_string(),
            )
        })?;
        let (status, payload_len) = self.reply_header(&header)?;
        let mut payload = vec![0u8; payload_len];
        read_before(&mut self.reader, &mut payload, deadline).map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading a response payload",
                &err.to_string(),
            )
        })?;
        self.decode_reply(code, status, &payload)
    }

    fn reply_header(&self, header: &[u8; HEADER_BYTES]) -> Result<(StatusCode, usize), ObjectError> {
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
        Ok((status, payload_len))
    }

    fn decode_reply(&mut self, code: crate::cluster::wire::RequestCode, status: StatusCode, payload: &[u8]) -> Result<ReplicaReply, ObjectError> {
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
            let reply = ReplicaReply::decode(code, payload)?;
            self.connection_usable = true;
            return Ok(reply);
        }
        let message = decode_failure(payload)?;
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

fn exchange_deadline(timeout: Duration, absolute: Option<Instant>) -> Result<Instant, ObjectError> {
    Instant::now()
        .checked_add(timeout)
        .map(|deadline| absolute.map_or(deadline, |limit| limit.min(deadline)))
        .ok_or_else(|| ObjectError::InvalidArgument("replica timeout exceeds clock range".into()))
}
fn time_left(deadline: Instant) -> std::io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "replica frame deadline"))
}
fn read_before(
    reader: &mut BufReader<super::api_tls::ApiSocket>,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        reader
            .get_ref()
            .set_read_timeout(Some(time_left(deadline)?))?;
        let count = match reader.read(bytes) {
            Ok(count) => count,
            Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted) => continue,
            Err(error) => return Err(error),
        };
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
    writer: &mut super::api_tls::ApiSocket,
    mut bytes: &[u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        writer.set_write_timeout(Some(time_left(deadline)?))?;
        let count = match writer.write(bytes) {
            Ok(count) => count,
            Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted) => continue,
            Err(error) => return Err(error),
        };
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "incomplete replica frame",
            ));
        }
        bytes = &bytes[count..];
    }
    loop {
        writer.set_write_timeout(Some(time_left(deadline)?))?;
        match writer.flush() {
            Ok(()) => return Ok(()),
            Err(error) if matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted) => continue,
            Err(error) => return Err(error),
        }
    }
}

fn not_leader() -> ObjectError {
    ObjectError::PreconditionFailed("chunk request was not admitted by a leader".into())
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn absolute_deadline_bounds_fanout_and_refuses_dns() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let options = ReplicaClientOptions {
            deadline: Some(Instant::now() + Duration::from_millis(300)),
            ..Default::default()
        };
        assert!(matches!(RemoteReplicaClient::connect("localhost:80", &options), Err(ObjectError::InvalidArgument(_))));
        let mut client = RemoteReplicaClient::connect(&listener.local_addr().unwrap().to_string(), &options).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let request = ReplicaRequest::Ping;
        let started = Instant::now();
        let pending = client.begin_standalone(&request).unwrap();
        let mut sent = vec![0; encode_request(&request).len()];
        server.read_exact(&mut sent).unwrap();
        assert!(pending.finish().is_err());
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(!client.connection_usable);
        let mut byte = [0];
        assert_eq!(server.read(&mut byte).unwrap(), 0);
    }

    #[test]
    fn fanout_reply_deadline_starts_when_request_is_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = RemoteReplicaClient::connect(
            &listener.local_addr().unwrap().to_string(),
            &ReplicaClientOptions { read_timeout: Duration::from_millis(100), ..Default::default() },
        ).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let request = ReplicaRequest::Ping;
        let pending = client.begin_standalone(&request).unwrap();
        let mut sent = vec![0; encode_request(&request).len()];
        server.read_exact(&mut sent).unwrap();
        std::thread::sleep(Duration::from_millis(150));
        server.write_all(&crate::cluster::message::encode_reply(&ReplicaReply::Pong { page_count: 0 })).unwrap();
        assert!(pending.finish().is_err(), "collecting a late reply must not grant a new timeout");
        assert!(!client.connection_usable);
        assert!(client.begin_standalone(&request).is_err());
    }

    #[test]
    fn unfinished_or_malformed_fanout_reply_closes_socket_without_replaying() {
        for malformed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut client = RemoteReplicaClient::connect(
                &listener.local_addr().unwrap().to_string(),
                &ReplicaClientOptions::default(),
            )
            .unwrap();
            let (mut server, _) = listener.accept().unwrap();
            server
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let request = ReplicaRequest::Ping;
            let pending = client.begin_standalone(&request).unwrap();
            let expected = encode_request(&request);
            let mut sent = vec![0; expected.len()];
            server.read_exact(&mut sent).unwrap();
            assert_eq!(sent, expected);
            if malformed {
                server.write_all(&[255, 0, 0, 0, 0, 0]).unwrap();
                assert!(pending.finish().is_err());
            } else {
                drop(pending);
            }
            assert!(!client.connection_usable);
            assert!(client.begin_standalone(&request).is_err());
            let mut byte = [0];
            assert_eq!(server.read(&mut byte).unwrap(), 0);
        }
    }
}
