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
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::cluster::message::{
    check_stamp, decode_failure, encode_request, ReplicaReply, ReplicaRequest,
};
use crate::cluster::wire::{StatusCode, HEADER_BYTES, MAX_PAYLOAD_BYTES};
use crate::{ObjectError, PageId, ReplicaHead, ReplicaReadResult};

/// How a client connects and how long it waits.
#[derive(Clone, Debug)]
pub struct ReplicaClientOptions {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
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

/// One connection to one replica node.
#[derive(Debug)]
pub struct RemoteReplicaClient {
    endpoint: String,
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    max_payload_bytes: usize,
}

impl RemoteReplicaClient {
    /// Open a connection to `endpoint` and keep it.
    ///
    /// The connection is held for the life of the client rather than opened per call. That
    /// is partly cost -- a handshake per replica per write is the dominant term in a fan-out
    /// of small writes -- and partly so that a cluster has something an operator can look at:
    /// an established socket with a PID on each end is the observable form of "these
    /// processes are talking to each other".
    pub fn connect(
        endpoint: &str,
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
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
        })
    }

    /// The endpoint this client was asked for, as written.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// This end of the connection.
    pub fn local_addr(&self) -> Result<SocketAddr, ObjectError> {
        self.writer
            .local_addr()
            .map_err(|err| transport_error(&self.endpoint, "reading the local address", &err.to_string()))
    }

    /// The replica's end of the connection.
    pub fn peer_addr(&self) -> Result<SocketAddr, ObjectError> {
        self.writer
            .peer_addr()
            .map_err(|err| transport_error(&self.endpoint, "reading the peer address", &err.to_string()))
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

    /// Ask whether the replica is answering, and how many pages it holds.
    pub fn ping(&mut self) -> Result<u64, ObjectError> {
        match self.call(ReplicaRequest::Ping)? {
            ReplicaReply::Pong { page_count } => Ok(page_count),
            other => Err(unexpected_reply("a ping", &other)),
        }
    }

    fn call(&mut self, request: ReplicaRequest) -> Result<ReplicaReply, ObjectError> {
        let code = request.code();
        let frame = encode_request(&request);
        self.writer
            .write_all(&frame)
            .map_err(|err| transport_error(&self.endpoint, "sending a request", &err.to_string()))?;
        self.writer
            .flush()
            .map_err(|err| transport_error(&self.endpoint, "flushing a request", &err.to_string()))?;
        let mut header = [0u8; HEADER_BYTES];
        self.reader.read_exact(&mut header).map_err(|err| {
            transport_error(&self.endpoint, "reading a response header", &err.to_string())
        })?;
        check_stamp(header[0])?;
        let status = StatusCode::from_u8(header[1]).ok_or_else(|| {
            ObjectError::Corruption(format!(
                "the replica at {} answered with status {}, which this build does not know",
                self.endpoint, header[1]
            ))
        })?;
        let payload_len =
            u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
        if payload_len > self.max_payload_bytes {
            return Err(ObjectError::Corruption(format!(
                "the replica at {} declared a {payload_len}-byte response, past the \
                 {}-byte bound",
                self.endpoint, self.max_payload_bytes
            )));
        }
        let mut payload = vec![0u8; payload_len];
        self.reader.read_exact(&mut payload).map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading a response payload",
                &err.to_string(),
            )
        })?;
        if status == StatusCode::Ok {
            return ReplicaReply::decode(code, &payload);
        }
        let message = decode_failure(&payload)?;
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
