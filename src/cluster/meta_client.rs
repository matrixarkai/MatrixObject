// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A metaserver connection with optional fixed-catalog leader routing.
//!
//! # A TRANSPORT FAILURE IS NOT A REFUSAL, AND THEY ARRIVE THE SAME WAY
//!
//! Every call here can fail two ways that look alike at the call site: the metaserver
//! answered and said no, or the metaserver was not there. A caller has to tell them apart --
//! "this object cannot be placed because every node is full" is a different operational fact
//! from "the control plane is down" -- and `Result<_, ObjectError>` alone does not carry the
//! difference.
//!
//! So a transport failure is marked: [`MetaCallOutcome`] separates `Refused` from
//! `Unreachable`, and [`RemoteMetaClient::try_call`] returns it. The convenience methods
//! return a plain `Result` for callers that do not care, and
//! [`crate::cluster::directory::MetaDirectory`] uses the marked form because deciding what a
//! store does without a control plane is exactly a decision about which of the two happened.
//!
//! # REACHABILITY IS READ FROM A ROUND TRIP, NEVER FROM A REMEMBERED VALUE
//!
//! There is deliberately no `last_known_good: bool` on this client, and no cached health.
//! A node that reports its own liveness from a value it last wrote down will go on reporting
//! it after it has stopped being true -- the comparison is against itself, so it can never
//! fire. [`RemoteMetaClient::probe`] therefore sends a frame and waits for the answer every
//! time it is called, and a caller that wants to know whether the control plane is up pays
//! for a round trip to find out. That is the cost of an answer that cannot be stale.

use std::io::{BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use crate::cluster::message::{check_stamp, decode_failure};
use crate::cluster::meta_message::{
    encode_meta_request, CommittedPlacement, MetaReply, MetaRequest, PlacementAssignment,
    PlacementNodeSummary,
};
use crate::cluster::meta_wire::MetaRequestCode;
use crate::cluster::wire::{StatusCode, HEADER_BYTES, MAX_PAYLOAD_BYTES};
use crate::ObjectError;

/// How a metaserver client is bounded.
#[derive(Clone, Debug)]
pub struct MetaClientOptions {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub max_payload_bytes: usize,
}

impl Default for MetaClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        }
    }
}

/// How a call to the metaserver turned out.
///
/// Three outcomes and not two. A caller deciding what to do without a control plane needs to
/// know whether there was one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaCallOutcome {
    /// The metaserver answered, and this is what it said.
    Answered(MetaReply),
    /// The metaserver answered, and it said no. Its own error, with its own message.
    Refused(ObjectError),
    /// The metaserver did not answer. The connection failed, or the process is not there.
    Unreachable(ObjectError),
}

impl MetaCallOutcome {
    /// The reply, with both failures collapsed into an error.
    ///
    /// For callers that do not distinguish the two. The ones that do should match on the
    /// outcome, which is why this is a method and not the only form available.
    pub fn into_result(self) -> Result<MetaReply, ObjectError> {
        match self {
            MetaCallOutcome::Answered(reply) => Ok(reply),
            MetaCallOutcome::Refused(err) | MetaCallOutcome::Unreachable(err) => Err(err),
        }
    }

    /// True when the metaserver could not be reached at all.
    pub fn is_unreachable(&self) -> bool {
        matches!(self, MetaCallOutcome::Unreachable(_))
    }
}

/// One active connection, optionally selected from a fixed metadata catalog.
#[derive(Debug)]
pub struct RemoteMetaClient {
    endpoint: String,
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    max_payload_bytes: usize,
    live: bool,
    read_timeout: Duration,
    write_timeout: Duration,
    catalog: Option<(Vec<String>, MetaClientOptions)>,
}

impl RemoteMetaClient {
    /// Open a connection to `endpoint` and keep it.
    ///
    /// Held for the life of the client rather than opened per call, partly so that a cluster
    /// has something an operator can look at: an established socket with a PID on each end
    /// is the observable form of "these processes are talking to each other".
    pub fn connect(endpoint: &str, options: &MetaClientOptions) -> Result<Self, ObjectError> {
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
            live: true,
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
            catalog: None,
        })
    }

    /// Connect to a fixed catalog of up to seven metadata API endpoints.
    /// Calls rotate on typed pre-admission refusals. Only reads may rotate after
    /// transport failure; a submitted mutation is never retried on uncertainty.
    pub fn connect_catalog(
        endpoints: &[String],
        options: &MetaClientOptions,
    ) -> Result<Self, ObjectError> {
        let unique: std::collections::BTreeSet<_> = endpoints.iter().collect();
        if endpoints.is_empty() || endpoints.len() > 7 || unique.len() != endpoints.len() {
            return Err(ObjectError::InvalidArgument(
                "metadata catalog requires one through seven distinct endpoints".into(),
            ));
        }
        let mut last = None;
        for endpoint in endpoints {
            match Self::connect(endpoint, options) {
                Ok(mut client) => {
                    client.catalog = Some((endpoints.to_vec(), options.clone()));
                    return Ok(client);
                }
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap())
    }

    /// The currently selected metadata API endpoint.
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

    /// The metaserver's end of the connection.
    pub fn peer_addr(&self) -> Result<SocketAddr, ObjectError> {
        self.writer.peer_addr().map_err(|err| {
            transport_error(&self.endpoint, "reading the peer address", &err.to_string())
        })
    }

    /// Send a frame and wait for the answer, RIGHT NOW.
    ///
    /// Named `probe` rather than `is_healthy` because it is not a property of this client. It
    /// is one round trip, made when it is called, and its answer is about the moment it
    /// returns and no later one.
    pub fn probe(&mut self) -> MetaCallOutcome {
        self.try_call(MetaRequest::Ping)
    }

    pub fn heartbeat_placement_node(
        &mut self,
        node_id: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        draining: bool,
        online: bool,
        now_ms: u64,
    ) -> Result<PlacementNodeSummary, ObjectError> {
        match self.call(MetaRequest::HeartbeatRootNode {
            node_id: node_id.to_string(),
            used_bytes,
            capacity_bytes,
            draining,
            online,
            now_ms,
        })? {
            MetaReply::PlacementNode(node) => Ok(node),
            other => Err(unexpected_reply("a node heartbeat", &other)),
        }
    }

    pub fn expire_placement_node_heartbeats(
        &mut self,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<u64, ObjectError> {
        match self.call(MetaRequest::ExpireRootNodeHeartbeats { ttl_ms, now_ms })? {
            MetaReply::Expired { offline_count } => Ok(offline_count),
            other => Err(unexpected_reply("a heartbeat expiry", &other)),
        }
    }

    pub fn list_placement_nodes(&mut self) -> Result<Vec<PlacementNodeSummary>, ObjectError> {
        match self.call(MetaRequest::ListRootNodes)? {
            MetaReply::RootNodes(nodes) => Ok(nodes),
            other => Err(unexpected_reply("a roster listing", &other)),
        }
    }

    pub fn plan_object_placement(
        &mut self,
        bucket: &str,
        key: &str,
        length: u64,
        replica_count: u64,
    ) -> Result<PlacementAssignment, ObjectError> {
        match self.call(MetaRequest::PlanObjectPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
            length,
            replica_count,
        })? {
            MetaReply::Plan(plan) => Ok(plan),
            other => Err(unexpected_reply("a placement plan", &other)),
        }
    }

    pub fn commit_object_placement(
        &mut self,
        placement: CommittedPlacement,
        expected_plan_checksum: &str,
    ) -> Result<CommittedPlacement, ObjectError> {
        match self.call(MetaRequest::CommitObjectPlacement {
            placement,
            expected_plan_checksum: expected_plan_checksum.to_string(),
        })? {
            MetaReply::Placement(placement) => Ok(placement),
            other => Err(unexpected_reply("a placement commit", &other)),
        }
    }

    pub fn object_placement(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<CommittedPlacement, ObjectError> {
        match self.call(MetaRequest::ReadObjectRootPlacement {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })? {
            MetaReply::Placement(placement) => Ok(placement),
            other => Err(unexpected_reply("a placement read", &other)),
        }
    }

    fn call(&mut self, request: MetaRequest) -> Result<MetaReply, ObjectError> {
        self.try_call(request).into_result()
    }

    /// Make a call and say which of the three things happened.
    pub fn try_call(&mut self, request: MetaRequest) -> MetaCallOutcome {
        let Some((endpoints, options)) = self.catalog.clone() else {
            return self.call_one(&request);
        };
        let start = endpoints
            .iter()
            .position(|e| e == &self.endpoint)
            .unwrap_or(0);
        let start = (start + usize::from(!self.live)) % endpoints.len();
        let read_only = matches!(
            request,
            MetaRequest::Ping
                | MetaRequest::ListRootNodes
                | MetaRequest::PlanObjectPlacement { .. }
                | MetaRequest::ReadObjectRootPlacement { .. }
        );
        let mut last = not_leader();
        for offset in 0..endpoints.len() {
            let endpoint = &endpoints[(start + offset) % endpoints.len()];
            if !self.live || self.endpoint != *endpoint {
                match Self::connect(endpoint, &options) {
                    Ok(mut next) => {
                        next.catalog = Some((endpoints.clone(), options.clone()));
                        *self = next;
                    }
                    Err(error) => {
                        last = MetaCallOutcome::Unreachable(error);
                        continue;
                    }
                }
            }
            match self.exchange(&request, request.code()) {
                Ok(Ok(MetaReply::NotLeader)) => last = not_leader(),
                Ok(Ok(reply)) => return MetaCallOutcome::Answered(reply),
                Ok(Err(error)) => return MetaCallOutcome::Refused(error),
                Err(error) => {
                    last = MetaCallOutcome::Unreachable(error);
                    if !read_only {
                        return last;
                    }
                }
            }
        }
        last
    }

    fn call_one(&mut self, request: &MetaRequest) -> MetaCallOutcome {
        match self.exchange(request, request.code()) {
            Ok(Ok(MetaReply::NotLeader)) => not_leader(),
            Ok(Ok(reply)) => MetaCallOutcome::Answered(reply),
            Ok(Err(error)) => MetaCallOutcome::Refused(error),
            Err(error) => MetaCallOutcome::Unreachable(error),
        }
    }

    fn retire(&mut self) {
        self.live = false;
        let _ = self.writer.shutdown(Shutdown::Both);
    }

    fn exchange(
        &mut self,
        request: &MetaRequest,
        code: MetaRequestCode,
    ) -> Result<Result<MetaReply, ObjectError>, ObjectError> {
        if !self.live {
            return Err(transport_error(
                &self.endpoint,
                "using a closed connection",
                "reconnect before another call",
            ));
        }
        let result = self.exchange_inner(request, code);
        if result.is_err() {
            self.retire();
        }
        result
    }

    /// `Err` means the connection failed; `Ok(Err(..))` means the metaserver said no.
    ///
    /// The two are separated here, at the one place that can tell them apart, rather than at
    /// the caller -- which cannot, because by then both are an `ObjectError`.
    #[allow(clippy::type_complexity)]
    fn exchange_inner(
        &mut self,
        request: &MetaRequest,
        code: MetaRequestCode,
    ) -> Result<Result<MetaReply, ObjectError>, ObjectError> {
        let frame = encode_meta_request(request);
        write_before(
            &mut self.writer,
            &frame,
            Instant::now()
                .checked_add(self.write_timeout)
                .ok_or_else(|| {
                    transport_error(
                        &self.endpoint,
                        "setting a deadline",
                        "write timeout exceeds clock range",
                    )
                })?,
        )
        .map_err(|err| transport_error(&self.endpoint, "sending a request", &err.to_string()))?;
        let deadline = Instant::now()
            .checked_add(self.read_timeout)
            .ok_or_else(|| {
                transport_error(
                    &self.endpoint,
                    "setting a deadline",
                    "read timeout exceeds clock range",
                )
            })?;
        let mut header = [0u8; HEADER_BYTES];
        read_before(&mut self.reader, &mut header, deadline).map_err(|err| {
            transport_error(
                &self.endpoint,
                "reading a response header",
                &err.to_string(),
            )
        })?;
        // A STAMP MISMATCH IS NOT UNREACHABILITY. The process answered; the two builds speak
        // different formats. Reported as a refusal so that a rolling restart does not look
        // like an outage.
        if let Err(err) = check_stamp(header[0]) {
            self.retire();
            return Ok(Err(err));
        }
        let Some(status) = StatusCode::from_u8(header[1]) else {
            self.retire();
            return Ok(Err(ObjectError::Corruption(format!(
                "the metaserver at {} answered with status {}, which this build does not know",
                self.endpoint, header[1]
            ))));
        };
        let payload_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
        if payload_len > self.max_payload_bytes {
            self.retire();
            return Ok(Err(ObjectError::Corruption(format!(
                "the metaserver at {} declared a {payload_len}-byte response, past the \
                 {}-byte bound",
                self.endpoint, self.max_payload_bytes
            ))));
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
                self.retire();
                return Ok(Err(ObjectError::Corruption(
                    "not-leader response must have an empty payload".into(),
                )));
            }
            return Ok(Ok(MetaReply::NotLeader));
        }
        if status == StatusCode::Ok {
            let reply = MetaReply::decode(code, &payload);
            if reply.is_err() {
                self.retire();
            }
            return Ok(reply);
        }
        Ok(Err(status.into_error(decode_failure(&payload)?)))
    }
}

fn resolve(endpoint: &str) -> Result<SocketAddr, ObjectError> {
    let mut addresses = endpoint.to_socket_addrs().map_err(|err| {
        ObjectError::InvalidArgument(format!(
            "the metaserver endpoint {endpoint:?} could not be resolved: {err}"
        ))
    })?;
    addresses.next().ok_or_else(|| {
        ObjectError::InvalidArgument(format!(
            "the metaserver endpoint {endpoint:?} resolved to no address at all"
        ))
    })
}

/// A failure of the connection rather than of the control plane.
///
/// `PreconditionFailed` and not `Corruption`: nothing about the store's contents is in
/// question when the metaserver cannot be reached, and the message says the control plane is
/// unreachable so that a caller reading only the text is not told its data is damaged.
fn transport_error(endpoint: &str, doing: &str, detail: &str) -> ObjectError {
    ObjectError::PreconditionFailed(format!(
        "the metaserver at {endpoint} could not be reached while {doing}: {detail}"
    ))
}

fn unexpected_reply(asked: &str, got: &MetaReply) -> ObjectError {
    ObjectError::Corruption(format!(
        "{asked} was answered with {got:?}, which does not belong to that request"
    ))
}

fn not_leader() -> MetaCallOutcome {
    MetaCallOutcome::Refused(ObjectError::PreconditionFailed(
        "metadata request was not admitted by a leader".into(),
    ))
}

fn time_left(deadline: Instant) -> std::io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "metadata frame deadline"))
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
                "incomplete metadata frame",
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
                "incomplete metadata frame",
            ));
        }
        bytes = &bytes[count..];
    }
    Ok(())
}
