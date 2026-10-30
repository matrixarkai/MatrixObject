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
    /// Optional preloaded mutual-TLS identity for API connections.
    pub tls: Option<super::ApiTlsConfig>,
    /// Optional absolute deadline shared by connection attempts and wire frames.
    /// Deadline-bound connections require numeric IP endpoints to avoid blocking DNS.
    pub deadline: Option<Instant>,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub max_payload_bytes: usize,
}

impl Default for MetaClientOptions {
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
    deadline: Option<Instant>,
    endpoint: String,
    reader: BufReader<super::api_tls::ApiSocket>,
    writer: super::api_tls::ApiSocket,
    max_payload_bytes: usize,
    live: bool,
    group_identity: Option<super::MetaGroupIdentity>,
    read_timeout: Duration,
    write_timeout: Duration,
    catalog: Option<(Vec<String>, MetaClientOptions)>,
}

#[cfg(any(feature = "replica-server", test))]
struct OperationDeadlineGuard<'a> {
    client: &'a mut RemoteMetaClient,
    previous: Option<Instant>,
    previous_catalog: Option<Instant>,
}
#[cfg(any(feature = "replica-server", test))]
impl Drop for OperationDeadlineGuard<'_> {
    fn drop(&mut self) {
        self.client.deadline = self.previous;
        if let Some((_, options)) = &mut self.client.catalog {
            options.deadline = self.previous_catalog;
        }
    }
}

impl RemoteMetaClient {
    /// Open a connection to `endpoint` and keep it.
    ///
    /// Held for the life of the client rather than opened per call, partly so that a cluster
    /// has something an operator can look at: an established socket with a PID on each end
    /// is the observable form of "these processes are talking to each other".
    pub fn connect(endpoint: &str, options: &MetaClientOptions) -> Result<Self, ObjectError> {
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
            reader: BufReader::new(stream),
            writer,
            max_payload_bytes: options.max_payload_bytes,
            live: true,
            group_identity: None,
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

    /// Connect to a Raft catalog and pin the first voter's complete configuration.
    /// The numeric group ID must match; every new connection is checked before use.
    pub fn connect_raft_catalog(
        endpoints: &[String],
        group_id: u64,
        options: &MetaClientOptions,
    ) -> Result<Self, ObjectError> {
        if group_id == 0 {
            return Err(ObjectError::InvalidArgument(
                "metadata group ID must be nonzero".into(),
            ));
        }
        let mut client = Self::connect_catalog(endpoints, options)?;
        let catalog = client.catalog.as_ref().unwrap().0.clone();
        let identity = loop {
            match client.metadata_group_identity() {
                Ok(identity) => break identity,
                Err(error) if !client.live => {
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
                "metadata group ID does not match catalog".into(),
            ));
        }
        client.group_identity = Some(identity);
        Ok(client)
    }
    /// Check even the first connection against a previously trusted configuration.
    /// This prevents configuration mistakes; it is not transport authentication.
    pub fn connect_verified_raft_catalog(
        endpoints: &[String],
        expected: &super::MetaGroupIdentity,
        options: &MetaClientOptions,
    ) -> Result<Self, ObjectError> {
        expected.validate()?;
        let client = Self::connect_raft_catalog(endpoints, expected.group_id, options)?;
        if !expected.same_group(client.group_identity.as_ref().unwrap()) {
            return Err(ObjectError::PreconditionFailed(
                "initial metadata configuration does not match catalog".into(),
            ));
        }
        Ok(client)
    }
    /// Commit a fenced chunk catalog observation or member health report.
    pub fn update_chunk_catalog(
        &mut self,
        command: super::ChunkCatalogCommand,
    ) -> Result<PlacementNodeSummary, ObjectError> {
        command.validate()?;
        match self.call(MetaRequest::ChunkCatalog(command))? {
            MetaReply::PlacementNode(node) => Ok(node),
            reply => Err(unexpected_reply("chunk catalog update", &reply)),
        }
    }

    /// Read quorum-confirmed membership or apply a committed membership change.
    pub fn metadata_membership(
        &mut self,
        request: super::MembershipRequest,
    ) -> Result<super::MembershipStatus, ObjectError> {
        request.validate()?;
        if let Some(identity) = &self.group_identity {
            if identity.version != 2 || identity.bootstrap_id != request.bootstrap_id {
                return Err(ObjectError::PreconditionFailed("membership request does not match trusted group".into()));
            }
        }
        match self.call(MetaRequest::Membership(request))? {
            MetaReply::Membership(status) => {
                if let Some(identity) = &self.group_identity {
                    status.validate_inventory(&identity.voters)?;
                }
                Ok(status)
            },
            reply => Err(unexpected_reply("metadata membership", &reply)),
        }
    }
    /// Describe this voter without leader routing or a read barrier.
    pub fn metadata_group_identity(&mut self) -> Result<super::MetaGroupIdentity, ObjectError> {
        match self.call_one(&MetaRequest::GroupIdentity).into_result()? {
            MetaReply::GroupIdentity(identity) => Ok(identity),
            other => Err(unexpected_reply("a metadata group identity", &other)),
        }
    }

    #[cfg(any(feature = "replica-server", test))]
    pub(super) fn with_operation_deadline<T>(
        &mut self,
        requested: Option<Instant>,
        operation: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = self.deadline;
        let previous_catalog = self.catalog.as_ref().and_then(|(_, options)| options.deadline);
        self.deadline = match (previous, requested) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        if let Some((_, options)) = &mut self.catalog {
            options.deadline = self.deadline;
        }
        let guard = OperationDeadlineGuard { client: self, previous, previous_catalog };
        operation(&mut *guard.client)
    }

    #[cfg(any(feature = "replica-server", test))]
    pub(super) fn operation_deadline(&self) -> Option<Instant> {
        self.deadline
    }

    #[cfg(any(feature = "replica-server", test))]
    pub(super) fn check_operation_deadline(&self) -> Result<(), ObjectError> {
        if let Some(deadline) = self.deadline {
            time_left(deadline).map_err(|error|
                transport_error(&self.endpoint, "checking object deadline", &error.to_string()))?;
        }
        Ok(())
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

    /// Report health with a timestamp assigned by the metadata leader.
    /// Requires metadata voters supporting request code 78; no legacy fallback.
    pub fn report_chunk_node(
        &mut self,
        node_id: &str,
        expected_endpoint: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        online: bool,
    ) -> Result<PlacementNodeSummary, ObjectError> {
        match self.call(MetaRequest::ReportChunkNode {
            node_id: node_id.into(),
            expected_endpoint: expected_endpoint.into(),
            used_bytes,
            capacity_bytes,
            online,
        })? {
            MetaReply::PlacementNode(node) => Ok(node),
            other => Err(unexpected_reply("a chunk health report", &other)),
        }
    }

    /// Report one chunk's health only while its endpoint matches metadata membership.
    pub fn heartbeat_chunk_node(
        &mut self,
        node_id: &str,
        expected_endpoint: &str,
        used_bytes: u64,
        capacity_bytes: u64,
        online: bool,
        now_ms: u64,
    ) -> Result<PlacementNodeSummary, ObjectError> {
        match self.call(MetaRequest::HeartbeatChunkNode {
            node_id: node_id.into(),
            expected_endpoint: expected_endpoint.into(),
            used_bytes,
            capacity_bytes,
            online,
            now_ms,
        })? {
            MetaReply::PlacementNode(node) => Ok(node),
            other => Err(unexpected_reply("a chunk heartbeat", &other)),
        }
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

    /// Resolve one persisted Raft chunk group through the metadata read path.
    pub fn read_chunk_group(
        &mut self,
        placement_id: &str,
    ) -> Result<super::ChunkGroupConfig, ObjectError> {
        match self.call(MetaRequest::ReadChunkGroup {
            placement_id: placement_id.into(),
        })? {
            MetaReply::ChunkGroup(group) if group.placement_id == placement_id => Ok(group),
            _ => Err(ObjectError::Corruption("missing chunk group reply".into())),
        }
    }
    /// Resolve metadata membership and verify the first chunk connection before data can be sent.
    #[cfg(feature = "replica-server")]
    pub fn connect_chunk_group(
        &mut self,
        placement_id: &str,
        options: &super::ReplicaClientOptions,
    ) -> Result<super::RemoteReplicaClient, ObjectError> {
        let group = self.read_chunk_group(placement_id)?;
        super::RemoteReplicaClient::connect_verified_raft_catalog(
            &group.api_endpoints,
            group.identity.as_ref().unwrap(),
            options,
        )
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

    /// Commit only if the complete current record matches, or is absent for None.
    /// This condition is independent of any data-node update.
    pub fn commit_object_placement_if_current(
        &mut self,
        placement: CommittedPlacement,
        expected: Option<CommittedPlacement>,
        expected_plan_checksum: &str,
    ) -> Result<CommittedPlacement, ObjectError> {
        match self.call(MetaRequest::CommitObjectPlacementIfCurrent {
            placement,
            expected,
            expected_plan_checksum: expected_plan_checksum.into(),
        })? {
            MetaReply::Placement(placement) => Ok(placement),
            other => Err(unexpected_reply("a conditional placement commit", &other)),
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

    /// Allocate a fresh page namespace. An uncertain response burns a namespace;
    /// never infer its ID from a later metadata index or reuse an older allocation.
    pub fn allocate_object_pages(
        &mut self,
        page_count: u32,
    ) -> Result<crate::object_manifest::ObjectPageAllocation, ObjectError> {
        if !(1..=4096).contains(&page_count) {
            return Err(ObjectError::InvalidArgument(
                "invalid object page count".into(),
            ));
        }
        match self.call(MetaRequest::AllocateObjectPages { page_count })? {
            MetaReply::ObjectPages(value) if value.page_count == page_count => Ok(value),
            _ => Err(ObjectError::Corruption(
                "unexpected object allocation reply".into(),
            )),
        }
    }

    pub fn read_manifest(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<crate::object_manifest::ObjectManifest, ObjectError> {
        match self.call(MetaRequest::ReadManifest {
            bucket: bucket.into(),
            key: key.into(),
        })? {
            MetaReply::Manifest(value) if value.bucket == bucket && value.key == key => Ok(value),
            _ => Err(ObjectError::Corruption("unexpected manifest reply".into())),
        }
    }
    /// Read one committed object generation through its configured data groups.
    /// The complete result is bounded by `max_bytes`; no bytes are returned until
    /// every extent matches its committed sequence, length and FNV-1a checksum.
    /// Writers must retain immutable pages while readers can still use them.
    #[cfg(feature = "replica-server")]
    pub fn read_object(
        &mut self,
        bucket: &str,
        key: &str,
        max_bytes: usize,
        options: &super::ReplicaClientOptions,
    ) -> Result<(crate::object_manifest::ObjectManifest, Vec<u8>), ObjectError> {
        self.with_operation_deadline(options.deadline, |client| {
            let mut bounded = options.clone();
            bounded.deadline = client.operation_deadline();
            client.read_object_inner(bucket, key, max_bytes, &bounded)
        })
    }

    #[cfg(feature = "replica-server")]
    fn read_object_inner(
        &mut self,
        bucket: &str,
        key: &str,
        max_bytes: usize,
        options: &super::ReplicaClientOptions,
    ) -> Result<(crate::object_manifest::ObjectManifest, Vec<u8>), ObjectError> {
        self.check_operation_deadline()?;
        let manifest = self.read_manifest(bucket, key)?;
        manifest.validate()?;
        if manifest.deleted {
            return Err(ObjectError::NotFound("object is deleted".into()));
        }
        let length = usize::try_from(manifest.length)
            .ok()
            .filter(|length| *length <= max_bytes)
            .ok_or_else(|| ObjectError::PreconditionFailed("object exceeds read limit".into()))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| {
            ObjectError::PreconditionFailed("object read allocation refused".into())
        })?;
        // Visit each group once, bounding both sockets and catalog lookups.
        // Manifest validation proves every destination range is in this buffer.
        bytes.resize(length, 0);
        let mut order: Vec<_> = manifest.extents.iter().collect();
        order.sort_by(|left, right| left.group.cmp(&right.group));
        let mut connection: Option<(String, super::RemoteReplicaClient)> = None;
        for extent in order {
            if connection.as_ref().map(|(group, _)| group) != Some(&extent.group) {
                connection = Some((
                    extent.group.clone(),
                    self.connect_chunk_group(&extent.group, options)?,
                ));
            }
            let page = crate::PageId {
                chunk_id: crate::ChunkId::new(extent.volume_id, extent.chunk_id)?,
                page_index: extent.page_index,
            };
            let read = connection.as_mut().unwrap().1.read_chunk(
                &page,
                extent.page_offset,
                extent.length,
            )?;
            let checksum = read.data.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            });
            if read.write_sequence != extent.write_sequence
                || read.data.len() as u64 != extent.length
                || checksum != extent.data_checksum
            {
                return Err(ObjectError::Corruption(
                    "object extent does not match manifest".into(),
                ));
            }
            let start = extent.object_offset as usize;
            bytes[start..start + read.data.len()].copy_from_slice(&read.data);
        }
        self.check_operation_deadline()?;
        Ok((manifest, bytes))
    }

    /// Publish only after immutable extent writes are confirmed durable. An uncertain
    /// response must be resolved by reading the manifest, not replaying the mutation.
    pub fn publish_manifest(
        &mut self,
        outcome: crate::object_manifest::ManifestOutcome,
        update_time_ms: u64,
    ) -> Result<crate::object_manifest::ObjectManifest, ObjectError> {
        self.publish_manifest_attempt(outcome, update_time_ms).0
    }

    /// The boolean proves that this publication was not admitted. It comes from
    /// routing state and the typed wire reply, never from an error message.
    pub(super) fn publish_manifest_attempt(
        &mut self,
        outcome: crate::object_manifest::ManifestOutcome,
        update_time_ms: u64,
    ) -> (
        Result<crate::object_manifest::ObjectManifest, ObjectError>,
        bool,
    ) {
        if let Err(error) = outcome.validate() {
            return (Err(error), true);
        }
        let expected = outcome.after.clone().unwrap();
        let (reply, unadmitted) = self.try_call_admission(MetaRequest::PublishManifest {
            outcome,
            update_time_ms,
        });
        let result = reply.into_result().and_then(|reply| match reply {
            MetaReply::Manifest(value) if value == expected => Ok(value),
            _ => Err(ObjectError::Corruption("unexpected manifest reply".into())),
        });
        (result, unadmitted)
    }

    fn call(&mut self, request: MetaRequest) -> Result<MetaReply, ObjectError> {
        self.try_call(request).into_result()
    }

    /// Make a call and say which of the three things happened.
    pub fn try_call(&mut self, request: MetaRequest) -> MetaCallOutcome {
        self.try_call_admission(request).0
    }

    fn try_call_admission(&mut self, request: MetaRequest) -> (MetaCallOutcome, bool) {
        let Some((endpoints, options)) = self.catalog.clone() else {
            return self.call_one_admission(&request);
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
                | MetaRequest::ReadManifest { .. }
                | MetaRequest::ReadObjectRootPlacement { .. }
                | MetaRequest::ReadChunkGroup { .. }
        ) || matches!(&request, MetaRequest::Membership(command) if command.is_read());
        let mut last = not_leader();
        let mut unadmitted = true;
        for offset in 0..endpoints.len() {
            let endpoint = &endpoints[(start + offset) % endpoints.len()];
            if !self.live || self.endpoint != *endpoint {
                match Self::connect(endpoint, &options) {
                    Ok(mut next) => {
                        if let Some(expected) = &self.group_identity {
                            match next.call_one(&MetaRequest::GroupIdentity) {
                                MetaCallOutcome::Answered(MetaReply::GroupIdentity(actual))
                                    if expected.same_group(&actual) =>
                                {
                                    next.group_identity = Some(expected.clone())
                                }
                                MetaCallOutcome::Answered(_) => {
                                    return (
                                        MetaCallOutcome::Refused(ObjectError::PreconditionFailed(
                                            "metadata voter configuration does not match catalog"
                                                .into(),
                                        )),
                                        unadmitted,
                                    )
                                }
                                MetaCallOutcome::Unreachable(error) => {
                                    last = MetaCallOutcome::Unreachable(error);
                                    continue;
                                }
                                outcome => return (outcome, unadmitted),
                            }
                        }
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
                Ok(Ok(reply)) => return (MetaCallOutcome::Answered(reply), false),
                Ok(Err(error)) => return (MetaCallOutcome::Refused(error), false),
                Err(error) => {
                    unadmitted = false;
                    last = MetaCallOutcome::Unreachable(error);
                    if !read_only {
                        return (last, false);
                    }
                }
            }
        }
        (last, unadmitted)
    }

    fn call_one(&mut self, request: &MetaRequest) -> MetaCallOutcome {
        self.call_one_admission(request).0
    }

    fn call_one_admission(&mut self, request: &MetaRequest) -> (MetaCallOutcome, bool) {
        match self.exchange(request, request.code()) {
            Ok(Ok(MetaReply::NotLeader)) => (not_leader(), true),
            Ok(Ok(reply)) => (MetaCallOutcome::Answered(reply), false),
            Ok(Err(error)) => (MetaCallOutcome::Refused(error), false),
            Err(error) => (MetaCallOutcome::Unreachable(error), false),
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
                .map(|deadline| self.deadline.map_or(deadline, |limit| limit.min(deadline)))
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
            .map(|deadline| self.deadline.map_or(deadline, |limit| limit.min(deadline)))
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
                "incomplete metadata frame",
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
                "incomplete metadata frame",
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

#[cfg(test)]
mod operation_deadline_tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn verified_metadata_membership_is_bound_to_the_trusted_inventory() {
        use crate::cluster::{MembershipRequest, MembershipStatus, MetaGroupIdentity};
        for field in 0..5 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut status = MembershipStatus { version: 1, node_id: 1, term: 2, applied_index: 9,
                voters: vec![1], outgoing_voters: vec![1, 2], learners: vec![] };
            match field { 1 => status.node_id = 3, 2 => status.voters = vec![3],
                3 => status.outgoing_voters = vec![3], 4 => status.learners = vec![3], _ => {} }
            let mut client = RemoteMetaClient::connect(&listener.local_addr().unwrap().to_string(), &MetaClientOptions::default()).unwrap();
            client.group_identity = Some(MetaGroupIdentity { version: 2, group_id: 42,
                node_id: 1, voters: vec![1, 2], peer_endpoints: vec!["127.0.0.1:18001".into(), "127.0.0.1:18002".into()],
                bootstrap_id: 91, bootstrap_voters: vec![1, 2] });
            let handle = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                let mut header = [0; 6];
                socket.read_exact(&mut header).unwrap();
                let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                assert!(length < 1024);
                let mut payload = vec![0; length];
                socket.read_exact(&mut payload).unwrap();
                assert_eq!(header[1], MetaRequestCode::Membership as u8);
                socket.write_all(&super::super::meta_message::encode_meta_reply(&MetaReply::Membership(status))).unwrap();
            });
            let mut request = MembershipRequest { version: 1, bootstrap_id: 92, operation: 0, node_id: 0 };
            assert!(client.metadata_membership(request.clone()).unwrap_err().to_string().contains("trusted group"));
            request.bootstrap_id = 91;
            let result = client.metadata_membership(request);
            assert_eq!(result.is_ok(), field == 0, "field={field}: {result:?}");
            if field != 0 { assert!(result.unwrap_err().to_string().contains("trusted inventory")); }
            handle.join().unwrap();
        }
    }

    #[test]
    fn operation_deadline_restores_catalog_after_unwind_and_uses_earliest_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let limit = Instant::now() + Duration::from_secs(10);
        let options = MetaClientOptions { deadline: Some(limit), ..Default::default() };
        let mut client = RemoteMetaClient::connect_catalog(&[listener.local_addr().unwrap().to_string()], &options).unwrap();
        let (_server, _) = listener.accept().unwrap();
        client.with_operation_deadline(Some(limit + Duration::from_secs(1)), |client| {
            assert_eq!(client.operation_deadline(), Some(limit));
        });
        let earlier = limit - Duration::from_secs(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            client.with_operation_deadline(Some(earlier), |client| {
                assert_eq!(client.operation_deadline(), Some(earlier));
                assert_eq!(client.catalog.as_ref().unwrap().1.deadline, Some(earlier));
                panic!("test scope restoration");
            });
        }));
        assert!(result.is_err());
        assert_eq!(client.operation_deadline(), Some(limit));
        assert_eq!(client.catalog.as_ref().unwrap().1.deadline, Some(limit));
    }

    #[test]
    fn operation_deadline_survives_catalog_rotation_without_new_budget() {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoints = vec![first.local_addr().unwrap().to_string(), second.local_addr().unwrap().to_string()];
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = first.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut request = [0; 6];
            stream.read_exact(&mut request).unwrap();
            std::thread::sleep(Duration::from_millis(200));
            stream.write_all(&super::super::meta_message::encode_meta_reply(&MetaReply::NotLeader)).unwrap();
            let (mut stream, _) = second.accept().unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            stream.read_exact(&mut request).unwrap();
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let mut client = RemoteMetaClient::connect_catalog(&endpoints, &MetaClientOptions::default()).unwrap();
        let started = Instant::now();
        let deadline = started + Duration::from_secs(1);
        client.with_operation_deadline(Some(deadline), |client| {
            assert!(matches!(client.try_call(MetaRequest::Ping), MetaCallOutcome::Unreachable(_)));
            assert_eq!(client.endpoint(), endpoints[1]);
            assert_eq!(client.operation_deadline(), Some(deadline));
            assert_eq!(client.catalog.as_ref().unwrap().1.deadline, Some(deadline));
        });
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(client.operation_deadline().is_none());
        assert!(client.catalog.as_ref().unwrap().1.deadline.is_none());
        worker.join().unwrap();
    }

    #[test]
    fn deadline_connections_refuse_dns_and_expired_budget_before_connect() {
        let options = MetaClientOptions { deadline: Some(Instant::now()), ..Default::default() };
        assert!(matches!(RemoteMetaClient::connect("localhost:80", &options), Err(ObjectError::InvalidArgument(_))));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(RemoteMetaClient::connect(&listener.local_addr().unwrap().to_string(), &options).is_err());
        assert!(matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
    }
}
