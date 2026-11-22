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

#[cfg(test)]
#[path = "manifest_copy_client_tests.rs"]
mod manifest_copy_client_tests;

/// How long a read may be asked to wait before the client gives up on it.
pub const SHORT_READ: Duration = Duration::from_secs(30);

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
    reconnect_options: MetaClientOptions,
    group_identity: Option<super::MetaGroupIdentity>,
    read_timeout: Duration,
    write_timeout: Duration,
    catalog: Option<(Vec<String>, MetaClientOptions)>,
    wide_route_reads: Option<bool>,
    wide_route_limit: usize,
    read_protection_fence: u64,
    /// Whether the cluster has raised the read grace feature, and when that was last asked. Once
    /// raised it stays raised; an answer of no (or an error) is asked again after a while.
    read_grace: Option<(bool, Instant)>,
    /// How long a read may run and still go without a read lease (half the grace window).
    short_read: Duration,
    /// An allocation the caller vouches for: the metadata plane returned it for the next write
    /// moments ago, so that write does not read it back first. Used once.
    pub(super) confirmed_allocation: Option<u64>,
    #[cfg(feature = "replica-server")]
    pub(super) object_connections: Vec<super::object_client::CachedGroupConnection>,
    #[cfg(feature = "replica-server")]
    pub(super) object_page_lease: Option<super::object_client::ObjectPageLease>,
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
            reconnect_options: options.clone(),
            group_identity: None,
            read_timeout: options.read_timeout,
            write_timeout: options.write_timeout,
            catalog: None,
            wide_route_reads: None,
            wide_route_limit: 32,
            read_protection_fence: 0,
            read_grace: None,
            short_read: SHORT_READ,
            confirmed_allocation: None,
            #[cfg(feature = "replica-server")]
            object_connections: Vec::new(),
            #[cfg(feature = "replica-server")]
            object_page_lease: None,
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
    /// The cluster feature set as committed.
    pub fn read_cluster_features(&mut self) -> Result<crate::cluster_features::ClusterFeatures, ObjectError> {
        self.feature_call(super::FeaturesRequest::read())
    }
    /// Record the feature bits `voter` reads. A repeat of what is already recorded changes nothing.
    pub fn declare_voter_features(&mut self, voter: u64, bits: u64) -> Result<crate::cluster_features::ClusterFeatures, ObjectError> {
        self.feature_call(super::FeaturesRequest::declare(voter, bits))
    }
    /// Raise `bits`. Supply the sorted union of current voters, outgoing voters and learners.
    /// The metadata leader requires that exact reader set and support from every member.
    pub fn raise_cluster_features(&mut self, bits: u64, voters: Vec<u64>) -> Result<crate::cluster_features::ClusterFeatures, ObjectError> {
        self.feature_call(super::FeaturesRequest::raise(bits, voters))
    }
    fn feature_call(&mut self, request: super::FeaturesRequest) -> Result<crate::cluster_features::ClusterFeatures, ObjectError> {
        match self.call(MetaRequest::Features(request))? {
            MetaReply::Features(table) => table.to_features(),
            other => Err(unexpected_reply("a cluster feature set", &other)),
        }
    }
    /// Lease `bucket/key` for log writing to `holder` and, if no chunk is open, open one in `group`.
    pub fn grant_log_chunk(&mut self, bucket: &str, key: &str, holder: &str, ttl_ms: u64, group: &str) -> Result<super::LogChunkReply, ObjectError> {
        self.log_chunk_call(super::LogChunkRequest::grant(bucket, key, holder, ttl_ms, group))
    }
    /// Seal the chunk being written at its final length and footer and open the next one.
    #[allow(clippy::too_many_arguments)]
    pub fn switch_log_chunk(&mut self, bucket: &str, key: &str, holder: &str, epoch: u64, group: &str, sealed_length: u64, footer_crc: u32, footer_sha256: Vec<u8>) -> Result<super::LogChunkReply, ObjectError> {
        self.log_chunk_call(super::LogChunkRequest::switch(bucket, key, holder, epoch, group, sealed_length, footer_crc, footer_sha256))
    }
    /// Renew every lease `holder` names (ascending by object) in one commit.
    pub fn renew_log_leases(&mut self, holder: &str, ttl_ms: u64, renewals: Vec<crate::log_chunk_outcome::LeaseRef>) -> Result<super::LogChunkReply, ObjectError> {
        self.log_chunk_call(super::LogChunkRequest::renew(holder, ttl_ms, renewals))
    }
    /// The lease and the open chunk of one object, from the committed state.
    pub fn read_log_chunk(&mut self, bucket: &str, key: &str) -> Result<super::LogChunkReply, ObjectError> {
        self.log_chunk_call(super::LogChunkRequest::read(bucket, key))
    }
    /// Publish a chunk manifest: `outcome` is decided from the manifest this client read (its
    /// generation and the one it wants next). The reply is the manifest the catalog now holds, which
    /// is also the answer to a replay of an outcome that already applied. A generation that moved is
    /// refused, and the caller reads again and decides again.
    pub fn publish_chunk_manifest(&mut self, outcome: crate::chunk_catalog::ChunkOutcome) -> Result<crate::chunk_manifest::ChunkManifest, ObjectError> {
        match self.call(MetaRequest::PublishChunkManifest(outcome))? {
            MetaReply::ChunkManifest(manifest) => Ok(manifest),
            other => Err(unexpected_reply("a chunk manifest", &other)),
        }
    }
    /// The chunk manifest of `bucket/key` from the committed state (a read barrier, not a stale copy).
    /// An object that was never published is `NotFound`; a deleted one answers with its tombstone.
    pub fn read_chunk_manifest(&mut self, bucket: &str, key: &str) -> Result<crate::chunk_manifest::ChunkManifest, ObjectError> {
        match self.call(MetaRequest::ReadChunkManifest { bucket: bucket.to_string(), key: key.to_string() })? {
            MetaReply::ChunkManifest(manifest) => Ok(manifest),
            other => Err(unexpected_reply("a chunk manifest", &other)),
        }
    }
    /// One page of the chunk manifest catalog, in the shape `list_manifests` answers with.
    pub fn list_chunk_manifests(&mut self, request: crate::object_manifest::ManifestListRequest) -> Result<crate::object_manifest::ManifestListPage, ObjectError> {
        match self.call(MetaRequest::ListChunkManifests(request))? {
            MetaReply::Manifests(page) => Ok(page),
            other => Err(unexpected_reply("a chunk manifest page", &other)),
        }
    }
    fn log_chunk_call(&mut self, request: super::LogChunkRequest) -> Result<super::LogChunkReply, ObjectError> {
        match self.call(MetaRequest::LogChunk(request))? {
            MetaReply::LogChunk(reply) => Ok(reply),
            other => Err(unexpected_reply("a log chunk reply", &other)),
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

    pub(super) fn peer_has_closed(&self) -> Result<bool, ObjectError> {
        self.writer.peer_has_closed().map_err(|error| {
            transport_error(
                &self.endpoint,
                "checking an idle metadata connection",
                &error.to_string(),
            )
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
    /// Read the inventory and retained membership fence after metadata quorum confirmation.
    pub fn read_chunk_route(&mut self, placement_id: &str) -> Result<super::ChunkGroupRoute, ObjectError> {
        match self.call(MetaRequest::ReadChunkRoute { placement_id: placement_id.into() })? {
            MetaReply::ChunkRoute(route) if route.group.as_ref().is_some_and(|g| g.placement_id == placement_id) => Ok(route),
            _ => Err(ObjectError::Corruption("missing chunk route reply".into())),
        }
    }

    /// Resolve up to thirty-two distinct groups after one metadata read barrier.
    /// Older servers are remembered and use eight- or four-route confirmed reads.
    pub fn read_chunk_routes(
        &mut self,
        placement_ids: &[String],
    ) -> Result<Vec<super::ChunkGroupRoute>, ObjectError> {
        if placement_ids.is_empty() || placement_ids.len() > 32
            || placement_ids.iter().collect::<std::collections::BTreeSet<_>>().len()
                != placement_ids.len()
        {
            return Err(ObjectError::InvalidArgument("invalid chunk route batch".into()));
        }
        if placement_ids.len() == 1 {
            return self.read_chunk_route(&placement_ids[0]).map(|route| vec![route]);
        }
        if placement_ids.len() > self.wide_route_limit {
            return placement_ids
                .chunks(self.wide_route_limit)
                .map(|ids| self.read_chunk_routes(ids))
                .collect::<Result<Vec<_>, _>>()
                .map(|routes| routes.into_iter().flatten().collect());
        }
        if placement_ids.len() > 4 && self.wide_route_reads == Some(false) {
            return placement_ids
                .chunks(4)
                .map(|ids| self.read_chunk_routes(ids))
                .collect::<Result<Vec<_>, _>>()
                .map(|routes| routes.into_iter().flatten().collect());
        }
        let wide = placement_ids.len() > 4;
        let request = if wide {
            MetaRequest::ReadChunkRoutesWide {
                placement_ids: placement_ids.to_vec(),
            }
        } else {
            MetaRequest::ReadChunkRoutes {
                placement_ids: placement_ids.to_vec(),
            }
        };
        match self.call(request) {
            Ok(MetaReply::ChunkRoutes(batch)) => {
                if batch.routes.len() != placement_ids.len()
                    || batch.routes.iter().zip(placement_ids).any(|(route, id)| {
                        route.group.as_ref().is_none_or(|group| &group.placement_id != id)
                    })
                {
                    return Err(ObjectError::Corruption("chunk route batch names another group".into()));
                }
                if wide {
                    self.wide_route_reads = Some(true);
                }
                Ok(batch.routes)
            }
            Ok(_) => Err(ObjectError::Corruption("unexpected chunk route batch reply".into())),
            Err(ObjectError::PreconditionFailed(message)) if wide
                && message.starts_with("the replica does not implement this request:") =>
            {
                self.wide_route_reads = Some(false);
                placement_ids
                    .chunks(4)
                    .map(|ids| self.read_chunk_routes(ids))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|routes| routes.into_iter().flatten().collect())
            }
            Err(ObjectError::InvalidArgument(message))
                if wide
                    && placement_ids.len() > 8
                    && message == "invalid chunk route batch request" =>
            {
                self.wide_route_limit = 8;
                placement_ids
                    .chunks(8)
                    .map(|ids| self.read_chunk_routes(ids))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|routes| routes.into_iter().flatten().collect())
            }
            Err(ObjectError::PreconditionFailed(message))
                if message.starts_with("the replica does not implement this request:") =>
            {
                placement_ids.iter().map(|id| self.read_chunk_route(id)).collect()
            }
            Err(error) => Err(error),
        }
    }

    /// List a bounded, ordered page of routes after one metadata read barrier.
    /// Separate pages are independently confirmed, not one frozen snapshot.
    pub fn list_chunk_routes(
        &mut self,
        after: Option<&str>,
        limit: u32,
    ) -> Result<super::ChunkRoutePage, ObjectError> {
        let after = after.unwrap_or("");
        if !(1..=32).contains(&limit)
            || after.len() > 128
            || !after.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(ObjectError::InvalidArgument("invalid route listing cursor or limit".into()));
        }
        match self.call(MetaRequest::ListChunkRoutes { after: after.into(), limit })? {
            MetaReply::ChunkRoutePage(page)
                if page.routes.len() <= limit as usize
                    && (!page.has_more || page.routes.len() == limit as usize)
                    && page.routes.first().is_none_or(|route| {
                        route.group.as_ref().unwrap().placement_id.as_str() > after
                    }) => Ok(page),
            _ => Err(ObjectError::Corruption("route listing does not respect cursor or limit".into())),
        }
    }

    /// Refresh retained membership from a quorum-confirmed chunk membership read.
    /// Uses the current metadata route and checks every chunk socket's identity.
    /// A concurrent catalog change or uncertain commit is returned to the caller;
    /// this method does not replay mutations or initiate membership changes.
    #[cfg(feature = "replica-server")]
    pub fn refresh_chunk_membership(
        &mut self,
        placement_id: &str,
        options: &super::ReplicaClientOptions,
    ) -> Result<super::ChunkMembershipFence, ObjectError> {
        let route = self.read_chunk_route(placement_id)?;
        let previous = route.membership_fence()?.ok_or_else(|| {
            ObjectError::PreconditionFailed("fixed chunk groups have no membership fence".into())
        })?;
        let identity = route.group.as_ref().unwrap().identity.as_ref().unwrap();
        let mut chunk = super::RemoteReplicaClient::connect_routed_raft_catalog(&route, options)?;
        let status = chunk.chunk_membership(super::MembershipRequest {
            version: 1,
            bootstrap_id: identity.bootstrap_id,
            operation: super::MembershipOperation::Read as i32,
            node_id: 0,
        })?;
        if !route.active_members()?.iter().any(|(endpoint, id)| {
            endpoint == chunk.endpoint() && *id == status.node_id
        }) {
            return Err(ObjectError::Corruption("membership observation names another socket member".into()));
        }
        let next = previous.advance(identity, &status)?;
        if next == previous {
            return Ok(previous);
        }
        let reply = self.update_chunk_catalog(super::ChunkCatalogCommand {
            version: 1,
            operation: 1,
            placement_id: placement_id.into(),
            expected_fence: previous.to_bytes(),
            bootstrap_id: identity.bootstrap_id,
            status: Some(status),
            member_id: String::new(),
            expected_endpoint: String::new(),
            used_bytes: 0,
            capacity_bytes: 0,
            online: false,
        })?;
        if reply.node_id != placement_id {
            return Err(ObjectError::Corruption("membership refresh reply names another group".into()));
        }
        Ok(next)
    }

    /// Resolve metadata membership and verify the first chunk connection before data can be sent.
    #[cfg(feature = "replica-server")]
    pub fn connect_chunk_group(
        &mut self,
        placement_id: &str,
        options: &super::ReplicaClientOptions,
    ) -> Result<super::RemoteReplicaClient, ObjectError> {
        let route = self.read_chunk_route(placement_id)?;
        super::RemoteReplicaClient::connect_routed_raft_catalog(&route, options)
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

    /// Resolve a small placement and its group routes under one metadata read.
    pub fn plan_object_placement_routes(
        &mut self,
        bucket: &str,
        key: &str,
        length: u64,
        replica_count: u64,
    ) -> Result<(PlacementAssignment, Vec<super::ChunkGroupRoute>), ObjectError> {
        match self.call(MetaRequest::PlanObjectPlacementRoutes {
            bucket: bucket.into(), key: key.into(), length, replica_count,
        })? {
            MetaReply::PlanRoutes { plan, routes }
                if plan.bucket == bucket && plan.key == key && plan.length == length
                    && plan.node_ids.len() == replica_count as usize
                    && routes.len() == plan.node_ids.len() => Ok((plan, routes)),
            _ => Err(ObjectError::Corruption("unexpected placement routes reply".into())),
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

    /// How many page-granular objects the metadata plane holds that are not deleted, in any bucket.
    /// The reclaim sweep asks before every round: the pages of such an object are named by no chunk
    /// manifest, so the sweep would take them for garbage.
    pub fn count_page_objects(&mut self) -> Result<u64, ObjectError> {
        let mut after = String::new();
        let mut after_bucket = String::new();
        let mut live = 0u64;
        loop {
            let page = self.list_manifests_request(crate::object_manifest::ManifestListRequest {
                version: 3,
                bucket: String::new(),
                after: after.clone(),
                limit: 32,
                expected_revision: Vec::new(),
                after_bucket: after_bucket.clone(),
            })?;
            live += page.entries.iter().filter(|entry| !entry.deleted).count() as u64;
            match (page.has_more, page.entries.last()) {
                (true, Some(last)) => {
                    after = last.key.clone();
                    after_bucket = last.bucket.clone();
                }
                _ => return Ok(live),
            }
        }
    }

    fn list_manifests_request(
        &mut self, request: crate::object_manifest::ManifestListRequest,
    ) -> Result<crate::object_manifest::ManifestListPage, ObjectError> {
        request.validate()?;
        match self.call(MetaRequest::ListManifests(request.clone()))? {
            MetaReply::Manifests(page) if page.bucket == request.bucket && page.version == request.version
                && (request.expected_revision.is_empty() || page.revision == request.expected_revision)
                && page.entries.len() <= request.limit as usize
                && page.entries.iter().all(|entry| if request.version == 3 {
                    (&entry.bucket, &entry.key) > (&request.after_bucket, &request.after)
                } else { entry.key > request.after }) => Ok(page),
            _ => Err(ObjectError::Corruption("manifest listing reply does not match request".into())),
        }
    }

    fn call(&mut self, request: MetaRequest) -> Result<MetaReply, ObjectError> {
        self.try_call(request).into_result()
    }

    /// Make a call and say which of the three things happened.
    pub fn try_call(&mut self, request: MetaRequest) -> MetaCallOutcome {
        self.try_call_admission(&request).0
    }

    fn try_call_admission(&mut self, request: &MetaRequest) -> (MetaCallOutcome, bool) {
        // Detect a completed idle close before sending any application bytes.
        // A close racing with this check still follows normal uncertainty rules.
        if self.live && self.peer_has_closed().unwrap_or(true) {
            self.retire();
        }
        let Some((endpoints, options)) = self.catalog.clone() else {
            if !self.live {
                let mut options = self.reconnect_options.clone();
                options.deadline = self.deadline;
                let mut next = match Self::connect(&self.endpoint, &options) {
                    Ok(next) => next,
                    Err(error) => return (MetaCallOutcome::Unreachable(error), true),
                };
                if let Some(expected) = &self.group_identity {
                    match next.metadata_group_identity() {
                        Ok(actual) if expected.same_group(&actual) => {
                            next.group_identity = Some(expected.clone());
                        }
                        Ok(_) => return (MetaCallOutcome::Refused(ObjectError::PreconditionFailed(
                            "metadata voter configuration changed before reconnect".into(),
                        )), true),
                        Err(error) => return (MetaCallOutcome::Unreachable(error), true),
                    }
                }
                self.adopt_connection(next);
            }
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
                | MetaRequest::PlanObjectPlacementRoutes { .. }
                | MetaRequest::ReadManifest { .. }
                | MetaRequest::ReadManifestRoutes { .. }
                | MetaRequest::ReadManifestRoutesWide { .. }
                | MetaRequest::ReadManifestRoutesBatch { .. }
                | MetaRequest::ReadManifestRange { .. }
                | MetaRequest::PlanObjectWriteRoutes { .. }
                | MetaRequest::PlanObjectWriteRoutesWide { .. }
                | MetaRequest::PlanAppendRoutes(_)
                | MetaRequest::ReadObjectRootPlacement { .. }
                | MetaRequest::ReadChunkGroup { .. }
                | MetaRequest::ReadChunkRoute { .. }
                | MetaRequest::ReadChunkRoutes { .. }
                | MetaRequest::ReadChunkRoutesWide { .. }
                | MetaRequest::ListChunkRoutes { .. }
                | MetaRequest::ListManifests(_)
                | MetaRequest::ReadChunkManifest { .. }
                | MetaRequest::ListChunkManifests(_)
        ) || matches!(&request, MetaRequest::Membership(command) if command.is_read())
          || matches!(&request, MetaRequest::ManifestRetention(command) if command.is_read())
          || matches!(&request, MetaRequest::StagedAllocation(command) if command.is_read());
        let mut last = not_leader();
        let mut saw_all_refused = false;
        let mut unadmitted = true;
        let mut retry_delay = Duration::from_millis(25);
        // Control-plane mutations with a caller deadline may wait for an election
        // after every voter explicitly refuses admission. Object publication
        // and allocation return that refusal without another attempt.
        let control_retry = matches!(
            &request,
            MetaRequest::Membership(_)
                | MetaRequest::ChunkCatalog(_)
                | MetaRequest::ReportChunkNode { .. }
                | MetaRequest::HeartbeatChunkNode { .. }
                | MetaRequest::HeartbeatRootNode { .. }
                | MetaRequest::ExpireRootNodeHeartbeats { .. }
        );
        let retry_until = if read_only {
            Some(options.deadline.unwrap_or_else(|| {
                Instant::now() + options.read_timeout.min(Duration::from_secs(1))
            }))
        } else if control_retry {
            options.deadline
        } else {
            None
        };
        loop {
            let mut refusals = 0;
            let mut unreachable = 0;
            for offset in 0..endpoints.len() {
                let endpoint = &endpoints[(start + offset) % endpoints.len()];
                if !self.live || self.endpoint != *endpoint {
                    match Self::connect(endpoint, &options) {
                        Ok(mut next) => {
                            if let Some(expected) = &self.group_identity {
                                match next.exchange(&MetaRequest::GroupIdentity, MetaRequestCode::GroupIdentity) {
                                    Ok(Ok(MetaReply::GroupIdentity(actual))) if expected.same_group(&actual) => {
                                        next.group_identity = Some(expected.clone());
                                    }
                                    Ok(Ok(MetaReply::NotLeader)) => {
                                        // No application request was sent. A follower's typed
                                        // refusal does not end the search for a verified leader.
                                        last = not_leader();
                                        refusals += 1;
                                        continue;
                                    }
                                    Ok(Ok(_)) => {
                                        return (MetaCallOutcome::Refused(ObjectError::PreconditionFailed(
                                            "metadata voter configuration does not match catalog".into(),
                                        )), unadmitted);
                                    }
                                    Err(error) => {
                                        last = MetaCallOutcome::Unreachable(error);
                                        unreachable += 1;
                                        continue;
                                    }
                                    Ok(Err(error)) => return (MetaCallOutcome::Refused(error), unadmitted),
                                }
                            }
                            next.catalog = Some((endpoints.clone(), options.clone()));
                            self.adopt_connection(next);
                        }
                        Err(error) => {
                            last = MetaCallOutcome::Unreachable(error);
                            unreachable += 1;
                            continue;
                        }
                    }
                }
                match self.exchange(&request, request.code()) {
                    Ok(Ok(MetaReply::NotLeader)) => {
                        last = not_leader();
                        refusals += 1;
                    }
                    Ok(Ok(reply)) => return (MetaCallOutcome::Answered(reply), false),
                    Ok(Err(error)) => {
                        let proven = refusal_proves_not_admitted(&error);
                        return (MetaCallOutcome::Refused(error), proven);
                    }
                    Err(error) => {
                        unadmitted = false;
                        last = MetaCallOutcome::Unreachable(error);
                        if !read_only {
                            return (last, false);
                        }
                        unreachable += 1;
                    }
                }
            }
            // Reads can move past a voter that is still restarting. Mutations
            // retry only when every voter explicitly refused admission.
            saw_all_refused |= refusals == endpoints.len();
            let retryable = refusals == endpoints.len()
                || (read_only && refusals + unreachable == endpoints.len());
            if !retryable || !retry_until.is_some_and(|deadline|
                deadline.saturating_duration_since(Instant::now()) > retry_delay) {
                break;
            }
            std::thread::sleep(retry_delay);
            retry_delay = (retry_delay * 2).min(Duration::from_millis(250));
        }
        if read_only && saw_all_refused {
            (not_leader(), unadmitted)
        } else {
            (last, unadmitted)
        }
    }

    fn call_one(&mut self, request: &MetaRequest) -> MetaCallOutcome {
        self.call_one_admission(request).0
    }

    fn adopt_connection(&mut self, mut next: Self) {
        next.read_protection_fence = self.read_protection_fence;
        next.read_grace = self.read_grace.take();
        next.short_read = self.short_read;
        // Settings and short-lived memory of the caller outlive the connection they ran on.
        next.confirmed_allocation = self.confirmed_allocation.take();
        #[cfg(feature = "replica-server")]
        {
            // Cached data sockets are still checked against each fresh route.
            next.object_connections = std::mem::take(&mut self.object_connections);
            if self.group_identity.is_some() {
                next.object_page_lease = self.object_page_lease.take();
            }
        }
        *self = next;
    }

    fn call_one_admission(&mut self, request: &MetaRequest) -> (MetaCallOutcome, bool) {
        match self.exchange(request, request.code()) {
            Ok(Ok(MetaReply::NotLeader)) => (not_leader(), true),
            Ok(Ok(reply)) => (MetaCallOutcome::Answered(reply), false),
            Ok(Err(error)) => {
                let proven = refusal_proves_not_admitted(&error);
                (MetaCallOutcome::Refused(error), proven)
            }
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

/// Whether a refusal from the metaserver proves that the refused request was not admitted.
///
/// A refusal is an answer, not a lost connection, but it does not always prove the request
/// never committed: "write outcome may be committed" after a leadership change is a refusal
/// too. The refusals listed here are the ones decided before a request is proposed, because
/// the manifest or the allocation it names is no longer what the request was built on. That
/// is what a lost race between two appenders to one object looks like, and it is safe for the
/// caller to re-read and rebuild its request: nothing was published. The caller never sends
/// the same publication twice, so such a refusal cannot be an earlier copy's effect.
fn refusal_proves_not_admitted(error: &ObjectError) -> bool {
    match error {
        // The leader refused it while deciding it, before any proposal: nothing was published and
        // asking again cannot change the answer, so it is neither unconfirmed nor to be read back.
        ObjectError::InvalidArgument(_) => crate::object_manifest::catalog_full_refusal(error),
        ObjectError::PreconditionFailed(message) => [
            "object manifest changed before publication",
            "compact append prior manifest changed",
            "append plan prior manifest changed",
            "object changed before staging",
            "could not be applied after preparation; nothing was published",
        ]
        .iter()
        .any(|known| message.contains(known)),
        _ => false,
    }
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
    fn thirty_two_routes_use_one_confirmed_metadata_read() {
        use crate::cluster::{ChunkGroupConfig, ChunkGroupIdentity, ChunkGroupRoute};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let names: Vec<String> = (0..32).map(|i| format!("group-{i}")).collect();
        let expected = names.clone();
        let worker = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().unwrap();
            server.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut header = [0u8; HEADER_BYTES];
            server.read_exact(&mut header).unwrap();
            let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
            let mut payload = vec![0; length];
            server.read_exact(&mut payload).unwrap();
            let code = MetaRequestCode::from_u8(header[1]).unwrap();
            assert_eq!(code, MetaRequestCode::ReadChunkRoutesWide);
            assert!(matches!(MetaRequest::decode(code, &payload).unwrap(),
                MetaRequest::ReadChunkRoutesWide { placement_ids } if placement_ids == expected));
            let routes = expected.into_iter().enumerate().map(|(index, name)| ChunkGroupRoute {
                version: 1,
                fence: Vec::new(),
                group: Some(ChunkGroupConfig {
                    placement_id: name,
                    failure_domain: "local".into(),
                    identity: Some(ChunkGroupIdentity {
                        version: 1,
                        group_id: index as u64 + 1,
                        node_id: 1,
                        capacity_bytes: 4096,
                        max_pages: 16,
                        voters: vec![1],
                        peer_endpoints: vec![format!("127.0.0.1:{}", 19000 + index)],
                        bootstrap_voters: Vec::new(),
                        bootstrap_id: 0,
                    }),
                    api_endpoints: vec![format!("127.0.0.1:{}", 20000 + index)],
                    member_ids: vec![format!("member-{index}")],
                }),
            }).collect();
            server.write_all(&super::super::meta_message::encode_meta_reply(
                &MetaReply::ChunkRoutes(super::super::chunk_group_route::RouteBatch {
                    version: 1,
                    routes,
                }),
            )).unwrap();
        });
        let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let routes = client.read_chunk_routes(&names).unwrap();
        assert_eq!(routes.len(), 32);
        assert_eq!(client.wide_route_reads, Some(true));
        worker.join().unwrap();
    }

    #[test]
    fn thirty_two_routes_fall_back_to_eight_route_frames() {
        use crate::cluster::{ChunkGroupConfig, ChunkGroupIdentity, ChunkGroupRoute};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let names: Vec<String> = (0..32).map(|i| format!("group-{i}")).collect();
        let expected = names.clone();
        let worker = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().unwrap();
            server.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            for request_index in 0..9 {
                let mut header = [0u8; HEADER_BYTES];
                server.read_exact(&mut header).unwrap();
                let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                let mut payload = vec![0; length];
                server.read_exact(&mut payload).unwrap();
                let code = MetaRequestCode::from_u8(header[1]).unwrap();
                if request_index == 0 {
                    assert_eq!(code, MetaRequestCode::ReadChunkRoutesWide);
                    server.write_all(&super::super::message::encode_failure(
                        StatusCode::InvalidArgument,
                        "invalid chunk route batch request",
                    )).unwrap();
                    continue;
                }
                assert_eq!(code, MetaRequestCode::ReadChunkRoutesWide);
                let ids = match MetaRequest::decode(code, &payload).unwrap() {
                    MetaRequest::ReadChunkRoutesWide { placement_ids } => placement_ids,
                    request => panic!("unexpected request {request:?}"),
                };
                let group = (request_index - 1) % 4;
                assert_eq!(ids, expected[group * 8..group * 8 + 8]);
                let routes = ids.into_iter().enumerate().map(|(offset, name)| {
                    let index = group * 8 + offset;
                    ChunkGroupRoute {
                        version: 1,
                        fence: Vec::new(),
                        group: Some(ChunkGroupConfig {
                            placement_id: name,
                            failure_domain: "local".into(),
                            identity: Some(ChunkGroupIdentity {
                                version: 1,
                                group_id: index as u64 + 1,
                                node_id: 1,
                                capacity_bytes: 4096,
                                max_pages: 16,
                                voters: vec![1],
                                peer_endpoints: vec![format!("127.0.0.1:{}", 19000 + index)],
                                bootstrap_voters: Vec::new(),
                                bootstrap_id: 0,
                            }),
                            api_endpoints: vec![format!("127.0.0.1:{}", 20000 + index)],
                            member_ids: vec![format!("member-{index}")],
                        }),
                    }
                }).collect();
                server.write_all(&super::super::meta_message::encode_meta_reply(
                    &MetaReply::ChunkRoutes(super::super::chunk_group_route::RouteBatch {
                        version: 1,
                        routes,
                    }),
                )).unwrap();
            }
        });
        let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        assert_eq!(client.read_chunk_routes(&names).unwrap().len(), 32);
        assert_eq!(client.wide_route_reads, Some(true));
        assert_eq!(client.wide_route_limit, 8);
        assert_eq!(client.read_chunk_routes(&names).unwrap().len(), 32);
        worker.join().unwrap();
    }

    #[test]
    fn route_batch_falls_back_to_confirmed_reads_on_older_server() {
        use crate::cluster::{ChunkGroupConfig, ChunkGroupIdentity, ChunkGroupRoute};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let names = vec!["group-a".to_string(), "group-b".to_string()];
        let worker = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().unwrap();
            server.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            for index in 0..3 {
                let mut header = [0u8; HEADER_BYTES];
                server.read_exact(&mut header).unwrap();
                let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                let mut payload = vec![0; length];
                server.read_exact(&mut payload).unwrap();
                let code = MetaRequestCode::from_u8(header[1]).unwrap();
                let request = MetaRequest::decode(code, &payload).unwrap();
                if index == 0 {
                    assert!(matches!(request, MetaRequest::ReadChunkRoutes { placement_ids }
                        if placement_ids == ["group-a", "group-b"]));
                    server.write_all(&super::super::message::encode_failure(
                        StatusCode::UnknownRequest, "unknown route batch",
                    )).unwrap();
                    continue;
                }
                let name = format!("group-{}", if index == 1 { 'a' } else { 'b' });
                assert!(matches!(request, MetaRequest::ReadChunkRoute { placement_id }
                    if placement_id == name));
                let route = ChunkGroupRoute {
                    version: 1,
                    fence: Vec::new(),
                    group: Some(ChunkGroupConfig {
                        placement_id: name,
                        failure_domain: "local".into(),
                        identity: Some(ChunkGroupIdentity {
                            version: 1,
                            group_id: index as u64,
                            node_id: 1,
                            capacity_bytes: 4096,
                            max_pages: 16,
                            voters: vec![1],
                            peer_endpoints: vec![format!("127.0.0.1:{}", 19000 + index)],
                            bootstrap_voters: Vec::new(),
                            bootstrap_id: 0,
                        }),
                        api_endpoints: vec![format!("127.0.0.1:{}", 20000 + index)],
                        member_ids: vec![format!("member-{index}")],
                    }),
                };
                server.write_all(&super::super::meta_message::encode_meta_reply(
                    &MetaReply::ChunkRoute(route),
                )).unwrap();
            }
        });
        let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let routes = client.read_chunk_routes(&names).unwrap();
        assert_eq!(routes.iter().map(|route| route.group.as_ref().unwrap().placement_id.as_str())
            .collect::<Vec<_>>(), vec!["group-a", "group-b"]);
        worker.join().unwrap();
    }

    #[test]
    fn catalog_read_waits_briefly_for_a_new_leader_without_an_explicit_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let options = MetaClientOptions {
            read_timeout: Duration::from_millis(250),
            ..Default::default()
        };
        let mut client = RemoteMetaClient::connect_catalog(&[endpoint], &options).unwrap();
        let expected = encode_meta_request(&MetaRequest::ListRootNodes);
        let worker = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().unwrap();
            server.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            for reply in [MetaReply::NotLeader, MetaReply::RootNodes(Vec::new())] {
                let mut request = vec![0; expected.len()];
                server.read_exact(&mut request).unwrap();
                assert_eq!(request, expected);
                server
                    .write_all(&super::super::meta_message::encode_meta_reply(&reply))
                    .unwrap();
            }
        });
        assert!(client.list_placement_nodes().unwrap().is_empty());
        worker.join().unwrap();
    }

    #[test]
    fn idle_metadata_close_reconnects_before_allocation_without_replaying_uncertainty() {
        for catalog in [false, true] {
            for lose_reply in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let endpoint = listener.local_addr().unwrap().to_string();
                let options = MetaClientOptions {
                    read_timeout: Duration::from_secs(2),
                    ..Default::default()
                };
                let mut client = if catalog {
                    RemoteMetaClient::connect_catalog(&[endpoint], &options).unwrap()
                } else { RemoteMetaClient::connect(&endpoint, &options).unwrap() };
                let (idle, _) = listener.accept().unwrap();
                idle.shutdown(Shutdown::Both).unwrap();
                drop(idle);
                let deadline = Instant::now() + Duration::from_secs(2);
                while !client.peer_has_closed().unwrap() {
                    assert!(Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(1));
                }
                let worker = std::thread::spawn(move || {
                    let (mut server, _) = listener.accept().unwrap();
                    server.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                    let expected = encode_meta_request(&MetaRequest::AllocateObjectPages { page_count: 1 });
                    let mut bytes = vec![0; expected.len()];
                    server.read_exact(&mut bytes).unwrap();
                    assert_eq!(bytes, expected);
                    if !lose_reply {
                        server.write_all(&super::super::meta_message::encode_meta_reply(
                            &MetaReply::ObjectPages(crate::object_manifest::ObjectPageAllocation {
                                format_version: 1, allocation_id: 9, page_count: 1,
                            }),
                        )).unwrap();
                    }
                    drop(server);
                    listener
                });
                let result = client.allocate_object_pages(1);
                if lose_reply { assert!(result.is_err()); }
                else { assert_eq!(result.unwrap().allocation_id, 9); }
                let listener = worker.join().unwrap();
                listener.set_nonblocking(true).unwrap();
                assert!(matches!(listener.accept(), Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock));
            }
        }
    }

    #[test]
    fn catalog_read_retries_mixed_refusal_and_disconnection() {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoints = vec![
            first.local_addr().unwrap().to_string(),
            second.local_addr().unwrap().to_string(),
        ];
        let options = MetaClientOptions {
            read_timeout: Duration::from_millis(250),
            ..Default::default()
        };
        let mut client = RemoteMetaClient::connect_catalog(&endpoints, &options).unwrap();
        let expected = encode_meta_request(&MetaRequest::ListRootNodes);
        let first_worker = std::thread::spawn(move || {
            for reply in [MetaReply::NotLeader, MetaReply::RootNodes(Vec::new())] {
                let (mut server, _) = first.accept().unwrap();
                server.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
                let mut request = vec![0; expected.len()];
                server.read_exact(&mut request).unwrap();
                assert_eq!(request, expected);
                server
                    .write_all(&super::super::meta_message::encode_meta_reply(&reply))
                    .unwrap();
            }
        });
        let second_worker = std::thread::spawn(move || {
            let (server, _) = second.accept().unwrap();
            drop(server);
        });
        assert!(client.list_placement_nodes().unwrap().is_empty());
        first_worker.join().unwrap();
        second_worker.join().unwrap();
    }

    #[test]
    fn catalog_retries_only_explicit_non_admission_within_deadline() {
        for mode in 0..5 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap().to_string();
            let started = Instant::now();
            let options = MetaClientOptions {
                deadline: (mode != 1).then_some(started + Duration::from_secs(1)),
                ..Default::default()
            };
            let mut client = RemoteMetaClient::connect_catalog(&[endpoint], &options).unwrap();
            let request = MetaRequest::ExpireRootNodeHeartbeats { ttl_ms: 10, now_ms: 20 };
            let expected = encode_meta_request(&request);
            let worker = std::thread::spawn(move || {
                let (mut server, _) = listener.accept().unwrap();
                server.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                let mut calls = 0;
                let mut bytes = vec![0; expected.len()];
                while server.read_exact(&mut bytes).is_ok() {
                    assert_eq!(bytes, expected);
                    calls += 1;
                    if mode == 2 { break; }
                    if mode == 4 {
                        server.write_all(&super::super::message::encode_failure(
                            StatusCode::PreconditionFailed, "rejected mutation",
                        )).unwrap();
                        continue;
                    }
                    let reply = if mode == 0 && calls == 2 {
                        MetaReply::Expired { offline_count: 0 }
                    } else { MetaReply::NotLeader };
                    server.write_all(&super::super::meta_message::encode_meta_reply(&reply)).unwrap();
                    if mode == 0 && calls == 2 { break; }
                }
                calls
            });
            let result = client.try_call(request);
            assert_eq!(matches!(result, MetaCallOutcome::Answered(MetaReply::Expired { .. })), mode == 0);
            if mode == 2 { assert!(matches!(result, MetaCallOutcome::Unreachable(_))); }
            drop(client);
            let calls = worker.join().unwrap();
            match mode {
                0 => assert_eq!(calls, 2),
                1 | 2 | 4 => assert_eq!(calls, 1),
                _ => assert!((2..=8).contains(&calls), "unadmitted requests={calls}"),
            }
            assert!(started.elapsed() < Duration::from_secs(3));
        }
    }

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
    fn deadline_connections_refuse_dns_and_expired_budget_before_connect() {
        let options = MetaClientOptions { deadline: Some(Instant::now()), ..Default::default() };
        assert!(matches!(RemoteMetaClient::connect("localhost:80", &options), Err(ObjectError::InvalidArgument(_))));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(RemoteMetaClient::connect(&listener.local_addr().unwrap().to_string(), &options).is_err());
        assert!(matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
    }

}

#[cfg(test)]
mod digest_memo_tests {
    
    

}

#[cfg(test)]
#[path = "metadata_voter_probe_tests.rs"]
mod metadata_voter_probe_tests;
