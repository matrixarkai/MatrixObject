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
use crate::cluster::message::{ConditionalChunkWriteRef, ReplicaReply, ReplicaRequest};
use crate::cluster::wire::RequestCode;
use crate::{ObjectError, PageId, ReplicaHead, ReplicaReadResult};

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

// Preserve sockets for replies already close behind the mutation quorum without
// adding a network-scale delay to every terminal star mutation.
const MUTATION_QUORUM_DRAIN: std::time::Duration = std::time::Duration::from_millis(5);
const LARGE_MUTATION_QUORUM_DRAIN: std::time::Duration =
    std::time::Duration::from_millis(25);
const LARGE_MUTATION_FRAME_BYTES: usize = 1024 * 1024;
const READ_QUORUM_HEDGE_DRAIN: std::time::Duration = std::time::Duration::from_millis(25);
const AUTHENTICATED_READ_HEDGE_BASE: std::time::Duration =
    std::time::Duration::from_millis(250);
const MAX_PARALLEL_RECONNECTS: usize = 8;
const ROUTED_RECONNECT_MIN_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
const ROUTED_RECONNECT_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
const CONDITIONAL_WRITE_FRAME_OVERHEAD: usize = 8;
const CONDITIONAL_WRITE_ENTRY_OVERHEAD: usize = 72;
const CONDITIONAL_WRITE_MAX_PAYLOAD_BYTES: usize =
    crate::cluster::wire::MAX_PAYLOAD_BYTES;

fn conditional_write_frame_count(
    group: &[(usize, ConditionalChunkWriteRef<'_>)],
    max_writes: usize,
    max_bytes: usize,
    frame_overhead: usize,
    entry_overhead: usize,
) -> usize {
    let mut frames = 0usize;
    let mut frame_writes = 0usize;
    let mut frame_bytes = frame_overhead;
    for (_, write) in group {
        let next_bytes = frame_bytes
            .saturating_add(entry_overhead)
            .saturating_add(write.data.len());
        if frame_writes == max_writes || (frame_writes > 0 && next_bytes > max_bytes) {
            frames += 1;
            frame_writes = 0;
            frame_bytes = frame_overhead;
        }
        frame_writes += 1;
        frame_bytes = frame_bytes
            .saturating_add(entry_overhead)
            .saturating_add(write.data.len());
    }
    frames + usize::from(frame_writes > 0)
}

fn authenticated_read_attempt_deadline(
    deadline: std::time::Instant,
    bytes: u64,
    has_fallback: bool,
) -> std::time::Instant {
    if !has_fallback {
        return deadline;
    }
    let hedge = AUTHENTICATED_READ_HEDGE_BASE
        .saturating_add(std::time::Duration::from_millis(bytes / (64 * 1024)));
    std::time::Instant::now()
        .checked_add(hedge)
        .unwrap_or(deadline)
        .min(deadline)
}

type ReconnectResult = (String, Result<RemoteReplicaClient, ObjectError>);

struct PendingRoutedReconnect {
    endpoints: BTreeSet<String>,
    options: ReplicaClientOptions,
    receiver: std::sync::mpsc::Receiver<ReconnectResult>,
}

impl std::fmt::Debug for PendingRoutedReconnect {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingRoutedReconnect")
            .field("endpoints", &self.endpoints)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct RoutedReconnect {
    unavailable_endpoints: Vec<String>,
    pending: Option<PendingRoutedReconnect>,
    retry_at: std::time::Instant,
    failures: u32,
}

impl RoutedReconnect {
    fn new(unavailable_endpoints: Vec<String>) -> Self {
        let failures = u32::from(!unavailable_endpoints.is_empty());
        let retry_at = std::time::Instant::now()
            .checked_add(if failures == 0 {
                std::time::Duration::ZERO
            } else {
                ROUTED_RECONNECT_MIN_DELAY
            })
            .unwrap_or_else(std::time::Instant::now);
        Self {
            unavailable_endpoints,
            pending: None,
            retry_at,
            failures,
        }
    }

    fn retry_later(&mut self) {
        self.failures = self.failures.saturating_add(1);
        let shift = self.failures.saturating_sub(1).min(7);
        let multiplier = 1u32 << shift;
        let delay = ROUTED_RECONNECT_MIN_DELAY
            .saturating_mul(multiplier)
            .min(ROUTED_RECONNECT_MAX_DELAY);
        self.retry_at = std::time::Instant::now()
            .checked_add(delay)
            .unwrap_or_else(std::time::Instant::now);
    }

    fn mark_complete(&mut self) {
        self.failures = 0;
        self.retry_at = std::time::Instant::now();
    }
}

fn run_reconnects_in_bounded_waves<T: Send, F: Fn(&mut T) + Sync>(
    mut items: Vec<&mut T>,
    operation: &F,
) {
    while !items.is_empty() {
        let width = items.len().min(MAX_PARALLEL_RECONNECTS);
        if width == 1 {
            operation(items.remove(0));
            continue;
        }
        let wave: Vec<_> = items.drain(..width).collect();
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(wave.len());
            for item in wave {
                if let Ok(handle) = std::thread::Builder::new()
                    .name("star-reconnect".into())
                    .spawn_scoped(scope, move || operation(item))
                {
                    handles.push(handle);
                }
            }
            for handle in handles {
                if let Err(payload) = handle.join() {
                    std::panic::resume_unwind(payload);
                }
            }
        });
    }
}

fn start_bounded_endpoint_jobs<T, F>(
    endpoints: Vec<String>,
    job: F,
) -> Result<(BTreeSet<String>, std::sync::mpsc::Receiver<(String, T)>), ObjectError>
where
    T: Send + 'static,
    F: Fn(&str) -> T + Send + Sync + 'static,
{
    let outstanding = endpoints.iter().cloned().collect();
    let worker_count = endpoints.len().min(MAX_PARALLEL_RECONNECTS);
    let queue = std::sync::Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
        endpoints,
    )));
    let job = std::sync::Arc::new(job);
    let (sender, receiver) = std::sync::mpsc::channel();
    let mut started = 0usize;
    for _ in 0..worker_count {
        let queue = queue.clone();
        let job = job.clone();
        let sender = sender.clone();
        if std::thread::Builder::new()
            .name("star-route-connect".into())
            .spawn(move || loop {
                let endpoint = match queue.lock() {
                    Ok(mut queue) => queue.pop_front(),
                    Err(_) => None,
                };
                let Some(endpoint) = endpoint else { break };
                let result = job(&endpoint);
                if sender.send((endpoint, result)).is_err() {
                    break;
                }
            })
            .is_ok()
        {
            started += 1;
        }
    }
    drop(sender);
    if worker_count > 0 && started == 0 {
        return Err(ObjectError::PreconditionFailed(
            "cannot start routed star connectors".into(),
        ));
    }
    Ok((outstanding, receiver))
}

#[cfg(unix)]
fn wait_for_pending(pending: &[Option<super::client::PendingReplicaReply<'_>>]) {
    use std::os::fd::AsRawFd;

    let mut deadline = None;
    let mut inline = [libc::pollfd {
        fd: -1,
        events: 0,
        revents: 0,
    }; 8];
    let mut overflow = Vec::new();
    let descriptors = if pending.len() <= inline.len() {
        &mut inline[..pending.len()]
    } else {
        overflow.resize(
            pending.len(),
            libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            },
        );
        overflow.as_mut_slice()
    };
    let mut active = 0usize;
    for reply in pending.iter().flatten() {
        let (socket, events) = reply.readiness();
        descriptors[active] = libc::pollfd {
            fd: socket.as_raw_fd(),
            events,
            revents: 0,
        };
        active += 1;
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
            active as libc::nfds_t,
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
    configured_endpoints: Vec<String>,
    routed_reconnect: Option<RoutedReconnect>,
    write_quorum: usize,
    read_quorum: usize,
    read_cursor: usize,
    state_conditional_write: Option<bool>,
    conditional_write_limits: Option<(usize, usize)>,
    read_batch_limits: Option<(usize, usize)>,
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
    pub(super) fn prepare_for_reuse(&mut self, options: &ReplicaClientOptions) -> bool {
        let same_tls = match (&self.options.tls, &options.tls) {
            (None, None) => true,
            (Some(left), Some(right)) => left.same_configuration(right),
            _ => false,
        };
        if !same_tls
            || self.options.connect_timeout != options.connect_timeout
            || self.options.read_timeout != options.read_timeout
            || self.options.write_timeout != options.write_timeout
            || self.options.max_payload_bytes != options.max_payload_bytes
        {
            return false;
        }
        if self
            .replicas
            .iter_mut()
            .filter(|replica| !replica.needs_standalone_reconnect())
            .any(|replica| !replica.prepare_direct_for_reuse(&self.options, options))
        {
            return false;
        }
        self.options = options.clone();
        true
    }

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
            configured_endpoints: endpoints.to_vec(),
            routed_reconnect: None,
            write_quorum,
            read_quorum,
            read_cursor: 0,
            state_conditional_write: None,
            conditional_write_limits: None,
            read_batch_limits: None,
            options: options.clone(),
        })
    }

    /// Connect a routed star group when its configured majority is available.
    ///
    /// The complete membership and its quorums remain fixed. Endpoints that are
    /// still connecting or unavailable remain in the membership. The call returns
    /// when the required quorum is connected while bounded workers finish optional
    /// members. A healthy quorum collects them beside later operations; a set below
    /// quorum waits only until enough members reconnect to serve the operation.
    pub(super) fn connect_routed_quorum(
        endpoints: &[String],
        write_quorum: usize,
        read_quorum: usize,
        options: &ReplicaClientOptions,
    ) -> Result<Self, ObjectError> {
        if endpoints.is_empty() {
            return Err(ObjectError::InvalidArgument(
                "a routed star group needs at least one endpoint".into(),
            ));
        }
        crate::cluster::placement::check_quorums(endpoints.len(), write_quorum, read_quorum)?;
        let mut addresses = BTreeSet::new();
        for endpoint in endpoints {
            let address = endpoint.parse::<SocketAddr>().map_err(|_| {
                ObjectError::InvalidArgument(
                    "routed star members require numeric IP endpoints".into(),
                )
            })?;
            if !addresses.insert(address) {
                return Err(ObjectError::InvalidArgument(format!(
                    "routed star endpoint {endpoint} is configured more than once"
                )));
            }
        }

        let required = write_quorum.max(read_quorum);
        let mut pending = Self::start_routed_connects(endpoints.to_vec(), options.clone())?;
        let mut connected = Vec::new();
        let mut unavailable = Vec::new();
        while connected.len() < required && !pending.endpoints.is_empty() {
            let (endpoint, result) = pending.receiver.recv().map_err(|_| {
                ObjectError::PreconditionFailed(
                    "routed star connectors stopped before reaching quorum".into(),
                )
            })?;
            pending.endpoints.remove(&endpoint);
            match result {
                Ok(replica) => connected.push(replica),
                Err(_) => unavailable.push(endpoint),
            }
        }
        if connected.len() < required {
            return Err(ObjectError::PreconditionFailed(format!(
                "routed star startup reached {} of {} configured members, short of the required quorum of {required}",
                connected.len(),
                endpoints.len()
            )));
        }
        connected.sort_by_key(|replica| {
            endpoints
                .iter()
                .position(|endpoint| endpoint == replica.endpoint())
                .unwrap_or(usize::MAX)
        });
        let mut routed_reconnect = RoutedReconnect::new(unavailable);
        if !pending.endpoints.is_empty() {
            routed_reconnect.pending = Some(pending);
        }
        Ok(Self {
            replicas: connected,
            configured_endpoints: endpoints.to_vec(),
            routed_reconnect: Some(routed_reconnect),
            write_quorum,
            read_quorum,
            read_cursor: 0,
            state_conditional_write: None,
            conditional_write_limits: None,
            read_batch_limits: None,
            options: options.clone(),
        })
    }

    fn start_routed_connects(
        endpoints: Vec<String>,
        options: ReplicaClientOptions,
    ) -> Result<PendingRoutedReconnect, ObjectError> {
        let worker_options = options.clone();
        let (endpoints, receiver) = start_bounded_endpoint_jobs(endpoints, move |endpoint| {
            RemoteReplicaClient::connect(endpoint, &worker_options)
        })?;
        Ok(PendingRoutedReconnect {
            endpoints,
            options,
            receiver,
        })
    }

    fn fan_out(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
    ) -> Vec<Result<ReplicaReply, ObjectError>> {
        self.fan_out_until_applied(request, selected, None)
    }

    fn fan_out_write_quorum(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
    ) -> Vec<Result<ReplicaReply, ObjectError>> {
        self.fan_out_until_applied(request, selected, Some(self.write_quorum))
    }

    fn fan_out_encoded_write_quorum(
        &mut self,
        frame: std::sync::Arc<Vec<u8>>,
        selected: Option<&[bool]>,
    ) -> Vec<Result<ReplicaReply, ObjectError>> {
        self.fan_out_encoded_until_applied(
            RequestCode::WriteChunkBatchIfSequence,
            frame,
            selected,
            Some(self.write_quorum),
        )
    }

    fn supports_state_conditional_write(&mut self) -> bool {
        if let Some(supported) = self.state_conditional_write {
            return supported;
        }
        let replica_count = self.replicas.len();
        let replies = self.fan_out(&ReplicaRequest::Capabilities, None);
        let supported = replies.len() == replica_count
            && replies.into_iter().all(|reply| {
                matches!(
                    reply,
                    Ok(ReplicaReply::Capabilities {
                        state_conditional_write: true
                    })
                )
            });
        self.state_conditional_write = Some(supported);
        supported
    }

    fn conditional_write_limits(&mut self) -> (usize, usize) {
        if let Some(limits) = self.conditional_write_limits {
            return limits;
        }
        let replica_count = self.replicas.len();
        let max_payload_bytes = self
            .options
            .max_payload_bytes
            .min(CONDITIONAL_WRITE_MAX_PAYLOAD_BYTES);
        let fold = |replies: Vec<Result<ReplicaReply, ObjectError>>| {
            (replies.len() == replica_count)
                .then_some(replies)?
                .into_iter()
                .try_fold(
                    (32usize, max_payload_bytes),
                    |limits, reply| match reply {
                        Ok(ReplicaReply::ConditionalWriteLimits {
                            max_writes,
                            max_payload_bytes,
                        }) => Some((
                            limits.0.min(max_writes as usize),
                            limits.1.min(max_payload_bytes as usize),
                        )),
                        _ => None,
                    },
                )
        };
        let wide_replies = self.fan_out(&ReplicaRequest::WideConditionalWriteLimits, None);
        let limits = match fold(wide_replies) {
            Some(limits) => limits,
            None => {
                let compatible_replies =
                    self.fan_out(&ReplicaRequest::ConditionalWriteLimits, None);
                fold(compatible_replies).unwrap_or((8, 4 * 1024 * 1024))
            }
        };
        self.conditional_write_limits = Some(limits);
        limits
    }

    fn read_batch_limits(&mut self) -> (usize, usize) {
        if let Some(limits) = self.read_batch_limits {
            return limits;
        }
        let replica_count = self.replicas.len();
        let replies = self.fan_out(&ReplicaRequest::ReadBatchLimits, None);
        let limits = if replies.len() == replica_count {
            replies
                .into_iter()
                .try_fold((32usize, self.options.max_payload_bytes), |limits, reply| {
                    match reply {
                        Ok(ReplicaReply::ReadBatchLimits {
                            max_reads,
                            max_reply_payload_bytes,
                        }) => Some((
                            limits.0.min(max_reads as usize),
                            limits.1.min(max_reply_payload_bytes as usize),
                        )),
                        _ => None,
                    }
                })
                .unwrap_or((8, 4 * 1024 * 1024))
        } else {
            (8, 4 * 1024 * 1024)
        };
        self.read_batch_limits = Some(limits);
        limits
    }

    fn fan_out_until_applied(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
        return_after_applied: Option<usize>,
    ) -> Vec<Result<ReplicaReply, ObjectError>> {
        self.fan_out_encoded_until_applied(
            request.code(),
            std::sync::Arc::new(super::message::encode_request(request)),
            selected,
            return_after_applied,
        )
    }

    fn fan_out_encoded_until_applied(
        &mut self,
        code: RequestCode,
        frame: std::sync::Arc<Vec<u8>>,
        selected: Option<&[bool]>,
        return_after_applied: Option<usize>,
    ) -> Vec<Result<ReplicaReply, ObjectError>> {
        let reply_count = self
            .replicas
            .iter()
            .enumerate()
            .filter(|(index, _)| selected.map_or(true, |members| members[*index]))
            .count();
        let mut replies: Vec<_> = (0..reply_count).map(|_| None).collect();
        self.fan_out_each_encoded_until_applied(
            code,
            frame,
            selected,
            return_after_applied,
            None,
            None,
            |index, result| replies[index] = Some(result),
        );
        replies.into_iter().flatten().collect()
    }

    fn fan_out_each_before<F>(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
        deadline: std::time::Instant,
        return_after_replies: Option<usize>,
        on_reply: F,
    ) where
        F: FnMut(usize, Result<ReplicaReply, ObjectError>),
    {
        self.fan_out_each_until_applied(
            request,
            selected,
            None,
            Some(deadline),
            return_after_replies,
            on_reply,
        );
    }

    fn fan_out_each_until_applied<F>(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
        return_after_applied: Option<usize>,
        operation_deadline: Option<std::time::Instant>,
        return_after_replies: Option<usize>,
        on_reply: F,
    ) where
        F: FnMut(usize, Result<ReplicaReply, ObjectError>),
    {
        self.fan_out_each_encoded_until_applied(
            request.code(),
            std::sync::Arc::new(super::message::encode_request(request)),
            selected,
            return_after_applied,
            operation_deadline,
            return_after_replies,
            on_reply,
        );
    }

    fn fan_out_each_encoded_until_applied<F>(
        &mut self,
        code: RequestCode,
        frame: std::sync::Arc<Vec<u8>>,
        selected: Option<&[bool]>,
        return_after_applied: Option<usize>,
        operation_deadline: Option<std::time::Instant>,
        return_after_replies: Option<usize>,
        mut on_reply: F,
    ) where
        F: FnMut(usize, Result<ReplicaReply, ObjectError>),
    {
        // One shared encoded frame and one pending exchange per selected member.
        // No worker threads or mutation retries. A terminal mutation may return at
        // quorum; dropping an incomplete exchange retires that member's socket.
        let mutation_drain = if frame.len() >= LARGE_MUTATION_FRAME_BYTES {
            LARGE_MUTATION_QUORUM_DRAIN
        } else {
            MUTATION_QUORUM_DRAIN
        };
        let selected_count = selected.map_or(self.replicas.len(), |members| {
            members.iter().filter(|member| **member).count()
        });
        let mut pending = Vec::with_capacity(selected_count);
        for (_, replica) in self
            .replicas
            .iter_mut()
            .enumerate()
            .filter(|(index, _)| selected.map_or(true, |members| members[*index]))
        {
            let index = pending.len();
            match replica.begin_encoded_before(code, frame.clone(), operation_deadline) {
                Ok(reply) => pending.push(Some(reply)),
                Err(error) => {
                    on_reply(index, Err(error));
                    pending.push(None);
                }
            }
        }
        let mut applied_replies = 0usize;
        let mut terminal_replies = 0usize;
        let mut quorum_reached_at = None;
        let mut hedge_reached_at = None;
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
                    terminal_replies += 1;
                    if matches!(&result, Ok(ReplicaReply::Applied)) {
                        applied_replies += 1;
                    }
                    on_reply(index, result);
                    // Drop retires an incomplete frame before the connection can be reused.
                    *slot = None;
                }
            }
            if return_after_applied.is_some_and(|required| applied_replies >= required) {
                let reached_at = quorum_reached_at.get_or_insert_with(std::time::Instant::now);
                let remaining = mutation_drain.saturating_sub(reached_at.elapsed());
                if remaining.is_zero() {
                    break;
                }
                if !progressed {
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
                }
                continue;
            }
            if return_after_replies.is_some_and(|required| terminal_replies >= required) {
                let reached_at = hedge_reached_at.get_or_insert_with(std::time::Instant::now);
                let remaining = READ_QUORUM_HEDGE_DRAIN.saturating_sub(reached_at.elapsed());
                if remaining.is_zero() {
                    break;
                }
                if !progressed {
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
                }
                continue;
            }
            if !progressed && pending.iter().any(Option::is_some) {
                wait_for_pending(&pending);
            }
        }
    }

    fn reconnect_failed(&mut self) {
        let reset_capabilities = self
            .replicas
            .iter()
            .any(RemoteReplicaClient::needs_standalone_reconnect);
        let connected_before = self.replicas.len();
        if self.routed_reconnect.is_some() {
            self.reconnect_routed();
            if reset_capabilities || self.replicas.len() != connected_before {
                self.state_conditional_write = None;
                self.conditional_write_limits = None;
                self.read_batch_limits = None;
            }
            return;
        }
        let options = self.options.clone();
        let failed: Vec<_> = self
            .replicas
            .iter_mut()
            .filter(|replica| replica.needs_standalone_reconnect())
            .collect();
        run_reconnects_in_bounded_waves(failed, &|replica| {
            let _ = replica.reconnect_standalone_if_failed(&options);
        });
        if reset_capabilities {
            self.state_conditional_write = None;
            self.conditional_write_limits = None;
            self.read_batch_limits = None;
        }
    }

    fn reconnect_routed(&mut self) {
        // Optional members must not put their connect timeout on the quorum
        // read/write path. Only a set that cannot currently reach quorum waits.
        self.retire_failed_routed_connections();
        let required = self.write_quorum.max(self.read_quorum);
        self.collect_routed_reconnect(self.replicas.len() < required);
        let Some(state) = self.routed_reconnect.as_ref() else {
            return;
        };
        if state.pending.is_some()
            || state.unavailable_endpoints.is_empty()
            || std::time::Instant::now() < state.retry_at
        {
            return;
        }
        if self.replicas.len() < required {
            self.reconnect_routed_now();
        } else {
            self.start_routed_reconnect();
        }
    }

    fn retire_failed_routed_connections(&mut self) {
        let state = self.routed_reconnect.as_mut().unwrap();
        let mut index = 0;
        while index < self.replicas.len() {
            if !self.replicas[index].needs_standalone_reconnect() {
                index += 1;
                continue;
            }
            let endpoint = self.replicas.remove(index).endpoint().to_string();
            if !state.unavailable_endpoints.contains(&endpoint) {
                state.unavailable_endpoints.push(endpoint);
            }
            state.retry_at = std::time::Instant::now();
        }
    }

    fn collect_routed_reconnect(&mut self, wait: bool) {
        let mut pending = self
            .routed_reconnect
            .as_mut()
            .and_then(|state| state.pending.take());
        let Some(mut pending) = pending.take() else {
            return;
        };
        let required = self.write_quorum.max(self.read_quorum);
        loop {
            let next = if wait && self.replicas.len() < required {
                match pending.receiver.recv() {
                    Ok(result) => result,
                    Err(_) => {
                        self.finish_disconnected_routed_connect(pending);
                        return;
                    }
                }
            } else {
                match pending.receiver.try_recv() {
                    Ok(result) => result,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        self.routed_reconnect.as_mut().unwrap().pending = Some(pending);
                        return;
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        self.finish_disconnected_routed_connect(pending);
                        return;
                    }
                }
            };
            pending.endpoints.remove(&next.0);
            self.apply_routed_reconnect_result(next, &pending.options);
            if pending.endpoints.is_empty() {
                let state = self.routed_reconnect.as_mut().unwrap();
                if state.unavailable_endpoints.is_empty() {
                    state.mark_complete();
                } else {
                    state.retry_later();
                }
                return;
            }
        }
    }

    // A routed client may start with only a connected quorum. If that quorum
    // disagrees on a page, wait for the remaining configured member up to the
    // read deadline before declaring the page unreadable.
    fn collect_routed_reconnect_before(&mut self, deadline: std::time::Instant) {
        let Some(state) = self.routed_reconnect.as_ref() else {
            return;
        };
        if state.pending.is_none() && !state.unavailable_endpoints.is_empty() {
            self.start_routed_reconnect();
        }
        let Some(mut pending) = self.routed_reconnect.as_mut().unwrap().pending.take() else {
            return;
        };
        let connected_before = self.replicas.len();
        while self.replicas.len() == connected_before && !pending.endpoints.is_empty() {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let next = match pending.receiver.recv_timeout(remaining) {
                Ok(next) => next,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    self.routed_reconnect.as_mut().unwrap().pending = Some(pending);
                    return;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    self.finish_disconnected_routed_connect(pending);
                    return;
                }
            };
            pending.endpoints.remove(&next.0);
            self.apply_routed_reconnect_result(next, &pending.options);
        }
        if pending.endpoints.is_empty() {
            let state = self.routed_reconnect.as_mut().unwrap();
            if state.unavailable_endpoints.is_empty() {
                state.mark_complete();
            } else {
                state.retry_later();
            }
        } else {
            self.routed_reconnect.as_mut().unwrap().pending = Some(pending);
        }
    }

    fn apply_routed_reconnect_result(
        &mut self,
        (endpoint, result): ReconnectResult,
        connected_options: &ReplicaClientOptions,
    ) {
        if let Ok(mut replica) = result {
            if replica.prepare_direct_for_reuse(connected_options, &self.options) {
                self.replicas.push(replica);
                self.replicas.sort_by_key(|replica| {
                    self.configured_endpoints
                        .iter()
                        .position(|endpoint| endpoint == replica.endpoint())
                        .unwrap_or(usize::MAX)
                });
                return;
            }
        }
        let state = self.routed_reconnect.as_mut().unwrap();
        if !state.unavailable_endpoints.contains(&endpoint) {
            state.unavailable_endpoints.push(endpoint);
        }
    }

    fn finish_disconnected_routed_connect(&mut self, pending: PendingRoutedReconnect) {
        let state = self.routed_reconnect.as_mut().unwrap();
        for endpoint in pending.endpoints {
            if !state.unavailable_endpoints.contains(&endpoint) {
                state.unavailable_endpoints.push(endpoint);
            }
        }
        state.retry_later();
    }

    fn reconnect_routed_now(&mut self) {
        self.start_routed_reconnect();
        self.collect_routed_reconnect(true);
    }

    fn start_routed_reconnect(&mut self) {
        let endpoints = std::mem::take(
            &mut self
                .routed_reconnect
                .as_mut()
                .unwrap()
                .unavailable_endpoints,
        );
        let options = self.options.clone();
        match Self::start_routed_connects(endpoints.clone(), options) {
            Ok(pending) => self.routed_reconnect.as_mut().unwrap().pending = Some(pending),
            Err(_) => {
                let state = self.routed_reconnect.as_mut().unwrap();
                state.unavailable_endpoints.extend(endpoints);
                state.retry_later();
            }
        }
    }

    pub fn replica_count(&self) -> usize {
        self.configured_endpoints.len()
    }

    pub fn write_quorum(&self) -> usize {
        self.write_quorum
    }

    pub fn read_quorum(&self) -> usize {
        self.read_quorum
    }

    fn next_read_waves(&mut self, quorum: usize) -> (Vec<bool>, Vec<bool>) {
        let member_count = self.replicas.len();
        let mut primary = vec![false; member_count];
        if member_count == 0 {
            return (primary.clone(), primary);
        }
        let primary_count = quorum.min(member_count);
        let start = self.read_cursor % member_count;
        for step in 0..primary_count {
            primary[(start + step) % member_count] = true;
        }
        self.read_cursor = (start + primary_count) % member_count;
        let fallback = primary.iter().map(|selected| !selected).collect();
        (primary, fallback)
    }

    fn read_wave_deadline(&self) -> Result<std::time::Instant, ObjectError> {
        std::time::Instant::now()
            .checked_add(self.options.read_timeout)
            .map(|deadline| {
                self.options
                    .deadline
                    .map_or(deadline, |limit| limit.min(deadline))
            })
            .ok_or_else(|| {
                ObjectError::InvalidArgument("replica read timeout exceeds clock range".into())
            })
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
    /// Returns how many acknowledgements were observed before return, which is always at least
    /// `write_quorum` when this returns `Ok`. A slower member may still apply a fully received
    /// request after the coordinator retires its incomplete reply socket, so this count is not
    /// a final membership-wide application census.
    pub fn write_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, ObjectError> {
        self.reconnect_failed();
        if self.supports_state_conditional_write() {
            let preflight_quorum = self.read_quorum.max(self.write_quorum);
            let expected = match self.head_chunk_with_quorum(page_id, preflight_quorum) {
                Ok(head) => Some(head),
                Err(ObjectError::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
            let request = ReplicaRequest::WriteChunkIfState {
                page_id: page_id.clone(),
                offset,
                data: data.to_vec(),
                expected_sequence: expected.as_ref().map_or(0, |head| head.write_sequence),
                expected_state_checksum: expected
                    .as_ref()
                    .map_or_else(String::new, |head| head.state_checksum.clone()),
            };
            return self.apply_page_write(&request, None);
        }
        let (expected, observed) = self.mutation_state(page_id)?;
        let sequence = expected.as_ref().map_or(0, |head| head.write_sequence);
        let request = ReplicaRequest::WriteChunkIfSequence {
            page_id: page_id.clone(),
            offset,
            data: data.to_vec(),
            expected_sequence: sequence,
        };
        self.apply_page_write(&request, Some(&observed))
    }

    fn apply_page_write(
        &mut self,
        request: &ReplicaRequest,
        selected: Option<&[bool]>,
    ) -> Result<usize, ObjectError> {
        let mut acknowledged = 0usize;
        let mut first_error: Option<ObjectError> = None;
        for result in self.fan_out_write_quorum(request, selected) {
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

    /// Apply up to thirty-two ordered writes through the standalone star path.
    ///
    /// Every distinct page is fenced against a quorum-agreed head before any
    /// mutation is sent. Repeated writes to one page form an ordered sequence
    /// chain in the same encoded batch. Pages with the same matching member set
    /// share conditional mutation frames, reducing head requests, mutation frames
    /// and journal syncs without letting a divergent member vote for a write. Each
    /// frame completes at write quorum so a slow optional member cannot delay the
    /// remaining frames.
    /// Frames carry at most four MiB of page data. An error can follow an
    /// acknowledged earlier frame; callers must probe candidate pages instead of
    /// replaying an uncertain batch.
    pub fn write_chunks(
        &mut self,
        writes: &[(PageId, u64, &[u8])],
    ) -> Result<Vec<usize>, ObjectError> {
        self.write_chunks_inner(
            writes
                .iter()
                .map(|(page, offset, data)| (page, *offset, *data, None)),
            false,
        )
    }

    /// Apply ordered writes only when each selected quorum state has the caller's
    /// sequence. Repeated writes to one page must provide consecutive sequences.
    pub fn write_chunks_if_sequence(
        &mut self,
        writes: &[(PageId, u64, &[u8], u64)],
    ) -> Result<Vec<usize>, ObjectError> {
        self.write_chunks_inner(
            writes
                .iter()
                .map(|(page, offset, data, sequence)| (page, *offset, *data, Some(*sequence))),
            true,
        )
    }

    fn write_chunks_inner<'a>(
        &mut self,
        writes: impl Clone + ExactSizeIterator<Item = (&'a PageId, u64, &'a [u8], Option<u64>)>,
        conditional: bool,
    ) -> Result<Vec<usize>, ObjectError> {
        if writes.len() > 32 {
            return Err(ObjectError::InvalidArgument(
                "star write batch exceeds thirty-two writes".into(),
            ));
        }
        if writes.len() == 0 {
            return Ok(Vec::new());
        }
        if writes.clone().any(|(_, _, data, _)| data.len() > 1024 * 1024) {
            return Err(ObjectError::InvalidArgument(
                "star write page exceeds one MiB".into(),
            ));
        }
        // A successful conditional insert from a write quorum is also proof that
        // the quorum held the absent state. Fresh whole pages therefore need no
        // separate head exchange. Send to every connected member concurrently and
        // return after a durable quorum; an optional member may finish later or
        // require repair. Repeated, partial, or nonzero-sequence writes keep the
        // exact head vote because equal sequences can hold different bytes.
        let mut fresh_pages = BTreeSet::new();
        let fresh_whole_pages = conditional
            && writes.clone().all(|(page, offset, _, sequence)| {
                sequence == Some(0) && offset == 0 && fresh_pages.insert(page.clone())
            });
        if fresh_whole_pages {
            self.reconnect_failed();
            let group = writes
                .clone()
                .enumerate()
                .map(|(index, (page_id, offset, data, _))| {
                    (
                        index,
                        ConditionalChunkWriteRef {
                            page_id,
                            offset,
                            data,
                            expected_sequence: 0,
                        },
                    )
                })
                .collect();
            let mut acknowledgements = vec![0; writes.len()];
            self.apply_conditional_write_group(group, None, &mut acknowledgements)?;
            return Ok(acknowledgements);
        }

        let mut page_indexes = BTreeMap::new();
        let mut page_ids = Vec::new();
        for (page, _, _, _) in writes.clone() {
            if !page_indexes.contains_key(page) {
                page_indexes.insert(page.clone(), page_ids.len());
                page_ids.push(page.clone());
            }
        }
        let states = self.mutation_states(&page_ids)?;
        let mut next_sequences: Vec<_> = states
            .iter()
            .map(|(expected, _)| expected.as_ref().map_or(0, |head| head.write_sequence))
            .collect();
        let mut groups: BTreeMap<Vec<bool>, Vec<(usize, ConditionalChunkWriteRef<'a>)>> =
            BTreeMap::new();
        for (index, (page_id, offset, data, required_sequence)) in writes.clone().enumerate() {
            let page_index = page_indexes[page_id];
            let expected_sequence = next_sequences[page_index];
            if required_sequence.is_some_and(|required| required != expected_sequence) {
                return Err(ObjectError::PreconditionFailed(
                    "star write sequence precondition changed".into(),
                ));
            }
            next_sequences[page_index] = expected_sequence.checked_add(1).ok_or_else(|| {
                ObjectError::PreconditionFailed("page write sequence is exhausted".into())
            })?;
            groups
                .entry(states[page_index].1.clone())
                .or_default()
                .push((
                    index,
                    ConditionalChunkWriteRef {
                        page_id,
                        offset,
                        data,
                        expected_sequence,
                    },
                ));
        }

        let mut acknowledgements = vec![0; writes.len()];
        for (matching, group) in groups {
            self.apply_conditional_write_group(group, Some(&matching), &mut acknowledgements)?;
        }
        Ok(acknowledgements)
    }

    fn apply_conditional_write_group(
        &mut self,
        group: Vec<(usize, ConditionalChunkWriteRef<'_>)>,
        selected: Option<&[bool]>,
        acknowledgements: &mut [usize],
    ) -> Result<(), ObjectError> {
        let legacy_frames = conditional_write_frame_count(&group, 8, 4 * 1024 * 1024, 0, 0);
        let wider_frames = conditional_write_frame_count(
            &group,
            32,
            CONDITIONAL_WRITE_MAX_PAYLOAD_BYTES,
            CONDITIONAL_WRITE_FRAME_OVERHEAD,
            CONDITIONAL_WRITE_ENTRY_OVERHEAD,
        );
        let (max_frame_writes, max_frame_bytes, frame_overhead, entry_overhead) =
            if wider_frames < legacy_frames {
                let (max_writes, max_bytes) = self.conditional_write_limits();
                (
                    max_writes,
                    max_bytes,
                    CONDITIONAL_WRITE_FRAME_OVERHEAD,
                    CONDITIONAL_WRITE_ENTRY_OVERHEAD,
                )
            } else {
                (8, 4 * 1024 * 1024, 0, 0)
            };
        let mut completed = 0;
        while completed < group.len() {
            let mut end = completed;
            let mut frame_bytes = frame_overhead;
            while end < group.len() && end - completed < max_frame_writes {
                let write = &group[end].1;
                let Some(next_bytes) = frame_bytes
                    .checked_add(entry_overhead)
                    .and_then(|bytes| bytes.checked_add(write.data.len()))
                else {
                    break;
                };
                if end > completed && next_bytes > max_frame_bytes {
                    break;
                }
                frame_bytes = next_bytes;
                end += 1;
            }
            let frame = &group[completed..end];
            let encoded = super::message::encode_conditional_write_batch_frame(
                frame.iter().map(|(_, write)| *write),
                RequestCode::WriteChunkBatchIfSequence,
            );
            let mut acknowledged = 0usize;
            let mut first_error = None;
            for result in self.fan_out_encoded_write_quorum(std::sync::Arc::new(encoded), selected) {
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
            for (index, _) in frame {
                acknowledgements[*index] = acknowledged;
            }
            completed = end;
        }
        Ok(())
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
        if pages.len() > 8 {
            if let Some(states) = self.mutation_states_batch(pages, true)? {
                return Ok(states);
            }
        }
        let mut states = Vec::with_capacity(pages.len());
        for batch in pages.chunks(8) {
            match self.mutation_states_batch(batch, false)? {
                Some(batch_states) => states.extend(batch_states),
                None => states.extend(
                    batch
                        .iter()
                        .map(|page| self.mutation_state(page))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            }
        }
        Ok(states)
    }

    fn mutation_states_batch(
        &mut self,
        pages: &[PageId],
        allow_bounded_refusal: bool,
    ) -> Result<Option<Vec<(Option<ReplicaHead>, Vec<bool>)>>, ObjectError> {
        let replies = self.fan_out(
            &ReplicaRequest::HeadChunkBatch {
                pages: pages.to_vec(),
            },
            None,
        );
        let unsupported = replies.iter().any(|reply| match reply {
            Err(ObjectError::PreconditionFailed(message)) => {
                message.starts_with("the replica does not implement this request:")
            }
            Err(ObjectError::InvalidArgument(_)) => allow_bounded_refusal,
            _ => false,
        });
        if unsupported {
            return Ok(None);
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
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
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

    /// Read a byte range of `page_id`, taking an exact vote over a read quorum.
    ///
    /// The first wave rotates across `read_quorum` members. Remaining members are asked
    /// only when the first wave cannot decide because a member failed or the copies differ.
    /// This keeps the steady-state physical read count at quorum while preserving the same
    /// exact sequence, checksum, and payload vote under failure or divergence.
    pub fn read_chunk(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
    ) -> Result<ReplicaReadResult, ObjectError> {
        let operation_deadline = self.read_wave_deadline()?;
        self.read_chunk_before(
            page_id,
            offset,
            length,
            operation_deadline,
            self.configured_endpoints.len(),
        )
    }

    fn read_chunk_before(
        &mut self,
        page_id: &PageId,
        offset: u64,
        length: u64,
        operation_deadline: std::time::Instant,
        reconnect_budget: usize,
    ) -> Result<ReplicaReadResult, ObjectError> {
        self.reconnect_failed();
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut votes = ReadVoteCollector::default();
        let mut not_found = 0usize;
        let mut unreachable = 0usize;
        let mut first_error: Option<(usize, ObjectError)> = None;
        let mut responded = vec![false; replica_count];
        let request = ReplicaRequest::ReadChunk {
            page_id: page_id.clone(),
            offset,
            length,
        };
        let (primary, fallback) = self.next_read_waves(read_quorum);
        let fallback_available = fallback.iter().any(|member| *member);
        for (wave, selected) in [&primary, &fallback].into_iter().enumerate() {
            if !selected.iter().any(|member| *member) {
                continue;
            }
            let hedge_after = (wave == 0 && fallback_available).then_some(1);
            let members: Vec<_> = selected
                .iter()
                .enumerate()
                .filter_map(|(index, selected)| selected.then_some(index))
                .collect();
            self.fan_out_each_before(
                &request,
                Some(selected),
                operation_deadline,
                hedge_after,
                |index, result| {
                    let member = members[index];
                    responded[member] = true;
                    let result = result.and_then(|reply| match reply {
                        ReplicaReply::Read(read) => Ok(read),
                        _ => Err(ObjectError::Corruption(
                            "unexpected replica read response".into(),
                        )),
                    });
                    match result {
                        Ok(read) => votes.push(read),
                        Err(err) => {
                            if matches!(&err, ObjectError::NotFound(_)) {
                                not_found += 1;
                            } else {
                                unreachable += 1;
                            }
                            if first_error
                                .as_ref()
                                .is_none_or(|(first_index, _)| member < *first_index)
                            {
                                first_error = Some((member, err));
                            }
                        }
                    }
                },
            );
            if votes.has_quorum(read_quorum) || not_found >= read_quorum {
                break;
            }
        }
        if !votes.has_quorum(read_quorum) && not_found < read_quorum {
            let missing: Vec<_> = responded.iter().map(|answered| !answered).collect();
            if missing.iter().any(|selected| *selected) {
                self.reconnect_failed();
                let members: Vec<_> = missing
                    .iter()
                    .enumerate()
                    .filter_map(|(index, selected)| selected.then_some(index))
                    .collect();
                self.fan_out_each_before(
                    &request,
                    Some(&missing),
                    operation_deadline,
                    None,
                    |index, result| {
                        let member = members[index];
                        let result = result.and_then(|reply| match reply {
                            ReplicaReply::Read(read) => Ok(read),
                            _ => Err(ObjectError::Corruption(
                                "unexpected replica read response".into(),
                            )),
                        });
                        match result {
                            Ok(read) => votes.push(read),
                            Err(err) => {
                                if matches!(&err, ObjectError::NotFound(_)) {
                                    not_found += 1;
                                } else {
                                    unreachable += 1;
                                }
                                if first_error
                                    .as_ref()
                                    .is_none_or(|(first_index, _)| member < *first_index)
                                {
                                    first_error = Some((member, err));
                                }
                            }
                        }
                    },
                );
            }
        }
        if !votes.has_quorum(read_quorum)
            && not_found < read_quorum
            && reconnect_budget > 0
            && self.replicas.len() < self.configured_endpoints.len()
        {
            let connected_before = self.replicas.len();
            self.collect_routed_reconnect_before(operation_deadline);
            if self.replicas.len() > connected_before {
                return self.read_chunk_before(
                    page_id,
                    offset,
                    length,
                    operation_deadline,
                    reconnect_budget - 1,
                );
            }
        }
        if votes.answered_count() < read_quorum {
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
                votes.answered_count(),
                first_error
                    .map(|(_, err)| err.to_string())
                    .unwrap_or_else(|| "none recorded".to_string())
            )));
        }
        votes.finish(replica_count, read_quorum)
    }

    /// Read published object pages from one verified member in the healthy case.
    /// The committed manifest supplies a sequence and SHA-256 digest for each
    /// range. A write quorum and the connected read quorum intersect, so a
    /// matching copy is sufficient; a missing or stale member is tried only
    /// for the pages it could not prove. Older manifests keep quorum reads.
    pub(super) fn read_chunks_verified(
        &mut self,
        reads: &[(PageId, u64, u64, u64, [u8; 32])],
    ) -> Result<Vec<ReplicaReadResult>, ObjectError> {
        if reads.len() > 32 || reads.iter().any(|(_, _, length, _, _)| *length > 1024 * 1024) {
            return Err(ObjectError::InvalidArgument(
                "authenticated star read exceeds batch bounds".into(),
            ));
        }
        if reads.is_empty() {
            return Ok(Vec::new());
        }
        self.reconnect_failed();
        if self.replicas.len() < self.read_quorum {
            return Err(ObjectError::Corruption(
                "authenticated star read has no connected read quorum".into(),
            ));
        }
        // The current wire bound is safe without an all-member capability
        // exchange on the read path. Older members can reject a wide batch;
        // the scalar probe below still verifies their returned bytes.
        let (max_reads, max_reply_bytes) = self.read_batch_limits.unwrap_or((
            32,
            self.options.max_payload_bytes.saturating_sub(1024),
        ));
        let max_reads = max_reads.max(1);
        let mut output = Vec::with_capacity(reads.len());
        let mut completed = 0usize;
        while completed < reads.len() {
            let mut end = completed;
            let mut frame_bytes = 0u64;
            while end < reads.len() && end - completed < max_reads {
                let Some(next_bytes) = frame_bytes.checked_add(reads[end].2) else {
                    break;
                };
                if end > completed
                    && next_bytes.saturating_add((end + 1 - completed) as u64 * 64)
                        > max_reply_bytes as u64
                {
                    break;
                }
                frame_bytes = next_bytes;
                end += 1;
            }
            let frame = &reads[completed..end];
            let deadline = self.read_wave_deadline()?;
            let (primary, _) = self.next_read_waves(1);
            let start = primary.iter().position(|selected| *selected).unwrap_or(0);
            let members = self.replicas.len();
            let mut found: Vec<Option<ReplicaReadResult>> = vec![None; frame.len()];
            for step in 0..members {
                if found.iter().all(Option::is_some) {
                    break;
                }
                let member = (start + step) % members;
                let missing: Vec<_> = found
                    .iter()
                    .enumerate()
                    .filter_map(|(index, read)| read.is_none().then_some(index))
                    .collect();
                let request = ReplicaRequest::ReadChunkBatch {
                    reads: missing
                        .iter()
                        .map(|index| {
                            let (page, offset, length, _, _) = &frame[*index];
                            (page.clone(), *offset, *length)
                        })
                        .collect(),
                };
                let mut selected = vec![false; members];
                selected[member] = true;
                let mut answer = None;
                let attempt_deadline = authenticated_read_attempt_deadline(
                    deadline,
                    missing.iter().map(|index| frame[*index].2).sum(),
                    step + 1 < members,
                );
                self.fan_out_each_before(&request, Some(&selected), attempt_deadline, None, |_, reply| {
                    answer = Some(reply);
                });
                if let Some(Ok(ReplicaReply::Reads(replies))) = answer {
                    if replies.len() == missing.len() {
                        for (index, read) in missing.iter().copied().zip(replies) {
                            if verified_object_page(&read, &frame[index]) {
                                found[index] = Some(read);
                            }
                        }
                    }
                }
            }
            // Batch replies are all-or-nothing on older nodes and when a page
            // is absent. Probe only the unresolved pages before reporting loss.
            for (index, slot) in found.iter_mut().enumerate() {
                if slot.is_some() {
                    continue;
                }
                let (page, offset, length, _, _) = &frame[index];
                let request = ReplicaRequest::ReadChunk {
                    page_id: page.clone(),
                    offset: *offset,
                    length: *length,
                };
                for step in 0..members {
                    let member = (start + step) % members;
                    let mut selected = vec![false; members];
                    selected[member] = true;
                    let mut answer = None;
                    let attempt_deadline = authenticated_read_attempt_deadline(
                        deadline,
                        *length,
                        step + 1 < members,
                    );
                    self.fan_out_each_before(&request, Some(&selected), attempt_deadline, None, |_, reply| {
                        answer = Some(reply);
                    });
                    if let Some(Ok(ReplicaReply::Read(read))) = answer {
                        if verified_object_page(&read, &frame[index]) {
                            *slot = Some(read);
                            break;
                        }
                    }
                }
            }
            for read in found {
                output.push(read.ok_or_else(|| {
                    ObjectError::Corruption(
                        "no connected replica returned a page matching the published digest"
                            .into(),
                    )
                })?);
            }
            completed = end;
        }
        Ok(output)
    }

    /// Read up to 32 independent ranges through quorum-voted star fan-out.
    /// Standalone nodes pack seven one-MiB ranges inside one bounded reply. A
    /// rotating quorum is contacted first and remaining members are used only
    /// when that wave cannot produce an exact vote for every range.
    pub fn read_chunks(
        &mut self,
        reads: &[(PageId, u64, u64)],
    ) -> Result<Vec<ReplicaReadResult>, ObjectError> {
        if reads.len() > 32 {
            return Err(ObjectError::InvalidArgument(
                "star read batch exceeds 32 pages".into(),
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
        let (max_reads, max_reply_bytes) = self.read_batch_limits();
        let replica_count = self.replicas.len();
        let read_quorum = self.read_quorum;
        let mut completed = 0usize;
        let mut output = Vec::with_capacity(reads.len());
        while completed < reads.len() {
            let mut end = completed;
            let mut frame_bytes = 0u64;
            while end < reads.len() && end - completed < max_reads {
                let Some(next_bytes) = frame_bytes.checked_add(reads[end].2) else {
                    break;
                };
                let framed_bytes = next_bytes.saturating_add(
                    (end + 1 - completed) as u64 * 64,
                );
                if end > completed && framed_bytes > max_reply_bytes as u64 {
                    break;
                }
                frame_bytes = next_bytes;
                end += 1;
            }
            let frame = &reads[completed..end];
            let request = ReplicaRequest::ReadChunkBatch {
                reads: frame.to_vec(),
            };
            let mut scalar_fallback = false;
            let mut votes: Vec<ReadVoteCollector> = (0..frame.len())
                .map(|_| ReadVoteCollector::default())
                .collect();
            let mut answered = 0usize;
            let mut not_found = 0usize;
            let mut unreachable = 0usize;
            let mut first_error: Option<(usize, ObjectError)> = None;
            let operation_deadline = self.read_wave_deadline()?;
            let (primary, fallback) = self.next_read_waves(read_quorum);
            let fallback_available = fallback.iter().any(|member| *member);
            for (wave, selected) in [&primary, &fallback].into_iter().enumerate() {
                if !selected.iter().any(|member| *member) {
                    continue;
                }
                let hedge_after = (wave == 0 && fallback_available).then_some(1);
                self.fan_out_each_before(
                    &request,
                    Some(selected),
                    operation_deadline,
                    hedge_after,
                    |index, reply| match reply {
                        Ok(ReplicaReply::Reads(reads)) if reads.len() == frame.len() => {
                            for (range_votes, read) in votes.iter_mut().zip(reads) {
                                range_votes.push(read);
                            }
                            answered += 1;
                        }
                        Ok(_) => {
                            unreachable += 1;
                            let error = ObjectError::Corruption(
                                "unexpected replica read batch response".into(),
                            );
                            if first_error
                                .as_ref()
                                .is_none_or(|(first_index, _)| index < *first_index)
                            {
                                first_error = Some((index, error));
                            }
                        }
                        Err(error) => {
                            scalar_fallback |= matches!(
                                &error,
                                ObjectError::PreconditionFailed(_)
                                    | ObjectError::InvalidArgument(_)
                                    | ObjectError::NotFound(_)
                            );
                            if matches!(&error, ObjectError::NotFound(_)) {
                                not_found += 1;
                            } else {
                                unreachable += 1;
                            }
                            if first_error
                                .as_ref()
                                .is_none_or(|(first_index, _)| index < *first_index)
                            {
                                first_error = Some((index, error));
                            }
                        }
                    },
                );
                if scalar_fallback
                    || not_found >= read_quorum
                    || votes.iter().all(|range_votes| range_votes.has_quorum(read_quorum))
                {
                    break;
                }
            }
            scalar_fallback |= answered < read_quorum
                || !votes
                    .iter()
                    .all(|range_votes| range_votes.has_quorum(read_quorum));
            if scalar_fallback {
                for (page_id, offset, length) in frame {
                    output.push(self.read_chunk(page_id, *offset, *length)?);
                }
                completed = end;
                continue;
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
                        .map(|(_, error)| error.to_string())
                        .unwrap_or_else(|| "none recorded".into())
                )));
            }
            for range_votes in votes {
                output.push(range_votes.finish(replica_count, read_quorum)?);
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
        self.read_chunk_state_with_quorum_and_sources(page_id, quorum)
            .map(|(state, _)| state)
    }

    fn read_chunk_state_with_quorum_and_sources(
        &mut self,
        page_id: &PageId,
        quorum: usize,
    ) -> Result<(crate::PageState, Vec<bool>), ObjectError> {
        let expected = self.head_chunk_with_quorum(page_id, quorum)?;
        let mut candidate: Option<crate::PageState> = None;
        let mut agreeing = 0;
        let mut verified = vec![false; self.replicas.len()];
        for (index, replica) in self.replicas.iter_mut().enumerate() {
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
                verified[index] = true;
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
        candidate
            .map(|state| (state, verified))
            .ok_or_else(|| ObjectError::Corruption("page state quorum has no payload".into()))
    }

    /// Read a page's metadata from a read quorum, voting on the whole head.
    /// A rotating quorum is contacted first; optional members are used only when
    /// failure or disagreement prevents that wave from deciding the complete head.
    pub fn head_chunk(&mut self, page_id: &PageId) -> Result<ReplicaHead, ObjectError> {
        self.head_chunk_with_quorum(page_id, self.read_quorum)
    }

    fn head_chunk_with_quorum(
        &mut self,
        page_id: &PageId,
        read_quorum: usize,
    ) -> Result<ReplicaHead, ObjectError> {
        self.reconnect_failed();
        let operation_deadline = self.read_wave_deadline()?;
        let replica_count = self.replicas.len();
        let mut answered = Vec::with_capacity(replica_count);
        let mut not_found = 0usize;
        let mut unreachable = 0usize;
        let mut responded = vec![false; replica_count];
        let request = ReplicaRequest::HeadChunk {
            page_id: page_id.clone(),
        };
        let (primary, fallback) = self.next_read_waves(read_quorum);
        let fallback_available = fallback.iter().any(|member| *member);
        for (wave, selected) in [&primary, &fallback].into_iter().enumerate() {
            if !selected.iter().any(|member| *member) {
                continue;
            }
            let hedge_after = (wave == 0 && fallback_available).then_some(1);
            let members: Vec<_> = selected
                .iter()
                .enumerate()
                .filter_map(|(index, selected)| selected.then_some(index))
                .collect();
            self.fan_out_each_before(
                &request,
                Some(selected),
                operation_deadline,
                hedge_after,
                |index, reply| {
                    responded[members[index]] = true;
                    match reply {
                        Ok(ReplicaReply::Head(head)) => answered.push(head),
                        Err(ObjectError::NotFound(_)) => not_found += 1,
                        _ => unreachable += 1,
                    }
                },
            );
            if heads_have_quorum(&answered, read_quorum) || not_found >= read_quorum {
                break;
            }
        }
        if !heads_have_quorum(&answered, read_quorum) && not_found < read_quorum {
            let missing: Vec<_> = responded.iter().map(|answered| !answered).collect();
            if missing.iter().any(|selected| *selected) {
                self.reconnect_failed();
                let retry_deadline = self.read_wave_deadline()?;
                self.fan_out_each_before(
                    &request,
                    Some(&missing),
                    retry_deadline,
                    None,
                    |_, reply| match reply {
                        Ok(ReplicaReply::Head(head)) => answered.push(head),
                        Err(ObjectError::NotFound(_)) => not_found += 1,
                        _ => unreachable += 1,
                    },
                );
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
        let (state, verified_sources) =
            self.read_chunk_state_with_quorum_and_sources(page_id, quorum)?;
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
            // These members already returned checksum-fenced complete state with
            // exact bytes matching the selected source quorum. Writers must be
            // fenced before a completed traversal is used for readmission.
            if verified_sources[index] {
                report.members.push(PageRepairStatus::Current);
                continue;
            }
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
        self.repair_inventory_batch_inner(after, limit, false)
    }

    /// Scan complete member inventories using heads before reading payloads.
    /// Equal heads avoid a full page transfer; differing or missing heads still
    /// use the validated repair path. This is a repair discovery pass, not an
    /// integrity scrub: matching stored heads cannot reveal silent payload damage.
    pub fn repair_inventory_batch_head_first(
        &mut self,
        after: Option<&PageId>,
        limit: usize,
    ) -> Result<PageRepairBatch, ObjectError> {
        self.repair_inventory_batch_inner(after, limit, true)
    }

    fn repair_inventory_batch_inner(
        &mut self,
        after: Option<&PageId>,
        limit: usize,
        head_first: bool,
    ) -> Result<PageRepairBatch, ObjectError> {
        if !(1..=64).contains(&limit) {
            return Err(ObjectError::InvalidArgument(
                "repair batch limit must be between 1 and 64".into(),
            ));
        }
        self.reconnect_failed();
        let request = ReplicaRequest::PageInventory {
            after: after.cloned(),
            limit: limit as u64,
        };
        let deadline = self.read_wave_deadline()?;
        let mut selected = BTreeSet::new();
        let mut has_more = false;
        let mut first_error = None;
        let mut answers = 0;
        self.fan_out_each_before(&request, None, deadline, None, |_, reply| {
            let inventory = match reply {
                Ok(ReplicaReply::PageInventory(inventory)) => inventory,
                Ok(other) => {
                    first_error.get_or_insert_with(|| {
                        ObjectError::Corruption(format!(
                            "unexpected page inventory reply: {other:?}"
                        ))
                    });
                    return;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    return;
                }
            };
            if inventory.entries.len() > limit
                || (inventory.has_more && inventory.entries.len() != limit)
                || after.is_some_and(|cursor| {
                    inventory
                        .entries
                        .first()
                        .is_some_and(|(id, _)| id <= cursor)
                })
            {
                first_error.get_or_insert_with(|| {
                    ObjectError::Corruption(
                        "page inventory does not respect the requested cursor or limit".into(),
                    )
                });
                return;
            }
            answers += 1;
            has_more |= inventory.has_more;
            for (page, _) in inventory.entries {
                selected.insert(page);
                if selected.len() > limit {
                    selected.pop_last();
                    has_more = true;
                }
            }
        });
        if let Some(error) = first_error {
            return Err(error);
        }
        if answers != self.replicas.len() {
            return Err(ObjectError::Corruption(format!(
                "page inventory answered by {answers} of {} replicas",
                self.replicas.len()
            )));
        }
        let mut batch = PageRepairBatch {
            pages: Vec::with_capacity(selected.len()),
            resume_after: after.cloned(),
            has_more,
            blocked: false,
        };
        for page in selected {
            let result = if head_first {
                self.repair_chunk_head_first(&page)
            } else {
                self.repair_chunk(&page)
            }
            .map_err(|error| error.to_string());
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

    fn repair_chunk_head_first(
        &mut self,
        page_id: &PageId,
    ) -> Result<PageRepairReport, ObjectError> {
        let deadline = self.read_wave_deadline()?;
        let request = ReplicaRequest::HeadChunk {
            page_id: page_id.clone(),
        };
        let mut heads = vec![None; self.replicas.len()];
        self.fan_out_each_before(&request, None, deadline, None, |index, reply| {
            heads[index] = match reply {
                Ok(ReplicaReply::Head(head)) => Some(head),
                _ => None,
            };
        });
        if let Some(source) = heads.first().and_then(Option::as_ref) {
            if heads.iter().all(|head| head.as_ref() == Some(source)) {
                return Ok(PageRepairReport {
                    source: source.clone(),
                    members: vec![PageRepairStatus::Current; self.replicas.len()],
                });
            }
        }
        self.repair_chunk(page_id)
    }

    /// Conditionally discard on replicas matching a quorum-agreed existing page.
    ///
    /// Returns after a write quorum acknowledges, with the same one-millisecond
    /// opportunistic drain used by writes. A slower member can still apply a
    /// fully received request after its incomplete reply socket is retired.
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
        for result in self.fan_out_write_quorum(&request, Some(&observed)) {
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

    /// Every configured endpoint, in membership order.
    pub fn endpoints(&self) -> Vec<String> {
        self.configured_endpoints.clone()
    }
}

fn heads_have_quorum(answered: &[ReplicaHead], read_quorum: usize) -> bool {
    answered.iter().any(|candidate| {
        answered.iter().filter(|head| *head == candidate).count() >= read_quorum
    })
}

fn verified_object_page(
    read: &ReplicaReadResult,
    expected: &(PageId, u64, u64, u64, [u8; 32]),
) -> bool {
    read.write_sequence == expected.3
        && read.data.len() as u64 == expected.2
        && ring::digest::digest(&ring::digest::SHA256, &read.data).as_ref()
            == expected.4.as_slice()
}

// Keep exact sequence/checksum/byte voting while moving each received payload
// directly into the vote table. Matching allocations are released as replies
// complete, so retained bytes scale with distinct copies rather than responders.
#[derive(Default)]
struct ReadVoteCollector {
    answered_count: usize,
    votes: BTreeMap<(u64, String, Vec<u8>), usize>,
}

impl ReadVoteCollector {
    fn push(&mut self, read: ReplicaReadResult) {
        self.answered_count += 1;
        let key = (read.write_sequence, read.checksum, read.data);
        *self.votes.entry(key).or_insert(0) += 1;
    }

    fn answered_count(&self) -> usize {
        self.answered_count
    }

    fn has_quorum(&self, read_quorum: usize) -> bool {
        self.votes.values().any(|count| *count >= read_quorum)
    }

    fn finish(
        self,
        replica_count: usize,
        read_quorum: usize,
    ) -> Result<ReplicaReadResult, ObjectError> {
        let (winning_key, winning_count) = self
            .votes
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
                self.answered_count
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
    fn retained_payloads(&self) -> usize {
        self.votes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_star_pages_read_one_member_and_retry_stale_or_slow_bytes() {
        use crate::cluster::message::{encode_reply, ReplicaReply, ReplicaRequest};
        use crate::cluster::wire::RequestCode;
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        for first_member in ["healthy", "stale", "slow"] {
            let listeners: Vec<_> = (0..3)
                .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
                .collect();
            let endpoints: Vec<_> = listeners
                .iter()
                .map(|listener| listener.local_addr().unwrap().to_string())
                .collect();
            let batches = Arc::new(std::array::from_fn::<_, 3, _>(|_| AtomicUsize::new(0)));
            let scalars = Arc::new(std::array::from_fn::<_, 3, _>(|_| AtomicUsize::new(0)));
            let workers: Vec<_> = listeners
                .into_iter()
                .enumerate()
                .map(|(index, listener)| {
                    let batches = batches.clone();
                    let scalars = scalars.clone();
                    std::thread::spawn(move || {
                        let (mut stream, _) = listener.accept().unwrap();
                        stream
                            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                            .unwrap();
                        loop {
                            let mut header = [0; 6];
                            if stream.read_exact(&mut header).is_err() {
                                break;
                            }
                            let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                            let mut bytes = vec![0; length];
                            stream.read_exact(&mut bytes).unwrap();
                            let request = ReplicaRequest::decode(
                                RequestCode::from_u8(header[1]).unwrap(),
                                &bytes,
                            )
                            .unwrap();
                            let reply = match request {
                                ReplicaRequest::ReadChunkBatch { reads } => {
                                    batches[index].fetch_add(1, Ordering::SeqCst);
                                    if index == 0 && first_member == "slow" {
                                        std::thread::sleep(std::time::Duration::from_secs(2));
                                    }
                                    ReplicaReply::Reads(
                                        reads
                                            .into_iter()
                                            .map(|_| ReplicaReadResult {
                                                data: if index == 0 && first_member == "stale" {
                                                    b"stale".to_vec()
                                                } else {
                                                    b"fresh".to_vec()
                                                },
                                                write_sequence: 1,
                                                checksum: "valid".into(),
                                            })
                                            .collect(),
                                    )
                                }
                                ReplicaRequest::ReadChunk { .. } => {
                                    scalars[index].fetch_add(1, Ordering::SeqCst);
                                    ReplicaReply::Read(ReplicaReadResult {
                                        data: b"fresh".to_vec(),
                                        write_sequence: 1,
                                        checksum: "valid".into(),
                                    })
                                }
                                _ => panic!("unexpected authenticated read request"),
                            };
                            if stream.write_all(&encode_reply(&reply)).is_err() {
                                break;
                            }
                        }
                    })
                })
                .collect();
            let mut set = RemoteReplicaSet::connect(
                &endpoints,
                2,
                2,
                &ReplicaClientOptions::default(),
            )
            .unwrap();
            let mut digest = [0; 32];
            digest.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, b"fresh").as_ref());
            let pages: Vec<_> = (0..2)
                .map(|index| {
                    (
                        PageId {
                            chunk_id: crate::ChunkId::new(1, 2).unwrap(),
                            page_index: index,
                        },
                        0,
                        5,
                        1,
                        digest,
                    )
                })
                .collect();
            let started = std::time::Instant::now();
            let reads = set.read_chunks_verified(&pages).unwrap();
            if first_member == "slow" {
                assert!(started.elapsed() < std::time::Duration::from_millis(1500));
            }
            assert!(reads.iter().all(|read| read.data == b"fresh"));
            drop(set);
            for worker in workers {
                worker.join().unwrap();
            }
            assert_eq!(batches[0].load(Ordering::SeqCst), 1);
            assert_eq!(batches[1].load(Ordering::SeqCst), usize::from(first_member != "healthy"));
            assert_eq!(batches[2].load(Ordering::SeqCst), 0);
            assert_eq!(scalars.iter().map(|value| value.load(Ordering::SeqCst)).sum::<usize>(), 0);
        }
    }

    #[test]
    fn failed_member_work_runs_concurrently_in_bounded_waves() {
        use std::sync::{Arc, Condvar, Mutex};
        use std::time::Duration;

        let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
        let mut values = [0usize; 4];
        let expected = values.len();
        run_reconnects_in_bounded_waves(values.iter_mut().collect(), &|value| {
            *value += 1;
            let (arrived, ready) = &*gate;
            let mut arrived = arrived.lock().unwrap();
            *arrived += 1;
            ready.notify_all();
            while *arrived < expected {
                let (next, timeout) = ready.wait_timeout(arrived, Duration::from_secs(3)).unwrap();
                assert!(!timeout.timed_out(), "reconnect work ran serially");
                arrived = next;
            }
        });
        assert_eq!(values, [1; 4]);
    }

    #[test]
    fn bounded_endpoint_jobs_deliver_ready_members_without_waiting_for_a_slow_member() {
        use std::sync::{Arc, Condvar, Mutex};
        use std::time::Duration;

        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let endpoints = vec!["ready-a".into(), "slow".into(), "ready-b".into()];
        let (_, receiver) = start_bounded_endpoint_jobs(endpoints, move |endpoint| {
            if endpoint == "slow" {
                let (released, ready) = &*worker_gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }
        })
        .unwrap();

        let mut ready_members = BTreeSet::new();
        for _ in 0..2 {
            let (endpoint, ()) = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
            ready_members.insert(endpoint);
        }
        assert_eq!(
            ready_members,
            BTreeSet::from(["ready-a".to_string(), "ready-b".to_string()])
        );

        let (released, ready) = &*gate;
        *released.lock().unwrap() = true;
        ready.notify_all();
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(1)).unwrap().0,
            "slow"
        );
    }

    #[test]
    fn routed_quorum_schedules_optional_reconnect_off_the_operation_path() {
        use std::net::TcpListener;

        let live: Vec<_> = (0..2)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let closed = TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_endpoint = closed.local_addr().unwrap().to_string();
        drop(closed);
        let endpoints = vec![
            live[0].local_addr().unwrap().to_string(),
            live[1].local_addr().unwrap().to_string(),
            closed_endpoint.clone(),
        ];
        let mut set = RemoteReplicaSet::connect_routed_quorum(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                connect_timeout: std::time::Duration::from_secs(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(set.replicas.len(), 2);

        let started = std::time::Instant::now();
        set.reconnect_failed();
        assert!(started.elapsed() < std::time::Duration::from_secs(1));

        let reconnect = set.routed_reconnect.as_ref().unwrap();
        assert!(
            reconnect.pending.is_some() || reconnect.unavailable_endpoints == [closed_endpoint]
        );
        assert_eq!(set.replicas.len(), 2);
    }

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
        let mut votes = ReadVoteCollector::default();
        votes.push(conflicting);
        votes.push(first);
        votes.push(second);
        assert_eq!(votes.answered_count(), 3);
        assert_eq!(votes.retained_payloads(), 2);
        let result = votes.finish(3, 2).unwrap();
        assert!(
            result.data.as_ptr() == original || result.data.as_ptr() == other,
            "the winning bytes must retain a received allocation"
        );
        assert_eq!(result.data, vec![3; 1024 * 1024]);
        assert_eq!(result.write_sequence, 7);
        assert_eq!(result.checksum, "same-checksum");
        let rejects = |left, right| {
            let mut votes = ReadVoteCollector::default();
            votes.push(left);
            votes.push(right);
            votes.finish(2, 2)
        };
        assert!(matches!(
            rejects(reply(vec![1]), reply(vec![2])),
            Err(ObjectError::Corruption(_))
        ));
        let mut newer = reply(vec![1]);
        newer.write_sequence = 8;
        assert!(rejects(reply(vec![1]), newer).is_err());
        let mut changed = reply(vec![1]);
        changed.checksum.push('x');
        assert!(rejects(reply(vec![1]), changed).is_err());
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
                    std::sync::Arc::new(super::super::message::encode_request(&request)),
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
    fn star_scalar_writes_cache_capabilities_and_fence_optional_members() {
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .unwrap();
        let workers: Vec<_> = listeners
            .into_iter()
            .map(|listener| {
                let (mut peer, _) = listener.accept().unwrap();
                std::thread::spawn(move || {
                    let mut requests = 0usize;
                    let mut writes = 0usize;
                    loop {
                        let mut header = [0; 6];
                        match peer.read_exact(&mut header) {
                            Ok(()) => {}
                            Err(error)
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::UnexpectedEof
                                        | std::io::ErrorKind::ConnectionReset
                                ) =>
                            {
                                break;
                            }
                            Err(error) => panic!("star write member failed: {error}"),
                        }
                        let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                        let mut payload = vec![0; length];
                        peer.read_exact(&mut payload).unwrap();
                        requests += 1;
                        let code = super::super::wire::RequestCode::from_u8(header[1]).unwrap();
                        let request = ReplicaRequest::decode(code, &payload).unwrap();
                        let reply = match request {
                            ReplicaRequest::Capabilities => ReplicaReply::Capabilities {
                                state_conditional_write: true,
                            },
                            ReplicaRequest::HeadChunk { .. } if writes == 0 => {
                                peer.write_all(&super::super::message::encode_failure(
                                    super::super::wire::StatusCode::NotFound,
                                    "absent",
                                ))
                                .unwrap();
                                continue;
                            }
                            ReplicaRequest::HeadChunk { .. } => ReplicaReply::Head(ReplicaHead {
                                logical_length: 5,
                                write_sequence: 1,
                                checksum: "payload-one".into(),
                                state_checksum: "state-one".into(),
                                discarded_ranges: Vec::new(),
                            }),
                            ReplicaRequest::WriteChunkIfState {
                                expected_sequence,
                                expected_state_checksum,
                                ..
                            } => {
                                if writes == 0 {
                                    assert_eq!(expected_sequence, 0);
                                    assert!(expected_state_checksum.is_empty());
                                } else {
                                    assert_eq!(expected_sequence, 1);
                                    assert_eq!(expected_state_checksum, "state-one");
                                }
                                writes += 1;
                                ReplicaReply::Applied
                            }
                            other => panic!("unexpected star write request: {other:?}"),
                        };
                        peer.write_all(&super::super::message::encode_reply(&reply))
                            .unwrap();
                    }
                    (requests, writes)
                })
            })
            .collect();

        let page = PageId {
            chunk_id: crate::ChunkId::new(9, 1).unwrap(),
            page_index: 0,
        };
        assert_eq!(set.write_chunk(&page, 0, b"first").unwrap(), 3);
        assert_eq!(set.write_chunk(&page, 5, b"second").unwrap(), 3);
        drop(set);

        let observed: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(observed.iter().map(|(_, writes)| *writes).sum::<usize>(), 6);
        assert_eq!(observed.iter().map(|(requests, _)| *requests).sum::<usize>(), 13);
        let mut request_counts: Vec<_> = observed
            .into_iter()
            .map(|(requests, _)| requests)
            .collect();
        request_counts.sort_unstable();
        assert_eq!(request_counts, vec![4, 4, 5]);
    }

    #[test]
    fn star_scalar_and_batch_reads_use_rotating_quorums() {
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .unwrap();
        let scalar_code = ReplicaRequest::ReadChunk {
            page_id: PageId {
                chunk_id: crate::ChunkId::new(1, 2).unwrap(),
                page_index: 0,
            },
            offset: 0,
            length: 11,
        }
        .code() as u8;
        let head_code = ReplicaRequest::HeadChunk {
            page_id: PageId {
                chunk_id: crate::ChunkId::new(1, 2).unwrap(),
                page_index: 0,
            },
        }
        .code() as u8;
        let read_limits_code = ReplicaRequest::ReadBatchLimits.code() as u8;
        let batch_code = ReplicaRequest::ReadChunkBatch { reads: Vec::new() }.code() as u8;
        let mut workers = Vec::new();
        for listener in listeners {
            let (mut peer, _) = listener.accept().unwrap();
            workers.push(std::thread::spawn(move || {
                let mut requests = 0usize;
                loop {
                    let mut header = [0; 6];
                    match peer.read_exact(&mut header) {
                        Ok(()) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::UnexpectedEof
                                    | std::io::ErrorKind::ConnectionReset
                            ) =>
                        {
                            break;
                        }
                        Err(error) => panic!("star read member failed: {error}"),
                    }
                    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut payload = vec![0; length];
                    peer.read_exact(&mut payload).unwrap();
                    requests += 1;
                    let first = ReplicaReadResult {
                        data: b"quorum-data".to_vec(),
                        write_sequence: 7,
                        checksum: "quorum-checksum".into(),
                    };
                    let reply = match header[1] {
                        code if code == read_limits_code => ReplicaReply::ReadBatchLimits {
                            max_reads: 32,
                            max_reply_payload_bytes: 4 * 1024 * 1024,
                        },
                        code if code == scalar_code => ReplicaReply::Read(first),
                        code if code == head_code => ReplicaReply::Head(ReplicaHead {
                            logical_length: 11,
                            write_sequence: 7,
                            checksum: "quorum-checksum".into(),
                            state_checksum: "quorum-state-checksum".into(),
                            discarded_ranges: Vec::new(),
                        }),
                        code if code == batch_code => {
                            let ReplicaRequest::ReadChunkBatch { reads } =
                                ReplicaRequest::decode(
                                    crate::cluster::RequestCode::ReadChunkBatch,
                                    &payload,
                                )
                                .unwrap()
                            else {
                                unreachable!()
                            };
                            ReplicaReply::Reads(
                                reads
                                    .iter()
                                    .enumerate()
                                    .map(|(index, _)| {
                                        if index == 0 {
                                            first.clone()
                                        } else {
                                            ReplicaReadResult {
                                                data: b"second".to_vec(),
                                                write_sequence: 7,
                                                checksum: "second-checksum".into(),
                                            }
                                        }
                                    })
                                    .collect(),
                            )
                        }
                        code => panic!("unexpected star read request code {code}"),
                    };
                    peer.write_all(&super::super::message::encode_reply(&reply))
                        .unwrap();
                }
                requests
            }));
        }

        let page = PageId {
            chunk_id: crate::ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        };
        for _ in 0..2 {
            let read = set.read_chunk(&page, 0, 11).unwrap();
            assert_eq!(read.data, b"quorum-data");
        }
        let batch: Vec<_> = (0..32)
            .map(|page_index| {
                (
                    PageId {
                        chunk_id: crate::ChunkId::new(1, 2).unwrap(),
                        page_index,
                    },
                    0,
                    if page_index == 0 { 11 } else { 6 },
                )
            })
            .collect();
        for _ in 0..2 {
            let reads = set.read_chunks(&batch).unwrap();
            assert_eq!(reads.len(), 32);
            assert_eq!(reads[0].data, b"quorum-data");
            assert!(reads[1..].iter().all(|read| read.data == b"second"));
        }
        for _ in 0..2 {
            let head = set.head_chunk(&page).unwrap();
            assert_eq!(head.logical_length, 11);
            assert_eq!(head.write_sequence, 7);
        }
        drop(set);

        let requests: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(requests, vec![5, 5, 5]);
        assert_eq!(requests.iter().sum::<usize>(), 15);
    }

    #[test]
    fn star_reads_use_optional_waves_when_quorum_copies_differ() {
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .unwrap();
        let head_code = ReplicaRequest::HeadChunk {
            page_id: PageId {
                chunk_id: crate::ChunkId::new(1, 3).unwrap(),
                page_index: 0,
            },
        }
        .code() as u8;
        let mut workers = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let (mut peer, _) = listener.accept().unwrap();
            workers.push(std::thread::spawn(move || {
                for _ in 0..2 {
                    let mut header = [0; 6];
                    peer.read_exact(&mut header).unwrap();
                    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                    let mut payload = vec![0; length];
                    peer.read_exact(&mut payload).unwrap();
                    let reply = if header[1] == head_code {
                        let selected = index != 2;
                        ReplicaReply::Head(ReplicaHead {
                            logical_length: if selected { 8 } else { 9 },
                            write_sequence: 7,
                            checksum: if selected { "selected" } else { "different" }.into(),
                            state_checksum: if selected {
                                "selected-state"
                            } else {
                                "different-state"
                            }
                            .into(),
                            discarded_ranges: Vec::new(),
                        })
                    } else {
                        let data = if index == 1 {
                            b"different".to_vec()
                        } else {
                            b"selected".to_vec()
                        };
                        ReplicaReply::Read(ReplicaReadResult {
                            checksum: format!("checksum-{data:?}"),
                            data,
                            write_sequence: 7,
                        })
                    };
                    peer.write_all(&super::super::message::encode_reply(&reply))
                        .unwrap();
                }
            }));
        }

        let page = PageId {
            chunk_id: crate::ChunkId::new(1, 3).unwrap(),
            page_index: 0,
        };
        let read = set.read_chunk(&page, 0, 8).unwrap();
        assert_eq!(read.data, b"selected");
        let head = set.head_chunk(&page).unwrap();
        assert_eq!(head.logical_length, 8);
        assert_eq!(head.checksum, "selected");
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn star_blocked_sender_does_not_delay_healthy_members_or_replay() {
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        // Bound the first receive window before the handshake to force backpressure.
        let size: libc::c_int = 4096;
        assert_eq!(
            unsafe {
                libc::setsockopt(
                    listeners[0].as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    &size as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&size) as libc::socklen_t,
                )
            },
            0
        );
        let endpoints: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().to_string())
            .collect();
        let mut set = RemoteReplicaSet::connect(
            &endpoints,
            2,
            2,
            &ReplicaClientOptions {
                write_timeout: Duration::from_secs(3),
                read_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        let sockets: Vec<_> = listeners
            .iter()
            .map(|listener| listener.accept().unwrap().0)
            .collect();
        let (ready, received) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let mut receivers = Vec::new();
        let mut released = Some(released);
        for (index, mut socket) in sockets.into_iter().enumerate() {
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let ready = ready.clone();
            let released = if index == 0 { released.take() } else { None };
            receivers.push(std::thread::spawn(move || {
                if let Some(released) = released {
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    let mut bytes = Vec::new();
                    socket.read_to_end(&mut bytes).unwrap();
                    assert!(!bytes.is_empty());
                    assert!(
                        bytes.len() < 8 * 1024 * 1024,
                        "stalled member must receive only a partial frame"
                    );
                    return;
                }
                let mut header = [0; 6];
                socket.read_exact(&mut header).unwrap();
                let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                assert!(length > 8 * 1024 * 1024 && length < 9 * 1024 * 1024);
                let mut payload = vec![0; length];
                socket.read_exact(&mut payload).unwrap();
                ready.send(index).unwrap();
                socket
                    .write_all(&super::super::message::encode_reply(&ReplicaReply::Applied))
                    .unwrap();
                let mut byte = [0];
                assert_eq!(
                    socket.read(&mut byte).unwrap(),
                    0,
                    "mutation must not replay"
                );
            }));
        }
        let writer = std::thread::spawn(move || {
            let replies = set.fan_out(
                &ReplicaRequest::WriteChunkIfSequence {
                    page_id: PageId {
                        chunk_id: crate::ChunkId::new(1, 1).unwrap(),
                        page_index: 0,
                    },
                    offset: 0,
                    data: vec![7; 8 * 1024 * 1024],
                    expected_sequence: 0,
                },
                None,
            );
            assert!(replies[0].is_err());
            assert!(matches!(replies[1], Ok(ReplicaReply::Applied)));
            assert!(matches!(replies[2], Ok(ReplicaReply::Applied)));
        });
        let first = received.recv_timeout(Duration::from_secs(1));
        let second = received.recv_timeout(Duration::from_secs(1));
        writer.join().unwrap();
        release.send(()).unwrap();
        for worker in receivers {
            worker.join().unwrap();
        }
        assert!(
            first.is_ok() && second.is_ok(),
            "healthy members waited behind the stalled sender"
        );
    }
}
