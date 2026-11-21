// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An S3 door over the replicated object plane, for the verbs a shared log needs.
//!
//! [`ObjectService`](super::ObjectService) serves one in-process store. This serves the cluster:
//! each request is answered by the cluster SDK against a metadata quorum and its chunk groups.
//! The verb subset and the mapping are written down in `docs/cluster-gateway.md`; the short
//! version is that every verb outside the subset answers an S3 `NotImplemented` error.
//!
//! The SDK is reached through [`ObjectPlane`], one method per SDK call the gateway makes. The
//! real implementation is [`SdkPlane`]; the unit tests run the same routing against an
//! in-memory plane that enforces the same generation compare-and-swap and the same 4096-extent
//! limit, so the append and conflict logic is tested without a cluster. The process-level run
//! against real metadata and chunk processes is `scripts/cluster_gateway_bringup.sh`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::http::{self, Request, ResolvedRange, Response};
use super::s3;
use super::RequestHandler;
use crate::cluster::{
    MetaClientOptions, ObjectWriteFailure, ObjectWriteOptions, RemoteMetaClient,
    ReplicaClientOptions,
};
use crate::cluster::{consensus_unavailable, outcome_may_be_committed};
use crate::object_manifest::{ManifestListPage, ObjectManifest};
use crate::ObjectError;

#[path = "cluster_gateway_s3.rs"]
mod s3_verbs;

/// The most extents the replica plane lets one object hold.
pub const MAX_OBJECT_EXTENTS: usize = 4096;

/// Offset header, spelled as the in-process door spells it.
pub const WRITE_OFFSET_HEADER: &str = super::s3_limits::WRITE_OFFSET_HEADER;

const LIST_PAGE: u32 = 32;

/// A write that did not publish, and whether it might have.
#[derive(Debug)]
pub struct PlaneFailure {
    pub error: ObjectError,
    /// Publication was attempted and its result is not confirmed visible.
    pub unconfirmed: bool,
    /// The extents of the manifest this attempt tried to publish, when the plane knows them (the SDK does for an
    /// unconfirmed publication). They are the attempt's IDENTITY: pages are allocated per attempt, so the
    /// committed manifest holds them if and only if this attempt landed, whatever bytes another writer appended.
    /// Empty when unknown.
    pub attempted: Vec<crate::object_manifest::ObjectExtent>,
}

impl From<ObjectError> for PlaneFailure {
    fn from(error: ObjectError) -> Self {
        Self {
            error,
            unconfirmed: false,
            attempted: Vec::new(),
        }
    }
}

impl From<ObjectWriteFailure> for PlaneFailure {
    fn from(failure: ObjectWriteFailure) -> Self {
        match failure {
            ObjectWriteFailure::BeforePublication { error, .. } => Self {
                error,
                unconfirmed: false,
                attempted: Vec::new(),
            },
            ObjectWriteFailure::PublicationUnconfirmed { error, outcome } => Self {
                error,
                unconfirmed: true,
                attempted: outcome.after.as_ref().map(|after| after.extents.clone()).unwrap_or_default(),
            },
        }
    }
}

/// The header that names one attempt of an append: 32 hex characters (16 random bytes) chosen by the client, the same
/// on every retry of that attempt and echoed in the response.
pub const ATTEMPT_HEADER: &str = "x-amz-attempt-id";

/// Echo the attempt id of a request that carried one, so a client can match the answer to the attempt.
fn with_attempt(response: Response, attempt: Option<[u8; 16]>) -> Response {
    match attempt {
        Some(id) => response.with_header(ATTEMPT_HEADER, id.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
        None => response,
    }
}

/// How many attempts a gateway remembers for `?append-resolve`.
const ATTEMPT_TABLE: usize = 4096;

/// What one attempt did, as far as this gateway saw it: the range it wrote and the extents it published (or tried to).
struct AttemptRecord {
    bucket: String,
    key: String,
    offset: u64,
    length: u64,
    extents: Vec<crate::object_manifest::ObjectExtent>,
}

#[derive(Default)]
struct AttemptTable {
    order: std::collections::VecDeque<[u8; 16]>,
    records: std::collections::HashMap<[u8; 16], AttemptRecord>,
}

impl AttemptTable {
    fn record(&mut self, id: [u8; 16], record: AttemptRecord) {
        if self.records.insert(id, record).is_none() {
            self.order.push_back(id);
            while self.order.len() > ATTEMPT_TABLE {
                if let Some(old) = self.order.pop_front() {
                    self.records.remove(&old);
                }
            }
        }
    }
}

/// The extents that cover `[offset, end)`.
fn extents_over(extents: &[crate::object_manifest::ObjectExtent], offset: u64, end: u64) -> Vec<crate::object_manifest::ObjectExtent> {
    extents
        .iter()
        .filter(|extent| extent.object_offset < end && extent.object_offset.saturating_add(extent.length) > offset)
        .cloned()
        .collect()
}

fn decode_hex_16(text: &str) -> Option<[u8; 16]> {
    if text.len() != 32 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; 16];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        out[index] = (high * 16 + low) as u8;
    }
    Some(out)
}

/// One connection's worth of cluster operations.
pub trait ObjectPlane: Send {
    fn read_manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError>;
    /// Publish a whole object on top of `expected` (`None`: the key has never existed).
    fn write(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure>;
    /// Append `data` to the live manifest `expected`.
    fn append(
        &mut self,
        expected: ObjectManifest,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure>;
    /// Append through whatever bounded-publication or compacting path this plane has, falling
    /// back to [`ObjectPlane::append`].
    fn append_best(
        &mut self,
        expected: ObjectManifest,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        self.append(expected, data)
    }
    fn read(&mut self, bucket: &str, key: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError>;
    fn read_range(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), ObjectError>;
    fn delete(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError>;
    /// Make `bucket`/`key` hold what `source_bucket`/`source_key` holds, on top of `expected` (the
    /// destination as the caller read it). The default reads and writes the bytes; the SDK plane
    /// overrides it with the metadata plane's atomic copy, which moves no data.
    fn copy(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
    ) -> Result<ObjectManifest, PlaneFailure> {
        let (_, data) = self.read(source_bucket, source_key)?;
        self.write(bucket, key, expected, &data)
    }
    fn list(
        &mut self,
        bucket: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<ManifestListPage, ObjectError>;
    /// Publish `expected` again at its generation plus one with exactly its extents. The bytes do
    /// not change, but no publication that built on an older generation can succeed afterwards:
    /// the fence an append resolve uses.
    fn publish_unchanged(&mut self, _expected: &ObjectManifest) -> Result<ObjectManifest, PlaneFailure> {
        Err(ObjectError::InvalidArgument("this plane cannot fence an append".into()).into())
    }
}

type Connector = dyn Fn() -> Result<Box<dyn ObjectPlane>, ObjectError> + Send + Sync;

/// How the gateway talks to the cluster.
#[derive(Clone)]
pub struct GatewayOptions {
    pub write: ObjectWriteOptions,
    pub replica: ReplicaClientOptions,
    /// Bound on one cluster operation.
    pub operation_timeout: Duration,
    /// Largest object a whole-object read or write may carry.
    pub max_object_bytes: usize,
    /// Manifests remembered so a repeated append can skip reading it: at most this many, ...
    pub cache_entries: usize,
    /// ... and at most this many bytes of them (by `manifest_weight`), the least recently used
    /// leaving first. A manifest of 4,000 extents weighs about three quarters of a megabyte, so a
    /// count alone either wastes the memory or misses a large working set.
    pub cache_bytes: usize,
    /// Idle metadata connections kept for reuse.
    pub pool_max: usize,
    /// Publish appends as compact digest-bound decisions under a staged allocation. Every
    /// metadata voter and journal reader must support the version 11 outcome.
    pub compact_append: bool,
    /// Keep a log object appendable past the extent bound by merging its short tail once it
    /// holds this many extents (2 to 4096). Uses the plain publish path.
    pub compact_extents: Option<usize>,
    /// Lease for a staged allocation made for one compact append.
    pub staged_ttl_ms: u64,
    /// With compact publication, reserve the next append's allocation in the same durable
    /// commit as the current append, so an append to a busy log costs one metadata commit.
    /// Every metadata voter and journal reader must support the version 13 outcome.
    pub reserve_next: bool,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        Self {
            write: ObjectWriteOptions::default(),
            replica: ReplicaClientOptions::default(),
            operation_timeout: Duration::from_secs(10),
            max_object_bytes: 64 * 1024 * 1024,
            cache_entries: 4096,
            cache_bytes: 64 * 1024 * 1024,
            pool_max: 8,
            compact_append: false,
            compact_extents: None,
            staged_ttl_ms: 60_000,
            reserve_next: false,
        }
    }
}

/// The real plane: one metadata client plus the options for its data-group traffic.
pub struct SdkPlane {
    client: RemoteMetaClient,
    options: GatewayOptions,
    compact: Arc<CompactState>,
}

/// Whether the compact append path is usable, shared by every connection of one gateway.
/// 0: untried, 1: enabled, 2: refused once and switched off for the life of the process.
#[derive(Default)]
pub struct CompactState(
    std::sync::atomic::AtomicU8,
    /// Allocations reserved by the last append to each object, with when they were stored.
    std::sync::Mutex<
        std::collections::HashMap<
            (String, String),
            (
                crate::staged_allocation::StagedAllocation,
                std::time::Instant,
            ),
        >,
    >,
    /// When each object's last successful append completed: a reservation
    /// is made only for an object that is being appended to repeatedly.
    std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>,
    /// The read leases of this gateway, shared by every connection: in a fenced catalog every
    /// read needs a lease on the generation it reads, and reads of one generation share one.
    std::sync::Arc<crate::cluster::ReadLeaseCache>,
);

/// An object counts as being appended to repeatedly when its previous append was this recent.
/// A reservation costs a slot of the metadata plane's table and, when nobody uses it, an expiry
/// commit later, so only an object likely to be appended to again soon should get one.
pub const HOT_WINDOW: std::time::Duration = std::time::Duration::from_secs(2);
/// Objects the gateway remembers an append time for.
const RECENT_APPENDS_CAP: usize = 4096;

/// Lease of an allocation reserved for the next append. Short, because a reservation nobody
/// uses (an idle log, another writer got there first) holds one of the metadata plane's 128
/// allocations until it expires.
const RESERVATION_TTL_MS: u64 = 10_000;

impl CompactState {
    fn append_is_hot_at(&self, bucket: &str, key: &str, now: Instant) -> bool {
        let Ok(map) = self.2.lock() else { return false };
        map.get(&(bucket.to_owned(), key.to_owned()))
            .and_then(|last| now.checked_duration_since(*last))
            .is_some_and(|elapsed| elapsed < HOT_WINDOW)
    }

    /// Record successful publication, so I/O time does not consume the next
    /// append's activity window and failed attempts do not create activity.
    /// Keep the map bounded even when all remembered objects are recent.
    fn record_append_at(&self, bucket: &str, key: &str, now: Instant) {
        let Ok(mut map) = self.2.lock() else { return };
        let key = (bucket.to_owned(), key.to_owned());
        if let Some(last) = map.get_mut(&key) {
            *last = (*last).max(now);
            return;
        }
        if map.len() >= RECENT_APPENDS_CAP {
            map.retain(|_, last| now.saturating_duration_since(*last) < HOT_WINDOW);
            if map.len() >= RECENT_APPENDS_CAP {
                map.clear();
            }
        }
        map.insert(key, now);
    }

    /// The allocation the last append to this object reserved, if it is still for the manifest
    /// being extended and well inside its lease (`ttl_ms` is the lease it was made with). A
    /// reservation is used once: it is removed whether or not it fits, and an append that fails
    /// with it (an expired lease, a changed object) is retried by the caller with a fresh
    /// allocation.
    pub(crate) fn take_reservation(
        &self,
        expected: &ObjectManifest,
        pages: u32,
        ttl_ms: u64,
    ) -> Option<crate::staged_allocation::StagedAllocation> {
        let mut map = self.1.lock().ok()?;
        let (allocation, stored_at) = map.remove(&(expected.bucket.clone(), expected.key.clone()))?;
        (stored_at.elapsed() < std::time::Duration::from_millis(ttl_ms / 2)
            && allocation.append
            && allocation.expected_generation == expected.generation
            && allocation.page_count >= pages)
            .then_some(allocation)
    }

    /// Remember the allocation an append reserved for the next one to the same object.
    pub(crate) fn store_reservation(&self, allocation: crate::staged_allocation::StagedAllocation) {
        if let Ok(mut map) = self.1.lock() {
            // The metadata plane holds at most 128 allocations; keep well under half of them.
            if map.len() >= 48 {
                map.clear();
            }
            map.insert((allocation.bucket.clone(), allocation.key.clone()), (allocation, std::time::Instant::now()));
        }
    }
}

impl SdkPlane {
    pub fn new(mut client: RemoteMetaClient, options: GatewayOptions, compact: Arc<CompactState>) -> Self {
        // The placement and routes of an object's appends rarely change: reuse them for a short
        // while (well inside the metadata plane's health lifetime) instead of a round trip each.
        client.cache_append_plans(Duration::from_secs(1));
        client.share_read_leases(Arc::clone(&compact.3));
        Self {
            client,
            options,
            compact,
        }
    }

    /// One append under a staged allocation. An error here means nothing was published unless
    /// the failure says it is unconfirmed.
    fn append_compact_path(
        &mut self,
        expected: &ObjectManifest,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        use std::sync::atomic::Ordering;
        if self.compact.0.load(Ordering::Relaxed) == 0 {
            // The durable publication fence is a cluster-wide switch the leased allocation needs.
            match self.client.enable_publication_fence() {
                Ok(_) => self.compact.0.store(1, Ordering::Relaxed),
                Err(error) => return Err(self.compact_refused("enabling the publication fence", error)),
            }
        }
        let pages = data.len().div_ceil(self.options.write.page_bytes.max(1)).max(1) as u32;
        // Reserve the next allocation only for an object appended to repeatedly: a cold object
        // would leave a reservation nobody uses in the metadata plane's table until it expires.
        let hot = self
            .compact
            .append_is_hot_at(&expected.bucket, &expected.key, Instant::now());
        let reserve_next = self.options.reserve_next && hot;
        let reserved = if self.options.reserve_next {
            self.compact.take_reservation(expected, pages, self.options.staged_ttl_ms.min(RESERVATION_TTL_MS))
        } else {
            None
        };
        let allocation = match reserved {
            Some(allocation) => allocation,
            None => match self.client.allocate_leased_staged_object(
                &expected.bucket,
                &expected.key,
                Some(expected.generation),
                pages,
                true,
                self.options.staged_ttl_ms,
            ) {
                Ok(allocation) => allocation,
                Err(error) => return Err(self.compact_refused("allocating a staged append", error)),
            },
        };
        // The allocation came from the metadata plane a moment ago (an allocation, or the reply
        // that published the previous append, used within half its lease): no read-back.
        self.client.confirm_staged_allocation(allocation.allocation_id);
        let options = self.write_options();
        let result = if reserve_next {
            self.client
                .append_staged_object_compact_reserving(
                    &allocation,
                    expected.clone(),
                    data,
                    &options,
                    pages,
                    self.options.staged_ttl_ms.min(RESERVATION_TTL_MS),
                )
                .map(|(manifest, next)| {
                    if let Some(next) = next {
                        self.compact.store_reservation(next);
                    }
                    manifest
                })
        } else {
            self.client.append_staged_object_compact(&allocation, expected.clone(), data, &options)
        };
        match result {
            Ok(manifest) => {
                self.compact
                    .record_append_at(&expected.bucket, &expected.key, Instant::now());
                Ok(manifest)
            }
            Err(failure) => {
                let failure: PlaneFailure = failure.into();
                if !failure.unconfirmed && !outcome_may_be_committed(&failure.error) {
                    // Nothing was published; give the namespace back.
                    let _ = self.client.abort_staged_allocation(allocation.allocation_id);
                }
                Err(failure)
            }
        }
    }

    /// A refusal to start the compact path. A lost generation race is the caller's to retry;
    /// anything else switches the compact path off and says so once.
    fn compact_refused(&self, what: &str, error: ObjectError) -> PlaneFailure {
        use std::sync::atomic::Ordering;
        // Only a refusal to ENABLE the fence falls back: once the fence is on, the plain path
        // is refused cluster-wide, so every later write must stay on the owned path and a
        // failure there is reported as the failure it is.
        if what.contains("fence") {
            if self.compact.0.swap(2, Ordering::Relaxed) != 2 {
                eprintln!(
                    "cluster gateway: compact append is unavailable ({what}: {error}); using the plain publish path from here on"
                );
            }
        }
        PlaneFailure::from(error)
    }

    fn write_options(&self) -> ObjectWriteOptions {
        let mut options = self.options.write.clone();
        options.max_object_bytes = self.options.max_object_bytes;
        options.update_time_ms = now_ms();
        options.replica = self.replica_options();
        options
    }

    fn replica_options(&self) -> ReplicaClientOptions {
        let mut replica = self.options.replica.clone();
        replica.deadline = Some(Instant::now() + self.options.operation_timeout);
        replica
    }
}

impl ObjectPlane for SdkPlane {
    fn read_manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        self.client.read_manifest(bucket, key)
    }
    fn write(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        let options = self.write_options();
        // ONCE THE PUBLICATION FENCE IS ON, fresh extents may only be published under an owned
        // allocation, for every write and not just an append. A whole-object write takes the
        // same staged path; an empty one has no extents and needs no ownership.
        if self.compact.0.load(std::sync::atomic::Ordering::Relaxed) == 1 && !data.is_empty() {
            let pages = data.len().div_ceil(self.options.write.page_bytes.max(1)) as u32;
            let allocation = self.client.allocate_leased_staged_object(
                bucket,
                key,
                expected.as_ref().map(|manifest| manifest.generation),
                pages,
                false,
                self.options.staged_ttl_ms,
            )?;
            return match self
                .client
                .write_staged_object(&allocation, expected, data, &options)
            {
                Ok(manifest) => Ok(manifest),
                Err(failure) => {
                    let failure: PlaneFailure = failure.into();
                    if !failure.unconfirmed && !outcome_may_be_committed(&failure.error) {
                        let _ = self.client.abort_staged_allocation(allocation.allocation_id);
                    }
                    Err(failure)
                }
            };
        }
        self.client
            .write_object(bucket, key, expected, data, &options)
            .map_err(Into::into)
    }
    fn append(
        &mut self,
        expected: ObjectManifest,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        let options = self.write_options();
        self.client
            .append_object(expected, data, &options)
            .map_err(Into::into)
    }
    fn append_best(
        &mut self,
        expected: ObjectManifest,
        data: &[u8],
    ) -> Result<ObjectManifest, PlaneFailure> {
        use std::sync::atomic::Ordering;
        let pages = data.len().div_ceil(self.options.write.page_bytes.max(1));
        let route = choose_append_route(
            &self.options,
            self.compact.0.load(Ordering::Relaxed),
            expected.extents.len(),
            pages,
            data.is_empty(),
        );
        match route {
            AppendRoute::Compacting(max) => {
                let options = self.write_options();
                self.client
                    .append_object_compacting(expected, data, &options, max)
                    .map_err(Into::into)
            }
            AppendRoute::Staged => match self.append_compact_path(&expected, data) {
                Ok(manifest) => Ok(manifest),
                // Refused before anything was published and now switched off: the plain path
                // is safe.
                Err(failure)
                    if !failure.unconfirmed && self.compact.0.load(Ordering::Relaxed) == 2 =>
                {
                    self.append(expected, data)
                }
                Err(failure) => Err(failure),
            },
            AppendRoute::Plain => self.append(expected, data),
        }
    }
    fn read(&mut self, bucket: &str, key: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        let replica = self.replica_options();
        self.client
            .read_object(bucket, key, self.options.max_object_bytes, &replica)
    }
    fn read_range(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
        let replica = self.replica_options();
        // Only the object's generation and length are used from the manifest, so the metaserver
        // is asked for the extents under the range and nothing else.
        self.client.read_object_range_partial(
            bucket,
            key,
            offset,
            length,
            self.options.max_object_bytes,
            &replica,
        )
    }
    fn delete(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
        self.client.delete_object(bucket, key, now_ms())
    }
    fn copy(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
    ) -> Result<ObjectManifest, PlaneFailure> {
        use crate::manifest_copy::AtomicCopyFailure;
        self.client
            .copy_object_atomic(
                source_bucket,
                source_key,
                bucket,
                key,
                expected,
                self.options.max_object_bytes as u64,
                now_ms(),
            )
            .map_err(|failure| match failure {
                AtomicCopyFailure::Refused(error) => PlaneFailure { error, unconfirmed: false, attempted: Vec::new() },
                AtomicCopyFailure::Unconfirmed(error) => PlaneFailure { error, unconfirmed: true, attempted: Vec::new() },
            })
    }
    fn publish_unchanged(&mut self, expected: &ObjectManifest) -> Result<ObjectManifest, PlaneFailure> {
        let mut next = expected.clone();
        next.generation = expected
            .generation
            .checked_add(1)
            .ok_or_else(|| ObjectError::PreconditionFailed("manifest generation exhausted".into()))?;
        let outcome = crate::object_manifest::ManifestOutcome::decide(Some(expected.clone()), next)?;
        // A refusal means the generation moved; a timeout leaves this step unconfirmed. Either way
        // the resolve is repeated, and reading the object first makes that safe.
        self.client.publish_manifest(outcome, now_ms()).map_err(PlaneFailure::from)
    }
    fn list(
        &mut self,
        bucket: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<ManifestListPage, ObjectError> {
        self.client.list_manifests(bucket, after, limit)
    }
}

/// Which publication an append takes. The default options always choose [`AppendRoute::Plain`].
#[derive(Debug, PartialEq, Eq)]
pub enum AppendRoute {
    /// `append_object`: the full manifest is published.
    Plain,
    /// A staged allocation and a compact digest-bound publication.
    Staged,
    /// `append_object_compacting` with this extent bound (plain publication).
    Compacting(usize),
}

/// Whether the next append's staged allocation is reserved in the publishing commit.
///
/// `compact_append` is whether compact publication is on; `requested` is the value of
/// `MATRIXOBJECT_CLUSTER_RESERVE_NEXT` when it is set. Reserving follows compact publication
/// when nothing is asked for, `0` turns it off, and asking for it without compact publication
/// is refused. Needs every metadata voter to support the version 13 outcome.
pub fn resolve_reserve_next(compact_append: bool, requested: Option<usize>) -> Result<bool, &'static str> {
    let reserve = requested.map_or(compact_append, |value| value != 0);
    if reserve && !compact_append {
        return Err("MATRIXOBJECT_CLUSTER_RESERVE_NEXT needs MATRIXOBJECT_CLUSTER_COMPACT_APPEND=1");
    }
    Ok(reserve)
}

/// Pick the route for one append. `compact_state` is [`CompactState`]'s value: 2 means the
/// compact path was refused once and is off for the life of the process.
pub fn choose_append_route(
    options: &GatewayOptions,
    compact_state: u8,
    extents: usize,
    pages: usize,
    empty: bool,
) -> AppendRoute {
    if let Some(max) = options.compact_extents {
        if extents + pages > max {
            return AppendRoute::Compacting(max);
        }
    }
    if options.compact_append && compact_state != 2 && !empty {
        return AppendRoute::Staged;
    }
    AppendRoute::Plain
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// Idle metadata connections the gateway keeps, from `MATRIXOBJECT_CLUSTER_POOL_MAX` when it is a
/// positive number (else 64) and never fewer than the worker threads: each open HTTP connection
/// holds a metadata connection for the length of a request, and one more than the pool holds is
/// opened for that request and dropped afterwards.
pub fn resolve_pool_max(requested: Option<&str>, worker_threads: usize) -> usize {
    requested
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(64)
        .max(worker_threads.max(1))
}

/// A positive number from an environment value, else `default`.
pub fn resolve_positive(requested: Option<&str>, default: usize) -> usize {
    requested
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(default)
}

/// What a manifest costs in the cache, in bytes: its header plus a fixed allowance per extent
/// (group name, SHA-256 and the fixed fields are about this much in memory). An estimate that
/// grows with the extents, which is the part that varies.
pub(crate) fn manifest_weight(manifest: &ObjectManifest) -> usize {
    256 + manifest.extents.len() * 192
}

/// The manifests the gateway remembers so a repeated append can skip reading one: at most a
/// fixed number, the least recently used leaving first. (Emptying the whole cache when it was
/// full, as this once did, made every object miss at once whenever the working set exceeded it.)
#[derive(Default)]
pub(crate) struct ManifestCache {
    entries: HashMap<(String, String), (ObjectManifest, u64, usize)>,
    /// Entries by the tick of their last use, oldest first.
    order: std::collections::BTreeMap<u64, (String, String)>,
    tick: u64,
    /// The summed `manifest_weight` of the entries.
    bytes: usize,
}

impl ManifestCache {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get(&mut self, bucket: &str, key: &str) -> Option<ObjectManifest> {
        let id = (bucket.to_owned(), key.to_owned());
        let (manifest, used, _) = self.entries.get_mut(&id)?;
        self.order.remove(used);
        self.tick += 1;
        *used = self.tick;
        self.order.insert(self.tick, id);
        Some(manifest.clone())
    }

    /// Keep `manifest`, dropping the least recently used entries until at most `capacity` entries
    /// and `max_bytes` bytes remain. A manifest heavier than `max_bytes` by itself is not kept.
    pub(crate) fn insert(&mut self, manifest: ObjectManifest, capacity: usize, max_bytes: usize) {
        let id = (manifest.bucket.clone(), manifest.key.clone());
        self.remove(&id.0, &id.1);
        let weight = manifest_weight(&manifest);
        if weight > max_bytes {
            return;
        }
        while self.entries.len() >= capacity.max(1) || self.bytes + weight > max_bytes {
            let Some((_, oldest)) = self.order.pop_first() else { break };
            if let Some((_, _, freed)) = self.entries.remove(&oldest) {
                self.bytes -= freed;
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, id.clone());
        self.entries.insert(id, (manifest, self.tick, weight));
        self.bytes += weight;
    }

    pub(crate) fn remove(&mut self, bucket: &str, key: &str) {
        if let Some((_, used, weight)) = self.entries.remove(&(bucket.to_owned(), key.to_owned())) {
            self.order.remove(&used);
            self.bytes -= weight;
        }
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// The S3 door over the cluster.
pub struct ClusterGateway {
    connect: Box<Connector>,
    pool: Mutex<Vec<Box<dyn ObjectPlane>>>,
    manifests: Mutex<ManifestCache>,
    /// The attempts of appends this gateway served that carried an attempt id (`?append-resolve` looks them up).
    attempts: Mutex<AttemptTable>,
    options: GatewayOptions,
    bucket_hint: String,
}

/// An error that leaves the connection fit for the next request.
fn connection_survives(error: &ObjectError) -> bool {
    matches!(
        error,
        ObjectError::NotFound(_) | ObjectError::PreconditionFailed(_) | ObjectError::InvalidArgument(_)
    )
}

impl ClusterGateway {
    pub fn new<F>(connect: F, options: GatewayOptions, bucket_hint: impl Into<String>) -> Self
    where
        F: Fn() -> Result<Box<dyn ObjectPlane>, ObjectError> + Send + Sync + 'static,
    {
        Self {
            connect: Box::new(connect),
            pool: Mutex::new(Vec::new()),
            manifests: Mutex::new(ManifestCache::default()),
            attempts: Mutex::new(AttemptTable::default()),
            options,
            bucket_hint: bucket_hint.into(),
        }
    }

    /// A gateway whose connections are `RemoteMetaClient`s opened with `open`.
    pub fn over_sdk<F>(open: F, options: GatewayOptions, bucket_hint: impl Into<String>) -> Self
    where
        F: Fn() -> Result<RemoteMetaClient, ObjectError> + Send + Sync + 'static,
    {
        let sdk_options = options.clone();
        let compact = Arc::new(CompactState::default());
        Self::new(
            move || {
                Ok(Box::new(SdkPlane::new(open()?, sdk_options.clone(), Arc::clone(&compact)))
                    as Box<dyn ObjectPlane>)
            },
            options,
            bucket_hint,
        )
    }

    /// Build a compact SDK gateway after durably enabling its publication fence.
    /// Every metadata voter and journal reader must support compact publication.
    /// The first connection is retained, and every connection shares the enabled state,
    /// so creating a new object works when a previous process enabled the fence.
    pub fn over_sdk_with_publication_fence<F>(open: F, options: GatewayOptions, bucket_hint: impl Into<String>) -> Result<Self, ObjectError>
    where
        F: Fn() -> Result<RemoteMetaClient, ObjectError> + Send + Sync + 'static,
    {
        if !options.compact_append || options.compact_extents.is_some() {
            return Err(ObjectError::InvalidArgument("compact publication needs compact append without plain extent merging".into()));
        }
        let mut first = open()?;
        first.enable_publication_fence()?;
        let compact = Arc::new(CompactState::default());
        compact.0.store(1, std::sync::atomic::Ordering::Release);
        let first = std::sync::Mutex::new(Some(first));
        let sdk_options = options.clone();
        Ok(Self::new(
            move || {
                let client = match first.lock().ok().and_then(|mut first| first.take()) {
                    Some(client) => client,
                    None => open()?,
                };
                Ok(Box::new(SdkPlane::new(client, sdk_options.clone(), Arc::clone(&compact))) as Box<dyn ObjectPlane>)
            },
            options,
            bucket_hint,
        ))
    }

    /// Connection options for a metadata client, from one timeout.
    pub fn meta_options(timeout: Duration) -> MetaClientOptions {
        MetaClientOptions {
            connect_timeout: timeout,
            read_timeout: timeout,
            write_timeout: timeout,
            ..Default::default()
        }
    }

    /// Run `work` on a pooled connection, returning it afterwards unless the failure says the
    /// connection may be broken.
    fn with_plane<T>(
        &self,
        work: impl FnOnce(&mut dyn ObjectPlane) -> Result<T, ObjectError>,
    ) -> Result<T, ObjectError> {
        let pooled = self.pool.lock().ok().and_then(|mut pool| pool.pop());
        let mut plane = match pooled {
            Some(plane) => plane,
            None => (self.connect)()?,
        };
        let result = work(plane.as_mut());
        let keep = match &result {
            Ok(_) => true,
            Err(error) => connection_survives(error),
        };
        if keep {
            if let Ok(mut pool) = self.pool.lock() {
                if pool.len() < self.options.pool_max {
                    pool.push(plane);
                }
            }
        }
        result
    }

    fn remember(&self, manifest: &ObjectManifest) {
        if let Ok(mut cache) = self.manifests.lock() {
            cache.insert(manifest.clone(), self.options.cache_entries.max(1), self.options.cache_bytes);
        }
    }

    fn forget(&self, bucket: &str, key: &str) {
        if let Ok(mut cache) = self.manifests.lock() {
            cache.remove(bucket, key);
        }
    }

    fn cached(&self, bucket: &str, key: &str) -> Option<ObjectManifest> {
        self.manifests.lock().ok().and_then(|mut cache| cache.get(bucket, key))
    }

    fn route(&self, request: &Request) -> Response {
        match request.path.as_str() {
            "/healthz" | "/health" => return self.health(request),
            "/" => {
                return self.list_buckets(request);
            }
            _ => {}
        }
        let trimmed = request.path.trim_start_matches('/');
        let (bucket, key) = match trimmed.split_once('/') {
            Some((bucket, key)) => (bucket, key),
            None => (trimmed, ""),
        };
        if bucket.is_empty() {
            return s3::error_response(
                s3::S3ErrorCode::InvalidArgument,
                "the path names no bucket; address a bucket as /{bucket} and an object as /{bucket}/{key}",
                &request.path,
            );
        }
        if key.is_empty() {
            self.bucket_request(request, bucket)
        } else {
            self.object_request(request, bucket, key)
        }
    }
}

/// How many times a resolve re-reads the object when the generation moves under its fence.
const RESOLVE_ROUNDS: usize = 3;

/// What an append whose outcome was not known turned out to be.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendResolution {
    /// The append's bytes are at the offset it named.
    Landed,
    /// They are not, and the append can never land: retry it as a new append.
    Absent,
    /// Bytes equal to the append's are at that offset, but nothing proves they are THIS attempt's: another writer's
    /// identical append at the same offset looks the same. The caller decides from its own record of what it
    /// acknowledged; this gateway never answers "landed" for it.
    Ambiguous,
}

/// 64 hex characters as 32 bytes.
fn decode_hex_32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        out[index] = (high * 16 + low) as u8;
    }
    Some(out)
}

impl ClusterGateway {
    /// Settle an append whose outcome is not known (a timeout, a reset, a consensus deadline).
    ///
    /// "Landed" means THIS ATTEMPT landed, never that equal bytes are there. Every attempt allocates its own pages, so
    /// the extents it published (`attempted`) are its identity: the committed manifest holds them if and only if the
    /// attempt landed. Content cannot say that: two writers appending identical bytes at one offset leave one copy,
    /// and a byte comparison told both of them it landed (a lost write).
    ///
    /// Every append publishes with a compare-and-set on the generation it read, so once the object has moved past
    /// that read no attempt still in flight can publish. The answers:
    ///
    /// * `attempted` is known and the manifest holds the attempt's extents over `[offset, offset + length)`:
    ///   [`AppendResolution::Landed`], exactly, even if later appends followed;
    /// * `attempted` is known and the object already extends past the offset without them: another append took the
    ///   offset and the generation moved, so the attempt can never land: `Absent`;
    /// * `attempted` is unknown (a gateway that never saw the attempt, or a plane that does not report it) and bytes
    ///   equal to the append's (`evidence_sha256`, or any bytes when none is given) fill the range:
    ///   [`AppendResolution::Ambiguous`], never "landed";
    /// * the object ends exactly at the offset: nothing landed there yet. Publish its extents again at the next
    ///   generation; success fences the attempt (`Absent`), and a refusal because the generation moved sends the
    ///   resolve round again. An object that does not exist is fenced by creating it empty;
    /// * the object ends before the offset or the range holds different bytes: `Absent`.
    ///
    /// After `Absent` the caller retries the append as a new request and it lands exactly once. An error means the
    /// outcome is STILL undecided (a timeout, a metadata outage, an object that kept changing): resolve again, and
    /// never read it as a refusal of the append.
    pub fn resolve_append(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
        attempted: &[crate::object_manifest::ObjectExtent],
        evidence_sha256: Option<&[u8]>,
    ) -> Result<AppendResolution, ObjectError> {
        let end = offset
            .checked_add(length)
            .filter(|_| length > 0)
            .ok_or_else(|| ObjectError::InvalidArgument("an append resolve needs a nonempty range inside u64".into()))?;
        if evidence_sha256.is_some_and(|sha| sha.len() != 32) {
            return Err(ObjectError::InvalidArgument("an append resolve needs a 32 byte SHA-256".into()));
        }
        for _ in 0..RESOLVE_ROUNDS {
            // What this gateway remembers of the object may predate the fence.
            if let Ok(mut cache) = self.manifests.lock() {
                cache.remove(bucket, key);
            }
            let manifest = match self.with_plane(|plane| plane.read_manifest(bucket, key)) {
                Ok(manifest) => Some(manifest),
                Err(ObjectError::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
            let live = manifest.as_ref().filter(|m| !m.deleted);
            let current = live.map_or(0, |m| m.length);
            if !attempted.is_empty() {
                let mine = extents_over(attempted, offset, end);
                if !mine.is_empty() && live.is_some_and(|m| mine.iter().all(|extent| m.extents.contains(extent))) {
                    return Ok(AppendResolution::Landed);
                }
                if current != offset {
                    return Ok(AppendResolution::Absent);
                }
            } else if current >= end {
                let present = match evidence_sha256 {
                    Some(sha) => {
                        let (_, bytes) = self.with_plane(|plane| plane.read_range(bucket, key, offset, length))?;
                        ring::digest::digest(&ring::digest::SHA256, &bytes).as_ref() == sha
                    }
                    None => true,
                };
                return Ok(if present { AppendResolution::Ambiguous } else { AppendResolution::Absent });
            } else if current != offset {
                return Ok(AppendResolution::Absent);
            }
            let fenced = match &manifest {
                Some(manifest) => self.with_plane(|plane| plane.publish_unchanged(manifest).map(|_| ()).map_err(|failure| failure.error)),
                None => self.with_plane(|plane| plane.write(bucket, key, None, &[]).map(|_| ()).map_err(|failure| failure.error)),
            };
            match fenced {
                Ok(()) => {
                    if let Ok(mut cache) = self.manifests.lock() {
                        cache.remove(bucket, key);
                    }
                    return Ok(AppendResolution::Absent);
                }
                Err(ObjectError::PreconditionFailed(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ObjectError::PreconditionFailed(
            "the object kept changing while an append was being resolved; resolve again".into(),
        ))
    }

    /// `POST /bucket/key?append-resolve&attempt=<32 hex>&offset=O&length=N[&sha256=<hex>]`. The attempt id is the one
    /// the append carried in `x-amz-attempt-id`; `sha256` is optional evidence about the bytes.
    fn append_resolve_request(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let bad = |message: &str| s3::error_response(s3::S3ErrorCode::InvalidArgument, message, &resource);
        let number = |name: &str| request.query_param(name).and_then(|value| value.parse::<u64>().ok());
        let (Some(offset), Some(length)) = (number("offset"), number("length")) else {
            return bad("append-resolve needs numeric offset and length parameters");
        };
        let Some(id) = request.query_param("attempt").and_then(|value| decode_hex_16(&value)) else {
            return bad("append-resolve needs the attempt parameter: the 32 hex characters the append sent in x-amz-attempt-id");
        };
        let digest = match request.query_param("sha256") {
            None => None,
            Some(value) => match decode_hex_32(&value) {
                Some(digest) => Some(digest),
                None => return bad("append-resolve's sha256 parameter must be 64 hex characters"),
            },
        };
        // What this gateway saw of the attempt: only a record for the SAME range of the same object counts.
        let attempted = self
            .attempts
            .lock()
            .ok()
            .and_then(|table| {
                table.records.get(&id).filter(|r| r.bucket == bucket && r.key == key && r.offset == offset && r.length == length).map(|r| r.extents.clone())
            })
            .unwrap_or_default();
        match self.resolve_append(bucket, key, offset, length, &attempted, digest.as_ref().map(|d| d.as_slice())) {
            Ok(resolution) => {
                let word = match resolution {
                    AppendResolution::Landed => "landed",
                    AppendResolution::Absent => "absent",
                    AppendResolution::Ambiguous => "ambiguous",
                };
                Response::text(200, format!("{word}\n")).with_header("x-append-resolution", word)
            }
            // Anything but a malformed request means the resolve is undecided, never "refused": a
            // client that read a definitive refusal would retry as a new append and could double it.
            Err(error @ ObjectError::InvalidArgument(_)) => error_response(error, &resource, false),
            Err(error) => s3::error_response(s3::S3ErrorCode::ServiceUnavailable, &format!("append undecided, resolve again: {error}"), &resource),
        }
    }

    /// The answer for an append whose bytes are in the object but cannot be proved to be this attempt's.
    fn append_ambiguous(&self, bucket: &str, key: &str, resource: &str) -> Response {
        let length = self
            .with_plane(|plane| plane.read_manifest(bucket, key))
            .ok()
            .filter(|manifest| !manifest.deleted)
            .map_or(0, |manifest| manifest.length);
        s3::error_response(
            s3::S3ErrorCode::AppendAmbiguous,
            &format!(
                "the outcome of this append is ambiguous: the object holds {length} bytes and bytes equal to this append's may already be at its offset, but nothing proves they are this attempt's (another writer's identical append at the same offset looks the same); decide from your own record of the length you last acknowledged"
            ),
            resource,
        )
        .with_header("x-amz-append-state", "ambiguous".to_string())
        .with_header("x-amz-object-size", length.to_string())
    }

    fn health(&self, request: &Request) -> Response {
        if !matches!(request.method.as_str(), "GET" | "HEAD") {
            return Response::text(405, "allowed: GET, HEAD\n")
                .with_header("allow", "GET, HEAD".to_string());
        }
        let bucket = self.bucket_hint.clone();
        match self.with_plane(|plane| plane.list(&bucket, None, 1).map(|_| ())) {
            Ok(()) => Response::text(200, format!("ok bucket={bucket} backend=cluster\n")),
            Err(error) => Response::text(503, format!("the cluster is not reachable: {error}\n")),
        }
    }

    fn bucket_request(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let method = request.method.as_str();
        // The sub-resources this door serves, which S3 selects on the same method as the plain
        // verb, come first.
        if method == "GET" && request.query_param("uploads").is_some() {
            return self.list_multipart_uploads(request, bucket);
        }
        if method == "POST" && request.query_param("delete").is_some() {
            return self.delete_objects(request, bucket);
        }
        if method == "GET" {
            for probe in ["versioning", "acl"] {
                if request.query_param(probe).is_some() {
                    return self.probe_answer(probe);
                }
            }
        }
        // Every sub-resource this door does not serve is named in the refusal.
        for sub in [
            "uploads", "versioning", "versions", "cors", "delete", "tagging", "policy", "acl",
            "lifecycle", "notification",
        ] {
            if request.query_param(sub).is_some() {
                return not_implemented(&format!("the bucket sub-resource ?{sub}"), &resource);
            }
        }
        match method {
            "GET" if request.query_param("location").is_some() => s3::xml_response(
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"></LocationConstraint>".to_string(),
            ),
            "GET" => self.list_bucket(request, bucket),
            // Buckets are implicit in the replica plane: a bucket exists as soon as an object
            // is published under it, so creating or probing one has nothing to check or store.
            "PUT" | "HEAD" => match crate::validate_bucket(bucket) {
                Ok(()) => Response::status(200).with_header("location", resource),
                Err(error) => error_response(error, &resource, true),
            },
            "DELETE" => self.delete_bucket(bucket),
            _ => method_not_allowed("GET, PUT, HEAD, DELETE, POST", &resource),
        }
    }

    fn object_request(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let method = request.method.as_str();
        let upload_id = request.query_param("uploadId");
        let part_number = request.query_param("partNumber");
        let starts_upload = request.query_param("uploads").is_some();
        if s3_verbs::is_reserved(key) {
            return s3::error_response(
                s3::S3ErrorCode::InvalidArgument,
                &format!("keys under {} are reserved for multipart uploads", s3_verbs::RESERVED_PREFIX),
                &resource,
            );
        }
        // The multipart family, selected by `uploads`, `uploadId` and `partNumber`.
        match (method, starts_upload, upload_id.as_deref(), part_number.as_deref()) {
            ("POST", true, None, None) => return self.create_multipart_upload(bucket, key),
            ("PUT", false, Some(id), Some(number)) => {
                if request.header("x-amz-copy-source").is_some() {
                    return not_implemented("UploadPartCopy", &resource);
                }
                return self.upload_part(request, bucket, key, id, number);
            }
            ("POST", false, Some(id), None) => {
                return self.complete_multipart_upload(request, bucket, key, id)
            }
            ("DELETE", false, Some(id), None) => return self.abort_multipart_upload(bucket, key, id),
            ("GET", false, Some(id), None) => return self.list_parts(request, bucket, key, id),
            _ => {}
        }
        if starts_upload || upload_id.is_some() || part_number.is_some() {
            return method_not_allowed("POST, PUT, DELETE, GET", &resource);
        }
        // Reads of an object's ACL and tags are answered (objects are private and untagged);
        // changing them is not served.
        if method == "GET" {
            for probe in ["acl", "tagging"] {
                if request.query_param(probe).is_some() {
                    return match self.with_plane(|plane| plane.read_manifest(bucket, key)) {
                        Ok(manifest) if !manifest.deleted => self.probe_answer(probe),
                        Ok(_) => no_such_key(&resource),
                        Err(error) => error_response(error, &resource, false),
                    };
                }
            }
        }
        for sub in [
            "versionId", "tagging", "acl", "retention", "legal-hold", "restore", "select",
        ] {
            if request.query_param(sub).is_some() {
                return not_implemented(&format!("the object sub-resource ?{sub}"), &resource);
            }
        }
        match method {
            "PUT" => {
                if let Some(source) = request.header("x-amz-copy-source") {
                    return self.copy_object(request, bucket, key, source);
                }
                self.put_object(request, bucket, key)
            }
            "GET" => self.get_object(request, bucket, key, false),
            "HEAD" => self.get_object(request, bucket, key, true),
            "DELETE" => self.delete_object(bucket, key),
            "POST" if request.query_param("append-resolve").is_some() => {
                self.append_resolve_request(request, bucket, key)
            }
            _ => method_not_allowed("PUT, GET, HEAD, DELETE, POST", &resource),
        }
    }

    fn put_object(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let offset = match request
            .header(WRITE_OFFSET_HEADER)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            None => None,
            Some(value) => match value.parse::<u64>() {
                Ok(offset) => Some(offset),
                Err(_) => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        &format!("{WRITE_OFFSET_HEADER} must be a whole number of bytes, and {value:?} is not"),
                        &resource,
                    )
                }
            },
        };
        let only_if_absent = request
            .header("if-none-match")
            .is_some_and(|value| value.trim() == "*");
        let required_etag = request.header("if-match").map(str::to_string);
        let attempt = match request.header(ATTEMPT_HEADER).map(str::trim).filter(|value| !value.is_empty()) {
            None => None,
            Some(value) => match decode_hex_16(value) {
                Some(id) => Some(id),
                None => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        &format!("{ATTEMPT_HEADER} must be 32 hex characters (16 bytes), and {value:?} is not"),
                        &resource,
                    )
                }
            },
        };
        match offset {
            Some(offset) => self.append_object(bucket, key, offset, &request.body, attempt),
            None => self.replace_object(
                bucket,
                key,
                &request.body,
                only_if_absent,
                required_etag.as_deref(),
            ),
        }
    }

    /// `PutObject` that replaces: publish a new generation on top of whatever is there.
    fn replace_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        only_if_absent: bool,
        required_etag: Option<&str>,
    ) -> Response {
        match self.replace_manifest(bucket, key, body, only_if_absent, required_etag) {
            Ok(manifest) => object_written(&manifest),
            Err(response) => response,
        }
    }

    /// The replacing write, answering with the manifest it published or the S3 refusal.
    /// `required_etag` is an `If-Match`: the object must exist and be that version.
    fn replace_manifest(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        only_if_absent: bool,
        required_etag: Option<&str>,
    ) -> Result<ObjectManifest, Response> {
        let resource = format!("/{bucket}/{key}");
        const ATTEMPTS: usize = 4;
        for _ in 0..ATTEMPTS {
            let outcome = self.with_plane(|plane| {
                let current = match plane.read_manifest(bucket, key) {
                    Ok(manifest) => Some(manifest),
                    Err(ObjectError::NotFound(_)) => None,
                    Err(error) => return Err(error),
                };
                if only_if_absent && current.as_ref().is_some_and(|m| !m.deleted) {
                    return Ok(Err(PlaneFailure::from(ObjectError::PreconditionFailed(
                        "If-None-Match: * and the object exists".into(),
                    ))));
                }
                if let Some(required) = required_etag {
                    let live = current.as_ref().filter(|m| !m.deleted);
                    if !live.is_some_and(|m| s3_verbs::etag_matches(required, &etag_of(m.generation))) {
                        return Ok(Err(PlaneFailure::from(ObjectError::PreconditionFailed(
                            "If-Match does not name the current version of the object".into(),
                        ))));
                    }
                }
                Ok(plane.write(bucket, key, current, body))
            });
            match outcome {
                Err(error) => return Err(error_response(error, &resource, false)),
                Ok(Ok(manifest)) => {
                    self.remember(&manifest);
                    return Ok(manifest);
                }
                Ok(Err(failure)) => {
                    self.forget(bucket, key);
                    if failure.unconfirmed || outcome_may_be_committed(&failure.error) {
                        // Equal values can belong to another writer. A conditional
                        // mutation needs proof that this attempt was published.
                        if only_if_absent || required_etag.is_some() {
                            return Err(unconfirmed(&failure.error, &resource));
                        }
                        if let Some(manifest) = self.resolve_landed_replace(bucket, key, body) {
                            self.remember(&manifest);
                            return Ok(manifest);
                        }
                    }
                    if failure.unconfirmed {
                        return Err(unconfirmed(&failure.error, &resource));
                    }
                    // Another writer published between the read and the write; the conditional
                    // publish refused it, so look again and decide on what is there now. A
                    // precondition of the request itself is decided once, not retried.
                    if lost_race(&failure.error) && !only_if_absent && required_etag.is_none() {
                        continue;
                    }
                    return Err(error_response(failure.error, &resource, false));
                }
            }
        }
        Err(s3::error_response(
            s3::S3ErrorCode::PreconditionFailed,
            "the object kept changing under this write; gave up after several attempts",
            &resource,
        ))
    }

    /// `PutObject` with a write offset: an append that lands at the end or not at all.
    fn append_object(&self, bucket: &str, key: &str, offset: u64, body: &[u8], attempt: Option<[u8; 16]>) -> Response {
        let resource = format!("/{bucket}/{key}");
        const ATTEMPTS: usize = 4;
        let mut last_staged_mismatch: Option<ObjectError> = None;
        // The refusal that made the last attempt retry, so a gateway that gives up can say why.
        let mut last_retried = String::new();
        for round in 0..ATTEMPTS {
            // A retry exists because the end moved, so it never trusts the cache.
            let cached = if round == 0 {
                self.cached(bucket, key)
            } else {
                None
            };
            let used_cache = cached.is_some();
            let outcome = self.with_plane(|plane| {
                let current = match cached {
                    Some(manifest) => Some(manifest),
                    None => match plane.read_manifest(bucket, key) {
                        Ok(manifest) => Some(manifest),
                        Err(ObjectError::NotFound(_)) => None,
                        Err(error) => return Err(error),
                    },
                };
                let live_length = current
                    .as_ref()
                    .map_or(0, |manifest| if manifest.deleted { 0 } else { manifest.length });
                if offset != live_length {
                    return Ok(Err((
                        PlaneFailure::from(ObjectError::PreconditionFailed(format!(
                            "{WRITE_OFFSET_HEADER} is {offset} and the object is {live_length} bytes long, so this append would not land at its end"
                        ))),
                        true,
                    )));
                }
                let result = match current {
                    Some(manifest) if !manifest.deleted => {
                        let pages = body.len().div_ceil(self.options.write.page_bytes.max(1));
                        if self.options.compact_extents.is_none()
                            && manifest.extents.len() + pages > MAX_OBJECT_EXTENTS
                        {
                            return Ok(Err((
                                PlaneFailure::from(ObjectError::InvalidArgument(format!(
                                    "append refused: the object already holds {} extents and this append adds {pages}; the replica plane allows {MAX_OBJECT_EXTENTS} extents per object",
                                    manifest.extents.len()
                                ))),
                                true,
                            )));
                        }
                        plane.append_best(manifest, body)
                    }
                    other => plane.write(bucket, key, other, body),
                };
                Ok(result.map_err(|failure| (failure, false)))
            });
            match outcome {
                Err(error) => return error_response(error, &resource, false),
                Ok(Ok(manifest)) => {
                    self.note_attempt(attempt, bucket, key, offset, body.len() as u64, &manifest.extents);
                    self.remember(&manifest);
                    return with_attempt(object_written(&manifest), attempt);
                }
                Ok(Err((failure, decided))) => {
                    self.forget(bucket, key);
                    self.note_attempt(attempt, bucket, key, offset, body.len() as u64, &failure.attempted);
                    // A write whose outcome is not known is resolved by reading, never by
                    // reporting an error for bytes that may be in the object: the client would
                    // retry, and its retry would append them a second time. It is resolved by
                    // the attempt's own extents (`attempted`), never by comparing bytes: equal
                    // bytes in the object may be another writer's identical append.
                    if failure.unconfirmed || outcome_may_be_committed(&failure.error) {
                        let sha = ring::digest::digest(&ring::digest::SHA256, body);
                        match self.resolve_append(bucket, key, offset, body.len() as u64, &failure.attempted, Some(sha.as_ref())) {
                            Ok(AppendResolution::Landed) => {
                                if let Ok(manifest) = self.with_plane(|plane| plane.read_manifest(bucket, key)) {
                                    self.remember(&manifest);
                                    return with_attempt(object_written(&manifest), attempt);
                                }
                            }
                            Ok(AppendResolution::Ambiguous) => return self.append_ambiguous(bucket, key, &resource),
                            // Fenced: it did not land and now cannot. The client may retry as a new append.
                            Ok(AppendResolution::Absent) => {
                                return s3::error_response(
                                    s3::S3ErrorCode::ServiceUnavailable,
                                    &format!("this append did not land and can no longer land (the object was fenced); retry it: {}", failure.error),
                                    &resource,
                                )
                            }
                            // Still undecided: answer as before.
                            Err(_) => {}
                        }
                    }
                    if failure.unconfirmed {
                        return unconfirmed(&failure.error, &resource);
                    }
                    // A refusal decided from the manifest this call read answers the client as
                    // it stands; only a lost publish race is worth another look.
                    if !decided && lost_race(&failure.error) {
                        last_retried = failure.error.to_string();
                        if staged_state_mismatch(&failure.error) {
                            last_staged_mismatch = Some(failure.error);
                        }
                        continue;
                    }
                    // A refusal decided from a REMEMBERED manifest may only mean the memory is
                    // stale (another gateway appended), so it is checked once against a read.
                    if decided && used_cache && lost_race(&failure.error) {
                        last_retried = failure.error.to_string();
                        continue;
                    }
                    return error_response(failure.error, &resource, false);
                }
            }
        }
        // A staged mismatch that survived every fresh allocation is the cluster's state and not
        // a competing writer: say so, as a retryable 503 with the reason.
        if let Some(error) = last_staged_mismatch {
            return error_response(error, &resource, false);
        }
        s3::error_response(
            s3::S3ErrorCode::PreconditionFailed,
            &format!(
                "the object kept changing under this append; gave up after several attempts (the last refusal: {last_retried})"
            ),
            &resource,
        )
    }

    /// Remember what an attempt that carried an id did, for `?append-resolve`.
    fn note_attempt(&self, attempt: Option<[u8; 16]>, bucket: &str, key: &str, offset: u64, length: u64, extents: &[crate::object_manifest::ObjectExtent]) {
        let Some(id) = attempt else { return };
        if let Ok(mut table) = self.attempts.lock() {
            table.record(
                id,
                AttemptRecord {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    offset,
                    length,
                    extents: extents_over(extents, offset, offset.saturating_add(length)),
                },
            );
        }
    }

    /// After a whole-object write whose outcome is unknown: is the object now exactly `body`?
    fn resolve_landed_replace(&self, bucket: &str, key: &str, body: &[u8]) -> Option<ObjectManifest> {
        let (manifest, bytes) = self.with_plane(|plane| plane.read(bucket, key)).ok()?;
        (!manifest.deleted && bytes == body).then_some(manifest)
    }

    fn get_object(&self, request: &Request, bucket: &str, key: &str, head: bool) -> Response {
        let resource = format!("/{bucket}/{key}");
        if head {
            return match self.with_plane(|plane| plane.read_manifest(bucket, key)) {
                Ok(manifest) if !manifest.deleted => {
                    if let Some(refused) = s3_verbs::read_condition(request, &manifest, &resource) {
                        return refused;
                    }
                    let mut response = Response::status(200);
                    object_headers(&mut response, &manifest);
                    response
                }
                Ok(_) => no_such_key(&resource),
                Err(error) => error_response(error, &resource, false),
            };
        }
        let range = request.header("range").map(str::trim);
        // A closed range inside the object is the hot read of a shared log, and it can be asked
        // of the data groups without a separate manifest read.
        let conditional = request.header("if-match").is_some() || request.header("if-none-match").is_some();
        if let Some((start, length)) = range.and_then(closed_range).filter(|_| !conditional) {
            match self.with_plane(|plane| plane.read_range(bucket, key, start, length)) {
                Ok((manifest, data)) => {
                    let last = start + length - 1;
                    let mut response = Response {
                        status: 206,
                        headers: Vec::new(),
                        body: data,
                    };
                    object_headers(&mut response, &manifest);
                    response.headers.push((
                        "content-range".into(),
                        format!("bytes {start}-{last}/{}", manifest.length),
                    ));
                    return response;
                }
                // Beyond the end, or the object is gone: the general path below says which.
                Err(ObjectError::InvalidArgument(_)) | Err(ObjectError::NotFound(_)) => {}
                Err(error) => return error_response(error, &resource, false),
            }
        }
        let manifest = match self.with_plane(|plane| plane.read_manifest(bucket, key)) {
            Ok(manifest) if !manifest.deleted => manifest,
            Ok(_) => return no_such_key(&resource),
            Err(error) => return error_response(error, &resource, false),
        };
        if let Some(refused) = s3_verbs::read_condition(request, &manifest, &resource) {
            return refused;
        }
        match http::resolve_range(request.header("range"), manifest.length) {
            ResolvedRange::Whole => match self.with_plane(|plane| plane.read(bucket, key)) {
                Ok((manifest, data)) => {
                    let mut response = Response {
                        status: 200,
                        headers: Vec::new(),
                        body: data,
                    };
                    object_headers(&mut response, &manifest);
                    response
                }
                Err(error) => error_response(error, &resource, false),
            },
            ResolvedRange::Partial { offset, length } => {
                match self.with_plane(|plane| plane.read_range(bucket, key, offset, length)) {
                    Ok((manifest, data)) => {
                        let mut response = Response {
                            status: 206,
                            headers: Vec::new(),
                            body: data,
                        };
                        object_headers(&mut response, &manifest);
                        response.headers.push((
                            "content-range".into(),
                            format!("bytes {offset}-{}/{}", offset + length - 1, manifest.length),
                        ));
                        response
                    }
                    Err(error) => error_response(error, &resource, false),
                }
            }
            ResolvedRange::Unsatisfiable => s3::error_response(
                s3::S3ErrorCode::InvalidRange,
                &format!(
                    "the requested range lies outside the object, which is {} bytes",
                    manifest.length
                ),
                &resource,
            )
            .with_header("content-range", format!("bytes */{}", manifest.length)),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        self.forget(bucket, key);
        match self.with_plane(|plane| plane.delete(bucket, key)) {
            Ok(manifest) => {
                self.remember(&manifest);
                Response::status(204)
            }
            // Deleting what is not there is a success, as S3 has it.
            Err(ObjectError::NotFound(_)) => Response::status(204),
            Err(error) => error_response(error, &resource, false),
        }
    }

    /// `ListObjectsV2`: keys in order, tombstones skipped, paged by the last key returned.
    fn list_bucket(&self, request: &Request, bucket: &str) -> Response {
        let resource = format!("/{bucket}");
        let delimiter = request.query_param("delimiter").filter(|d| !d.is_empty());
        let prefix = request.query_param("prefix").unwrap_or_default();
        let continuation = request
            .query_param("continuation-token")
            .or_else(|| request.query_param("marker"));
        let start_after = request.query_param("start-after");
        let resume = continuation.clone().or_else(|| start_after.clone());
        let max_keys = match request.query_param("max-keys") {
            Some(raw) => match raw.parse::<usize>() {
                Ok(parsed) => parsed.clamp(1, super::s3_limits::MAX_LIST_LIMIT),
                Err(_) => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        "max-keys is not a number",
                        &resource,
                    )
                }
            },
            None => super::s3_limits::DEFAULT_LIST_LIMIT,
        };
        let walked = self.with_plane(|plane| {
            let mut entries: Vec<s3::ListEntry> = Vec::new();
            let mut prefixes: Vec<String> = Vec::new();
            let mut after = resume.clone();
            let mut truncated = false;
            // Where a following page resumes: the last key returned, or past the last common prefix.
            let mut last: Option<String> = None;
            'pages: loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                let mut jumped = false;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if summary.key.as_str() < prefix.as_str() {
                        continue;
                    }
                    if !summary.key.starts_with(prefix.as_str()) {
                        // Keys are in order, so nothing later can match.
                        break 'pages;
                    }
                    if summary.deleted || s3_verbs::is_reserved(&summary.key) {
                        continue;
                    }
                    if entries.len() + prefixes.len() == max_keys {
                        truncated = true;
                        break 'pages;
                    }
                    // A key whose remainder holds the delimiter is folded into its common prefix,
                    // and every key under that prefix is skipped.
                    if let Some(delimiter) = &delimiter {
                        let rest = &summary.key[prefix.len()..];
                        if let Some(at) = rest.find(delimiter.as_str()) {
                            let common = format!("{prefix}{}", &rest[..at + delimiter.len()]);
                            let resume_after = s3_verbs::past_prefix(&common);
                            prefixes.push(common);
                            last = Some(resume_after.clone());
                            after = Some(resume_after);
                            jumped = true;
                            break;
                        }
                    }
                    last = Some(summary.key.clone());
                    entries.push(s3::ListEntry {
                        key: summary.key.clone(),
                        last_modified_ms: 0,
                        etag: etag_of(summary.generation),
                        size: summary.length,
                        storage_class: s3::DEFAULT_STORAGE_CLASS.to_string(),
                    });
                }
                if jumped {
                    continue 'pages;
                }
                if !page.has_more {
                    break;
                }
            }
            Ok((entries, prefixes, truncated, last))
        });
        let (entries, prefixes, truncated, last) = match walked {
            Ok(value) => value,
            Err(error) => return error_response(error, &resource, true),
        };
        let next = if truncated { last } else { None };
        let result = s3::ListResult {
            bucket: bucket.to_string(),
            prefix,
            delimiter: delimiter.clone(),
            max_keys,
            is_truncated: truncated,
            continuation_token: continuation,
            next_continuation_token: next,
            start_after,
            entries,
            common_prefixes: prefixes,
        };
        s3::xml_response(200, s3::list_objects_v2_xml(&result))
    }
}

impl RequestHandler for ClusterGateway {
    fn handle(&self, request: &Request) -> Response {
        let response = self.route(request);
        // The whole reason goes to stderr: a client log may cut the body, and a failed request
        // is rare enough that one line each costs nothing.
        if let Some(line) = failure_log_line(request, &response) {
            eprintln!("{line}");
        }
        response
    }
}

/// `Range: bytes=a-b` with both ends given and `a <= b`, as an offset and a length.
fn closed_range(header: &str) -> Option<(u64, u64)> {
    let spec = header.strip_prefix("bytes=")?;
    let (first, last) = spec.split_once('-')?;
    let first: u64 = first.trim().parse().ok()?;
    let last: u64 = last.trim().parse().ok()?;
    if last < first {
        return None;
    }
    Some((first, last - first + 1))
}

/// Whether a refusal says another writer published first, as opposed to a refusal of the
/// request itself.
fn lost_race(error: &ObjectError) -> bool {
    (matches!(error, ObjectError::PreconditionFailed(_))
        && !plane_unreachable(error)
        && !placement_unavailable(error)
        && !leader_unavailable(error))
        || staged_state_mismatch(error)
}

/// The metadata plane has no healthy data group to place the write on (the chunk servers'
/// heartbeats have lapsed, for example when one is missed under load). Nothing is wrong with the
/// client's request and nothing changed under it: it is the cluster being briefly unable, which
/// clears when the next heartbeat lands, so it is a retryable 503, not a lost race to retry at
/// once (which would only be refused again) and not a 412.
/// No metadata leader admitted the request (an election, or a quorum that has not settled, as in
/// the first seconds of a cluster): nothing changed under the client's request, and retrying it at
/// once only meets the same answer. A retryable 503, like [`placement_unavailable`].
fn leader_unavailable(error: &ObjectError) -> bool {
    matches!(error, ObjectError::PreconditionFailed(message) if message.contains("was not admitted by a leader"))
}

fn placement_unavailable(error: &ObjectError) -> bool {
    matches!(error, ObjectError::PreconditionFailed(message) if message.contains("could not satisfy replica count"))
}

/// The SDK refuses a staged write as an invalid argument when the allocation it holds no longer
/// matches the metadata record (a reservation that went stale, a generation that moved, a lease
/// that was renewed or swept). Nothing is wrong with the client's request, and a fresh
/// allocation succeeds, so it is a race to retry and never a 400.
fn staged_state_mismatch(error: &ObjectError) -> bool {
    matches!(error, ObjectError::InvalidArgument(message) if message.starts_with("staged "))
}

/// A failure of the connection to a metadata or chunk node, as opposed to a refusal by the
/// plane. The SDK words these as "the metaserver at <endpoint> could not be reached while ..."
/// (reported as a failed precondition so that a caller is not told its data is damaged) and
/// "the replica at <endpoint> failed while ..." (reported as corruption). Neither says
/// anything about the request or the stored object, so neither is a lost publish race nor a
/// server fault in the stored state: the client should retry later.
fn plane_unreachable(error: &ObjectError) -> bool {
    match error {
        ObjectError::PreconditionFailed(message) => {
            message.starts_with("the metaserver at ") && message.contains(" could not be reached while ")
        }
        ObjectError::Corruption(message) => {
            message.starts_with("the replica at ") && message.contains(" failed while ")
        }
        _ => false,
    }
}

fn etag_of(generation: u64) -> String {
    format!("gen-{generation}")
}

fn object_headers(response: &mut Response, manifest: &ObjectManifest) {
    response
        .headers
        .push(("etag".into(), format!("\"{}\"", etag_of(manifest.generation))));
    response
        .headers
        .push(("content-type".into(), super::s3_limits::DEFAULT_CONTENT_TYPE.into()));
    response.headers.push(("accept-ranges".into(), "bytes".into()));
    response
        .headers
        .push(("x-amz-version-id".into(), manifest.generation.to_string()));
    response
        .headers
        .push(("x-amz-object-size".into(), manifest.length.to_string()));
    response
        .headers
        .push(("x-matrixobject-length".into(), manifest.length.to_string()));
}

/// The answer to a write: what the object is now, so an append's receipt says where it landed.
fn object_written(manifest: &ObjectManifest) -> Response {
    let mut response = Response::status(200);
    object_headers(&mut response, manifest);
    response
}

fn no_such_key(resource: &str) -> Response {
    s3::error_response(s3::S3ErrorCode::NoSuchKey, "the object does not exist", resource)
}

fn unconfirmed(error: &ObjectError, resource: &str) -> Response {
    s3::error_response(
        s3::S3ErrorCode::ServiceUnavailable,
        &format!("the publication is unconfirmed and was not retried: {error}"),
        resource,
    )
}

fn not_implemented(what: &str, resource: &str) -> Response {
    s3::error_response(
        s3::S3ErrorCode::NotImplemented,
        &format!(
            "{what} is not served by the cluster gateway; it serves PUT (with If-Match and If-None-Match), append (x-amz-write-offset-bytes), CopyObject, GET (with Range and conditions), HEAD, DELETE, DeleteObjects, the multipart family, ListObjectsV2 (with a delimiter), ListBuckets and DeleteBucket"
        ),
        resource,
    )
}

fn method_not_allowed(allowed: &str, resource: &str) -> Response {
    s3::error_response(
        s3::S3ErrorCode::MethodNotAllowed,
        "the method is not allowed on this resource",
        resource,
    )
    .with_header("allow", allowed.to_string())
}

/// One stderr line for a failed request, or `None` for the answers a client is expected to act
/// on quietly (not found, and the offset conflict of an append).
fn failure_log_line(request: &Request, response: &Response) -> Option<String> {
    if response.status < 400 || matches!(response.status, 404 | 412 | 416) {
        return None;
    }
    let body = String::from_utf8_lossy(&response.body);
    let between = |tag: &str| {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        body.split_once(&open)
            .and_then(|(_, rest)| rest.split_once(&close))
            .map(|(value, _)| value.to_string())
    };
    Some(format!(
        "cluster gateway: {} {} -> {} {}: {}",
        request.method,
        request.path,
        response.status,
        between("Code").unwrap_or_default(),
        between("Message").unwrap_or_else(|| body.trim().to_string()),
    ))
}

/// An SDK error as the S3 document a client branches on.
fn error_response(error: ObjectError, resource: &str, missing_is_bucket: bool) -> Response {
    // The SDK's refusal of an append past its extent limit and its refusal of a write that is
    // too large are the caller's request, not a server fault.
    let code = match &error {
        ObjectError::InvalidArgument(message) if message.starts_with("append refused") => {
            s3::S3ErrorCode::InvalidRequest
        }
        // The catalog has no room: permanent until something is removed, so not a 503.
        _ if crate::object_manifest::catalog_full_refusal(&error) => s3::S3ErrorCode::InsufficientStorage,
        _ if plane_unreachable(&error)
            || placement_unavailable(&error)
            || leader_unavailable(&error)
            || staged_state_mismatch(&error)
            || consensus_unavailable(&error) =>
        {
            s3::S3ErrorCode::ServiceUnavailable
        }
        _ => s3::code_for(&error, missing_is_bucket),
    };
    s3::error_response(code, &error.to_string(), resource)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn compact_sdk_startup_rejects_plain_options_before_opening() {
        for options in [GatewayOptions::default(), GatewayOptions { compact_append: true, compact_extents: Some(2), ..Default::default() }] {
            let result = ClusterGateway::over_sdk_with_publication_fence(
                || panic!("incompatible options must be refused before opening"),
                options,
                "test",
            );
            assert!(matches!(result, Err(ObjectError::InvalidArgument(_))));
        }
    }

    #[test]
    fn compact_sdk_startup_reports_an_unreachable_metadata_client() {
        let result = ClusterGateway::over_sdk_with_publication_fence(
            || Err(ObjectError::NotFound("startup metadata".into())),
            GatewayOptions { compact_append: true, ..Default::default() },
            "test",
        );
        assert!(matches!(result, Err(ObjectError::NotFound(message)) if message == "startup metadata"));
    }

    #[test]
    fn reserve_next_default_follows_the_gateway_mode() {
        assert_eq!(resolve_reserve_next(true, None), Ok(true), "compact on, unset");
        assert_eq!(resolve_reserve_next(true, Some(1)), Ok(true), "compact on, =1");
        assert_eq!(resolve_reserve_next(true, Some(0)), Ok(false), "compact on, =0");
        assert_eq!(resolve_reserve_next(false, None), Ok(false), "compact off, unset");
        assert_eq!(resolve_reserve_next(false, Some(0)), Ok(false), "compact off, =0");
        assert!(resolve_reserve_next(false, Some(1)).is_err(), "compact off, =1 is refused");
    }

    fn reservation(generation: u64, pages: u32) -> crate::staged_allocation::StagedAllocation {
        crate::staged_allocation::StagedAllocation {
            version: 2,
            allocation_id: 9,
            page_count: pages,
            bucket: "b".into(),
            key: "k".into(),
            expected_generation: generation,
            append: true,
            expires_at_ms: 1_000_000,
            ..Default::default()
        }
    }
    fn manifest_at(generation: u64) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: "b".into(),
            key: "k".into(),
            generation,
            ..Default::default()
        }
    }

    #[test]
    fn a_reservation_is_used_once_and_only_for_the_manifest_and_size_it_was_made_for() {
        let state = CompactState::default();
        state.store_reservation(reservation(5, 2));
        // the manifest it extends, a request it covers
        assert_eq!(state.take_reservation(&manifest_at(5), 2, 60_000), Some(reservation(5, 2)));
        // used once
        assert_eq!(state.take_reservation(&manifest_at(5), 2, 60_000), None);
        // another generation (another writer got there first): removed, not reused
        state.store_reservation(reservation(5, 2));
        assert_eq!(state.take_reservation(&manifest_at(6), 1, 60_000), None);
        assert_eq!(state.take_reservation(&manifest_at(5), 1, 60_000), None);
        // too few pages for the append
        state.store_reservation(reservation(5, 1));
        assert_eq!(state.take_reservation(&manifest_at(5), 2, 60_000), None);
        // a smaller append can use a larger reservation
        state.store_reservation(reservation(5, 4));
        assert!(state.take_reservation(&manifest_at(5), 1, 60_000).is_some());
        // too old to trust: half the lease it was made with
        state.store_reservation(reservation(5, 1));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(state.take_reservation(&manifest_at(5), 1, 40), None);
    }

    #[test]
    fn an_object_is_hot_only_when_it_was_appended_to_lately_and_the_memory_is_bounded() {
        let state = CompactState::default();
        let now = Instant::now();
        assert!(
            !state.append_is_hot_at("b", "k", now),
            "the first append to an object is cold"
        );
        state.record_append_at("b", "k", now);
        assert!(
            state.append_is_hot_at("b", "k", now),
            "the next one, at once, is hot"
        );
        assert!(
            !state.append_is_hot_at("b", "other", now),
            "another object is cold"
        );
        // an append older than the window no longer makes the next one hot
        state.2.lock().unwrap().insert(
            ("b".into(), "k".into()),
            now.checked_sub(HOT_WINDOW + Duration::from_millis(50))
                .unwrap(),
        );
        assert!(
            !state.append_is_hot_at("b", "k", now),
            "an old append is cold"
        );
        // the memory is bounded however many objects are appended to
        for index in 0..RECENT_APPENDS_CAP * 3 {
            state.record_append_at("b", &format!("object-{index}"), now);
        }
        assert!(state.2.lock().unwrap().len() <= RECENT_APPENDS_CAP);
    }

    #[test]
    fn the_pool_holds_a_connection_for_every_thread_and_more_by_default() {
        assert_eq!(resolve_pool_max(None, 4), 64);
        assert_eq!(resolve_pool_max(Some(""), 4), 64);
        assert_eq!(resolve_pool_max(Some("0"), 4), 64);
        assert_eq!(resolve_pool_max(Some("nonsense"), 4), 64);
        assert_eq!(resolve_pool_max(Some(" 200 "), 4), 200);
        assert_eq!(resolve_pool_max(Some("8"), 16), 16, "never fewer than the worker threads");
        assert_eq!(resolve_pool_max(None, 256), 256);
    }

    fn manifest_of(key: &str, generation: u64) -> ObjectManifest {
        ObjectManifest { format_version: 2, bucket: "b".into(), key: key.into(), generation, ..Default::default() }
    }

    #[test]
    fn the_manifest_cache_is_bounded_and_drops_the_least_recently_used_first() {
        let mut cache = ManifestCache::default();
        for index in 0..4 {
            cache.insert(manifest_of(&format!("k{index}"), 1), 4, usize::MAX);
        }
        // touching k0 makes k1 the oldest
        assert!(cache.get("b", "k0").is_some());
        cache.insert(manifest_of("k4", 1), 4, usize::MAX);
        assert_eq!(cache.len(), 4);
        assert!(cache.get("b", "k1").is_none(), "the least recently used left");
        for key in ["k0", "k2", "k3", "k4"] {
            assert!(cache.get("b", key).is_some(), "{key} must still be cached");
        }
        // a replaced entry keeps one slot and holds the new value
        cache.insert(manifest_of("k0", 9), 4, usize::MAX);
        assert_eq!(cache.len(), 4);
        assert_eq!(cache.get("b", "k0").unwrap().generation, 9);
        cache.remove("b", "k0");
        assert!(cache.get("b", "k0").is_none());
        assert_eq!(cache.len(), 3);
    }

    fn manifest_with_extents(key: &str, extents: usize) -> ObjectManifest {
        ObjectManifest {
            extents: (0..extents).map(|_| Default::default()).collect(),
            ..manifest_of(key, 1)
        }
    }

    /// The cache is bounded in bytes as well as entries: a few large manifests push each other out
    /// long before the entry count is reached, and the byte count always matches what is held.
    #[test]
    fn the_manifest_cache_is_bounded_in_bytes_and_the_count_matches_what_it_holds() {
        let mut cache = ManifestCache::default();
        let big = manifest_weight(&manifest_with_extents("x", 1000));
        let budget = big * 3 + 10;
        for index in 0..10 {
            cache.insert(manifest_with_extents(&format!("big-{index}"), 1000), 4096, budget);
            assert!(cache.bytes() <= budget, "{} bytes over a budget of {budget}", cache.bytes());
        }
        assert_eq!(cache.len(), 3, "three large manifests fit");
        for index in 7..10 {
            assert!(cache.get("b", &format!("big-{index}")).is_some(), "the three newest stay");
        }
        assert!(cache.get("b", "big-0").is_none());
        // many small manifests are still limited by count, not wasted on bytes
        let mut small = ManifestCache::default();
        for index in 0..100 {
            small.insert(manifest_of(&format!("s{index}"), 1), 10, budget);
        }
        assert_eq!(small.len(), 10);
        assert_eq!(small.bytes(), 10 * manifest_weight(&manifest_of("s", 1)));
        // removal and replacement keep the count exact
        small.remove("b", "s99");
        small.insert(manifest_with_extents("s98", 500), 10, budget);
        let held: usize = (90..99)
            .filter_map(|index| small.get("b", &format!("s{index}")))
            .map(|manifest| manifest_weight(&manifest))
            .sum();
        assert_eq!(small.bytes(), held, "the counted bytes are the held bytes");
        // a manifest heavier than the whole budget is not kept, and does not push others out
        let before = small.len();
        small.insert(manifest_with_extents("huge", 100_000), 10, budget);
        assert!(small.get("b", "huge").is_none());
        assert_eq!(small.len(), before);
    }

    /// A working set larger than the old 64-entry cap no longer re-reads each manifest.
    #[test]
    fn a_working_set_far_larger_than_the_old_default_stays_cached() {
        let options = GatewayOptions::default();
        assert!(options.cache_entries >= 4096 && options.cache_bytes >= 32 * 1024 * 1024);
        let mut cache = ManifestCache::default();
        let (mut hits, mut requests) = (0u32, 0u32);
        // 1000 objects appended to round robin, 5 passes
        for round in 0..5000u64 {
            let key = format!("object-{}", round % 1000);
            requests += 1;
            if cache.get("b", &key).is_some() {
                hits += 1;
            }
            cache.insert(manifest_with_extents(&key, 20), options.cache_entries, options.cache_bytes);
        }
        // only the first pass misses (1000 of 5000); with 64 entries every request missed
        assert_eq!(requests - hits, 1000, "{hits} hits of {requests}");
    }

    #[test]
    fn positive_numbers_from_the_environment_fall_back_to_the_default() {
        assert_eq!(resolve_positive(None, 7), 7);
        assert_eq!(resolve_positive(Some("0"), 7), 7);
        assert_eq!(resolve_positive(Some("x"), 7), 7);
        assert_eq!(resolve_positive(Some(" 12 "), 7), 12);
    }

    #[test]
    fn a_hot_set_survives_a_working_set_larger_than_the_cache() {
        let mut cache = ManifestCache::default();
        let capacity = 64;
        let (mut hits, mut requests) = (0u32, 0u32);
        // 32 objects appended to in turn, and between two appends to one of them 32 appends to
        // objects never seen again: more objects than the cache holds, a hot set that fits
        for round in 0..2000u64 {
            let hot = format!("hot-{}", round % 32);
            requests += 1;
            if cache.get("b", &hot).is_some() {
                hits += 1;
            }
            cache.insert(manifest_of(&hot, round), capacity, usize::MAX);
            cache.insert(manifest_of(&format!("cold-{round}"), 1), capacity, usize::MAX);
            assert!(cache.len() <= capacity);
        }
        // emptying the cache when it filled made the hot set miss after every wipe (about every
        // 64 inserts); least-recently-used misses only while the hot set is first loaded
        assert!(hits * 100 >= requests * 95, "hot set hit {hits} of {requests}");
    }

    #[test]
    fn append_activity_window_starts_when_successful_publication_completes() {
        let state = CompactState::default();
        let started = Instant::now();
        let completed = started + HOT_WINDOW * 3;
        assert!(!state.append_is_hot_at("b", "k", started));
        assert!(!state.append_is_hot_at("b", "k", completed));
        assert!(
            state.2.lock().unwrap().is_empty(),
            "checking activity cannot warm an object"
        );
        state.record_append_at("b", "k", completed);
        assert!(state.append_is_hot_at("b", "k", completed + HOT_WINDOW - Duration::from_nanos(1)));
        assert!(!state.append_is_hot_at("b", "k", completed + HOT_WINDOW));
        assert!(
            !state.append_is_hot_at("b", "k", started),
            "an older observation is conservative"
        );
    }

    #[test]
    fn append_activity_keeps_the_latest_completion_when_recording_is_delayed() {
        let state = CompactState::default();
        let earlier = Instant::now();
        let later = earlier + HOT_WINDOW;
        for index in 0..RECENT_APPENDS_CAP {
            state.record_append_at("b", &format!("object-{index}"), later);
        }
        state.record_append_at("b", "object-0", earlier);
        assert!(state.append_is_hot_at(
            "b",
            "object-0",
            later + HOT_WINDOW - Duration::from_nanos(1)
        ));
        assert_eq!(
            state.2.lock().unwrap().len(),
            RECENT_APPENDS_CAP,
            "updating an existing object cannot empty a full activity map"
        );
    }

    #[test]
    fn failed_compact_append_does_not_create_object_activity() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let client = RemoteMetaClient::connect(
            &endpoint,
            &MetaClientOptions {
                connect_timeout: Duration::from_millis(100),
                read_timeout: Duration::from_millis(100),
                write_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap();
        let (connection, _) = listener.accept().unwrap();
        connection.shutdown(std::net::Shutdown::Both).unwrap();
        drop(connection);
        drop(listener);
        let state = Arc::new(CompactState::default());
        state.0.store(1, std::sync::atomic::Ordering::Relaxed);
        let mut plane = SdkPlane::new(
            client,
            GatewayOptions {
                compact_append: true,
                reserve_next: true,
                ..Default::default()
            },
            Arc::clone(&state),
        );
        assert!(plane.append_compact_path(&manifest_at(1), b"x").is_err());
        assert!(state.2.lock().unwrap().is_empty());
        assert!(!state.append_is_hot_at("b", "k", Instant::now()));
    }

    #[test]
    fn the_reservation_cache_is_bounded() {
        let state = CompactState::default();
        for index in 0..200usize {
            let mut value = reservation(1, 1);
            value.key = format!("key-{index}");
            state.store_reservation(value);
        }
        assert!(state.1.lock().unwrap().len() <= 48);
    }

    #[test]
    fn an_unreachable_node_answers_service_unavailable_and_is_not_a_lost_race() {
        let metadata = ObjectError::PreconditionFailed(
            "the metaserver at 127.0.0.1:1 could not be reached while reading a response header: metadata frame deadline".into(),
        );
        let replica = ObjectError::Corruption(
            "the replica at 127.0.0.1:2 failed while collecting a fan-out response: incomplete frame".into(),
        );
        for error in [metadata.clone(), replica.clone()] {
            assert!(plane_unreachable(&error));
            assert!(!lost_race(&error));
            assert_eq!(error_response(error, "/b/k", false).status, 503);
        }
        // A real conflict or real corruption keeps its answer.
        let conflict = ObjectError::PreconditionFailed("object manifest changed before write".into());
        assert!(lost_race(&conflict));
        assert_eq!(error_response(conflict, "/b/k", false).status, 412);
        let damaged = ObjectError::Corruption("manifest digest mismatch".into());
        assert!(!plane_unreachable(&damaged));
        assert_eq!(error_response(damaged, "/b/k", false).status, 500);
    }

    /// An in-memory plane with the SDK's contract: a write names the generation it builds on,
    /// an append adds extents, and an object holds at most `MAX_OBJECT_EXTENTS` of them.
    #[derive(Default)]
    struct Shared {
        objects: BTreeMap<(String, String), (ObjectManifest, Vec<u8>)>,
        calls: Vec<&'static str>,
        /// Another writer moves the generation under every fence attempt.
        racing_fence: bool,
        /// Pages are allocated per attempt: the next chunk id the plane hands out.
        chunks: u64,
    }

    /// The extent an attempt publishes: its own chunk, which no other attempt shares.
    fn own_extent(shared: &mut Shared, offset: u64, length: u64) -> crate::object_manifest::ObjectExtent {
        shared.chunks += 1;
        crate::object_manifest::ObjectExtent {
            group: "g".into(),
            volume_id: 1,
            chunk_id: shared.chunks,
            object_offset: offset,
            length,
            ..Default::default()
        }
    }

    struct MemoryPlane(Arc<Mutex<Shared>>);

    fn manifest(bucket: &str, key: &str, generation: u64, length: u64, extents: usize) -> ObjectManifest {
        ObjectManifest {
            format_version: 2,
            bucket: bucket.into(),
            key: key.into(),
            generation,
            length,
            extents: (0..extents)
                .map(|_| crate::object_manifest::ObjectExtent::default())
                .collect(),
            deleted: false,
        }
    }

    impl ObjectPlane for MemoryPlane {
        fn read_manifest(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
            let mut shared = self.0.lock().unwrap();
            shared.calls.push("read_manifest");
            shared
                .objects
                .get(&(bucket.to_string(), key.to_string()))
                .map(|(m, _)| m.clone())
                .ok_or_else(|| ObjectError::NotFound("manifest".into()))
        }
        fn write(
            &mut self,
            bucket: &str,
            key: &str,
            expected: Option<ObjectManifest>,
            data: &[u8],
        ) -> Result<ObjectManifest, PlaneFailure> {
            let mut shared = self.0.lock().unwrap();
            shared.calls.push("write");
            let slot = (bucket.to_string(), key.to_string());
            let current = shared.objects.get(&slot).map(|(m, _)| m.clone());
            if current != expected {
                return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
            }
            let generation = current.map_or(1, |m| m.generation + 1);
            let mut m = manifest(bucket, key, generation, data.len() as u64, 0);
            if !data.is_empty() {
                m.extents = vec![own_extent(&mut shared, 0, data.len() as u64)];
            }
            shared.objects.insert(slot, (m.clone(), data.to_vec()));
            Ok(m)
        }
        fn append(&mut self, expected: ObjectManifest, data: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
            let mut shared = self.0.lock().unwrap();
            shared.calls.push("append");
            let slot = (expected.bucket.clone(), expected.key.clone());
            let Some((current, bytes)) = shared.objects.get(&slot).cloned() else {
                return Err(ObjectError::NotFound("append target".into()).into());
            };
            if current != expected {
                return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
            }
            if current.extents.len() + 1 > MAX_OBJECT_EXTENTS {
                return Err(ObjectError::InvalidArgument("invalid object append size or generation".into()).into());
            }
            let mut joined = bytes;
            joined.extend_from_slice(data);
            let mut m = manifest(&expected.bucket, &expected.key, current.generation + 1, joined.len() as u64, 0);
            m.extents = current.extents.clone();
            m.extents.push(own_extent(&mut shared, current.length, data.len() as u64));
            shared.objects.insert(slot, (m.clone(), joined));
            Ok(m)
        }
        fn read(&mut self, bucket: &str, key: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
            let shared = self.0.lock().unwrap();
            match shared.objects.get(&(bucket.to_string(), key.to_string())) {
                Some((m, bytes)) if !m.deleted => Ok((m.clone(), bytes.clone())),
                _ => Err(ObjectError::NotFound("object".into())),
            }
        }
        fn read_range(&mut self, bucket: &str, key: &str, offset: u64, length: u64) -> Result<(ObjectManifest, Vec<u8>), ObjectError> {
            let mut shared = self.0.lock().unwrap();
            shared.calls.push("read_range");
            match shared.objects.get(&(bucket.to_string(), key.to_string())) {
                Some((m, bytes)) if !m.deleted => {
                    if offset + length > m.length {
                        return Err(ObjectError::InvalidArgument("object read range exceeds length".into()));
                    }
                    Ok((m.clone(), bytes[offset as usize..(offset + length) as usize].to_vec()))
                }
                _ => Err(ObjectError::NotFound("object".into())),
            }
        }
        fn publish_unchanged(&mut self, expected: &ObjectManifest) -> Result<ObjectManifest, PlaneFailure> {
            let mut shared = self.0.lock().unwrap();
            shared.calls.push("publish_unchanged");
            let slot = (expected.bucket.clone(), expected.key.clone());
            let Some((current, bytes)) = shared.objects.get(&slot).cloned() else {
                return Err(ObjectError::NotFound("manifest".into()).into());
            };
            if shared.racing_fence {
                let mut moved = current.clone();
                moved.generation += 1;
                shared.objects.insert(slot.clone(), (moved, bytes.clone()));
            }
            let (current, bytes) = shared.objects[&slot].clone();
            if &current != expected {
                return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
            }
            let mut next = current;
            next.generation += 1;
            shared.objects.insert(slot, (next.clone(), bytes));
            Ok(next)
        }
        fn delete(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError> {
            let mut shared = self.0.lock().unwrap();
            let slot = (bucket.to_string(), key.to_string());
            let Some((current, _)) = shared.objects.get(&slot).cloned() else {
                return Err(ObjectError::NotFound("manifest".into()));
            };
            let mut tomb = current.clone();
            tomb.generation += 1;
            tomb.length = 0;
            tomb.extents.clear();
            tomb.deleted = true;
            shared.objects.insert(slot, (tomb.clone(), Vec::new()));
            Ok(tomb)
        }
        fn list(&mut self, bucket: &str, after: Option<&str>, limit: u32) -> Result<ManifestListPage, ObjectError> {
            let shared = self.0.lock().unwrap();
            let mut entries: Vec<crate::object_manifest::ManifestSummary> = shared
                .objects
                .iter()
                .filter(|((b, k), _)| b == bucket && after.map_or(true, |a| k.as_str() > a))
                .map(|((b, k), (m, _))| crate::object_manifest::ManifestSummary {
                    key: k.clone(),
                    generation: m.generation,
                    length: m.length,
                    deleted: m.deleted,
                    bucket: b.clone(),
                })
                .collect();
            let has_more = entries.len() > limit as usize;
            entries.truncate(limit as usize);
            Ok(ManifestListPage {
                version: 1,
                bucket: bucket.into(),
                entries,
                has_more,
                revision: Vec::new(),
            })
        }
    }

    fn sha_hex(data: &[u8]) -> String {
        ring::digest::digest(&ring::digest::SHA256, data)
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// An attempt id: 32 hex characters.
    fn attempt(n: u128) -> String {
        format!("{n:032x}")
    }

    fn resolve_test_append(gateway: &ClusterGateway, offset: u64, data: &[u8], id: &str) -> Response {
        gateway.handle(&request(
            "PUT",
            "/b/k",
            &[(WRITE_OFFSET_HEADER, &offset.to_string()), (ATTEMPT_HEADER, id)],
            data,
        ))
    }

    fn resolve(gateway: &ClusterGateway, offset: u64, data: &[u8], id: &str) -> Response {
        let target = format!("/b/k?append-resolve&attempt={id}&offset={offset}&length={}&sha256={}", data.len(), sha_hex(data));
        gateway.handle(&request("POST", &target, &[], b""))
    }

    fn bytes_of(shared: &Arc<Mutex<Shared>>) -> Vec<u8> {
        shared.lock().unwrap().objects.get(&("b".to_string(), "k".to_string())).map(|(_, b)| b.clone()).unwrap_or_default()
    }

    fn resolve_manifest_of(shared: &Arc<Mutex<Shared>>) -> ObjectManifest {
        shared.lock().unwrap().objects[&("b".to_string(), "k".to_string())].0.clone()
    }

    /// "Landed" means THIS ATTEMPT landed: the attempt's own extents are in the manifest. A later append does not
    /// change that (a comparison of bytes at the end of the object would have said absent), and an attempt this
    /// gateway never saw is never "landed", however equal the bytes.
    #[test]
    fn a_resolve_finds_the_attempt_that_landed_by_its_extents_and_fences_nothing() {
        let (gateway, shared) = gateway();
        assert_eq!(resolve_test_append(&gateway, 0, b"aaaa", &attempt(1)).status, 200);
        assert_eq!(resolve_test_append(&gateway, 4, b"bbb", &attempt(2)).status, 200);
        let response = resolve(&gateway, 4, b"bbb", &attempt(2));
        assert_eq!((response.status, header(&response, "x-append-resolution")), (200, Some("landed")));
        // a later append does not unland it
        assert_eq!(resolve_test_append(&gateway, 7, b"ccc", &attempt(3)).status, 200);
        assert_eq!(header(&resolve(&gateway, 4, b"bbb", &attempt(2)), "x-append-resolution"), Some("landed"));
        assert!(!shared.lock().unwrap().calls.contains(&"publish_unchanged"), "a landed append needs no fence");
        // An attempt this gateway never saw, with bytes equal to what is there: ambiguous, NEVER landed.
        let unseen = resolve(&gateway, 4, b"bbb", &attempt(99));
        assert_eq!(header(&unseen, "x-append-resolution"), Some("ambiguous"));
        // Different bytes at the offset: another append took it, and still no fence is needed.
        let other = resolve(&gateway, 4, b"zzz", &attempt(98));
        assert_eq!(header(&other, "x-append-resolution"), Some("absent"));
        assert!(!shared.lock().unwrap().calls.contains(&"publish_unchanged"));
    }

    /// THE LOST WRITE. Two writers append identical bytes at one offset; one physical append is possible. The
    /// writer that took the offset is told landed, the other is told ambiguous (never landed) and the object holds
    /// the bytes once. With a comparison of bytes both were told landed.
    #[test]
    fn two_identical_appends_at_one_offset_are_not_both_told_landed() {
        let (gateway, shared) = gateway();
        assert_eq!(resolve_test_append(&gateway, 0, b"xx", &attempt(1)).status, 200);
        let late = resolve_test_append(&gateway, 0, b"xx", &attempt(2));
        assert_eq!(late.status, 412, "the second writer is refused: the offset is taken");
        assert_eq!(bytes_of(&shared), b"xx", "one physical append");
        assert_eq!(header(&resolve(&gateway, 0, b"xx", &attempt(1)), "x-append-resolution"), Some("landed"));
        let second = resolve(&gateway, 0, b"xx", &attempt(2));
        assert_eq!(header(&second, "x-append-resolution"), Some("ambiguous"), "the identical bytes are not its own");
    }

    /// An append whose outcome is unknown and whose plane reports nothing of the attempt, with equal bytes already
    /// in the object, is AMBIGUOUS (409 with the current length), not "landed": the client decides from its own
    /// record, and the bytes are in the object exactly once.
    #[test]
    fn an_unknown_outcome_with_equal_bytes_and_no_attempt_identity_is_ambiguous_not_landed() {
        let (gateway, armed) = timeout_gateway_with(ObjectError::InvalidArgument(RAFT_DEADLINE.into()), true, false);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 409, "{}", String::from_utf8_lossy(&response.body));
        assert_eq!(header(&response, "x-amz-append-state"), Some("ambiguous"));
        assert_eq!(header(&response, "x-amz-object-size"), Some("5"));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde", "stored once");
    }

    #[test]
    fn a_resolve_fences_a_lost_append_so_a_late_attempt_is_refused_and_the_retry_lands_once() {
        let (gateway, shared) = gateway();
        assert_eq!(resolve_test_append(&gateway, 0, b"aaaa", &attempt(1)).status, 200);
        // The lost attempt read this manifest and is still in flight.
        let lost_expected = resolve_manifest_of(&shared);
        let response = resolve(&gateway, 4, b"bbb", &attempt(2));
        assert_eq!(header(&response, "x-append-resolution"), Some("absent"));
        let fenced = resolve_manifest_of(&shared);
        assert_eq!((fenced.generation, fenced.length), (lost_expected.generation + 1, 4), "only the generation moves");
        assert_eq!(bytes_of(&shared), b"aaaa");
        // The late attempt now reaches the plane: its compare-and-set on the old generation fails.
        let mut plane = MemoryPlane(Arc::clone(&shared));
        assert!(plane.append(lost_expected, b"bbb").is_err());
        assert_eq!(bytes_of(&shared), b"aaaa", "the late attempt must not land");
        // The caller retries as a NEW attempt: stored exactly once.
        assert_eq!(resolve_test_append(&gateway, 4, b"bbb", &attempt(3)).status, 200);
        assert_eq!(bytes_of(&shared), b"aaaabbb");
        assert_eq!(header(&resolve(&gateway, 4, b"bbb", &attempt(3)), "x-append-resolution"), Some("landed"));
    }

    #[test]
    fn a_resolve_of_an_object_that_was_never_created_fences_the_late_first_append() {
        let (other, other_shared) = gateway();
        let (gateway, shared) = gateway();
        let response = resolve(&gateway, 0, b"xyz", &attempt(1));
        assert_eq!(header(&response, "x-append-resolution"), Some("absent"));
        assert_eq!(resolve_manifest_of(&shared).length, 0, "the key now exists, empty");
        // The late first append builds on 'no object': refused.
        let mut plane = MemoryPlane(Arc::clone(&shared));
        assert!(plane.write("b", "k", None, b"xyz").is_err());
        assert_eq!(resolve_test_append(&gateway, 0, b"xyz", &attempt(2)).status, 200);
        assert_eq!(bytes_of(&shared), b"xyz");
        // An append at an offset past a missing object can never land: absent, nothing created.
        assert_eq!(header(&resolve(&other, 8, b"xyz", &attempt(3)), "x-append-resolution"), Some("absent"));
        assert!(other_shared.lock().unwrap().objects.is_empty());
    }

    #[test]
    fn a_resolve_gives_up_after_three_rounds_of_a_generation_that_keeps_moving() {
        let (gateway, shared) = gateway();
        assert_eq!(resolve_test_append(&gateway, 0, b"aaaa", &attempt(1)).status, 200);
        shared.lock().unwrap().racing_fence = true;
        let response = resolve(&gateway, 4, b"bbb", &attempt(2));
        assert_eq!(response.status, 503, "{}", String::from_utf8_lossy(&response.body));
        let calls = shared.lock().unwrap().calls.iter().filter(|c| **c == "publish_unchanged").count();
        assert_eq!(calls, 3, "three bounded rounds, no more");
        assert_eq!(bytes_of(&shared), b"aaaa");
    }

    #[test]
    fn a_resolve_refuses_malformed_parameters_and_an_append_refuses_a_malformed_attempt_id() {
        let (gateway, _) = gateway();
        let id = attempt(1);
        for target in [
            "/b/k?append-resolve".to_string(),
            format!("/b/k?append-resolve&offset=4&length=3&sha256={}", sha_hex(b"abc")),
            format!("/b/k?append-resolve&attempt=zz&offset=4&length=3"),
            format!("/b/k?append-resolve&attempt={id}&offset=4&length=3&sha256=zz"),
            format!("/b/k?append-resolve&attempt={id}&offset=x&length=3"),
        ] {
            assert_eq!(gateway.handle(&request("POST", &target, &[], b"")).status, 400, "{target}");
        }
        let zero = format!("/b/k?append-resolve&attempt={id}&offset=4&length=0");
        assert_eq!(gateway.handle(&request("POST", &zero, &[], b"")).status, 400, "an empty append has nothing to resolve");
        for bad in ["zz", "00", &"0".repeat(31), &"g".repeat(32)] {
            let put = gateway.handle(&request("PUT", "/b/k", &[(WRITE_OFFSET_HEADER, "0"), (ATTEMPT_HEADER, bad)], b"x"));
            assert_eq!(put.status, 400, "{bad}");
        }
    }

    fn gateway() -> (ClusterGateway, Arc<Mutex<Shared>>) {
        let shared = Arc::new(Mutex::new(Shared::default()));
        let handle = Arc::clone(&shared);
        let gateway = ClusterGateway::new(
            move || Ok(Box::new(MemoryPlane(Arc::clone(&handle))) as Box<dyn ObjectPlane>),
            GatewayOptions::default(),
            "b",
        );
        (gateway, shared)
    }

    fn request(method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]) -> Request {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        Request {
            method: method.into(),
            path: path.into(),
            query: query.into(),
            headers: headers
                .iter()
                .map(|(n, v)| (n.to_ascii_lowercase(), (*v).to_string()))
                .collect(),
            body: body.to_vec(),
            keep_alive: true,
        }
    }

    fn header<'a>(response: &'a Response, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn append(gateway: &ClusterGateway, key: &str, offset: u64, body: &[u8]) -> Response {
        gateway.handle(&request(
            "PUT",
            &format!("/b/{key}"),
            &[(WRITE_OFFSET_HEADER, &offset.to_string())],
            body,
        ))
    }

    #[test]
    fn put_get_head_delete_round_trip() {
        let (gateway, _) = gateway();
        assert_eq!(gateway.handle(&request("PUT", "/b/k", &[], b"hello")).status, 200);
        let got = gateway.handle(&request("GET", "/b/k", &[], b""));
        assert_eq!((got.status, got.body.as_slice()), (200, b"hello".as_slice()));
        let head = gateway.handle(&request("HEAD", "/b/k", &[], b""));
        assert_eq!(head.status, 200);
        assert_eq!(header(&head, "x-amz-object-size"), Some("5"));
        assert_eq!(gateway.handle(&request("DELETE", "/b/k", &[], b"")).status, 204);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).status, 404);
        assert_eq!(gateway.handle(&request("HEAD", "/b/k", &[], b"")).status, 404);
        // Deleting what is gone is still a success, and the key can be written again.
        assert_eq!(gateway.handle(&request("DELETE", "/b/k", &[], b"")).status, 204);
        assert_eq!(gateway.handle(&request("DELETE", "/b/never", &[], b"")).status, 204);
        assert_eq!(gateway.handle(&request("PUT", "/b/k", &[], b"again")).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).body, b"again");
    }

    #[test]
    fn if_none_match_star_refuses_an_existing_object() {
        let (gateway, _) = gateway();
        let put = |body: &[u8]| gateway.handle(&request("PUT", "/b/k", &[("If-None-Match", "*")], body));
        assert_eq!(put(b"one").status, 200);
        assert_eq!(put(b"two").status, 412);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).body, b"one");
    }

    #[test]
    fn appends_land_at_the_end_and_report_where() {
        let (gateway, _) = gateway();
        let first = append(&gateway, "wal", 0, b"abc");
        assert_eq!((first.status, header(&first, "x-amz-object-size")), (200, Some("3")));
        let second = append(&gateway, "wal", 3, b"de");
        assert_eq!((second.status, header(&second, "x-amz-object-size")), (200, Some("5")));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde");
    }

    #[test]
    fn an_append_at_the_wrong_offset_is_refused_with_412() {
        let (gateway, _) = gateway();
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        for wrong in [0, 2, 4, 100] {
            let refused = append(&gateway, "wal", wrong, b"x");
            assert_eq!(refused.status, 412, "offset {wrong}");
        }
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abc");
        // A key nothing has written is created by an append at zero, and only at zero.
        assert_eq!(append(&gateway, "fresh", 1, b"x").status, 412);
        assert_eq!(append(&gateway, "fresh", 0, b"x").status, 200);
    }

    #[test]
    fn a_stale_cached_manifest_is_refreshed_when_another_writer_appended() {
        let (gateway, shared) = gateway();
        assert_eq!(append(&gateway, "wal", 0, b"aaa").status, 200);
        // Another gateway appends behind this one's cache.
        let mut other = MemoryPlane(Arc::clone(&shared));
        let current = other.read_manifest("b", "wal").unwrap();
        other.append(current, b"bb").unwrap();
        // This gateway still believes the end is 3: the append at 3 is refused as stale.
        let stale = append(&gateway, "wal", 3, b"c");
        assert_eq!(stale.status, 412);
        // The client re-reads the length and tries again at the true end.
        let head = gateway.handle(&request("HEAD", "/b/wal", &[], b""));
        assert_eq!(header(&head, "x-amz-object-size"), Some("5"));
        assert_eq!(append(&gateway, "wal", 5, b"c").status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"aaabbc");
    }

    #[test]
    fn an_append_after_a_delete_starts_a_new_object_at_zero() {
        let (gateway, _) = gateway();
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        assert_eq!(gateway.handle(&request("DELETE", "/b/wal", &[], b"")).status, 204);
        assert_eq!(append(&gateway, "wal", 3, b"x").status, 412);
        assert_eq!(append(&gateway, "wal", 0, b"x").status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"x");
    }

    #[test]
    fn the_extent_limit_is_a_clear_error_at_exactly_the_limit() {
        let (gateway, shared) = gateway();
        // One extent from the first write, then one per append.
        let mut offset = 0u64;
        let mut landed = 0usize;
        loop {
            let response = append(&gateway, "wal", offset, b"x");
            if response.status != 200 {
                assert_eq!(response.status, 400);
                let body = String::from_utf8(response.body).unwrap();
                assert!(body.contains("InvalidRequest"), "{body}");
                assert!(body.contains("4096 extents per object"), "{body}");
                break;
            }
            offset += 1;
            landed += 1;
            assert!(landed <= MAX_OBJECT_EXTENTS + 1);
        }
        assert_eq!(landed, MAX_OBJECT_EXTENTS);
        // Nothing was written by the refused append, and the object is intact.
        let shared = shared.lock().unwrap();
        let (m, bytes) = shared.objects.get(&("b".to_string(), "wal".to_string())).unwrap();
        assert_eq!((m.length, bytes.len()), (MAX_OBJECT_EXTENTS as u64, MAX_OBJECT_EXTENTS));
    }

    #[test]
    fn plain_publication_is_the_default_and_the_options_are_off() {
        let options = GatewayOptions::default();
        assert!(!options.compact_append);
        assert_eq!(options.compact_extents, None);
        for (extents, pages, empty) in [(0, 1, false), (1, 1, false), (4095, 1, false), (4096, 1, false), (3, 0, true)] {
            for state in [0u8, 1, 2] {
                assert_eq!(choose_append_route(&options, state, extents, pages, empty), AppendRoute::Plain);
            }
        }
    }

    #[test]
    fn the_staged_path_is_chosen_until_the_fence_refuses_it() {
        let options = GatewayOptions { compact_append: true, ..GatewayOptions::default() };
        assert_eq!(choose_append_route(&options, 0, 10, 1, false), AppendRoute::Staged);
        assert_eq!(choose_append_route(&options, 1, 10, 1, false), AppendRoute::Staged);
        // Refused once (state 2): plain from then on.
        assert_eq!(choose_append_route(&options, 2, 10, 1, false), AppendRoute::Plain);
        // An empty append has no extents to own.
        assert_eq!(choose_append_route(&options, 1, 10, 0, true), AppendRoute::Plain);
    }

    #[test]
    fn tail_compaction_starts_exactly_at_its_bound_and_wins_over_compact_publication() {
        let options = GatewayOptions { compact_extents: Some(3500), compact_append: true, ..GatewayOptions::default() };
        assert_eq!(choose_append_route(&options, 1, 3498, 2, false), AppendRoute::Staged);
        assert_eq!(choose_append_route(&options, 1, 3499, 2, false), AppendRoute::Compacting(3500));
        assert_eq!(choose_append_route(&options, 0, 3500, 1, false), AppendRoute::Compacting(3500));
        let only = GatewayOptions { compact_extents: Some(3500), ..GatewayOptions::default() };
        assert_eq!(choose_append_route(&only, 0, 3499, 1, false), AppendRoute::Plain);
    }

    /// A plane whose `append_best` refuses the first `fail_times` appends the way the SDK does
    /// when a reserved staged allocation no longer matches the metadata record.
    struct StaleAllocationPlane {
        inner: MemoryPlane,
        fail_times: usize,
        seen: usize,
    }

    impl ObjectPlane for StaleAllocationPlane {
        fn read_manifest(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.inner.read_manifest(b, k) }
        fn write(&mut self, b: &str, k: &str, e: Option<ObjectManifest>, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.inner.write(b, k, e, d) }
        fn append(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.inner.append(e, d) }
        fn append_best(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
            self.seen += 1;
            if self.seen <= self.fail_times {
                return Err(ObjectError::InvalidArgument("staged allocation does not match object write".into()).into());
            }
            self.inner.append(e, d)
        }
        fn read(&mut self, b: &str, k: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.inner.read(b, k) }
        fn read_range(&mut self, b: &str, k: &str, o: u64, l: u64) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.inner.read_range(b, k, o, l) }
        fn delete(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.inner.delete(b, k) }
        fn list(&mut self, b: &str, a: Option<&str>, l: u32) -> Result<ManifestListPage, ObjectError> { self.inner.list(b, a, l) }
    }

    fn stale_gateway(fail_times: usize) -> ClusterGateway {
        let shared = Arc::new(Mutex::new(Shared::default()));
        // One plane for the whole test, so its failure counter survives the pool.
        let plane = Arc::new(Mutex::new(Some(StaleAllocationPlane {
            inner: MemoryPlane(shared),
            fail_times,
            seen: 0,
        })));
        struct Shim(Arc<Mutex<Option<StaleAllocationPlane>>>);
        impl ObjectPlane for Shim {
            fn read_manifest(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.0.lock().unwrap().as_mut().unwrap().read_manifest(b, k) }
            fn write(&mut self, b: &str, k: &str, e: Option<ObjectManifest>, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.0.lock().unwrap().as_mut().unwrap().write(b, k, e, d) }
            fn append(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.0.lock().unwrap().as_mut().unwrap().append(e, d) }
            fn append_best(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.0.lock().unwrap().as_mut().unwrap().append_best(e, d) }
            fn read(&mut self, b: &str, k: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.0.lock().unwrap().as_mut().unwrap().read(b, k) }
            fn read_range(&mut self, b: &str, k: &str, o: u64, l: u64) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.0.lock().unwrap().as_mut().unwrap().read_range(b, k, o, l) }
            fn delete(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.0.lock().unwrap().as_mut().unwrap().delete(b, k) }
            fn list(&mut self, b: &str, a: Option<&str>, l: u32) -> Result<ManifestListPage, ObjectError> { self.0.lock().unwrap().as_mut().unwrap().list(b, a, l) }
        }
        ClusterGateway::new(
            move || Ok(Box::new(Shim(Arc::clone(&plane))) as Box<dyn ObjectPlane>),
            GatewayOptions::default(),
            "b",
        )
    }

    /// A plane that fails ONE write the way a metadata Raft group does when its proposal does
    /// not complete in time, optionally after the write really committed.
    struct TimeoutPlane {
        inner: Arc<Mutex<MemoryPlane>>,
        error: ObjectError,
        commit_first: bool,
        /// Whether the failure carries the extents the attempt published, as the SDK's does.
        report: bool,
        armed: Arc<Mutex<bool>>,
    }

    impl TimeoutPlane {
        fn fire(&self) -> bool {
            std::mem::replace(&mut *self.armed.lock().unwrap(), false)
        }
    }

    impl ObjectPlane for TimeoutPlane {
        fn read_manifest(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.inner.lock().unwrap().read_manifest(b, k) }
        fn write(&mut self, b: &str, k: &str, e: Option<ObjectManifest>, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
            self.inner.lock().unwrap().write(b, k, e, d)
        }
        fn append(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.inner.lock().unwrap().append(e, d) }
        fn append_best(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
            if !self.fire() {
                return self.inner.lock().unwrap().append(e, d);
            }
            if self.commit_first {
                let kept = e.extents.len();
                let committed = self.inner.lock().unwrap().append(e, d)?;
                let attempted = if self.report { committed.extents[kept..].to_vec() } else { Vec::new() };
                return Err(PlaneFailure { error: self.error.clone_error(), unconfirmed: false, attempted });
            }
            Err(self.error.clone_error().into())
        }
        fn read(&mut self, b: &str, k: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.inner.lock().unwrap().read(b, k) }
        fn read_range(&mut self, b: &str, k: &str, o: u64, l: u64) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.inner.lock().unwrap().read_range(b, k, o, l) }
        fn delete(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.inner.lock().unwrap().delete(b, k) }
        fn list(&mut self, b: &str, a: Option<&str>, l: u32) -> Result<ManifestListPage, ObjectError> { self.inner.lock().unwrap().list(b, a, l) }
    }

    trait CloneError {
        fn clone_error(&self) -> ObjectError;
    }
    impl CloneError for ObjectError {
        fn clone_error(&self) -> ObjectError {
            match self {
                ObjectError::InvalidArgument(m) => ObjectError::InvalidArgument(m.clone()),
                ObjectError::PreconditionFailed(m) => ObjectError::PreconditionFailed(m.clone()),
                ObjectError::Corruption(m) => ObjectError::Corruption(m.clone()),
                ObjectError::NotFound(m) => ObjectError::NotFound(m.clone()),
            }
        }
    }
    const RAFT_DEADLINE: &str = "metadata consensus deadline; last observed phase=proposal completion; write outcome may be committed; do not replay";

    /// The first append (or whole-object write) after `arm` fails with `error`; when
    /// `commit_first` the bytes really were written before the failure.
    fn timeout_gateway(error: ObjectError, commit_first: bool) -> (ClusterGateway, Arc<Mutex<bool>>) {
        timeout_gateway_with(error, commit_first, true)
    }

    fn timeout_gateway_with(error: ObjectError, commit_first: bool, report: bool) -> (ClusterGateway, Arc<Mutex<bool>>) {
        let inner = Arc::new(Mutex::new(MemoryPlane(Arc::new(Mutex::new(Shared::default())))));
        let armed = Arc::new(Mutex::new(false));
        let handle_armed = Arc::clone(&armed);
        let gateway = ClusterGateway::new(
            move || {
                Ok(Box::new(TimeoutPlane {
                    inner: Arc::clone(&inner),
                    error: error.clone_error(),
                    commit_first,
                    report,
                    armed: Arc::clone(&handle_armed),
                }) as Box<dyn ObjectPlane>)
            },
            GatewayOptions::default(),
            "b",
        );
        (gateway, armed)
    }

    /// The shape a refusal has once it crossed the metadata wire: `error.to_string()` rebuilt as an
    /// `InvalidArgument`, so the message starts with the `Display` prefix.
    fn over_the_wire(error: ObjectError) -> ObjectError {
        crate::cluster::StatusCode::for_error(&error).into_error(error.to_string())
    }

    #[test]
    fn a_consensus_deadline_that_crossed_the_wire_is_a_503_before_the_append_landed() {
        let wire = over_the_wire(ObjectError::InvalidArgument(RAFT_DEADLINE.into()));
        let (gateway, armed) = timeout_gateway(wire, false);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 503, "{}", String::from_utf8_lossy(&response.body));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abc");
    }

    #[test]
    fn a_consensus_deadline_that_crossed_the_wire_after_the_append_landed_is_answered_as_landed() {
        let wire = over_the_wire(ObjectError::InvalidArgument(RAFT_DEADLINE.into()));
        let (gateway, armed) = timeout_gateway(wire, true);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde", "landed exactly once");
    }

    const LEADERSHIP_CHANGED: &str =
        "metadata leadership changed; write outcome may be committed; last observed phase=proposal completion";

    #[test]
    fn a_leader_side_queue_expiry_is_a_503_and_nothing_is_written() {
        for message in [
            "metadata request expired before proposal",
            "metadata request expired before read confirmation",
            "metadata expiry request expired before read confirmation",
            "chunk request expired before proposal",
        ] {
            let (gateway, armed) = timeout_gateway(ObjectError::InvalidArgument(message.into()), false);
            assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
            *armed.lock().unwrap() = true;
            let response = append(&gateway, "wal", 3, b"de");
            assert_eq!(response.status, 503, "{message}: {}", String::from_utf8_lossy(&response.body));
            assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abc", "{message}");
        }
    }

    #[test]
    fn a_leadership_change_after_the_append_landed_is_answered_as_landed() {
        let (gateway, armed) = timeout_gateway(ObjectError::InvalidArgument(LEADERSHIP_CHANGED.into()), true);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde", "landed exactly once");
    }

    #[test]
    fn a_leadership_change_before_the_append_landed_is_a_503_and_nothing_is_written() {
        let (gateway, armed) = timeout_gateway(ObjectError::InvalidArgument(LEADERSHIP_CHANGED.into()), false);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 503, "{}", String::from_utf8_lossy(&response.body));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abc");
    }

    /// A plane that refuses every CREATE (a write with nothing expected) once `full` is set, the way the
    /// metaserver refuses it after the metadata wire, and counts how often a create was attempted.
    struct FullPlane {
        inner: Arc<Mutex<MemoryPlane>>,
        error: ObjectError,
        full: Arc<Mutex<bool>>,
        creates: Arc<Mutex<usize>>,
    }

    impl ObjectPlane for FullPlane {
        fn read_manifest(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.inner.lock().unwrap().read_manifest(b, k) }
        fn write(&mut self, b: &str, k: &str, e: Option<ObjectManifest>, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
            if e.is_none() && *self.full.lock().unwrap() {
                *self.creates.lock().unwrap() += 1;
                return Err(self.error.clone_error().into());
            }
            self.inner.lock().unwrap().write(b, k, e, d)
        }
        fn append(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.inner.lock().unwrap().append(e, d) }
        fn append_best(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.inner.lock().unwrap().append_best(e, d) }
        fn read(&mut self, b: &str, k: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.inner.lock().unwrap().read(b, k) }
        fn read_range(&mut self, b: &str, k: &str, o: u64, l: u64) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.inner.lock().unwrap().read_range(b, k, o, l) }
        fn delete(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.inner.lock().unwrap().delete(b, k) }
        fn list(&mut self, b: &str, a: Option<&str>, l: u32) -> Result<ManifestListPage, ObjectError> { self.inner.lock().unwrap().list(b, a, l) }
    }

    /// A create the catalog has no room for, in the shape it has after the metadata wire, is a 507 that
    /// says so and is attempted ONCE (no retry, no read-back loop); it is never "corruption" and never
    /// "unconfirmed". An object that already exists can still be overwritten and deleted.
    #[test]
    fn a_catalog_full_refusal_is_a_507_answered_once_and_never_corruption() {
        for inner in [
            crate::object_manifest::catalog_full(8),
            crate::object_manifest::catalog_image_too_large(),
        ] {
            let wire = crate::cluster::StatusCode::for_error(&inner).into_error(inner.to_string());
            let shared = Arc::new(Mutex::new(MemoryPlane(Arc::new(Mutex::new(Shared::default())))));
            let full = Arc::new(Mutex::new(false));
            let creates = Arc::new(Mutex::new(0usize));
            let (plane, flag, count) = (Arc::clone(&shared), Arc::clone(&full), Arc::clone(&creates));
            let gateway = ClusterGateway::new(
                move || {
                    Ok(Box::new(FullPlane {
                        inner: Arc::clone(&plane),
                        error: wire.clone_error(),
                        full: Arc::clone(&flag),
                        creates: Arc::clone(&count),
                    }) as Box<dyn ObjectPlane>)
                },
                GatewayOptions::default(),
                "b",
            );
            assert_eq!(gateway.handle(&request("PUT", "/b/first", &[], b"one")).status, 200);
            *full.lock().unwrap() = true;
            let response = gateway.handle(&request("PUT", "/b/second", &[], b"two"));
            let text = String::from_utf8_lossy(&response.body).to_string();
            assert_eq!(response.status, 507, "{text}");
            assert!(text.contains("catalog full") || text.contains("catalog image too large"), "{text}");
            assert!(!text.contains("corruption") && !text.contains("unconfirmed"), "{text}");
            assert_eq!(*creates.lock().unwrap(), 1, "the create is attempted once");
            assert_eq!(gateway.handle(&request("GET", "/b/second", &[], b"")).status, 404);
            assert_eq!(gateway.handle(&request("PUT", "/b/first", &[], b"uno")).status, 200, "an existing object can be overwritten");
            assert_eq!(gateway.handle(&request("DELETE", "/b/first", &[], b"")).status, 204, "and deleted");
        }
    }

    #[test]
    fn a_consensus_timeout_after_the_append_landed_is_answered_as_landed() {
        let (gateway, armed) = timeout_gateway(ObjectError::InvalidArgument(RAFT_DEADLINE.into()), true);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 200, "{}", String::from_utf8_lossy(&response.body));
        assert_eq!(header(&response, "x-amz-object-size"), Some("5"));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde", "landed exactly once");
        // A client that retried the same append anyway is told the offset is taken, and the
        // record is not written a second time.
        assert_eq!(append(&gateway, "wal", 3, b"de").status, 412);
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde");
    }

    #[test]
    fn a_consensus_timeout_before_the_append_landed_is_a_503_and_nothing_is_written() {
        let (gateway, armed) = timeout_gateway(ObjectError::InvalidArgument(RAFT_DEADLINE.into()), false);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 503, "{}", String::from_utf8_lossy(&response.body));
        assert!(String::from_utf8_lossy(&response.body).contains("metadata consensus deadline"));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abc");
        // The client's retry at the same offset then lands.
        assert_eq!(append(&gateway, "wal", 3, b"de").status, 200);
    }

    const NO_HEALTHY_GROUP: &str = "precondition failed: root placement could not satisfy replica count";

    #[test]
    fn a_placement_with_no_healthy_group_is_a_503_and_not_a_lost_race() {
        let error = ObjectError::PreconditionFailed(NO_HEALTHY_GROUP.into());
        assert!(!lost_race(&error), "it must not be retried at once as a race");
        let (gateway, armed) = timeout_gateway(error, false);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 503, "{}", String::from_utf8_lossy(&response.body));
        assert!(String::from_utf8_lossy(&response.body).contains("could not satisfy replica count"));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abc", "nothing was written");
        // the client's retry at the same offset lands once the groups are healthy again
        assert_eq!(append(&gateway, "wal", 3, b"de").status, 200);
    }

    #[test]
    fn a_request_no_leader_admitted_is_a_503_and_not_a_lost_race() {
        let error = ObjectError::PreconditionFailed("precondition failed: metadata request was not admitted by a leader".into());
        assert!(!lost_race(&error));
        let (gateway, armed) = timeout_gateway(error, false);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        let response = append(&gateway, "wal", 3, b"de");
        assert_eq!(response.status, 503, "{}", String::from_utf8_lossy(&response.body));
        assert!(String::from_utf8_lossy(&response.body).contains("not admitted by a leader"));
        assert_eq!(append(&gateway, "wal", 3, b"de").status, 200);
    }

    #[test]
    fn a_real_lost_race_is_still_a_412() {
        // another writer moved the object: a precondition refusal that is not the placement's
        let error = ObjectError::PreconditionFailed("object manifest changed before write".into());
        assert!(lost_race(&error));
        let response = error_response(error, "/b/k", false);
        assert_eq!(response.status, 412);
    }

    #[test]
    fn a_full_consensus_queue_is_a_503_and_never_a_400() {
        let (gateway, armed) = timeout_gateway(
            ObjectError::InvalidArgument("metadata consensus unavailable or request queue full".into()),
            false,
        );
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        *armed.lock().unwrap() = true;
        assert_eq!(append(&gateway, "wal", 3, b"de").status, 503);
    }

    #[test]
    fn a_request_the_gateway_itself_rejects_is_still_a_400() {
        let (gateway, _) = timeout_gateway(ObjectError::InvalidArgument("unused".into()), false);
        assert_eq!(gateway.handle(&request("PUT", "/b/k", &[(WRITE_OFFSET_HEADER, "soon")], b"x")).status, 400);
    }

    #[test]
    fn a_stale_staged_allocation_is_retried_and_never_answers_400() {
        let gateway = stale_gateway(1);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        // The first staged append is refused as a mismatch; the gateway drops the reservation,
        // looks again and lands the bytes.
        let second = append(&gateway, "wal", 3, b"de");
        assert_eq!(second.status, 200, "{}", String::from_utf8_lossy(&second.body));
        assert_eq!(gateway.handle(&request("GET", "/b/wal", &[], b"")).body, b"abcde");
    }

    #[test]
    fn a_staged_mismatch_that_does_not_clear_is_a_retryable_503_not_a_400() {
        let gateway = stale_gateway(usize::MAX);
        assert_eq!(append(&gateway, "wal", 0, b"abc").status, 200);
        let refused = append(&gateway, "wal", 3, b"de");
        assert_eq!(refused.status, 503, "{}", String::from_utf8_lossy(&refused.body));
        let body = String::from_utf8_lossy(&refused.body).to_string();
        assert!(body.contains("ServiceUnavailable") && body.contains("staged allocation"), "{body}");
    }

    #[test]
    fn only_failures_a_client_must_act_on_are_logged() {
        let get = request("GET", "/b/k", &[], b"");
        let body = |code: &str, message: &str| {
            s3::error_response(
                match code {
                    "503" => s3::S3ErrorCode::ServiceUnavailable,
                    "400" => s3::S3ErrorCode::InvalidRequest,
                    "412" => s3::S3ErrorCode::PreconditionFailed,
                    _ => s3::S3ErrorCode::NoSuchKey,
                },
                message,
                "/b/k",
            )
        };
        let line = failure_log_line(&get, &body("503", "the cluster said no")).expect("a 503 is logged");
        assert!(line.contains("GET /b/k") && line.contains("503") && line.contains("the cluster said no"), "{line}");
        assert!(failure_log_line(&get, &body("400", "bad")).is_some());
        assert!(failure_log_line(&get, &body("412", "offset")).is_none(), "412 is the normal append conflict");
        assert!(failure_log_line(&get, &body("404", "gone")).is_none());
        assert!(failure_log_line(&get, &Response::status(200)).is_none());
    }

    /// A plane whose `append_best` keeps the extent count bounded the way tail compaction does.
    struct CompactingPlane(MemoryPlane, usize);

    impl ObjectPlane for CompactingPlane {
        fn read_manifest(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.0.read_manifest(b, k) }
        fn write(&mut self, b: &str, k: &str, e: Option<ObjectManifest>, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.0.write(b, k, e, d) }
        fn append(&mut self, e: ObjectManifest, d: &[u8]) -> Result<ObjectManifest, PlaneFailure> { self.0.append(e, d) }
        fn append_best(&mut self, expected: ObjectManifest, data: &[u8]) -> Result<ObjectManifest, PlaneFailure> {
            let mut shared = self.0 .0.lock().unwrap();
            shared.calls.push("append_best");
            let slot = (expected.bucket.clone(), expected.key.clone());
            let (current, bytes) = shared.objects.get(&slot).cloned().unwrap();
            if current != expected {
                return Err(ObjectError::PreconditionFailed("object manifest changed before write".into()).into());
            }
            let mut joined = bytes;
            joined.extend_from_slice(data);
            let extents = if current.extents.len() + 1 > self.1 { 2 } else { current.extents.len() + 1 };
            let m = manifest(&expected.bucket, &expected.key, current.generation + 1, joined.len() as u64, extents);
            shared.objects.insert(slot, (m.clone(), joined));
            Ok(m)
        }
        fn read(&mut self, b: &str, k: &str) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.0.read(b, k) }
        fn read_range(&mut self, b: &str, k: &str, o: u64, l: u64) -> Result<(ObjectManifest, Vec<u8>), ObjectError> { self.0.read_range(b, k, o, l) }
        fn delete(&mut self, b: &str, k: &str) -> Result<ObjectManifest, ObjectError> { self.0.delete(b, k) }
        fn list(&mut self, b: &str, a: Option<&str>, l: u32) -> Result<ManifestListPage, ObjectError> { self.0.list(b, a, l) }
    }

    #[test]
    fn with_tail_compaction_an_object_stays_appendable_past_the_extent_limit() {
        let shared = Arc::new(Mutex::new(Shared::default()));
        let handle = Arc::clone(&shared);
        let gateway = ClusterGateway::new(
            move || Ok(Box::new(CompactingPlane(MemoryPlane(Arc::clone(&handle)), 3500)) as Box<dyn ObjectPlane>),
            GatewayOptions { compact_extents: Some(3500), ..GatewayOptions::default() },
            "b",
        );
        for offset in 0..(MAX_OBJECT_EXTENTS as u64 + 200) {
            let response = append(&gateway, "wal", offset, b"x");
            assert_eq!(response.status, 200, "append {} was refused", offset + 1);
        }
        let got = gateway.handle(&request("GET", "/b/wal", &[], b""));
        assert_eq!(got.body.len(), MAX_OBJECT_EXTENTS + 200);
        assert!(shared.lock().unwrap().calls.iter().any(|c| *c == "append_best"));
    }

    #[test]
    fn ranged_reads_clamp_and_report_the_total() {
        let (gateway, shared) = gateway();
        gateway.handle(&request("PUT", "/b/k", &[], b"0123456789"));
        let closed = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=2-4")], b""));
        assert_eq!((closed.status, closed.body.as_slice()), (206, b"234".as_slice()));
        assert_eq!(header(&closed, "content-range"), Some("bytes 2-4/10"));
        // The hot path asked the data groups directly: no manifest read for it.
        assert!(!shared.lock().unwrap().calls.iter().skip(2).any(|c| *c == "read_manifest"));
        let open = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=7-")], b""));
        assert_eq!((open.status, open.body.as_slice()), (206, b"789".as_slice()));
        let clamped = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=8-99")], b""));
        assert_eq!((clamped.status, clamped.body.as_slice()), (206, b"89".as_slice()));
        let past = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=10-12")], b""));
        assert_eq!(past.status, 416);
        assert_eq!(header(&past, "content-range"), Some("bytes */10"));
        assert_eq!(gateway.handle(&request("GET", "/b/none", &[("Range", "bytes=0-1")], b"")).status, 404);
    }

    #[test]
    fn listing_filters_by_prefix_skips_tombstones_and_pages() {
        let (gateway, _) = gateway();
        for key in ["a/1", "a/2", "a/3", "b/1", "c"] {
            gateway.handle(&request("PUT", &format!("/b/{key}"), &[], b"x"));
        }
        gateway.handle(&request("DELETE", "/b/a/2", &[], b""));
        let keys = |response: &Response| -> Vec<String> {
            let body = String::from_utf8(response.body.clone()).unwrap();
            body.split("<Key>").skip(1).map(|s| s.split('<').next().unwrap().to_string()).collect()
        };
        let all = gateway.handle(&request("GET", "/b?list-type=2&prefix=a%2F", &[], b""));
        assert_eq!(keys(&all), ["a/1", "a/3"]);
        let page = gateway.handle(&request("GET", "/b?list-type=2&max-keys=2", &[], b""));
        let text = String::from_utf8(page.body.clone()).unwrap();
        assert_eq!(keys(&page), ["a/1", "a/3"]);
        assert!(text.contains("<IsTruncated>true</IsTruncated>"), "{text}");
        assert!(text.contains("<NextContinuationToken>a/3</NextContinuationToken>"), "{text}");
        let rest = gateway.handle(&request("GET", "/b?list-type=2&continuation-token=a%2F3", &[], b""));
        assert_eq!(keys(&rest), ["b/1", "c"]);
        let after = gateway.handle(&request("GET", "/b?list-type=2&start-after=b%2F1", &[], b""));
        assert_eq!(keys(&after), ["c"]);
    }

    #[test]
    fn listing_walks_more_than_one_catalog_page() {
        let (gateway, _) = gateway();
        for index in 0..70 {
            gateway.handle(&request("PUT", &format!("/b/k{index:03}"), &[], b"x"));
        }
        let all = gateway.handle(&request("GET", "/b?list-type=2", &[], b""));
        let text = String::from_utf8(all.body).unwrap();
        assert_eq!(text.matches("<Key>").count(), 70);
        assert!(text.contains("<IsTruncated>false</IsTruncated>"));
    }

    #[test]
    fn every_verb_outside_the_subset_is_not_implemented() {
        let (gateway, _) = gateway();
        for (method, target, headers) in [
            ("PUT", "/b/k?partNumber=1&uploadId=u", vec![("x-amz-copy-source", "/b/other")]),
            ("PUT", "/b/k?tagging", vec![]),
            ("PUT", "/b/k?acl", vec![]),
            ("GET", "/b/k?versionId=3", vec![]),
            ("GET", "/b?versions", vec![]),
            ("PUT", "/b?cors", vec![]),
            ("PUT", "/b?versioning", vec![]),
            ("PUT", "/b?acl", vec![]),
            ("GET", "/b?policy", vec![]),
            ("GET", "/b?lifecycle", vec![]),
        ] {
            let response = gateway.handle(&request(method, target, &headers, b""));
            assert_eq!(response.status, 501, "{method} {target}");
            assert!(
                String::from_utf8_lossy(&response.body).contains("NotImplemented"),
                "{method} {target}"
            );
        }
    }

    #[test]
    fn bucket_probes_are_answered_without_the_cluster() {
        let (gateway, shared) = gateway();
        assert_eq!(gateway.handle(&request("PUT", "/b", &[], b"")).status, 200);
        assert_eq!(gateway.handle(&request("HEAD", "/b", &[], b"")).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b?location", &[], b"")).status, 200);
        assert!(shared.lock().unwrap().calls.is_empty());
        assert_eq!(gateway.handle(&request("GET", "/healthz", &[], b"")).status, 200);
    }

    #[test]
    fn a_bad_offset_header_is_a_400() {
        let (gateway, _) = gateway();
        let response = gateway.handle(&request("PUT", "/b/k", &[(WRITE_OFFSET_HEADER, "soon")], b"x"));
        assert_eq!(response.status, 400);
    }

    #[test]
    fn a_failed_connect_is_not_pooled_and_surfaces_as_an_error() {
        let gateway = ClusterGateway::new(
            || Err(ObjectError::Corruption("no metadata endpoint answered".into())),
            GatewayOptions::default(),
            "b",
        );
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).status, 500);
        assert_eq!(gateway.handle(&request("GET", "/healthz", &[], b"")).status, 503);
    }

    // ----- the S3 verbs beyond put, get, append and delete -----

    fn text(response: &Response) -> String {
        String::from_utf8_lossy(&response.body).to_string()
    }

    fn between(body: &str, tag: &str) -> Option<String> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        body.split_once(&open)?.1.split_once(&close).map(|(value, _)| value.to_string())
    }

    fn all_between(body: &str, tag: &str) -> Vec<String> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        body.split(&open)
            .skip(1)
            .filter_map(|rest| rest.split_once(&close).map(|(value, _)| value.to_string()))
            .collect()
    }

    fn put(gateway: &ClusterGateway, key: &str, body: &[u8]) -> Response {
        gateway.handle(&request("PUT", &format!("/b/{key}"), &[], body))
    }

    fn begin(gateway: &ClusterGateway, key: &str) -> String {
        let response = gateway.handle(&request("POST", &format!("/b/{key}?uploads"), &[], b""));
        assert_eq!(response.status, 200, "{}", text(&response));
        between(&text(&response), "UploadId").unwrap()
    }

    fn part(gateway: &ClusterGateway, key: &str, id: &str, number: u32, body: &[u8]) -> Response {
        gateway.handle(&request(
            "PUT",
            &format!("/b/{key}?partNumber={number}&uploadId={id}"),
            &[],
            body,
        ))
    }

    fn complete_body(parts: &[(u32, &str)]) -> String {
        let mut body = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            body.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>&quot;{etag}&quot;</ETag></Part>"
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        body
    }

    fn etag_header(response: &Response) -> String {
        header(response, "etag").unwrap().trim_matches('"').to_string()
    }

    #[test]
    fn a_listing_with_a_delimiter_folds_keys_into_common_prefixes() {
        let (gateway, _) = gateway();
        for key in ["a/1", "a/2", "a/x/3", "b/1", "c", "d"] {
            assert_eq!(put(&gateway, key, b"x").status, 200);
        }
        let all = text(&gateway.handle(&request("GET", "/b?list-type=2&delimiter=%2F", &[], b"")));
        assert_eq!(all_between(&all, "Key"), ["c", "d"], "{all}");
        assert_eq!(all_between(&all, "Prefix").iter().filter(|p| p.ends_with('/')).count(), 2, "{all}");
        assert!(all.contains("<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"), "{all}");
        assert!(all.contains("<CommonPrefixes><Prefix>b/</Prefix></CommonPrefixes>"), "{all}");
        // Under a prefix the next level is folded.
        let under = text(&gateway.handle(&request("GET", "/b?list-type=2&prefix=a%2F&delimiter=%2F", &[], b"")));
        assert_eq!(all_between(&under, "Key"), ["a/1", "a/2"], "{under}");
        assert!(under.contains("<CommonPrefixes><Prefix>a/x/</Prefix></CommonPrefixes>"), "{under}");
        // A delimiter nothing contains folds nothing.
        let none = text(&gateway.handle(&request("GET", "/b?list-type=2&delimiter=%7C", &[], b"")));
        assert_eq!(all_between(&none, "Key").len(), 6, "{none}");
    }

    #[test]
    fn a_delimited_listing_pages_without_losing_or_repeating_anything() {
        let (gateway, _) = gateway();
        for key in ["a/1", "a/2", "a/3", "b/1", "b/2", "c", "d/1", "e"] {
            assert_eq!(put(&gateway, key, b"x").status, 200);
        }
        let mut seen: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..20 {
            let target = match &token {
                None => "/b?list-type=2&delimiter=%2F&max-keys=2".to_string(),
                Some(token) => format!("/b?list-type=2&delimiter=%2F&max-keys=2&continuation-token={}", http_escape(token)),
            };
            let page = text(&gateway.handle(&request("GET", &target, &[], b"")));
            seen.extend(all_between(&page, "Key"));
            seen.extend(
                page.split("<CommonPrefixes><Prefix>")
                    .skip(1)
                    .filter_map(|rest| rest.split_once("</Prefix>").map(|(value, _)| value.to_string())),
            );
            if between(&page, "IsTruncated").as_deref() == Some("true") {
                token = Some(between(&page, "NextContinuationToken").expect("a truncated page names its token"));
            } else {
                break;
            }
        }
        seen.sort();
        assert_eq!(seen, ["a/", "b/", "c", "d/", "e"]);
    }

    fn http_escape(value: &str) -> String {
        value
            .bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (byte as char).to_string(),
                _ => format!("%{byte:02X}"),
            })
            .collect()
    }

    #[test]
    fn a_multipart_upload_assembles_one_object_and_leaves_nothing_behind() {
        let (gateway, shared) = gateway();
        let id = begin(&gateway, "big");
        let two = part(&gateway, "big", &id, 2, b"-world");
        let one = part(&gateway, "big", &id, 1, b"hello");
        assert_eq!((one.status, two.status), (200, 200));
        let (e1, e2) = (etag_header(&one), etag_header(&two));
        // The object does not exist, and the upload's records are not listed, before completion.
        assert_eq!(gateway.handle(&request("GET", "/b/big", &[], b"")).status, 404);
        let listing = text(&gateway.handle(&request("GET", "/b?list-type=2", &[], b"")));
        assert!(!listing.contains(".multipart"), "{listing}");
        let parts = text(&gateway.handle(&request("GET", &format!("/b/big?uploadId={id}"), &[], b"")));
        assert_eq!(all_between(&parts, "PartNumber"), ["1", "2"], "{parts}");
        assert_eq!(all_between(&parts, "Size"), ["5", "6"], "{parts}");
        let uploads = text(&gateway.handle(&request("GET", "/b?uploads", &[], b"")));
        assert!(uploads.contains(&format!("<UploadId>{id}</UploadId>")) && uploads.contains("<Key>big</Key>"), "{uploads}");
        // Completing publishes the concatenation, once.
        let done = gateway.handle(&request(
            "POST",
            &format!("/b/big?uploadId={id}"),
            &[],
            complete_body(&[(1, &e1), (2, &e2)]).as_bytes(),
        ));
        assert_eq!(done.status, 200, "{}", text(&done));
        assert!(text(&done).contains("<CompleteMultipartUploadResult"), "{}", text(&done));
        assert_eq!(gateway.handle(&request("GET", "/b/big", &[], b"")).body, b"hello-world");
        // The parts and the record are gone, and the upload with them.
        let gone = gateway.handle(&request("GET", &format!("/b/big?uploadId={id}"), &[], b""));
        assert_eq!(gone.status, 404, "{}", text(&gone));
        let live: Vec<String> = shared
            .lock()
            .unwrap()
            .objects
            .iter()
            .filter(|(_, (m, _))| !m.deleted)
            .map(|((_, key), _)| key.clone())
            .collect();
        assert_eq!(live, ["big"]);
        let again = gateway.handle(&request(
            "POST",
            &format!("/b/big?uploadId={id}"),
            &[],
            complete_body(&[(1, &e1), (2, &e2)]).as_bytes(),
        ));
        assert_eq!(again.status, 404, "completing twice");
    }

    #[test]
    fn a_multipart_completion_is_checked_against_the_parts_the_upload_holds() {
        let (gateway, _) = gateway();
        let id = begin(&gateway, "k");
        let one = etag_header(&part(&gateway, "k", &id, 1, b"aaa"));
        let two = etag_header(&part(&gateway, "k", &id, 2, b"bbb"));
        let complete = |parts: &[(u32, &str)]| {
            gateway.handle(&request("POST", &format!("/b/k?uploadId={id}"), &[], complete_body(parts).as_bytes()))
        };
        let order = complete(&[(2, &two), (1, &one)]);
        assert_eq!(order.status, 400);
        assert!(text(&order).contains("InvalidPartOrder"), "{}", text(&order));
        let wrong = complete(&[(1, "gen-999")]);
        assert_eq!(wrong.status, 400);
        assert!(text(&wrong).contains("InvalidPart"), "{}", text(&wrong));
        let missing = complete(&[(1, &one), (3, &two)]);
        assert_eq!(missing.status, 400);
        assert!(text(&missing).contains("InvalidPart"), "{}", text(&missing));
        let malformed = gateway.handle(&request("POST", &format!("/b/k?uploadId={id}"), &[], b"<nope/>"));
        assert_eq!(malformed.status, 400);
        assert!(text(&malformed).contains("MalformedXML"), "{}", text(&malformed));
        // Nothing was published by any refusal, and the upload is still usable.
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).status, 404);
        assert_eq!(complete(&[(1, &one)]).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).body, b"aaa");
        // A part uploaded again replaces the part, and its old etag no longer completes.
        let id = begin(&gateway, "r");
        let first = etag_header(&part(&gateway, "r", &id, 1, b"old"));
        let second = etag_header(&part(&gateway, "r", &id, 1, b"new"));
        assert_ne!(first, second);
        let stale = gateway.handle(&request("POST", &format!("/b/r?uploadId={id}"), &[], complete_body(&[(1, &first)]).as_bytes()));
        assert_eq!(stale.status, 400);
        let fresh = gateway.handle(&request("POST", &format!("/b/r?uploadId={id}"), &[], complete_body(&[(1, &second)]).as_bytes()));
        assert_eq!(fresh.status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/r", &[], b"")).body, b"new");
    }

    #[test]
    fn multipart_requests_for_the_wrong_upload_or_part_are_refused() {
        let (gateway, _) = gateway();
        let id = begin(&gateway, "k");
        for (target, status) in [
            (format!("/b/k?partNumber=0&uploadId={id}"), 400),
            (format!("/b/k?partNumber=10001&uploadId={id}"), 400),
            (format!("/b/k?partNumber=x&uploadId={id}"), 400),
            (format!("/b/other?partNumber=1&uploadId={id}"), 404),
            ("/b/k?partNumber=1&uploadId=nonsense".to_string(), 404),
            (format!("/b/k?partNumber=1&uploadId={}", "0".repeat(32)), 404),
        ] {
            assert_eq!(gateway.handle(&request("PUT", &target, &[], b"x")).status, status, "{target}");
        }
        // An upload part copy is not served, and the method on an upload is checked.
        let copy = gateway.handle(&request("PUT", &format!("/b/k?partNumber=1&uploadId={id}"), &[("x-amz-copy-source", "/b/a")], b""));
        assert_eq!(copy.status, 501);
        assert_eq!(gateway.handle(&request("PATCH", &format!("/b/k?uploadId={id}"), &[], b"")).status, 405);
        // Abort removes the upload and everything it holds; aborting again says it is gone.
        part(&gateway, "k", &id, 1, b"x");
        assert_eq!(gateway.handle(&request("DELETE", &format!("/b/k?uploadId={id}"), &[], b"")).status, 204);
        assert_eq!(gateway.handle(&request("DELETE", &format!("/b/k?uploadId={id}"), &[], b"")).status, 404);
        let uploads = text(&gateway.handle(&request("GET", "/b?uploads", &[], b"")));
        assert!(!uploads.contains("<Upload>"), "{uploads}");
    }

    #[test]
    fn an_assembled_object_over_the_gateways_limit_is_refused_and_the_upload_survives() {
        let shared = Arc::new(Mutex::new(Shared::default()));
        let handle = Arc::clone(&shared);
        let gateway = ClusterGateway::new(
            move || Ok(Box::new(MemoryPlane(Arc::clone(&handle))) as Box<dyn ObjectPlane>),
            GatewayOptions { max_object_bytes: 8, ..GatewayOptions::default() },
            "b",
        );
        let id = begin(&gateway, "k");
        let one = etag_header(&part(&gateway, "k", &id, 1, b"12345"));
        let two = etag_header(&part(&gateway, "k", &id, 2, b"67890"));
        let refused = gateway.handle(&request("POST", &format!("/b/k?uploadId={id}"), &[], complete_body(&[(1, &one), (2, &two)]).as_bytes()));
        assert_eq!(refused.status, 400, "{}", text(&refused));
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).status, 404);
        let fits = gateway.handle(&request("POST", &format!("/b/k?uploadId={id}"), &[], complete_body(&[(1, &one)]).as_bytes()));
        assert_eq!(fits.status, 200);
    }

    #[test]
    fn a_copy_makes_the_destination_hold_the_source() {
        let (gateway, _) = gateway();
        put(&gateway, "src", b"payload");
        let copied = gateway.handle(&request("PUT", "/b/dst", &[("x-amz-copy-source", "/b/src")], b""));
        assert_eq!(copied.status, 200, "{}", text(&copied));
        assert!(text(&copied).contains("<CopyObjectResult"), "{}", text(&copied));
        assert!(between(&text(&copied), "ETag").is_some(), "{}", text(&copied));
        assert_eq!(gateway.handle(&request("GET", "/b/dst", &[], b"")).body, b"payload");
        // The source is untouched, and copying over an existing destination replaces it.
        put(&gateway, "src", b"newer");
        let over = gateway.handle(&request("PUT", "/b/dst", &[("x-amz-copy-source", "b/src")], b""));
        assert_eq!(over.status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/dst", &[], b"")).body, b"newer");
        assert_eq!(gateway.handle(&request("GET", "/b/src", &[], b"")).body, b"newer");
        // A source with escapes is decoded.
        put(&gateway, "a b", b"spaced");
        let spaced = gateway.handle(&request("PUT", "/b/d2", &[("x-amz-copy-source", "/b/a%20b")], b""));
        assert_eq!(spaced.status, 200, "{}", text(&spaced));
        assert_eq!(gateway.handle(&request("GET", "/b/d2", &[], b"")).body, b"spaced");
    }

    #[test]
    fn a_copy_refuses_what_it_cannot_do_and_changes_nothing() {
        let (gateway, _) = gateway();
        put(&gateway, "src", b"payload");
        put(&gateway, "dst", b"keep");
        let copy = |source: &str, extra: &[(&str, &str)]| {
            let mut headers = vec![("x-amz-copy-source", source)];
            headers.extend_from_slice(extra);
            gateway.handle(&request("PUT", "/b/dst", &headers, b""))
        };
        let missing = copy("/b/absent", &[]);
        assert_eq!(missing.status, 404);
        assert!(text(&missing).contains("/b/absent"), "the missing thing is the source: {}", text(&missing));
        assert_eq!(copy("/b/dst", &[]).status, 400, "onto itself");
        assert_eq!(copy("/b/src?versionId=3", &[]).status, 400, "a version is not served");
        assert_eq!(copy("/b", &[]).status, 400, "no key");
        assert_eq!(copy("/b/.multipart/x/meta", &[]).status, 400, "reserved");
        assert_eq!(copy("/b/src", &[("x-amz-copy-source-if-match", "\"gen-999\"")]).status, 412);
        assert_eq!(copy("/b/src", &[("x-amz-copy-source-if-none-match", "\"gen-1\"")]).status, 412);
        assert_eq!(gateway.handle(&request("GET", "/b/dst", &[], b"")).body, b"keep");
        assert_eq!(copy("/b/src", &[("x-amz-copy-source-if-match", "\"gen-1\"")]).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/dst", &[], b"")).body, b"payload");
    }

    #[test]
    fn a_put_with_if_match_replaces_only_that_version() {
        let (gateway, _) = gateway();
        let created = put(&gateway, "k", b"one");
        let etag = etag_header(&created);
        let conditional = |if_match: &str, body: &[u8]| {
            gateway.handle(&request("PUT", "/b/k", &[("If-Match", if_match)], body))
        };
        assert_eq!(conditional("\"gen-999\"", b"x").status, 412);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).body, b"one");
        assert_eq!(conditional(&format!("\"{etag}\""), b"two").status, 200);
        // The version moved on, so the old etag no longer matches.
        assert_eq!(conditional(&format!("\"{etag}\""), b"three").status, 412);
        assert_eq!(conditional("*", b"four").status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).body, b"four");
        // An absent object matches no etag, `*` included.
        let absent = gateway.handle(&request("PUT", "/b/never", &[("If-Match", "*")], b"x"));
        assert_eq!(absent.status, 412);
        assert_eq!(gateway.handle(&request("GET", "/b/never", &[], b"")).status, 404);
        // A deleted object is absent too.
        gateway.handle(&request("DELETE", "/b/k", &[], b""));
        assert_eq!(conditional("*", b"x").status, 412);
    }

    #[test]
    fn a_conditional_read_answers_304_or_412_from_the_etag() {
        let (gateway, _) = gateway();
        let etag = etag_header(&put(&gateway, "k", b"hello"));
        let quoted = format!("\"{etag}\"");
        for method in ["GET", "HEAD"] {
            let same = gateway.handle(&request(method, "/b/k", &[("If-None-Match", &quoted)], b""));
            assert_eq!(same.status, 304, "{method}");
            assert!(same.body.is_empty());
            assert_eq!(header(&same, "etag"), Some(quoted.as_str()));
            let other = gateway.handle(&request(method, "/b/k", &[("If-None-Match", "\"gen-999\"")], b""));
            assert_eq!(other.status, 200, "{method}");
            let star = gateway.handle(&request(method, "/b/k", &[("If-None-Match", "*")], b""));
            assert_eq!(star.status, 304, "{method}");
            let mismatch = gateway.handle(&request(method, "/b/k", &[("If-Match", "\"gen-999\"")], b""));
            assert_eq!(mismatch.status, 412, "{method}");
            let ok = gateway.handle(&request(method, "/b/k", &[("If-Match", &quoted)], b""));
            assert_eq!(ok.status, 200, "{method}");
        }
        // The condition is decided before a ranged read as well.
        let ranged = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=0-1"), ("If-None-Match", &quoted)], b""));
        assert_eq!(ranged.status, 304);
        let ranged = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=0-1"), ("If-Match", "\"gen-999\"")], b""));
        assert_eq!(ranged.status, 412);
        let ranged = gateway.handle(&request("GET", "/b/k", &[("Range", "bytes=0-1")], b""));
        assert_eq!((ranged.status, ranged.body.as_slice()), (206, b"he".as_slice()));
        assert_eq!(gateway.handle(&request("GET", "/b/absent", &[("If-None-Match", "*")], b"")).status, 404);
    }

    #[test]
    fn a_batch_delete_reports_each_key_and_a_missing_key_is_deleted() {
        let (gateway, _) = gateway();
        put(&gateway, "a", b"1");
        put(&gateway, "b", b"2");
        let body = "<Delete><Object><Key>a</Key></Object><Object><Key>never</Key></Object><Object><Key>.multipart/x/meta</Key></Object></Delete>";
        let response = gateway.handle(&request("POST", "/b?delete", &[], body.as_bytes()));
        assert_eq!(response.status, 200, "{}", text(&response));
        let result = text(&response);
        assert_eq!(all_between(&result, "Key").len(), 3, "{result}");
        assert!(result.contains("<Deleted><Key>a</Key></Deleted>"), "{result}");
        assert!(result.contains("<Deleted><Key>never</Key></Deleted>"), "{result}");
        assert!(result.contains("<Error><Key>.multipart/x/meta</Key>"), "{result}");
        assert_eq!(gateway.handle(&request("GET", "/b/a", &[], b"")).status, 404);
        assert_eq!(gateway.handle(&request("GET", "/b/b", &[], b"")).status, 200);
        let malformed = gateway.handle(&request("POST", "/b?delete", &[], b"<nope/>"));
        assert_eq!(malformed.status, 400);
        let quiet = gateway.handle(&request("POST", "/b?delete", &[], b"<Delete><Quiet>true</Quiet><Object><Key>b</Key></Object></Delete>"));
        assert!(!text(&quiet).contains("<Deleted>"), "{}", text(&quiet));
    }

    #[test]
    fn a_bucket_is_deleted_only_when_it_holds_no_object() {
        let (gateway, _) = gateway();
        assert_eq!(gateway.handle(&request("DELETE", "/b", &[], b"")).status, 204, "an empty bucket");
        put(&gateway, "k", b"x");
        let refused = gateway.handle(&request("DELETE", "/b", &[], b""));
        assert_eq!(refused.status, 409, "{}", text(&refused));
        assert!(text(&refused).contains("BucketNotEmpty"), "{}", text(&refused));
        gateway.handle(&request("DELETE", "/b/k", &[], b""));
        assert_eq!(gateway.handle(&request("DELETE", "/b", &[], b"")).status, 204, "only a tombstone is left");
        // An upload in progress holds no object of the bucket's.
        begin(&gateway, "pending");
        assert_eq!(gateway.handle(&request("DELETE", "/b", &[], b"")).status, 204);
    }

    #[test]
    fn the_bucket_list_names_the_gateways_bucket() {
        let (gateway, shared) = gateway();
        let listed = gateway.handle(&request("GET", "/", &[], b""));
        assert_eq!(listed.status, 200);
        assert!(text(&listed).contains("<Name>b</Name>"), "{}", text(&listed));
        assert_eq!(gateway.handle(&request("POST", "/", &[], b"")).status, 405);
        assert!(shared.lock().unwrap().calls.is_empty(), "no cluster call is needed");
    }

    #[test]
    fn the_sub_resources_a_client_probes_for_are_answered() {
        let (gateway, _) = gateway();
        put(&gateway, "k", b"x");
        let versioning = gateway.handle(&request("GET", "/b?versioning", &[], b""));
        assert_eq!(versioning.status, 200);
        assert!(text(&versioning).contains("<VersioningConfiguration"), "{}", text(&versioning));
        assert!(!text(&versioning).contains("<Status>"), "versioning is not enabled");
        let acl = gateway.handle(&request("GET", "/b?acl", &[], b""));
        assert!(text(&acl).contains("FULL_CONTROL"), "{}", text(&acl));
        assert_eq!(gateway.handle(&request("GET", "/b/k?acl", &[], b"")).status, 200);
        let tags = gateway.handle(&request("GET", "/b/k?tagging", &[], b""));
        assert!(text(&tags).contains("<TagSet>"), "{}", text(&tags));
        assert_eq!(gateway.handle(&request("GET", "/b/absent?tagging", &[], b"")).status, 404);
        assert_eq!(gateway.handle(&request("GET", "/b/absent?acl", &[], b"")).status, 404);
    }

    #[test]
    fn the_reserved_prefix_is_not_an_object_namespace() {
        let (gateway, shared) = gateway();
        for method in ["PUT", "GET", "HEAD", "DELETE"] {
            let response = gateway.handle(&request(method, "/b/.multipart/x/meta", &[], b"x"));
            assert_eq!(response.status, 400, "{method}");
        }
        // Even a record that reached the plane some other way is not listed.
        shared.lock().unwrap().objects.insert(
            ("b".into(), ".multipart/abc/meta".into()),
            (manifest("b", ".multipart/abc/meta", 1, 1, 1), b"x".to_vec()),
        );
        put(&gateway, "visible", b"x");
        let listing = text(&gateway.handle(&request("GET", "/b?list-type=2", &[], b"")));
        assert_eq!(all_between(&listing, "Key"), ["visible"], "{listing}");
    }
}

#[cfg(test)]
#[path = "conditional_put_outcome_tests.rs"]
mod conditional_put_outcome_tests;
