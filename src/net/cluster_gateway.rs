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

/// The most extents the replica plane lets one object hold.
pub const MAX_OBJECT_EXTENTS: usize = 4096;

/// Offset header, spelled as the in-process door spells it.
pub const WRITE_OFFSET_HEADER: &str = super::service::WRITE_OFFSET_HEADER;

const LIST_PAGE: u32 = 32;

/// A write that did not publish, and whether it might have.
#[derive(Debug)]
pub struct PlaneFailure {
    pub error: ObjectError,
    /// Publication was attempted and its result is not confirmed visible.
    pub unconfirmed: bool,
}

impl From<ObjectError> for PlaneFailure {
    fn from(error: ObjectError) -> Self {
        Self {
            error,
            unconfirmed: false,
        }
    }
}

impl From<ObjectWriteFailure> for PlaneFailure {
    fn from(failure: ObjectWriteFailure) -> Self {
        match failure {
            ObjectWriteFailure::BeforePublication { error, .. } => Self {
                error,
                unconfirmed: false,
            },
            ObjectWriteFailure::PublicationUnconfirmed { error, .. } => Self {
                error,
                unconfirmed: true,
            },
        }
    }
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
    fn list(
        &mut self,
        bucket: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<ManifestListPage, ObjectError>;
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
    /// Manifests remembered so a repeated append can skip reading it.
    pub cache_entries: usize,
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
            cache_entries: 64,
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
    std::sync::Mutex<std::collections::HashMap<(String, String), (crate::staged_allocation::StagedAllocation, std::time::Instant)>>,
    /// When each object was last appended to, for the objects appended to lately: a reservation
    /// is made only for an object that is being appended to repeatedly.
    std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>,
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
    /// Whether this object was appended to within [`HOT_WINDOW`], and note that it is being
    /// appended to now. The map is bounded: when it is full the entries older than the window are
    /// dropped, and if it is still full it is emptied, which only makes objects cold.
    pub(crate) fn note_append(&self, bucket: &str, key: &str) -> bool {
        let Ok(mut map) = self.2.lock() else { return false };
        let now = std::time::Instant::now();
        let hot = map
            .get(&(bucket.to_owned(), key.to_owned()))
            .is_some_and(|last| now.duration_since(*last) < HOT_WINDOW);
        if map.len() >= RECENT_APPENDS_CAP {
            map.retain(|_, last| now.duration_since(*last) < HOT_WINDOW);
            if map.len() >= RECENT_APPENDS_CAP {
                map.clear();
            }
        }
        map.insert((bucket.to_owned(), key.to_owned()), now);
        hot
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
        let hot = self.compact.note_append(&expected.bucket, &expected.key);
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
            Ok(manifest) => Ok(manifest),
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
        self.client.read_object_range(
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

/// The S3 door over the cluster.
pub struct ClusterGateway {
    connect: Box<Connector>,
    pool: Mutex<Vec<Box<dyn ObjectPlane>>>,
    manifests: Mutex<HashMap<(String, String), ObjectManifest>>,
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
            manifests: Mutex::new(HashMap::new()),
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
            if cache.len() >= self.options.cache_entries.max(1) {
                cache.clear();
            }
            cache.insert(
                (manifest.bucket.clone(), manifest.key.clone()),
                manifest.clone(),
            );
        }
    }

    fn forget(&self, bucket: &str, key: &str) {
        if let Ok(mut cache) = self.manifests.lock() {
            cache.remove(&(bucket.to_string(), key.to_string()));
        }
    }

    fn cached(&self, bucket: &str, key: &str) -> Option<ObjectManifest> {
        self.manifests
            .lock()
            .ok()
            .and_then(|cache| cache.get(&(bucket.to_string(), key.to_string())).cloned())
    }

    fn route(&self, request: &Request) -> Response {
        match request.path.as_str() {
            "/healthz" | "/health" => return self.health(request),
            "/" => {
                return not_implemented("ListBuckets", "/");
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
        // Every sub-resource this door does not serve is named in the refusal. Query-selected
        // verbs come first, because S3 selects them on the same method as the plain verb.
        for sub in [
            "uploads", "versioning", "versions", "cors", "delete", "tagging", "policy", "acl",
            "lifecycle", "notification",
        ] {
            if request.query_param(sub).is_some() {
                return not_implemented(&format!("the bucket sub-resource ?{sub}"), &resource);
            }
        }
        match request.method.as_str() {
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
            "DELETE" => not_implemented("DeleteBucket", &resource),
            _ => method_not_allowed("GET, PUT, HEAD", &resource),
        }
    }

    fn object_request(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        for sub in [
            "uploads", "uploadId", "partNumber", "versionId", "tagging", "acl", "retention",
            "legal-hold", "restore", "select",
        ] {
            if request.query_param(sub).is_some() {
                return not_implemented(&format!("the object sub-resource ?{sub}"), &resource);
            }
        }
        match request.method.as_str() {
            "PUT" => {
                if request.header("x-amz-copy-source").is_some() {
                    return not_implemented("CopyObject", &resource);
                }
                if request.header("if-match").is_some() {
                    return not_implemented("a PUT conditioned on If-Match", &resource);
                }
                self.put_object(request, bucket, key)
            }
            "GET" => self.get_object(request, bucket, key, false),
            "HEAD" => self.get_object(request, bucket, key, true),
            "DELETE" => self.delete_object(bucket, key),
            "POST" => not_implemented("POST on an object (multipart upload)", &resource),
            _ => method_not_allowed("PUT, GET, HEAD, DELETE", &resource),
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
        match offset {
            Some(offset) => self.append_object(bucket, key, offset, &request.body),
            None => self.replace_object(bucket, key, &request.body, only_if_absent),
        }
    }

    /// `PutObject` that replaces: publish a new generation on top of whatever is there.
    fn replace_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        only_if_absent: bool,
    ) -> Response {
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
                Ok(plane.write(bucket, key, current, body))
            });
            match outcome {
                Err(error) => return error_response(error, &resource, false),
                Ok(Ok(manifest)) => {
                    self.remember(&manifest);
                    return object_written(&manifest);
                }
                Ok(Err(failure)) => {
                    self.forget(bucket, key);
                    if failure.unconfirmed || outcome_may_be_committed(&failure.error) {
                        if let Some(manifest) = self.resolve_landed_replace(bucket, key, body) {
                            self.remember(&manifest);
                            return object_written(&manifest);
                        }
                    }
                    if failure.unconfirmed {
                        return unconfirmed(&failure.error, &resource);
                    }
                    // Another writer published between the read and the write; the conditional
                    // publish refused it, so look again and decide on what is there now.
                    if lost_race(&failure.error) && !only_if_absent {
                        continue;
                    }
                    return error_response(failure.error, &resource, false);
                }
            }
        }
        s3::error_response(
            s3::S3ErrorCode::PreconditionFailed,
            "the object kept changing under this write; gave up after several attempts",
            &resource,
        )
    }

    /// `PutObject` with a write offset: an append that lands at the end or not at all.
    fn append_object(&self, bucket: &str, key: &str, offset: u64, body: &[u8]) -> Response {
        let resource = format!("/{bucket}/{key}");
        const ATTEMPTS: usize = 4;
        let mut last_staged_mismatch: Option<ObjectError> = None;
        // The refusal that made the last attempt retry, so a gateway that gives up can say why.
        let mut last_retried = String::new();
        for attempt in 0..ATTEMPTS {
            // A retry exists because the end moved, so it never trusts the cache.
            let cached = if attempt == 0 {
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
                    self.remember(&manifest);
                    return object_written(&manifest);
                }
                Ok(Err((failure, decided))) => {
                    self.forget(bucket, key);
                    // A write whose outcome is not known is resolved by reading, never by
                    // reporting an error for bytes that may be in the object: the client would
                    // retry, and its retry would append them a second time.
                    if failure.unconfirmed || outcome_may_be_committed(&failure.error) {
                        if let Some(manifest) = self.resolve_landed_append(bucket, key, offset, body) {
                            self.remember(&manifest);
                            return object_written(&manifest);
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

    /// After an append whose outcome is unknown: did these exact bytes land at `offset`?
    /// The object must now end at `offset + body.len()` with the body at that range.
    fn resolve_landed_append(&self, bucket: &str, key: &str, offset: u64, body: &[u8]) -> Option<ObjectManifest> {
        if body.is_empty() {
            return None;
        }
        let (manifest, bytes) = self
            .with_plane(|plane| plane.read_range(bucket, key, offset, body.len() as u64))
            .ok()?;
        (!manifest.deleted && manifest.length == offset + body.len() as u64 && bytes == body)
            .then_some(manifest)
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
        if let Some((start, length)) = range.and_then(closed_range) {
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
        if request.query_param("delimiter").is_some_and(|d| !d.is_empty()) {
            return not_implemented("ListObjectsV2 with a delimiter", &resource);
        }
        let prefix = request.query_param("prefix").unwrap_or_default();
        let continuation = request
            .query_param("continuation-token")
            .or_else(|| request.query_param("marker"));
        let start_after = request.query_param("start-after");
        let resume = continuation.clone().or_else(|| start_after.clone());
        let max_keys = match request.query_param("max-keys") {
            Some(raw) => match raw.parse::<usize>() {
                Ok(parsed) => parsed.clamp(1, super::service::MAX_LIST_LIMIT),
                Err(_) => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        "max-keys is not a number",
                        &resource,
                    )
                }
            },
            None => super::service::DEFAULT_LIST_LIMIT,
        };
        let walked = self.with_plane(|plane| {
            let mut entries: Vec<s3::ListEntry> = Vec::new();
            let mut after = resume.clone();
            let mut truncated = false;
            'pages: loop {
                let page = plane.list(bucket, after.as_deref(), LIST_PAGE)?;
                for summary in &page.entries {
                    after = Some(summary.key.clone());
                    if summary.key.as_str() < prefix.as_str() {
                        continue;
                    }
                    if !summary.key.starts_with(prefix.as_str()) {
                        // Keys are in order, so nothing later can match.
                        break 'pages;
                    }
                    if summary.deleted {
                        continue;
                    }
                    if entries.len() == max_keys {
                        truncated = true;
                        break 'pages;
                    }
                    entries.push(s3::ListEntry {
                        key: summary.key.clone(),
                        last_modified_ms: 0,
                        etag: etag_of(summary.generation),
                        size: summary.length,
                        storage_class: s3::DEFAULT_STORAGE_CLASS.to_string(),
                    });
                }
                if !page.has_more {
                    break;
                }
            }
            Ok((entries, truncated))
        });
        let (entries, truncated) = match walked {
            Ok(value) => value,
            Err(error) => return error_response(error, &resource, true),
        };
        let next = if truncated {
            entries.last().map(|entry| entry.key.clone())
        } else {
            None
        };
        let result = s3::ListResult {
            bucket: bucket.to_string(),
            prefix,
            delimiter: None,
            max_keys,
            is_truncated: truncated,
            continuation_token: continuation,
            next_continuation_token: next,
            start_after,
            entries,
            common_prefixes: Vec::new(),
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
        && !placement_unavailable(error))
        || staged_state_mismatch(error)
}

/// The metadata plane has no healthy data group to place the write on (the chunk servers'
/// heartbeats have lapsed, for example when one is missed under load). Nothing is wrong with the
/// client's request and nothing changed under it: it is the cluster being briefly unable, which
/// clears when the next heartbeat lands, so it is a retryable 503, not a lost race to retry at
/// once (which would only be refused again) and not a 412.
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
        .push(("content-type".into(), super::service::DEFAULT_CONTENT_TYPE.into()));
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
        &format!("the publication is unconfirmed and was not retried; read the object to see whether it landed: {error}"),
        resource,
    )
}

fn not_implemented(what: &str, resource: &str) -> Response {
    s3::error_response(
        s3::S3ErrorCode::NotImplemented,
        &format!(
            "{what} is not served by the cluster gateway; it serves PUT, append (x-amz-write-offset-bytes), GET (with Range), HEAD, DELETE and ListObjectsV2"
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
        _ if plane_unreachable(&error)
            || placement_unavailable(&error)
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
        assert!(!state.note_append("b", "k"), "the first append to an object is cold");
        assert!(state.note_append("b", "k"), "the next one, at once, is hot");
        assert!(!state.note_append("b", "other"), "another object is cold");
        // an append older than the window no longer makes the next one hot
        state.2.lock().unwrap().insert(
            ("b".into(), "k".into()),
            std::time::Instant::now().checked_sub(HOT_WINDOW + std::time::Duration::from_millis(50)).unwrap(),
        );
        assert!(!state.note_append("b", "k"), "an old append is cold");
        // the memory is bounded however many objects are appended to
        for index in 0..RECENT_APPENDS_CAP * 3 {
            state.note_append("b", &format!("object-{index}"));
        }
        assert!(state.2.lock().unwrap().len() <= RECENT_APPENDS_CAP);
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
            let m = manifest(bucket, key, generation, data.len() as u64, usize::from(!data.is_empty()));
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
            let m = manifest(&expected.bucket, &expected.key, current.generation + 1, joined.len() as u64, current.extents.len() + 1);
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
                self.inner.lock().unwrap().append(e, d)?;
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
        let inner = Arc::new(Mutex::new(MemoryPlane(Arc::new(Mutex::new(Shared::default())))));
        let armed = Arc::new(Mutex::new(false));
        let handle_armed = Arc::clone(&armed);
        let gateway = ClusterGateway::new(
            move || {
                Ok(Box::new(TimeoutPlane {
                    inner: Arc::clone(&inner),
                    error: error.clone_error(),
                    commit_first,
                    armed: Arc::clone(&handle_armed),
                }) as Box<dyn ObjectPlane>)
            },
            GatewayOptions::default(),
            "b",
        );
        (gateway, armed)
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
            ("POST", "/b/k?uploads", vec![]),
            ("PUT", "/b/k?partNumber=1&uploadId=u", vec![]),
            ("POST", "/b/k?uploadId=u", vec![]),
            ("PUT", "/b/k", vec![("x-amz-copy-source", "/b/other")]),
            ("PUT", "/b/k", vec![("If-Match", "\"gen-1\"")]),
            ("GET", "/b/k?tagging", vec![]),
            ("PUT", "/b/k?tagging", vec![]),
            ("GET", "/b/k?versionId=3", vec![]),
            ("GET", "/b?versioning", vec![]),
            ("GET", "/b?versions", vec![]),
            ("PUT", "/b?cors", vec![]),
            ("POST", "/b?delete", vec![]),
            ("DELETE", "/b", vec![]),
            ("GET", "/", vec![]),
            ("GET", "/b?list-type=2&delimiter=%2F", vec![]),
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
}
