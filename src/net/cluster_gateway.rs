// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! An S3 door over the replicated object plane, for the verbs a shared log needs.
//!
//! Each request is answered by the cluster SDK against a metadata quorum and its chunk groups.
//! The verb subset and the mapping are written down in `docs/cluster-gateway.md`; the short
//! version is that every verb outside the subset answers an S3 `NotImplemented` error.
//!
//! The SDK is reached through [`ObjectPlane`], one method per SDK call the gateway makes. The
//! real implementation is the chunk plane (`ChunkPlane`); the unit tests run the same routing against an
//! in-memory plane that enforces the same generation compare-and-swap and the same 4096-extent
//! limit, so the append and conflict logic is tested without a cluster. The process-level run
//! against real metadata and chunk processes is `scripts/cluster_gateway_bringup.sh`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::http::{self, Request, ResolvedRange, Response};
use super::s3;
use super::RequestHandler;
use matrixobject_client::wire::{
    AttemptId, Resolution, ResolveRequest, APPEND_STATE_AMBIGUOUS, APPEND_STATE_HEADER, OBJECT_SIZE_HEADER,
    RESOLUTION_HEADER,
};
use crate::cluster::{
    MetaClientOptions, ObjectWriteFailure, ObjectWriteOptions,
    ReplicaClientOptions,
};
use crate::cluster::{consensus_unavailable, outcome_may_be_committed};
use crate::chunk_manifest::ChunkAttributes;
use crate::object_manifest::{ManifestListPage, ObjectManifest};
use crate::ObjectError;

#[path = "cluster_gateway_s3.rs"]
mod s3_verbs;
#[path = "cluster_gateway_buckets.rs"]
mod buckets;
#[path = "cluster_gateway_attributes.rs"]
mod attributes;
#[path = "cluster_gateway_cors.rs"]
mod cors;
#[path = "cluster_gateway_quota.rs"]
mod quota;
#[path = "cluster_gateway_lifecycle.rs"]
mod lifecycle;
#[path = "cluster_gateway_versions.rs"]
mod versions;
#[path = "cluster_gateway_feed.rs"]
mod feed;
pub use feed::Change;
pub use lifecycle::LifecycleReport;

/// The most extents the replica plane lets one object hold.
pub const MAX_OBJECT_EXTENTS: usize = 4096;

/// Offset header, spelled as S3 spells it.
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

pub use matrixobject_client::wire::ATTEMPT_HEADER;

/// Echo the attempt id of a request that carried one, so a client can match the answer to the attempt.
fn with_attempt(response: Response, attempt: Option<[u8; 16]>) -> Response {
    match attempt {
        Some(id) => response.with_header(ATTEMPT_HEADER, AttemptId::from_bytes(id).hex()),
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
    /// The manifest and what the object carries besides its bytes. A plane that keeps no attributes answers none.
    fn read_manifest_with_attributes(
        &mut self,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectManifest, ChunkAttributes), ObjectError> {
        Ok((self.read_manifest(bucket, key)?, ChunkAttributes::default()))
    }
    /// [`ObjectPlane::write`] with the attributes the new generation carries; a plane that keeps none drops them.
    fn write_with(
        &mut self,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        data: &[u8],
        _attributes: &ChunkAttributes,
    ) -> Result<ObjectManifest, PlaneFailure> {
        self.write(bucket, key, expected, data)
    }
    /// Make `to_key` of the same bucket name what `from_key` names, on top of `expected` (the key as the caller read
    /// it; `None` when it must not exist yet), without moving data. A key that exists when none was expected, or
    /// that moved, is a precondition failure.
    fn alias(
        &mut self,
        _bucket: &str,
        _from_key: &str,
        _to_key: &str,
        _expected: Option<&ObjectManifest>,
    ) -> Result<ObjectManifest, PlaneFailure> {
        Err(PlaneFailure::from(ObjectError::InvalidArgument("this plane keeps no versions".into())))
    }
    /// Publish the live manifest `expected` again with other attributes and no change to its bytes.
    fn update_attributes(
        &mut self,
        _expected: &ObjectManifest,
        _attributes: &ChunkAttributes,
    ) -> Result<ObjectManifest, PlaneFailure> {
        Err(PlaneFailure::from(ObjectError::InvalidArgument("this plane keeps no object attributes".into())))
    }
    fn read_range(
        &mut self,
        bucket: &str,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<(ObjectManifest, Vec<u8>), ObjectError>;
    fn delete(&mut self, bucket: &str, key: &str) -> Result<ObjectManifest, ObjectError>;
    /// Delete the object exactly as `expected` describes it; the generation moving since is a precondition failure.
    fn delete_at(&mut self, expected: &ObjectManifest) -> Result<ObjectManifest, ObjectError> {
        self.delete(&expected.bucket, &expected.key)
    }
    /// [`ObjectPlane::copy`] giving the destination `attributes`.
    fn copy_with(
        &mut self,
        source_bucket: &str,
        source_key: &str,
        bucket: &str,
        key: &str,
        expected: Option<ObjectManifest>,
        attributes: &ChunkAttributes,
    ) -> Result<ObjectManifest, PlaneFailure> {
        let (_, data) = self.read(source_bucket, source_key)?;
        self.write_with(bucket, key, expected, &data, attributes)
    }
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
    /// Refuse a request to a bucket that has no record (`NoSuchBucket`). Off, buckets are implicit and exist as
    /// soon as an object is written under them, which is what a process that never creates buckets wants.
    pub require_buckets: bool,
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
            require_buckets: false,
        }
    }
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
    /// Buckets this gateway found to exist, or not to, and when: so an object request costs no extra read.
    buckets_known: Mutex<std::collections::HashMap<String, (Instant, bool)>>,
    /// The configuration of the buckets requests recently named, so an object request reads no record to apply a rule.
    bucket_configs: Mutex<std::collections::HashMap<String, (Instant, Option<Arc<crate::bucket_config::BucketConfig>>)>>,
    /// The live bytes of the buckets that have a quota, as last listed.
    bucket_usage: Mutex<std::collections::HashMap<String, (Instant, u64)>>,
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
            buckets_known: Mutex::new(std::collections::HashMap::new()),
            bucket_configs: Mutex::new(std::collections::HashMap::new()),
            bucket_usage: Mutex::new(std::collections::HashMap::new()),
            options,
            bucket_hint: bucket_hint.into(),
        }
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
                // implicit buckets cannot be enumerated: the gateway names the one it was started for
                return if self.options.require_buckets { self.list_buckets_from_records(request) } else { self.list_buckets(request) };
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
            // creating a bucket and configuring one are what the bucket requires nothing of
            let creates = request.method == "PUT" && request.query.is_empty();
            if !creates {
                if let Some(refusal) = self.bucket_gate(bucket) {
                    return refusal;
                }
            }
            self.bucket_request(request, bucket)
        } else {
            if let Some(refusal) = self.bucket_gate(bucket) {
                return refusal;
            }
            self.object_request(request, bucket, key)
        }
    }
}

/// How many times a resolve re-reads the object when the generation moves under its fence.
const RESOLVE_ROUNDS: usize = 3;

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
    ///   [`Resolution::Landed`], exactly, even if later appends followed;
    /// * `attempted` is known and the object already extends past the offset without them: another append took the
    ///   offset and the generation moved, so the attempt can never land: `Absent`;
    /// * `attempted` is unknown (a gateway that never saw the attempt, or a plane that does not report it) and bytes
    ///   equal to the append's (`evidence_sha256`, or any bytes when none is given) fill the range:
    ///   [`Resolution::Ambiguous`], never "landed";
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
    ) -> Result<Resolution, ObjectError> {
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
                    return Ok(Resolution::Landed);
                }
                if current != offset {
                    return Ok(Resolution::Absent);
                }
            } else if current >= end {
                let present = match evidence_sha256 {
                    Some(sha) => {
                        let (_, bytes) = self.with_plane(|plane| plane.read_range(bucket, key, offset, length))?;
                        ring::digest::digest(&ring::digest::SHA256, &bytes).as_ref() == sha
                    }
                    None => true,
                };
                return Ok(if present { Resolution::Ambiguous } else { Resolution::Absent });
            } else if current != offset {
                return Ok(Resolution::Absent);
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
                    return Ok(Resolution::Absent);
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
        let ResolveRequest { attempt, offset, length, sha256: digest } = match ResolveRequest::parse(|name| request.query_param(name)) {
            Ok(parsed) => parsed,
            Err(message) => return bad(message),
        };
        let id = attempt.into_bytes();
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
                let word = resolution.word();
                Response::text(200, format!("{word}\n")).with_header(RESOLUTION_HEADER, word)
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
        .with_header(APPEND_STATE_HEADER, APPEND_STATE_AMBIGUOUS.to_string())
        .with_header(OBJECT_SIZE_HEADER, length.to_string())
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
        if method == "DELETE" && request.query_param("delete-prefix").is_some() {
            return self.delete_prefix(request, bucket);
        }
        if request.query_param("changes").is_some() && matches!(method, "GET" | "DELETE") {
            return self.change_feed_request(request, bucket);
        }
        for sub in ["versioning", "tagging", "cors", "policy", "lifecycle", "object-lock", "quota", "changefeed"] {
            if request.query_param(sub).is_some() {
                if self.options.require_buckets {
                    if let Some(reply) = self.bucket_configuration(request, bucket, sub) {
                        return reply;
                    }
                } else if sub == "versioning" && method == "GET" {
                    // buckets are implicit here, so they hold no configuration: not versioned, and nothing to change
                    return self.probe_answer("versioning");
                } else {
                    return not_implemented(&format!("the bucket sub-resource ?{sub} (this gateway keeps no bucket records)"), &resource);
                }
            }
        }
        if method == "GET" && request.query_param("acl").is_some() {
            return self.probe_answer("acl");
        }
        if method == "GET" && request.query_param("versions").is_some() {
            return self.list_versions(request, bucket);
        }
        // Every sub-resource this door does not serve is named in the refusal.
        for sub in [
            "uploads", "delete", "acl", "notification",
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
            "PUT" => self.create_bucket(request, bucket),
            "HEAD" => self.head_bucket(bucket),
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
            ("POST", true, None, None) => return self.create_multipart_upload(request, bucket, key),
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
            for probe in ["acl"] {
                if request.query_param(probe).is_some() {
                    return match self.with_plane(|plane| plane.read_manifest(bucket, key)) {
                        Ok(manifest) if !manifest.deleted => self.probe_answer(probe),
                        Ok(_) => no_such_key(&resource),
                        Err(error) => error_response(error, &resource, false),
                    };
                }
            }
        }
        for sub in ["tagging", "retention", "legal-hold", "restore"] {
            if request.query_param(sub).is_some() {
                return self
                    .object_attribute_request(request, bucket, key, sub)
                    .unwrap_or_else(|| method_not_allowed("GET, PUT, DELETE", &resource));
            }
        }
        if let Some(version) = request.query_param("versionId") {
            return self.version_request(request, bucket, key, &version);
        }
        for sub in [
            "acl", "select",
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
            "DELETE" => self.delete_object(request, bucket, key),
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
            Some(value) => match AttemptId::parse(value) {
                Some(id) => Some(id.into_bytes()),
                None => {
                    return s3::error_response(
                        s3::S3ErrorCode::InvalidArgument,
                        &format!("{ATTEMPT_HEADER} must be 32 hex characters (16 bytes), and {value:?} is not"),
                        &resource,
                    )
                }
            },
        };
        let attributes = match self.request_attributes(request, bucket, &resource) {
            Ok(attributes) => attributes,
            Err(refusal) => return refusal,
        };
        let lock = match self.lock_check(request, bucket, &resource).and_then(|lock| self.with_quota(lock, bucket, &resource)) {
            Ok(lock) => lock,
            Err(refusal) => return refusal,
        };
        match offset {
            Some(offset) => self.append_object(bucket, key, offset, &request.body, attempt, &attributes, lock),
            None => self.replace_object(
                bucket,
                key,
                &request.body,
                only_if_absent,
                required_etag.as_deref(),
                &attributes,
                lock,
            ),
        }
    }

    /// `PutObject` that replaces: publish a new generation on top of whatever is there.
    #[allow(clippy::too_many_arguments)]
    fn replace_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        only_if_absent: bool,
        required_etag: Option<&str>,
        attributes: &ChunkAttributes,
        lock: attributes::LockCheck,
    ) -> Response {
        match self.replace_manifest_with(bucket, key, body, only_if_absent, required_etag, attributes, lock) {
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
        self.replace_manifest_with(bucket, key, body, only_if_absent, required_etag, &ChunkAttributes::default(), attributes::LockCheck::OFF)
    }

    /// [`Self::replace_manifest`] publishing `attributes` with the new generation, and refusing to replace an object a
    /// retention or a legal hold locks when `lock` is on.
    #[allow(clippy::too_many_arguments)]
    fn replace_manifest_with(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        only_if_absent: bool,
        required_etag: Option<&str>,
        attributes: &ChunkAttributes,
        lock: attributes::LockCheck,
    ) -> Result<ObjectManifest, Response> {
        let resource = format!("/{bucket}/{key}");
        const ATTEMPTS: usize = 4;
        let freed = std::cell::Cell::new(0u64);
        let versioned = self.versioning_on(bucket).map_err(|error| error_response(error, &resource, true))?;
        for _ in 0..ATTEMPTS {
            let outcome = self.with_plane(|plane| {
                let (current, current_attributes) = match plane.read_manifest_with_attributes(bucket, key) {
                    Ok((manifest, carried)) => (Some(manifest), carried),
                    Err(ObjectError::NotFound(_)) => (None, ChunkAttributes::default()),
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
                let replaced = current.as_ref().filter(|manifest| !manifest.deleted).map_or(0, |manifest| manifest.length);
                if current.as_ref().is_some_and(|manifest| !manifest.deleted) {
                    if let Some(refusal) = lock.refuse(&current_attributes) {
                        return Ok(Err(refusal));
                    }
                }
                if let Some(refusal) = lock.refuse_quota(body.len() as u64, replaced) {
                    return Ok(Err(refusal));
                }
                freed.set(replaced);
                if versioned {
                    if let Some(kept) = current.as_ref() {
                        if let Err(failure) = ClusterGateway::retain(plane, kept) {
                            return Ok(Err(failure));
                        }
                    }
                }
                Ok(plane.write_with(bucket, key, current, body, attributes))
            });
            match outcome {
                Err(error) => return Err(error_response(error, &resource, false)),
                Ok(Ok(manifest)) => {
                    self.remember(&manifest);
                    self.note_usage(bucket, body.len() as i64 - freed.get() as i64);
                    self.note_change(bucket, "put", key, manifest.generation, manifest.length);
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
    #[allow(clippy::too_many_arguments)]
    fn append_object(
        &self,
        bucket: &str,
        key: &str,
        offset: u64,
        body: &[u8],
        attempt: Option<[u8; 16]>,
        attributes: &ChunkAttributes,
        lock: attributes::LockCheck,
    ) -> Response {
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
                if let Some(refusal) = lock.refuse_quota(body.len() as u64, 0) {
                    return Ok(Err((refusal, true)));
                }
                if lock.enabled {
                    match plane.read_manifest_with_attributes(bucket, key) {
                        Ok((manifest, carried)) if !manifest.deleted => {
                            if let Some(refusal) = lock.refuse(&carried) {
                                return Ok(Err((refusal, true)));
                            }
                        }
                        Ok(_) | Err(ObjectError::NotFound(_)) => {}
                        Err(error) => return Err(error),
                    }
                }
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
                        if manifest.extents.len() + pages > MAX_OBJECT_EXTENTS {
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
                    other => plane.write_with(bucket, key, other, body, attributes),
                };
                Ok(result.map_err(|failure| (failure, false)))
            });
            match outcome {
                Err(error) => return error_response(error, &resource, false),
                Ok(Ok(manifest)) => {
                    self.note_attempt(attempt, bucket, key, offset, body.len() as u64, &manifest.extents);
                    self.remember(&manifest);
                    self.note_usage(bucket, body.len() as i64);
                    self.note_change(bucket, "append", key, manifest.generation, manifest.length);
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
                            Ok(Resolution::Landed) => {
                                if let Ok(manifest) = self.with_plane(|plane| plane.read_manifest(bucket, key)) {
                                    self.remember(&manifest);
                                    return with_attempt(object_written(&manifest), attempt);
                                }
                            }
                            Ok(Resolution::Ambiguous) => return self.append_ambiguous(bucket, key, &resource),
                            // Fenced: it did not land and now cannot. The client may retry as a new append.
                            Ok(Resolution::Absent) => {
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
            return match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, key)) {
                Ok((manifest, carried)) if !manifest.deleted => {
                    if let Some(refused) = s3_verbs::read_condition(request, &manifest, &resource) {
                        return refused;
                    }
                    let mut response = Response::status(200);
                    object_headers(&mut response, &manifest);
                    attributes::apply_attribute_headers(&mut response, &carried);
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
        let (manifest, carried) = match self.with_plane(|plane| plane.read_manifest_with_attributes(bucket, key)) {
            Ok((manifest, carried)) if !manifest.deleted => (manifest, carried),
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
                    attributes::apply_attribute_headers(&mut response, &carried);
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
                        attributes::apply_attribute_headers(&mut response, &carried);
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

    fn delete_object(&self, request: &Request, bucket: &str, key: &str) -> Response {
        let resource = format!("/{bucket}/{key}");
        let lock = match self.lock_check(request, bucket, &resource) {
            Ok(lock) => lock,
            Err(refusal) => return refusal,
        };
        self.forget(bucket, key);
        let versioned = match self.versioning_on(bucket) {
            Ok(on) => on,
            Err(error) => return error_response(error, &resource, true),
        };
        if versioned {
            return match self.versioned_delete(bucket, key, lock) {
                Ok(Some(marker)) => {
                    self.remember(&marker);
                    self.note_change(bucket, "delete", key, marker.generation, 0);
                    Response::status(204)
                        .with_header("x-amz-delete-marker", "true")
                        .with_header("x-amz-version-id", marker.generation.to_string())
                }
                Ok(None) => Response::status(204),
                Err(error) => error_response(error, &resource, false),
            };
        }
        match self.delete_checked(bucket, key, lock) {
            Ok(manifest) => {
                self.remember(&manifest);
                self.note_change(bucket, "delete", key, manifest.generation, 0);
                Response::status(204)
            }
            // Deleting what is not there is a success, as S3 has it.
            Err(ObjectError::NotFound(_)) => Response::status(204),
            Err(error) => error_response(error, &resource, false),
        }
    }

    /// Delete `bucket`/`key` unless `lock` finds it locked. With lock on, the tombstone is published on top of exactly the
    /// manifest whose attributes were checked.
    fn delete_checked(&self, bucket: &str, key: &str, lock: attributes::LockCheck) -> Result<ObjectManifest, ObjectError> {
        self.with_plane(|plane| {
            if !lock.enabled {
                return plane.delete(bucket, key);
            }
            let (manifest, carried) = plane.read_manifest_with_attributes(bucket, key)?;
            if manifest.deleted {
                return Err(ObjectError::NotFound(format!("{bucket}/{key} is already deleted")));
            }
            if let Some(refusal) = lock.refuse(&carried) {
                return Err(refusal.error);
            }
            plane.delete_at(&manifest)
        })
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
        // a browser's preflight is answered from the bucket's CORS rules; everything else is routed and then dressed
        if let Some(preflight) = self.cors_preflight(request) {
            return preflight;
        }
        let mut response = self.route(request);
        self.cors_headers(request, &mut response);
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
    (matches!(error, ObjectError::PreconditionFailed(message) if !message.starts_with(attributes::LOCKED) && !message.starts_with(quota::OVER_QUOTA))
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
        .push((OBJECT_SIZE_HEADER.into(), manifest.length.to_string()));
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
        ObjectError::PreconditionFailed(message) if message.starts_with(attributes::LOCKED) => s3::S3ErrorCode::AccessDenied,
        ObjectError::PreconditionFailed(message) if message.starts_with(quota::OVER_QUOTA) => s3::S3ErrorCode::QuotaExceeded,
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
    use std::sync::Arc;
    use std::collections::BTreeMap;






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
        /// What each object carries besides its bytes.
        attributes: BTreeMap<(String, String), ChunkAttributes>,
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
        fn read_manifest_with_attributes(&mut self, bucket: &str, key: &str) -> Result<(ObjectManifest, ChunkAttributes), ObjectError> {
            let manifest = self.read_manifest(bucket, key)?;
            let carried = self.0.lock().unwrap().attributes.get(&(bucket.to_string(), key.to_string())).cloned().unwrap_or_default();
            Ok((manifest, carried))
        }
        fn write_with(
            &mut self,
            bucket: &str,
            key: &str,
            expected: Option<ObjectManifest>,
            data: &[u8],
            attributes: &ChunkAttributes,
        ) -> Result<ObjectManifest, PlaneFailure> {
            let written = self.write(bucket, key, expected, data)?;
            let mut carried = attributes.clone();
            if carried.modified_ms == 0 {
                carried.modified_ms = now_ms();
            }
            self.0.lock().unwrap().attributes.insert((bucket.to_string(), key.to_string()), carried);
            Ok(written)
        }
        fn alias(&mut self, bucket: &str, from_key: &str, to_key: &str, expected: Option<&ObjectManifest>) -> Result<ObjectManifest, PlaneFailure> {
            let mut shared = self.0.lock().unwrap();
            let from = (bucket.to_string(), from_key.to_string());
            let to = (bucket.to_string(), to_key.to_string());
            let Some((source, bytes)) = shared.objects.get(&from).cloned() else {
                return Err(ObjectError::NotFound("alias source".into()).into());
            };
            let existing = shared.objects.get(&to).map(|(m, _)| m.clone());
            if existing.as_ref() != expected {
                return Err(ObjectError::PreconditionFailed("object manifest changed before the version was kept".into()).into());
            }
            let mut named = source;
            named.key = to_key.to_string();
            named.generation = existing.map_or(1, |m| m.generation + 1);
            let carried = shared.attributes.get(&from).cloned();
            shared.objects.insert(to.clone(), (named.clone(), bytes));
            match carried {
                Some(carried) => {
                    shared.attributes.insert(to, carried);
                }
                None => {
                    shared.attributes.remove(&to);
                }
            }
            Ok(named)
        }
        fn update_attributes(&mut self, expected: &ObjectManifest, attributes: &ChunkAttributes) -> Result<ObjectManifest, PlaneFailure> {
            let mut shared = self.0.lock().unwrap();
            let slot = (expected.bucket.clone(), expected.key.clone());
            let Some((current, bytes)) = shared.objects.get(&slot).cloned() else {
                return Err(ObjectError::NotFound("manifest".into()).into());
            };
            if &current != expected {
                return Err(ObjectError::PreconditionFailed("object manifest changed before the attributes were set".into()).into());
            }
            let mut next = current;
            next.generation += 1;
            shared.objects.insert(slot.clone(), (next.clone(), bytes));
            shared.attributes.insert(slot, attributes.clone());
            Ok(next)
        }
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
            shared.attributes.remove(&slot);
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
            ("PUT", "/b/k?acl", vec![]),
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
        assert_eq!(copy("/b/src?versionId=3", &[]).status, 404, "a version that does not exist is not copied");
        assert_eq!(copy("/b/src?versionId=x", &[]).status, 400, "a version id is a number");
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

    /// The client SDK against this gateway's own request handling, through an in-process door.
    ///
    /// The SDK's unit tests run it against a door written to match the documented contract; these
    /// run it against the real handler, so a client and a gateway that disagree about an answer
    /// fail here and not in a deployment.
    mod client_conformance {
        use super::*;
        use matrixobject_client::{ClientError, Reply, S3Client, Transport, TransportError};

        struct Door {
            gateway: ClusterGateway,
            _shared: Arc<Mutex<Shared>>,
            /// How many of the next offset appends commit and then lose their reply.
            lose_replies: Mutex<usize>,
        }

        impl std::fmt::Debug for Door {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("Door")
            }
        }

        impl Transport for Door {
            fn send(&self, method: &str, target: &str, headers: &[(&str, String)], body: &[u8]) -> Result<Reply, TransportError> {
                let headers: Vec<(&str, &str)> = headers.iter().map(|(n, v)| (*n, v.as_str())).collect();
                let response = self.gateway.handle(&request(method, target, &headers, body));
                let is_append = method == "PUT" && headers.iter().any(|(n, _)| n.eq_ignore_ascii_case(WRITE_OFFSET_HEADER));
                if is_append {
                    let mut lose = self.lose_replies.lock().unwrap();
                    if *lose > 0 {
                        *lose -= 1;
                        return Err(TransportError("connection reset".into()));
                    }
                }
                Ok(Reply { status: response.status, headers: response.headers, body: response.body })
            }
        }

        fn door() -> Arc<Door> {
            let (gateway, shared) = gateway();
            Arc::new(Door { gateway, _shared: shared, lose_replies: Mutex::new(0) })
        }

        #[test]
        fn the_client_reads_writes_lists_and_deletes_through_the_gateway() {
            let client = S3Client::new(door(), "b");
            client.put("w/1", b"0123456789").unwrap();
            client.put("w/2", b"x").unwrap();
            client.put("z", b"y").unwrap();
            assert_eq!(client.get("w/1").unwrap(), b"0123456789");
            assert_eq!(client.get_range("w/1", 2, 3).unwrap(), b"234");
            assert_eq!(client.get_range_from("w/1", 7).unwrap(), Some(b"789".to_vec()));
            assert_eq!(client.length_or_zero("w/1").unwrap(), 10);
            assert_eq!(client.length_or_zero("never").unwrap(), 0);
            assert_eq!(client.list("w/", None).unwrap(), vec!["w/1", "w/2"]);
            assert_eq!(client.list("", Some("w/1")).unwrap(), vec!["w/2", "z"]);
            assert_eq!(client.get("never").unwrap_err(), ClientError::NotFound("never".into()));
            client.delete("w/1").unwrap();
            client.delete("w/1").unwrap();
            assert_eq!(client.list("w/", None).unwrap(), vec!["w/2"]);
        }

        #[test]
        fn appends_report_where_they_landed_and_a_second_client_follows_the_first() {
            let door = door();
            let (a, b) = (S3Client::new(door.clone(), "b"), S3Client::new(door.clone(), "b"));
            let first = a.append("log", b"abc").unwrap();
            assert_eq!((first.start_offset, first.object_length), (0, 3));
            // B has never looked at the object: it learns the end, then appends there.
            let second = b.append("log", b"de").unwrap();
            assert_eq!((second.start_offset, second.object_length), (3, 5));
            // A's cached end is stale; the refusal is settled by the attempt and the retry lands at 5.
            let third = a.append("log", b"f").unwrap();
            assert_eq!((third.start_offset, third.object_length), (5, 6));
            assert_eq!(a.get("log").unwrap(), b"abcdef");
        }

        #[test]
        fn an_append_whose_reply_was_lost_is_resolved_by_the_gateway_as_landed_and_is_not_repeated() {
            let door = door();
            let client = S3Client::new(door.clone(), "b");
            *door.lose_replies.lock().unwrap() = 1;
            let receipt = client.append("log", b"abc").unwrap();
            assert_eq!((receipt.start_offset, receipt.end_offset, receipt.object_length), (0, 3, 3));
            assert_eq!(client.get("log").unwrap(), b"abc", "the bytes are in the object once");
        }

        #[test]
        fn two_writers_of_identical_bytes_at_one_offset_are_not_both_told_they_landed() {
            let door = door();
            let b = S3Client::new(door.clone(), "b");
            b.append("log", b"head").unwrap();
            let a = S3Client::new(door.clone(), "b");
            *door.lose_replies.lock().unwrap() = 1;
            let landed = a.append("log", b"same").unwrap();
            assert_eq!((landed.start_offset, landed.end_offset), (4, 8));
            // B's cached end is 4: its append of the SAME bytes at the SAME offset is refused, equal
            // bytes are there, and they are A's. B must not be told its own copy landed.
            let refused = b.append("log", b"same").unwrap_err();
            assert!(matches!(refused, ClientError::OutcomeUnknown { .. }), "{refused:?}");
            assert_eq!(a.get("log").unwrap(), b"headsame", "one physical copy, claimed by exactly one writer");
        }
    }

    fn gateway_requiring_buckets() -> (ClusterGateway, Arc<Mutex<Shared>>) {
        let shared = Arc::new(Mutex::new(Shared::default()));
        let handle = Arc::clone(&shared);
        let gateway = ClusterGateway::new(
            move || Ok(Box::new(MemoryPlane(Arc::clone(&handle))) as Box<dyn ObjectPlane>),
            GatewayOptions { require_buckets: true, ..GatewayOptions::default() },
            "b",
        );
        (gateway, shared)
    }

    #[test]
    fn a_bucket_is_created_probed_listed_and_deleted_and_its_record_is_not_a_client_object() {
        let (gateway, _) = gateway_requiring_buckets();
        assert_eq!(gateway.handle(&request("PUT", "/photos", &[], b"")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/photos", &[], b"")).status, 200, "creating a bucket that exists succeeds");
        assert_eq!(gateway.handle(&request("PUT", "/docs", &[], b"")).status, 200);
        assert_eq!(gateway.handle(&request("HEAD", "/photos", &[], b"")).status, 200);
        let missing = gateway.handle(&request("HEAD", "/nope", &[], b""));
        assert_eq!(missing.status, 404);
        let listed = text(&gateway.handle(&request("GET", "/", &[], b"")));
        assert_eq!(all_between(&listed, "Name"), ["docs", "photos"], "{listed}");
        // an object in it keeps it
        assert_eq!(gateway.handle(&request("PUT", "/photos/k", &[], b"x")).status, 200);
        assert_eq!(gateway.handle(&request("DELETE", "/photos", &[], b"")).status, 409);
        assert_eq!(gateway.handle(&request("DELETE", "/photos/k", &[], b"")).status, 204);
        assert_eq!(gateway.handle(&request("DELETE", "/photos", &[], b"")).status, 204);
        assert_eq!(gateway.handle(&request("HEAD", "/photos", &[], b"")).status, 404);
        let listed = text(&gateway.handle(&request("GET", "/", &[], b"")));
        assert_eq!(all_between(&listed, "Name"), ["docs"], "{listed}");
        // the records live in the system bucket, which a client cannot address
        let system = format!("/{}/bucket/docs", crate::bucket_config::SYSTEM_BUCKET);
        assert_eq!(gateway.handle(&request("GET", &system, &[], b"")).status, 404);
        assert_eq!(gateway.handle(&request("PUT", &system, &[], b"x")).status, 404);
        assert_eq!(gateway.handle(&request("PUT", &format!("/{}", crate::bucket_config::SYSTEM_BUCKET), &[], b"")).status, 400);
    }

    #[test]
    fn a_gateway_that_requires_buckets_refuses_requests_to_one_that_does_not_exist() {
        let (gateway, _) = gateway_requiring_buckets();
        let refused = gateway.handle(&request("PUT", "/b/k", &[], b"x"));
        assert_eq!(refused.status, 404, "{}", text(&refused));
        assert!(text(&refused).contains("NoSuchBucket"), "{}", text(&refused));
        assert_eq!(gateway.handle(&request("GET", "/b?list-type=2", &[], b"")).status, 404);
        assert_eq!(gateway.handle(&request("PUT", "/b", &[], b"")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/b/k", &[], b"x")).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/b/k", &[], b"")).body, b"x");
        // the gateway's own bucket can be made at startup, and making it twice is harmless
        assert_eq!(gateway.ensure_bucket("start").unwrap(), true);
        assert_eq!(gateway.ensure_bucket("start").unwrap(), false);
        assert_eq!(gateway.handle(&request("PUT", "/start/k", &[], b"y")).status, 200);
        // a gateway that does not require them keeps buckets implicit
        let (implicit, _) = self::gateway();
        assert_eq!(implicit.handle(&request("PUT", "/anything/k", &[], b"x")).status, 200);
    }

    #[test]
    fn bucket_configuration_is_stored_read_back_and_removed() {
        let (gateway, _) = gateway_requiring_buckets();
        gateway.handle(&request("PUT", "/cfg", &[], b""));
        let tagging = "<Tagging><TagSet><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet></Tagging>";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?tagging", &[], tagging.as_bytes())).status, 200);
        let read = text(&gateway.handle(&request("GET", "/cfg?tagging", &[], b"")));
        assert!(read.contains("<Key>team</Key><Value>storage</Value>"), "{read}");
        let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://a.example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod><MaxAgeSeconds>60</MaxAgeSeconds></CORSRule></CORSConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?cors", &[], cors.as_bytes())).status, 200);
        let read = text(&gateway.handle(&request("GET", "/cfg?cors", &[], b"")));
        assert!(read.contains("<AllowedOrigin>https://a.example.com</AllowedOrigin>") && read.contains("<MaxAgeSeconds>60</MaxAgeSeconds>"), "{read}");
        let policy = "{\"Version\":\"2012-10-17\"}";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?policy", &[], policy.as_bytes())).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/cfg?policy", &[], b"")).body, policy.as_bytes());
        let lifecycle = "<LifecycleConfiguration><Rule><ID>old</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?lifecycle", &[], lifecycle.as_bytes())).status, 200);
        let read = text(&gateway.handle(&request("GET", "/cfg?lifecycle", &[], b"")));
        assert!(read.contains("<Days>30</Days>") && read.contains("<Prefix>logs/</Prefix>"), "{read}");
        assert_eq!(gateway.handle(&request("PUT", "/cfg?quota", &[], b"<Quota><Bytes>1048576</Bytes></Quota>")).status, 200);
        assert!(text(&gateway.handle(&request("GET", "/cfg?quota", &[], b""))).contains("<Bytes>1048576</Bytes>"));
        // versioning: not configured, then enabled, then suspended
        assert!(!text(&gateway.handle(&request("GET", "/cfg?versioning", &[], b""))).contains("<Status>"));
        let on = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?versioning", &[], on.as_bytes())).status, 200);
        assert!(text(&gateway.handle(&request("GET", "/cfg?versioning", &[], b""))).contains("<Status>Enabled</Status>"));
        // object lock needs versioning on, and once on cannot be turned off by suspending versioning
        let lock = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?object-lock", &[], lock.as_bytes())).status, 200);
        let read = text(&gateway.handle(&request("GET", "/cfg?object-lock", &[], b"")));
        assert!(read.contains("<Mode>GOVERNANCE</Mode>") && read.contains("<Days>1</Days>"), "{read}");
        let off = "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/cfg?versioning", &[], off.as_bytes())).status, 400);
        // removal
        for sub in ["tagging", "cors", "policy", "lifecycle", "quota"] {
            assert_eq!(gateway.handle(&request("DELETE", &format!("/cfg?{sub}"), &[], b"")).status, 204, "{sub}");
        }
        assert_eq!(gateway.handle(&request("GET", "/cfg?cors", &[], b"")).status, 404);
        assert!(text(&gateway.handle(&request("GET", "/cfg?cors", &[], b""))).contains("NoSuchCORSConfiguration"));
        assert!(text(&gateway.handle(&request("GET", "/cfg?policy", &[], b""))).contains("NoSuchBucketPolicy"));
        assert!(text(&gateway.handle(&request("GET", "/cfg?lifecycle", &[], b""))).contains("NoSuchLifecycleConfiguration"));
        // nothing but the removed settings changed
        assert!(text(&gateway.handle(&request("GET", "/cfg?versioning", &[], b""))).contains("<Status>Enabled</Status>"));
    }

    #[test]
    fn malformed_configuration_and_configuration_of_a_missing_bucket_are_refused() {
        let (gateway, _) = gateway_requiring_buckets();
        gateway.handle(&request("PUT", "/cfg", &[], b""));
        for (sub, body) in [
            ("tagging", "not xml"),
            ("cors", "<CORSConfiguration/>"),
            ("lifecycle", "<LifecycleConfiguration><Rule><Status>Maybe</Status></Rule></LifecycleConfiguration>"),
            ("versioning", "<VersioningConfiguration><Status>Never</Status></VersioningConfiguration>"),
            ("object-lock", "<ObjectLockConfiguration/>"),
            ("quota", "<Quota/>"),
        ] {
            let reply = gateway.handle(&request("PUT", &format!("/cfg?{sub}"), &[], body.as_bytes()));
            assert_eq!(reply.status, 400, "{sub}: {}", text(&reply));
        }
        // a bucket that was not created with object lock has no object-lock configuration to read
        assert_eq!(gateway.handle(&request("GET", "/cfg?object-lock", &[], b"")).status, 404);
        assert_eq!(gateway.handle(&request("PUT", "/missing?tagging", &[], b"<Tagging><TagSet/></Tagging>")).status, 404);
        assert_eq!(gateway.handle(&request("GET", "/missing?cors", &[], b"")).status, 404);
    }

    #[test]
    fn concurrent_changes_to_one_buckets_configuration_all_land() {
        let (gateway, _) = gateway_requiring_buckets();
        gateway.handle(&request("PUT", "/race", &[], b""));
        let gateway = Arc::new(gateway);
        let mut workers = Vec::new();
        for worker in 0..4u32 {
            let gateway = Arc::clone(&gateway);
            workers.push(std::thread::spawn(move || {
                for round in 0..10u32 {
                    let key = format!("w{worker}r{round}");
                    // each writer adds its own tag by reading the bucket's tags and putting them back with its own
                    for _ in 0..50 {
                        let current = text(&gateway.handle(&request("GET", "/race?tagging", &[], b"")));
                        let mut tags: Vec<(String, String)> = all_between(&current, "Key").into_iter().zip(all_between(&current, "Value")).collect();
                        tags.push((key.clone(), "1".into()));
                        let mut body = String::from("<Tagging><TagSet>");
                        for (k, v) in &tags {
                            body.push_str(&format!("<Tag><Key>{k}</Key><Value>{v}</Value></Tag>"));
                        }
                        body.push_str("</TagSet></Tagging>");
                        if gateway.handle(&request("PUT", "/race?tagging", &[], body.as_bytes())).status == 200 {
                            break;
                        }
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        // whatever interleaving happened, the record is one coherent document of valid tags
        let end = text(&gateway.handle(&request("GET", "/race?tagging", &[], b"")));
        assert!(!all_between(&end, "Key").is_empty(), "{end}");
        assert_eq!(gateway.handle(&request("HEAD", "/race", &[], b"")).status, 200);
    }

    // ----- object attributes, locks, quotas, CORS and lifecycle -----

    fn header_of(response: &Response, name: &str) -> Option<String> {
        response.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone())
    }

    fn bucket_with(gateway: &ClusterGateway, name: &str, configuration: &[(&str, &str)]) {
        assert_eq!(gateway.handle(&request("PUT", &format!("/{name}"), &[], b"")).status, 200);
        for (sub, body) in configuration {
            let reply = gateway.handle(&request("PUT", &format!("/{name}?{sub}"), &[], body.as_bytes()));
            assert_eq!(reply.status, 200, "{sub}: {}", text(&reply));
        }
    }

    const LOCKED_BUCKET: &str = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>";

    #[test]
    fn an_object_carries_its_content_type_metadata_and_tags_until_it_is_overwritten() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "docs", &[]);
        let put = gateway.handle(&request(
            "PUT",
            "/docs/report",
            &[("content-type", "text/csv"), ("x-amz-meta-owner", "ana"), ("x-amz-tagging", "team=storage&tier=cold")],
            b"a,b",
        ));
        assert_eq!(put.status, 200, "{}", text(&put));
        let head = gateway.handle(&request("HEAD", "/docs/report", &[], &[]));
        assert_eq!(header_of(&head, "content-type").as_deref(), Some("text/csv"));
        assert_eq!(header_of(&head, "x-amz-meta-owner").as_deref(), Some("ana"));
        assert_eq!(header_of(&head, "x-amz-tagging-count").as_deref(), Some("2"));
        assert!(header_of(&head, "last-modified").is_some(), "a written object says when");
        let tags = text(&gateway.handle(&request("GET", "/docs/report?tagging", &[], &[])));
        assert_eq!(all_between(&tags, "Key"), ["team", "tier"], "{tags}");

        // tags are replaced and removed without touching the bytes, and an append keeps what the object carries
        let replaced = "<Tagging><TagSet><Tag><Key>team</Key><Value>data</Value></Tag></TagSet></Tagging>";
        assert_eq!(gateway.handle(&request("PUT", "/docs/report?tagging", &[], replaced.as_bytes())).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/docs/report", &[("x-amz-write-offset-bytes", "3")], b",c")).status, 200);
        let head = gateway.handle(&request("HEAD", "/docs/report", &[], &[]));
        assert_eq!(header_of(&head, "content-type").as_deref(), Some("text/csv"));
        assert_eq!(header_of(&head, "x-amz-tagging-count").as_deref(), Some("1"));
        assert_eq!(gateway.handle(&request("GET", "/docs/report", &[], &[])).body, b"a,b,c");
        assert_eq!(gateway.handle(&request("DELETE", "/docs/report?tagging", &[], &[])).status, 204);
        assert!(header_of(&gateway.handle(&request("HEAD", "/docs/report", &[], &[])), "x-amz-tagging-count").is_none());

        // an overwrite names its own attributes: it does not inherit the old ones
        assert_eq!(gateway.handle(&request("PUT", "/docs/report", &[], b"plain")).status, 200);
        let head = gateway.handle(&request("HEAD", "/docs/report", &[], &[]));
        assert!(header_of(&head, "x-amz-meta-owner").is_none());
        assert_ne!(header_of(&head, "content-type").as_deref(), Some("text/csv"));
        // too many tags and a missing object are refused
        let many: String = (0..11).map(|i| format!("k{i}=v")).collect::<Vec<_>>().join("&");
        assert_eq!(gateway.handle(&request("PUT", "/docs/other", &[("x-amz-tagging", &many)], b"x")).status, 400);
        assert_eq!(gateway.handle(&request("GET", "/docs/missing?tagging", &[], &[])).status, 404);
    }

    #[test]
    fn a_retention_or_legal_hold_refuses_overwrite_and_delete_and_only_governance_can_be_bypassed() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "vault", &[("object-lock", LOCKED_BUCKET)]);
        let until = "2099-01-01T00:00:00Z";
        // retention headers are refused where object lock was not enabled
        bucket_with(&gateway, "plain", &[]);
        let refused = gateway.handle(&request("PUT", "/plain/k", &[("x-amz-object-lock-mode", "GOVERNANCE"), ("x-amz-object-lock-retain-until-date", until)], b"x"));
        assert_eq!(refused.status, 400, "{}", text(&refused));

        let governed = gateway.handle(&request("PUT", "/vault/g", &[("x-amz-object-lock-mode", "GOVERNANCE"), ("x-amz-object-lock-retain-until-date", until)], b"one"));
        assert_eq!(governed.status, 200, "{}", text(&governed));
        let head = gateway.handle(&request("HEAD", "/vault/g", &[], &[]));
        assert_eq!(header_of(&head, "x-amz-object-lock-mode").as_deref(), Some("GOVERNANCE"));
        assert!(header_of(&head, "x-amz-object-lock-retain-until-date").unwrap().starts_with("2099-01-01T00:00:00"));
        for method in ["DELETE", "PUT"] {
            let refusal = gateway.handle(&request(method, "/vault/g", &[], b"two"));
            assert_eq!(refusal.status, 403, "{method}: {}", text(&refusal));
            assert!(text(&refusal).contains("AccessDenied"), "{}", text(&refusal));
        }
        let batch = gateway.handle(&request("POST", "/vault?delete", &[], b"<Delete><Object><Key>g</Key></Object></Delete>"));
        assert!(text(&batch).contains("AccessDenied"), "a batch delete respects the lock: {}", text(&batch));
        assert_eq!(gateway.handle(&request("PUT", "/vault/copy", &[("x-amz-copy-source", "/vault/g")], &[])).status, 200, "copying out of a locked object is allowed");
        assert_eq!(gateway.handle(&request("PUT", "/vault/g2", &[], b"x")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/vault/g", &[("x-amz-copy-source", "/vault/g2")], &[])).status, 403, "a copy onto a locked object is an overwrite");
        // a governance retention is bypassed on purpose; the bytes are untouched until then
        assert_eq!(gateway.handle(&request("GET", "/vault/g", &[], &[])).body, b"one");
        assert_eq!(gateway.handle(&request("DELETE", "/vault/g", &[("x-amz-bypass-governance-retention", "true")], &[])).status, 204);
        assert_eq!(gateway.handle(&request("GET", "/vault/g", &[], &[])).status, 404);

        // a compliance retention is not bypassed, shortened or relaxed
        let compliance = "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
        assert_eq!(gateway.handle(&request("PUT", "/vault/c", &[], b"c")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/vault/c?retention", &[], compliance.as_bytes())).status, 200);
        assert_eq!(gateway.handle(&request("DELETE", "/vault/c", &[("x-amz-bypass-governance-retention", "true")], &[])).status, 403);
        let shorter = "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2098-01-01T00:00:00Z</RetainUntilDate></Retention>";
        assert_eq!(gateway.handle(&request("PUT", "/vault/c?retention", &[("x-amz-bypass-governance-retention", "true")], shorter.as_bytes())).status, 403);
        let longer = "<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2100-01-01T00:00:00Z</RetainUntilDate></Retention>";
        assert_eq!(gateway.handle(&request("PUT", "/vault/c?retention", &[], longer.as_bytes())).status, 200, "a retention is lengthened");
        let read = text(&gateway.handle(&request("GET", "/vault/c?retention", &[], &[])));
        assert!(read.contains("<Mode>COMPLIANCE</Mode>") && read.contains("2100-01-01"), "{read}");

        // a legal hold locks until it is released
        assert_eq!(gateway.handle(&request("PUT", "/vault/h", &[], b"h")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/vault/h?legal-hold", &[], b"<LegalHold><Status>ON</Status></LegalHold>")).status, 200);
        assert!(text(&gateway.handle(&request("GET", "/vault/h?legal-hold", &[], &[]))).contains("<Status>ON</Status>"));
        assert_eq!(gateway.handle(&request("DELETE", "/vault/h", &[("x-amz-bypass-governance-retention", "true")], &[])).status, 403);
        assert_eq!(gateway.handle(&request("PUT", "/vault/h?legal-hold", &[], b"<LegalHold><Status>OFF</Status></LegalHold>")).status, 200);
        assert_eq!(gateway.handle(&request("DELETE", "/vault/h", &[], &[])).status, 204);
        // retention and a hold cannot be set on an object of a bucket without object lock
        // refused before the object is looked at: this bucket cannot lock anything
        assert_eq!(gateway.handle(&request("PUT", "/plain/k?legal-hold", &[], b"<LegalHold><Status>ON</Status></LegalHold>")).status, 400);
        assert_eq!(gateway.handle(&request("PUT", "/plain/k", &[], b"x")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/plain/k?legal-hold", &[], b"<LegalHold><Status>ON</Status></LegalHold>")).status, 400);
    }

    #[test]
    fn a_default_retention_locks_what_the_bucket_receives() {
        let (gateway, _) = gateway_requiring_buckets();
        let configuration = "<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>";
        bucket_with(&gateway, "archive", &[("object-lock", configuration)]);
        assert_eq!(gateway.handle(&request("PUT", "/archive/k", &[], b"x")).status, 200);
        let head = gateway.handle(&request("HEAD", "/archive/k", &[], &[]));
        assert_eq!(header_of(&head, "x-amz-object-lock-mode").as_deref(), Some("GOVERNANCE"));
        assert_eq!(gateway.handle(&request("DELETE", "/archive/k", &[], &[])).status, 403);
    }

    #[test]
    fn a_quota_refuses_the_write_that_would_pass_it_and_counts_an_overwrite_once() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "small", &[("quota", "<Quota><Bytes>10</Bytes></Quota>")]);
        assert_eq!(gateway.handle(&request("PUT", "/small/a", &[], b"123456")).status, 200);
        let over = gateway.handle(&request("PUT", "/small/b", &[], b"123456"));
        assert_eq!(over.status, 403, "{}", text(&over));
        assert!(text(&over).contains("QuotaExceeded"), "{}", text(&over));
        assert_eq!(gateway.handle(&request("GET", "/small/b", &[], &[])).status, 404, "nothing of the refused write is stored");
        // replacing an object frees its bytes first; an append adds to them
        assert_eq!(gateway.handle(&request("PUT", "/small/a", &[], b"12345678")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/small/a", &[("x-amz-write-offset-bytes", "8")], b"90")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/small/a", &[("x-amz-write-offset-bytes", "10")], b"1")).status, 403);
        // a copy counts the bytes it adds, and a bucket with no quota is unbounded
        assert_eq!(gateway.handle(&request("PUT", "/small/c", &[("x-amz-copy-source", "/small/a")], &[])).status, 403);
        bucket_with(&gateway, "big", &[]);
        assert_eq!(gateway.handle(&request("PUT", "/big/a", &[], &vec![7u8; 4096])).status, 200);
    }

    #[test]
    fn cors_rules_answer_a_preflight_and_dress_a_cross_origin_response() {
        let (gateway, _) = gateway_requiring_buckets();
        let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://app.example</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedHeader>x-amz-*</AllowedHeader><ExposeHeader>etag</ExposeHeader><MaxAgeSeconds>600</MaxAgeSeconds></CORSRule></CORSConfiguration>";
        bucket_with(&gateway, "web", &[("cors", cors)]);
        assert_eq!(gateway.handle(&request("PUT", "/web/page", &[], b"<html>")).status, 200);
        // an ordinary request from an allowed origin carries the headers; one from another origin does not
        let allowed = gateway.handle(&request("GET", "/web/page", &[("origin", "https://app.example")], &[]));
        assert_eq!(header_of(&allowed, "access-control-allow-origin").as_deref(), Some("https://app.example"));
        assert_eq!(header_of(&allowed, "access-control-expose-headers").as_deref(), Some("etag"));
        let stranger = gateway.handle(&request("GET", "/web/page", &[("origin", "https://evil.example")], &[]));
        assert_eq!(stranger.status, 200);
        assert!(header_of(&stranger, "access-control-allow-origin").is_none());
        assert!(header_of(&gateway.handle(&request("GET", "/web/page", &[], &[])), "access-control-allow-origin").is_none());
        // a preflight is answered from the rule, or refused
        let preflight = gateway.handle(&request("OPTIONS", "/web/page", &[("origin", "https://app.example"), ("access-control-request-method", "PUT")], &[]));
        assert_eq!(preflight.status, 200, "{}", text(&preflight));
        assert_eq!(header_of(&preflight, "access-control-max-age").as_deref(), Some("600"));
        assert!(header_of(&preflight, "access-control-allow-methods").unwrap().contains("PUT"));
        let with_headers = gateway.handle(&request("OPTIONS", "/web/page", &[("origin", "https://app.example"), ("access-control-request-method", "PUT"), ("access-control-request-headers", "x-amz-meta-a, x-amz-tagging")], &[]));
        assert_eq!(with_headers.status, 200);
        for refused in [
            [("origin", "https://app.example"), ("access-control-request-method", "DELETE"), ("x", "")],
            [("origin", "https://evil.example"), ("access-control-request-method", "GET"), ("x", "")],
            [("origin", "https://app.example"), ("access-control-request-method", "PUT"), ("access-control-request-headers", "authorization")],
        ] {
            let headers: Vec<(&str, &str)> = refused.iter().copied().filter(|(n, _)| *n != "x").collect();
            assert_eq!(gateway.handle(&request("OPTIONS", "/web/page", &headers, &[])).status, 403, "{headers:?}");
        }
        // a bucket without rules refuses every preflight
        bucket_with(&gateway, "closed", &[]);
        assert_eq!(gateway.handle(&request("OPTIONS", "/closed/k", &[("origin", "https://app.example"), ("access-control-request-method", "GET")], &[])).status, 403);
    }

    #[test]
    fn a_lifecycle_pass_expires_old_objects_under_the_prefix_and_leaves_locked_ones() {
        let (gateway, shared) = gateway_requiring_buckets();
        let rules = "<LifecycleConfiguration><Rule><ID>tmp</ID><Filter><Prefix>tmp/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
        bucket_with(&gateway, "logs", &[("object-lock", LOCKED_BUCKET), ("lifecycle", rules)]);
        for key in ["tmp/a", "tmp/b", "keep/c"] {
            assert_eq!(gateway.handle(&request("PUT", &format!("/logs/{key}"), &[], b"x")).status, 200, "{key}");
        }
        let held = gateway.handle(&request("PUT", "/logs/tmp/held", &[("x-amz-object-lock-mode", "COMPLIANCE"), ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z")], b"x"));
        assert_eq!(held.status, 200, "{}", text(&held));
        // too early: nothing is a day old
        let early = gateway.run_lifecycle(now_ms()).unwrap();
        assert_eq!((early.expired, early.kept_locked), (0, 0));
        let later = now_ms() + 2 * 86_400_000;
        let report = gateway.run_lifecycle(later).unwrap();
        assert_eq!((report.buckets, report.expired, report.kept_locked), (1, 2, 1), "{report:?}");
        assert_eq!(gateway.handle(&request("GET", "/logs/tmp/a", &[], &[])).status, 404);
        assert_eq!(gateway.handle(&request("GET", "/logs/keep/c", &[], &[])).body, b"x", "outside the prefix");
        assert_eq!(gateway.handle(&request("GET", "/logs/tmp/held", &[], &[])).body, b"x", "a lock outlasts a rule");
        // a pass repeats harmlessly
        let again = gateway.run_lifecycle(later).unwrap();
        assert_eq!((again.expired, again.kept_locked), (0, 1));
        // an abandoned multipart upload is removed by a rule that names it
        let abort = "<LifecycleConfiguration><Rule><ID>uploads</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/logs?lifecycle", &[], abort.as_bytes())).status, 200);
        let started = gateway.handle(&request("POST", "/logs/big?uploads", &[], &[]));
        assert_eq!(started.status, 200, "{}", text(&started));
        let id = between(&text(&started), "UploadId").expect("an upload id");
        assert_eq!(gateway.handle(&request("PUT", &format!("/logs/big?uploadId={id}&partNumber=1"), &[], b"part")).status, 200);
        let swept = gateway.run_lifecycle(now_ms() + 3 * 86_400_000).unwrap();
        assert_eq!(swept.aborted_uploads, 1, "{swept:?}");
        assert_eq!(gateway.handle(&request("GET", &format!("/logs/big?uploadId={id}"), &[], &[])).status, 404, "the upload is gone");
        let _ = shared;
    }

    const VERSIONED: &str = "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>";

    #[test]
    fn versioning_keeps_what_an_overwrite_or_a_delete_replaces_and_serves_each_version() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "wiki", &[("versioning", VERSIONED)]);
        let first = gateway.handle(&request("PUT", "/wiki/page", &[("content-type", "text/plain")], b"one"));
        let second = gateway.handle(&request("PUT", "/wiki/page", &[], b"two!"));
        let (v1, v2) = (header_of(&first, "x-amz-version-id").unwrap(), header_of(&second, "x-amz-version-id").unwrap());
        assert_ne!(v1, v2);
        // the current version is the key; each older one is read by its id, with what it carried
        assert_eq!(gateway.handle(&request("GET", "/wiki/page", &[], &[])).body, b"two!");
        let old = gateway.handle(&request("GET", &format!("/wiki/page?versionId={v1}"), &[], &[]));
        assert_eq!((old.status, old.body.as_slice()), (200, b"one".as_slice()));
        assert_eq!(header_of(&old, "content-type").as_deref(), Some("text/plain"));
        assert_eq!(header_of(&old, "x-amz-version-id").as_deref(), Some(v1.as_str()));
        assert_eq!(gateway.handle(&request("GET", &format!("/wiki/page?versionId={v2}"), &[], &[])).body, b"two!");
        assert_eq!(gateway.handle(&request("GET", "/wiki/page?versionId=999", &[], &[])).status, 404);
        // a delete hides the object behind a marker and keeps every version
        let deleted = gateway.handle(&request("DELETE", "/wiki/page", &[], &[]));
        assert_eq!(deleted.status, 204);
        assert_eq!(header_of(&deleted, "x-amz-delete-marker").as_deref(), Some("true"));
        let marker = header_of(&deleted, "x-amz-version-id").unwrap();
        assert_eq!(gateway.handle(&request("GET", "/wiki/page", &[], &[])).status, 404);
        assert_eq!(gateway.handle(&request("GET", &format!("/wiki/page?versionId={v1}"), &[], &[])).body, b"one");
        assert_eq!(gateway.handle(&request("GET", &format!("/wiki/page?versionId={marker}"), &[], &[])).status, 405, "a marker is not an object");
        let listed = text(&gateway.handle(&request("GET", "/wiki?list-type=2", &[], &[])));
        assert!(all_between(&listed, "Key").is_empty(), "a deleted key is not listed: {listed}");
        let versions = text(&gateway.handle(&request("GET", "/wiki?versions", &[], &[])));
        assert_eq!(all_between(&versions, "VersionId").len(), 3, "{versions}");
        assert!(versions.contains("<DeleteMarker>") && versions.contains("<IsLatest>true</IsLatest>"), "{versions}");
        // writing again restores the key, and the marker is kept as a version
        assert_eq!(gateway.handle(&request("PUT", "/wiki/page", &[], b"three")).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/wiki/page", &[], &[])).body, b"three");
        assert_eq!(all_between(&text(&gateway.handle(&request("GET", "/wiki?versions", &[], &[]))), "VersionId").len(), 4);
        // deleting the current version brings the latest kept one back; the marker was the latest kept
        let current = header_of(&gateway.handle(&request("HEAD", "/wiki/page", &[], &[])), "x-amz-version-id").unwrap();
        assert_eq!(gateway.handle(&request("DELETE", &format!("/wiki/page?versionId={current}"), &[], &[])).status, 204);
        assert_eq!(gateway.handle(&request("GET", "/wiki/page", &[], &[])).status, 404, "the marker is current again");
        // a specific old version is deleted for good
        assert_eq!(gateway.handle(&request("DELETE", &format!("/wiki/page?versionId={v1}"), &[], &[])).status, 204);
        assert_eq!(gateway.handle(&request("GET", &format!("/wiki/page?versionId={v1}"), &[], &[])).status, 404);
        // appends extend the current version and keep no copy
        assert_eq!(gateway.handle(&request("PUT", "/wiki/log", &[], b"a")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/wiki/log", &[("x-amz-write-offset-bytes", "1")], b"b")).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/wiki/log", &[], &[])).body, b"ab");
        let log_versions = text(&gateway.handle(&request("GET", "/wiki?versions&prefix=log", &[], &[])));
        assert_eq!(all_between(&log_versions, "VersionId").len(), 1, "{log_versions}");
        // a bucket that holds only kept versions is not empty
        assert_eq!(gateway.handle(&request("DELETE", "/wiki/log", &[], &[])).status, 204);
        assert_eq!(gateway.handle(&request("DELETE", "/wiki", &[], &[])).status, 409);
    }

    #[test]
    fn a_bucket_without_versioning_keeps_nothing_and_a_lock_protects_a_kept_version() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "plain", &[]);
        assert_eq!(gateway.handle(&request("PUT", "/plain/k", &[], b"one")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/plain/k", &[], b"two")).status, 200);
        assert_eq!(all_between(&text(&gateway.handle(&request("GET", "/plain?versions", &[], &[]))), "VersionId").len(), 1);
        bucket_with(&gateway, "vault", &[("versioning", VERSIONED), ("object-lock", LOCKED_BUCKET)]);
        let held = gateway.handle(&request("PUT", "/vault/k", &[("x-amz-object-lock-mode", "COMPLIANCE"), ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z")], b"one"));
        let version = header_of(&held, "x-amz-version-id").unwrap();
        // overwriting a locked object is refused even with versioning; a delete only adds a marker
        assert_eq!(gateway.handle(&request("PUT", "/vault/k", &[], b"two")).status, 403);
        assert_eq!(gateway.handle(&request("DELETE", "/vault/k", &[], &[])).status, 204);
        assert_eq!(gateway.handle(&request("GET", &format!("/vault/k?versionId={version}"), &[], &[])).body, b"one");
        assert_eq!(gateway.handle(&request("DELETE", &format!("/vault/k?versionId={version}"), &[("x-amz-bypass-governance-retention", "true")], &[])).status, 403, "the locked version is not deleted");
    }

    #[test]
    fn a_noncurrent_expiration_removes_old_versions_and_an_expiration_leaves_a_marker() {
        let (gateway, _) = gateway_requiring_buckets();
        let rules = "<LifecycleConfiguration><Rule><ID>old</ID><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>";
        bucket_with(&gateway, "hist", &[("versioning", VERSIONED), ("lifecycle", rules)]);
        for body in ["a", "b", "c"] {
            assert_eq!(gateway.handle(&request("PUT", "/hist/k", &[], body.as_bytes())).status, 200);
        }
        assert_eq!(all_between(&text(&gateway.handle(&request("GET", "/hist?versions", &[], &[]))), "VersionId").len(), 3);
        let report = gateway.run_lifecycle(now_ms() + 2 * 86_400_000).unwrap();
        assert_eq!(report.expired_versions, 2, "{report:?}");
        assert_eq!(gateway.handle(&request("GET", "/hist/k", &[], &[])).body, b"c", "the current version stays");
        // five days on, the current version expires into a delete marker and the version under it is kept
        let later = gateway.run_lifecycle(now_ms() + 6 * 86_400_000).unwrap();
        assert_eq!(later.expired, 1, "{later:?}");
        assert_eq!(gateway.handle(&request("GET", "/hist/k", &[], &[])).status, 404);
        assert_eq!(all_between(&text(&gateway.handle(&request("GET", "/hist?versions", &[], &[]))), "VersionId").len(), 2);
    }

    #[test]
    fn a_bucket_that_opts_in_keeps_an_ordered_change_feed_read_by_cursor_and_trimmed_by_the_consumer() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "orders", &[]);
        // a bucket that did not opt in has no feed, and its mutations write nothing for one
        assert_eq!(gateway.handle(&request("PUT", "/orders/early", &[], b"x")).status, 200);
        assert_eq!(gateway.handle(&request("GET", "/orders?changes", &[], &[])).status, 400);
        let on = "<ChangeFeedConfiguration><Status>Enabled</Status></ChangeFeedConfiguration>";
        assert_eq!(gateway.handle(&request("PUT", "/orders?changefeed", &[], on.as_bytes())).status, 200);
        assert!(text(&gateway.handle(&request("GET", "/orders?changefeed", &[], &[]))).contains("Enabled"));
        assert_eq!(gateway.handle(&request("PUT", "/orders/a", &[], b"one")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/orders/a", &[("x-amz-write-offset-bytes", "3")], b"two")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/orders/b", &[("x-amz-copy-source", "/orders/a")], &[])).status, 200);
        assert_eq!(gateway.handle(&request("DELETE", "/orders/a", &[], &[])).status, 204);
        assert_eq!(gateway.handle(&request("POST", "/orders?delete", &[], b"<Delete><Object><Key>b</Key></Object></Delete>")).status, 200);
        let feed = text(&gateway.handle(&request("GET", "/orders?changes", &[], &[])));
        let operations = all_between(&feed, "Operation");
        assert_eq!(operations, ["put", "append", "copy", "delete", "delete"], "{feed}");
        assert_eq!(all_between(&feed, "Key"), ["a", "a", "b", "a", "b"], "{feed}");
        assert!(!feed.contains("early"), "what happened before the feed was on is not in it");
        assert!(!feed.contains(".changes") || feed.contains("<Cursor>.changes/"), "the entries themselves are not changes");
        // a cursor continues where the last page stopped, and pages are bounded
        let first = text(&gateway.handle(&request("GET", "/orders?changes&limit=2", &[], &[])));
        assert_eq!(all_between(&first, "Operation"), ["put", "append"]);
        assert!(first.contains("<HasMore>true</HasMore>"));
        let cursor = between(&first, "NextCursor").unwrap();
        let rest = text(&gateway.handle(&request("GET", &format!("/orders?changes&after={cursor}"), &[], &[])));
        assert_eq!(all_between(&rest, "Operation"), ["copy", "delete", "delete"], "{rest}");
        // the feed is not listed with the objects, and a consumer trims what it has processed
        let listed = text(&gateway.handle(&request("GET", "/orders?list-type=2", &[], &[])));
        assert_eq!(all_between(&listed, "Key"), ["early"], "{listed}");
        let trimmed = gateway.handle(&request("DELETE", &format!("/orders?changes&before={cursor}"), &[], &[]));
        assert_eq!(trimmed.status, 204);
        assert_eq!(header_of(&trimmed, "x-matrixobject-entries-dropped").as_deref(), Some("2"));
        let after_trim = text(&gateway.handle(&request("GET", "/orders?changes", &[], &[])));
        assert_eq!(all_between(&after_trim, "Operation"), ["copy", "delete", "delete"]);
    }

    #[test]
    fn a_multipart_upload_publishes_what_it_was_begun_with_and_respects_a_lock() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "big", &[("quota", "<Quota><Bytes>20</Bytes></Quota>")]);
        let started = gateway.handle(&request("POST", "/big/file?uploads", &[("content-type", "video/mp4"), ("x-amz-meta-camera", "north"), ("x-amz-tagging", "kind=clip")], &[]));
        assert_eq!(started.status, 200, "{}", text(&started));
        let id = between(&text(&started), "UploadId").unwrap();
        let part = gateway.handle(&request("PUT", &format!("/big/file?uploadId={id}&partNumber=1"), &[], b"0123456789"));
        let etag = header_of(&part, "etag").unwrap();
        let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>");
        let done = gateway.handle(&request("POST", &format!("/big/file?uploadId={id}"), &[], body.as_bytes()));
        assert_eq!(done.status, 200, "{}", text(&done));
        let head = gateway.handle(&request("HEAD", "/big/file", &[], &[]));
        assert_eq!(header_of(&head, "content-type").as_deref(), Some("video/mp4"));
        assert_eq!(header_of(&head, "x-amz-meta-camera").as_deref(), Some("north"));
        assert_eq!(header_of(&head, "x-amz-tagging-count").as_deref(), Some("1"));
        assert_eq!(gateway.handle(&request("GET", "/big/file", &[], &[])).body, b"0123456789");
        // the quota applies to the assembled object: a second upload that would pass it is refused and the upload survives
        let started = gateway.handle(&request("POST", "/big/second?uploads", &[], &[]));
        let id = between(&text(&started), "UploadId").unwrap();
        let part = gateway.handle(&request("PUT", &format!("/big/second?uploadId={id}&partNumber=1"), &[], b"abcdefghijklm"));
        let etag = header_of(&part, "etag").unwrap();
        let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>");
        assert_eq!(gateway.handle(&request("POST", &format!("/big/second?uploadId={id}"), &[], body.as_bytes())).status, 403);
        assert_eq!(gateway.handle(&request("GET", "/big/second", &[], &[])).status, 404);
        // a lock stops a completion that would replace a locked object
        bucket_with(&gateway, "vault", &[("object-lock", LOCKED_BUCKET)]);
        let until = "2099-01-01T00:00:00Z";
        assert_eq!(gateway.handle(&request("PUT", "/vault/k", &[("x-amz-object-lock-mode", "COMPLIANCE"), ("x-amz-object-lock-retain-until-date", until)], b"held")).status, 200);
        let started = gateway.handle(&request("POST", "/vault/k?uploads", &[], &[]));
        let id = between(&text(&started), "UploadId").unwrap();
        let part = gateway.handle(&request("PUT", &format!("/vault/k?uploadId={id}&partNumber=1"), &[], b"new"));
        let etag = header_of(&part, "etag").unwrap();
        let body = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>");
        assert_eq!(gateway.handle(&request("POST", &format!("/vault/k?uploadId={id}"), &[], body.as_bytes())).status, 403);
        assert_eq!(gateway.handle(&request("GET", "/vault/k", &[], &[])).body, b"held");
    }


    #[test]
    fn a_kept_version_carries_its_own_tags_and_retention_and_is_read_by_range() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "vh", &[("versioning", VERSIONED), ("object-lock", LOCKED_BUCKET)]);
        let v1 = header_of(&gateway.handle(&request("PUT", "/vh/k", &[], b"0123456789")), "x-amz-version-id").unwrap();
        let v2 = header_of(&gateway.handle(&request("PUT", "/vh/k", &[], b"abcdefghij")), "x-amz-version-id").unwrap();
        // a tag set on a kept version is that version's, and the current one is untouched
        let tags = "<Tagging><TagSet><Tag><Key>era</Key><Value>old</Value></Tag></TagSet></Tagging>";
        assert_eq!(gateway.handle(&request("PUT", &format!("/vh/k?tagging&versionId={v1}"), &[], tags.as_bytes())).status, 200);
        assert!(text(&gateway.handle(&request("GET", &format!("/vh/k?tagging&versionId={v1}"), &[], &[]))).contains("<Key>era</Key>"));
        assert!(!text(&gateway.handle(&request("GET", &format!("/vh/k?tagging&versionId={v2}"), &[], &[]))).contains("<Key>era</Key>"));
        assert!(!text(&gateway.handle(&request("GET", "/vh/k?tagging", &[], &[]))).contains("<Key>era</Key>"), "the current version was not changed");
        assert_eq!(gateway.handle(&request("GET", "/vh/k?tagging&versionId=999", &[], &[])).status, 404);
        // a retention on a kept version protects that version
        let governance = "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>";
        assert_eq!(gateway.handle(&request("PUT", &format!("/vh/k?retention&versionId={v1}"), &[], governance.as_bytes())).status, 200);
        assert!(text(&gateway.handle(&request("GET", &format!("/vh/k?retention&versionId={v1}"), &[], &[]))).contains("GOVERNANCE"));
        assert_eq!(gateway.handle(&request("GET", "/vh/k?retention", &[], &[])).status, 404, "the current version has no retention");
        // a version is read by range, with the answer a range read of an object gives
        let part = gateway.handle(&request("GET", &format!("/vh/k?versionId={v1}"), &[("range", "bytes=2-4")], &[]));
        assert_eq!((part.status, part.body.as_slice()), (206, b"234".as_slice()));
        assert_eq!(header_of(&part, "content-range").as_deref(), Some("bytes 2-4/10"));
        let suffix = gateway.handle(&request("GET", &format!("/vh/k?versionId={v1}"), &[("range", "bytes=-3")], &[]));
        assert_eq!(suffix.body, b"789");
        assert_eq!(gateway.handle(&request("GET", &format!("/vh/k?versionId={v1}"), &[("range", "bytes=50-60")], &[])).status, 416);
        // and the locked version is not deleted without the bypass
        assert_eq!(gateway.handle(&request("DELETE", &format!("/vh/k?versionId={v1}"), &[], &[])).status, 403);
        assert_eq!(gateway.handle(&request("DELETE", &format!("/vh/k?versionId={v1}"), &[("x-amz-bypass-governance-retention", "true")], &[])).status, 204);
        assert_eq!(gateway.handle(&request("GET", &format!("/vh/k?versionId={v1}"), &[], &[])).status, 404);
    }


    #[test]
    fn lifecycle_rules_filter_by_tag_keep_versions_transition_objects_and_remove_empty_markers() {
        let (gateway, _) = gateway_requiring_buckets();
        let rules = "<LifecycleConfiguration>\
            <Rule><ID>tagged</ID><Filter><Tag><Key>team</Key><Value>x</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>\
            <Rule><ID>cold</ID><Filter><Prefix>arch/</Prefix></Filter><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>STANDARD_IA</StorageClass></Transition></Rule>\
            <Rule><ID>keep</ID><Filter><Prefix>ver/</Prefix></Filter><Status>Enabled</Status><NoncurrentVersionExpiration><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>\
            <Rule><ID>markers</ID><Filter><Prefix>mk/</Prefix></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>\
            </LifecycleConfiguration>";
        bucket_with(&gateway, "ops", &[("versioning", VERSIONED), ("lifecycle", rules)]);
        // the configuration reads back with its filters, counts and transition
        let read = text(&gateway.handle(&request("GET", "/ops?lifecycle", &[], &[])));
        for needle in ["<Tag><Key>team</Key><Value>x</Value></Tag>", "<NewerNoncurrentVersions>2</NewerNoncurrentVersions>", "<StorageClass>STANDARD_IA</StorageClass>", "<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>"] {
            assert!(read.contains(needle), "{needle} in {read}");
        }
        // a tag filter: only the tagged object expires
        assert_eq!(gateway.handle(&request("PUT", "/ops/t1", &[("x-amz-tagging", "team=x")], b"1")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/ops/t2", &[("x-amz-tagging", "team=y")], b"2")).status, 200);
        // a transition: the class changes and the bytes do not
        assert_eq!(gateway.handle(&request("PUT", "/ops/arch/a", &[], b"archive me")).status, 200);
        // keep the 2 newest of the non-current versions
        for body in ["a", "b", "c", "d", "e"] {
            assert_eq!(gateway.handle(&request("PUT", "/ops/ver/k", &[], body.as_bytes())).status, 200);
        }
        // a delete marker left over nothing: write, delete, write again (the marker is kept), delete the first version
        let first = header_of(&gateway.handle(&request("PUT", "/ops/mk/m", &[], b"one")), "x-amz-version-id").unwrap();
        assert_eq!(gateway.handle(&request("DELETE", "/ops/mk/m", &[], &[])).status, 204);
        assert_eq!(gateway.handle(&request("PUT", "/ops/mk/m", &[], b"two")).status, 200);
        assert_eq!(gateway.handle(&request("DELETE", &format!("/ops/mk/m?versionId={first}"), &[], &[])).status, 204);
        let report = gateway.run_lifecycle(now_ms() + 2 * 86_400_000).unwrap();
        assert_eq!(report.expired, 1, "{report:?}");
        assert_eq!(gateway.handle(&request("GET", "/ops/t1", &[], &[])).status, 404, "the tagged object expired");
        assert_eq!(gateway.handle(&request("GET", "/ops/t2", &[], &[])).body, b"2", "the other tag did not");
        assert_eq!(report.transitioned, 1, "{report:?}");
        let head = gateway.handle(&request("HEAD", "/ops/arch/a", &[], &[]));
        assert_eq!(header_of(&head, "x-amz-storage-class").as_deref(), Some("STANDARD_IA"));
        assert_eq!(gateway.handle(&request("GET", "/ops/arch/a", &[], &[])).body, b"archive me");
        assert_eq!(report.expired_versions, 2, "5 writes keep 4 versions, the 2 newest stay: {report:?}");
        let versions = text(&gateway.handle(&request("GET", "/ops?versions&prefix=ver/", &[], &[])));
        assert_eq!(all_between(&versions, "VersionId").len(), 3, "the current version and two kept: {versions}");
        assert_eq!(report.expired_markers, 1, "{report:?}");
        // a second pass finds nothing more to do
        let again = gateway.run_lifecycle(now_ms() + 2 * 86_400_000).unwrap();
        assert_eq!((again.transitioned, again.expired_versions, again.expired_markers), (0, 0, 0), "{again:?}");
    }

    #[test]
    fn an_object_in_a_colder_class_is_restored_for_a_window_and_a_version_is_copied_back() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "cold", &[("versioning", VERSIONED)]);
        let put = gateway.handle(&request("PUT", "/cold/deep", &[("x-amz-storage-class", "GLACIER")], b"frozen"));
        assert_eq!(put.status, 200, "{}", text(&put));
        assert_eq!(header_of(&gateway.handle(&request("HEAD", "/cold/deep", &[], &[])), "x-amz-storage-class").as_deref(), Some("GLACIER"));
        assert!(header_of(&gateway.handle(&request("HEAD", "/cold/deep", &[], &[])), "x-amz-restore").is_none());
        let restore = gateway.handle(&request("POST", "/cold/deep?restore", &[], b"<RestoreRequest><Days>2</Days></RestoreRequest>"));
        assert_eq!(restore.status, 202, "{}", text(&restore));
        let restored = header_of(&gateway.handle(&request("HEAD", "/cold/deep", &[], &[])), "x-amz-restore").expect("a restore window");
        assert!(restored.contains("ongoing-request=\"false\"") && restored.contains("expiry-date"), "{restored}");
        assert_eq!(gateway.handle(&request("GET", "/cold/deep", &[], &[])).body, b"frozen");
        // an object in the standard class has nothing to restore, and a bad request is refused
        assert_eq!(gateway.handle(&request("PUT", "/cold/plain", &[], b"x")).status, 200);
        let standard = gateway.handle(&request("POST", "/cold/plain?restore", &[], b"<RestoreRequest><Days>2</Days></RestoreRequest>"));
        assert_eq!(standard.status, 403);
        assert!(text(&standard).contains("InvalidObjectState"), "{}", text(&standard));
        assert_eq!(gateway.handle(&request("POST", "/cold/deep?restore", &[], b"<RestoreRequest/>")).status, 400);
        assert_eq!(gateway.handle(&request("PUT", "/cold/bad", &[("x-amz-storage-class", "cold storage")], b"x")).status, 400);

        // a version is brought back by copying it onto its own key, and the version it replaces is kept
        let v1 = header_of(&gateway.handle(&request("PUT", "/cold/doc", &[("content-type", "text/plain")], b"one")), "x-amz-version-id").unwrap();
        assert_eq!(gateway.handle(&request("PUT", "/cold/doc", &[], b"two")).status, 200);
        assert_eq!(gateway.handle(&request("PUT", "/cold/doc", &[("x-amz-copy-source", "/cold/doc")], &[])).status, 400, "a copy onto itself is refused");
        let back = gateway.handle(&request("PUT", "/cold/doc", &[("x-amz-copy-source", &format!("/cold/doc?versionId={v1}"))], &[]));
        assert_eq!(back.status, 200, "{}", text(&back));
        assert_eq!(gateway.handle(&request("GET", "/cold/doc", &[], &[])).body, b"one");
        assert_eq!(header_of(&gateway.handle(&request("HEAD", "/cold/doc", &[], &[])), "content-type").as_deref(), Some("text/plain"));
        let versions = text(&gateway.handle(&request("GET", "/cold?versions&prefix=doc", &[], &[])));
        assert_eq!(all_between(&versions, "VersionId").len(), 3, "the copy over the key kept 'two': {versions}");
        // a version that does not exist is not copied
        assert_eq!(gateway.handle(&request("PUT", "/cold/other", &[("x-amz-copy-source", "/cold/doc?versionId=999")], &[])).status, 404);
    }


    #[test]
    fn versions_are_listed_in_pages_and_a_prefix_is_deleted_in_batches() {
        let (gateway, _) = gateway_requiring_buckets();
        bucket_with(&gateway, "pg", &[("versioning", VERSIONED), ("object-lock", LOCKED_BUCKET)]);
        for key in ["a", "b"] {
            for body in ["1", "2", "3"] {
                assert_eq!(gateway.handle(&request("PUT", &format!("/pg/{key}"), &[], body.as_bytes())).status, 200);
            }
        }
        // six versions in pages of four, resumed inside a key by the version marker
        let mut seen: Vec<(String, String)> = Vec::new();
        let (mut key_marker, mut version_marker) = (String::new(), String::new());
        for _ in 0..4 {
            let mut target = "/pg?versions&max-keys=4".to_string();
            if !key_marker.is_empty() {
                target.push_str(&format!("&key-marker={key_marker}&version-id-marker={version_marker}"));
            }
            let page = text(&gateway.handle(&request("GET", &target, &[], &[])));
            let keys = all_between(&page, "Key");
            let versions = all_between(&page, "VersionId");
            seen.extend(keys.into_iter().zip(versions));
            match (between(&page, "NextKeyMarker"), between(&page, "NextVersionIdMarker")) {
                (Some(key), Some(version)) => {
                    key_marker = key;
                    version_marker = version;
                }
                _ => break,
            }
        }
        assert_eq!(seen.len(), 6, "{seen:?}");
        let unique: std::collections::BTreeSet<_> = seen.iter().cloned().collect();
        assert_eq!(unique.len(), 6, "no version repeats or is lost across pages: {seen:?}");
        // newest first within a key, keys in order
        assert!(seen.windows(2).all(|pair| pair[0].0 < pair[1].0 || (pair[0].0 == pair[1].0 && pair[0].1.parse::<u64>().unwrap() > pair[1].1.parse::<u64>().unwrap())), "{seen:?}");

        // a prefix is deleted in batches; a locked object is refused and counted
        bucket_with(&gateway, "bulk", &[("object-lock", LOCKED_BUCKET)]);
        for index in 0..5 {
            assert_eq!(gateway.handle(&request("PUT", &format!("/bulk/old/{index}"), &[], b"x")).status, 200);
        }
        assert_eq!(gateway.handle(&request("PUT", "/bulk/keep/0", &[], b"x")).status, 200);
        let held = [("x-amz-object-lock-mode", "COMPLIANCE"), ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z")];
        assert_eq!(gateway.handle(&request("PUT", "/bulk/old/held", &held, b"x")).status, 200);
        let empty = gateway.handle(&request("DELETE", "/bulk?delete-prefix=", &[], &[]));
        assert_eq!(empty.status, 400, "an empty prefix is refused");
        let first = text(&gateway.handle(&request("DELETE", "/bulk?delete-prefix=old/&limit=3", &[], &[])));
        assert!(first.contains("<Deleted>3</Deleted>") && first.contains("<IsTruncated>true</IsTruncated>"), "{first}");
        let next = between(&first, "NextMarker").expect("a marker to resume after");
        let second = text(&gateway.handle(&request("DELETE", &format!("/bulk?delete-prefix=old/&limit=10&marker={next}"), &[], &[])));
        assert!(second.contains("<Refused>1</Refused>") && second.contains("<IsTruncated>false</IsTruncated>"), "{second}");
        let left = text(&gateway.handle(&request("GET", "/bulk?list-type=2", &[], &[])));
        assert_eq!(all_between(&left, "Key"), ["keep/0", "old/held"], "{left}");
    }

}

#[cfg(test)]
#[path = "conditional_put_outcome_tests.rs"]
mod conditional_put_outcome_tests;
