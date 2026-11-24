// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The commands that lease an object for log writing and move it from one chunk to the next.
//!
//! * grant (action 1): `holder` takes the writer lease and, when the object is not already
//!   writing a chunk, a new empty chunk is added to its manifest, in one commit. The identity of
//!   that chunk is derived from the raft index of the commit that grants it, so it is reserved
//!   and used by one entry: there is no id that is reserved and never used, and no two grants can
//!   name the same one.
//! * switch (action 2): the holder of the live lease seals the chunk it was writing at its final
//!   length and footer and opens the next one, in one commit.
//! * renew (action 3): ONE entry extends the leases a holder has on many objects.
//! * read (action 4): the lease and the open chunk of one object, from the committed state.
//! * sweep (action 5): ONE entry drops many leases whose term is over. Issued by the leader's
//!   cleanup loop, not by a writer.
//!
//! Everything that writes is refused unless the cluster has raised `FEATURE_CHUNK_MANIFEST` and
//! `FEATURE_LOG_CHUNK_LEASES`: a
//! format 3 manifest cannot be read by a build that predates it, and nothing raises the feature
//! unless an operator does (see `features_commands`). Decisions are made here on the leader
//! against the committed state and carried by the outcome, which every node checks again when it
//! applies it.
use super::meta_message::MetaReply;
use crate::cluster_features::{FEATURE_CHUNK_MANIFEST, FEATURE_LOG_CHUNK_LEASES};
use crate::log_chunk_outcome::{
    LeaseBatch, LeaseRef, LogChunkChange, WriterLease, ACTION_GRANT, ACTION_SEAL_SWITCH, MAX_BATCH_LEASES,
    MAX_LEASE_TTL_MS,
};
use crate::object_manifest::{
    ManifestOutcome, ObjectExtent, ObjectManifest, EXTENT_FLAG_FOOTER_SHA256, EXTENT_FLAG_SEALED, EXTENT_FLAG_TAIL,
    EXTENT_KIND_LOG_CHUNK, MANIFEST_FORMAT_CHUNKED,
};
use crate::placement_outcome::PlacementOutcome;
use crate::{MatrixMetaService, ObjectError, ObjectId};
use prost::Message;

pub const ACTION_RENEW: u32 = 3;
pub const ACTION_READ: u32 = 4;
pub const ACTION_SWEEP: u32 = 5;
/// The shortest and the longest term a lease is granted or renewed for.
pub const MIN_TTL_MS: u64 = 1_000;
/// Most entries (chunks and pages together) an object may hold, the bound the replica plane
/// already puts on every object. At 64 MiB a chunk that is 256 GiB of log per object; past it a
/// chunk switch is refused and the writer must end the object (it may still fill the open chunk).
pub const MAX_OBJECT_ENTRIES: usize = 4096;
/// Leases one sweep names.
pub const SWEEP_BATCH: usize = 256;
const MAX_REQUEST_BYTES: usize = 128 * 1024;

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogChunkRequest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(uint32, tag = "2")]
    pub action: u32,
    #[prost(string, tag = "3")]
    pub bucket: String,
    #[prost(string, tag = "4")]
    pub key: String,
    /// Grant, switch and renew: the writer.
    #[prost(string, tag = "5")]
    pub holder: String,
    /// Grant, renew and switch: the term asked for, from now. A switch extends the lease to
    /// `now + ttl_ms` if that is later than it runs to, and never shortens it.
    #[prost(uint64, tag = "6")]
    pub ttl_ms: u64,
    /// Grant and switch: the group that holds the chunk the commit opens. The caller picked it
    /// from the routes the metadata plane planned, as it does for pages; it is checked here
    /// against the configured groups.
    #[prost(string, tag = "7")]
    pub group: String,
    /// Switch: the epoch of the lease the writer holds.
    #[prost(uint64, tag = "8")]
    pub epoch: u64,
    /// Switch: the final length of the chunk being sealed, and its footer.
    #[prost(uint64, tag = "9")]
    pub sealed_length: u64,
    #[prost(uint32, tag = "10")]
    pub footer_crc: u32,
    #[prost(bytes = "vec", tag = "11")]
    pub footer_sha256: Vec<u8>,
    /// Renew: the leases the holder renews, ascending by object.
    #[prost(message, repeated, tag = "12")]
    pub renewals: Vec<LeaseRef>,
    /// Sweep: the leases the cleanup loop saw run out, ascending by object.
    #[prost(message, repeated, tag = "13")]
    pub drops: Vec<LeaseRef>,
}

impl LogChunkRequest {
    pub fn grant(bucket: &str, key: &str, holder: &str, ttl_ms: u64, group: &str) -> Self {
        Self {
            version: 1,
            action: ACTION_GRANT,
            bucket: bucket.into(),
            key: key.into(),
            holder: holder.into(),
            ttl_ms,
            group: group.into(),
            ..Default::default()
        }
    }
    /// The term a switch renews the lease for when the caller names none.
    pub const SWITCH_TERM_MS: u64 = 30_000;
    pub fn with_ttl(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }
    pub fn switch(
        bucket: &str,
        key: &str,
        holder: &str,
        epoch: u64,
        group: &str,
        sealed_length: u64,
        footer_crc: u32,
        footer_sha256: Vec<u8>,
    ) -> Self {
        Self {
            version: 1,
            action: ACTION_SEAL_SWITCH,
            bucket: bucket.into(),
            key: key.into(),
            holder: holder.into(),
            ttl_ms: Self::SWITCH_TERM_MS,
            group: group.into(),
            epoch,
            sealed_length,
            footer_crc,
            footer_sha256,
            ..Default::default()
        }
    }
    pub fn renew(holder: &str, ttl_ms: u64, renewals: Vec<LeaseRef>) -> Self {
        Self { version: 1, action: ACTION_RENEW, holder: holder.into(), ttl_ms, renewals, ..Default::default() }
    }
    pub fn read(bucket: &str, key: &str) -> Self {
        Self { version: 1, action: ACTION_READ, bucket: bucket.into(), key: key.into(), ..Default::default() }
    }
    pub fn sweep(drops: Vec<LeaseRef>) -> Self {
        Self { version: 1, action: ACTION_SWEEP, drops, ..Default::default() }
    }
    /// Select the unified chunk catalog for a grant, switch or read.
    /// Renewals and sweeps use the shared lease table and keep version 1.
    pub fn unified(mut self) -> Self {
        self.version = 2;
        self
    }
    pub fn is_read(&self) -> bool {
        self.action == ACTION_READ
    }
    fn names_object(&self) -> bool {
        !self.bucket.is_empty() || !self.key.is_empty()
    }
    pub fn validate(&self) -> Result<(), ObjectError> {
        let holder_ok = !self.holder.is_empty()
            && self.holder.len() <= 128
            && self.holder.bytes().all(|b| b.is_ascii_graphic());
        let ttl_ok = (MIN_TTL_MS..=MAX_LEASE_TTL_MS).contains(&self.ttl_ms);
        let group_ok = !self.group.is_empty() && self.group.len() <= 256;
        let object_ok = crate::validate_bucket(&self.bucket).is_ok() && crate::validate_key(&self.key).is_ok();
        let none_of = |renewals: bool, drops: bool, switch: bool| {
            (!renewals || self.renewals.is_empty())
                && (!drops || self.drops.is_empty())
                && (!switch
                    || (self.epoch == 0
                        && self.sealed_length == 0
                        && self.footer_crc == 0
                        && self.footer_sha256.is_empty()))
        };
        let shaped = (self.version == 1 || (self.version == 2 && matches!(self.action, ACTION_GRANT | ACTION_SEAL_SWITCH | ACTION_READ)))
            && match self.action {
                ACTION_GRANT => {
                    object_ok && holder_ok && ttl_ok && group_ok && none_of(true, true, true) && self.epoch == 0
                }
                ACTION_SEAL_SWITCH => {
                    object_ok
                        && holder_ok
                        && group_ok
                        && ttl_ok
                        && self.epoch != 0
                        && self.sealed_length != 0
                        && (self.footer_sha256.is_empty() || self.footer_sha256.len() == 32)
                        && none_of(true, true, false)
                }
                ACTION_RENEW => {
                    holder_ok
                        && ttl_ok
                        && !self.names_object()
                        && self.group.is_empty()
                        && !self.renewals.is_empty()
                        && self.renewals.len() <= MAX_BATCH_LEASES
                        && none_of(false, true, true)
                }
                ACTION_READ => {
                    object_ok
                        && self.holder.is_empty()
                        && self.ttl_ms == 0
                        && self.group.is_empty()
                        && none_of(true, true, true)
                }
                ACTION_SWEEP => {
                    !self.names_object()
                        && self.holder.is_empty()
                        && self.ttl_ms == 0
                        && self.group.is_empty()
                        && !self.drops.is_empty()
                        && self.drops.len() <= MAX_BATCH_LEASES
                        && none_of(true, false, true)
                }
                _ => false,
            }
            && self.renewals.windows(2).all(|pair| (&pair[0].bucket, &pair[0].key) < (&pair[1].bucket, &pair[1].key))
            && self.drops.windows(2).all(|pair| (&pair[0].bucket, &pair[0].key) < (&pair[1].bucket, &pair[1].key))
            && self.renewals.iter().chain(self.drops.iter()).all(|lease| {
                crate::validate_bucket(&lease.bucket).is_ok() && crate::validate_key(&lease.key).is_ok() && lease.epoch != 0
            });
        if !shaped {
            return Err(ObjectError::InvalidArgument("invalid log chunk request".into()));
        }
        Ok(())
    }
    pub fn from_payload(bytes: &[u8]) -> Result<Self, ObjectError> {
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err(invalid("log chunk request exceeds its bound"));
        }
        let request = Self::decode(bytes).map_err(|_| invalid("invalid log chunk request"))?;
        if request.encode_to_vec() != bytes {
            return Err(invalid("noncanonical log chunk request"));
        }
        request.validate().map_err(|_| invalid("invalid log chunk request"))?;
        Ok(request)
    }
}

/// What a command answers: the lease, and the chunk the object is writing.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct LogChunkReply {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// The lease the object holds after the command (absent when it holds none).
    #[prost(message, optional, tag = "2")]
    pub lease: Option<WriterLease>,
    /// The chunk being written: where it lives and where it starts in the object. Absent when the
    /// object is not writing one.
    #[prost(message, optional, tag = "3")]
    pub open: Option<ObjectExtent>,
    #[prost(uint64, tag = "4")]
    pub generation: u64,
    /// The object's length as recorded: the end of the last sealed chunk.
    #[prost(uint64, tag = "5")]
    pub length: u64,
    /// Renew: how many were renewed or already ran longer; the ones that were not are in `lost`.
    #[prost(uint64, tag = "6")]
    pub renewed: u64,
    #[prost(message, repeated, tag = "7")]
    pub lost: Vec<LeaseRef>,
    /// Sweep: how many leases were dropped.
    #[prost(uint64, tag = "8")]
    pub dropped: u64,
    /// Version 2: the open tail in the unified chunk catalog. `open` is absent.
    #[prost(message, optional, tag = "9")]
    pub chunk_open: Option<crate::chunk_manifest::ChunkExtent>,
}

pub fn reply_from_payload(bytes: &[u8]) -> Result<LogChunkReply, ObjectError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(invalid("log chunk reply exceeds its bound"));
    }
    let reply = LogChunkReply::decode(bytes).map_err(|_| invalid("invalid log chunk reply"))?;
    let shape = match reply.version {
        1 => reply.chunk_open.is_none(),
        2 => reply.open.is_none() && reply.renewed == 0 && reply.lost.is_empty() && reply.dropped == 0,
        _ => false,
    };
    if reply.encode_to_vec() != bytes || !shape {
        return Err(invalid("noncanonical log chunk reply"));
    }
    Ok(reply)
}

/// The volume and the chunk of the chunk a commit at raft index `index` opens. This is the very
/// function a page allocation derives its chunk from (`ObjectPageAllocation::page`): both name a
/// chunk after the index of the one entry that creates it, in one id space with one encoding.
/// An entry is either an allocation or a grant, never both, so a chunk id is the index of
/// exactly one entry: unique, never reused after a lease ends or a chunk seals (an index is never
/// issued twice, and it is part of the snapshot), and never equal to a page's. Each half fits 32
/// bits, which the chunkserver's store requires.
pub fn chunk_identity(index: u64) -> (u64, u64) {
    crate::object_manifest::chunk_identity_of_index(index)
}

pub fn reply(
    lease: Option<WriterLease>,
    manifest: Option<&ObjectManifest>,
) -> MetaReply {
    let live = manifest.filter(|manifest| !manifest.deleted);
    MetaReply::LogChunk(LogChunkReply {
        version: 1,
        lease,
        open: live.and_then(|manifest| manifest.extents.last().filter(|extent| crate::log_chunk_outcome::is_open_tail(extent)).cloned()),
        generation: live.map_or(0, |manifest| manifest.generation),
        length: live.map_or(0, |manifest| manifest.length),
        ..Default::default()
    })
}

fn stored_manifest(service: &MatrixMetaService, bucket: &str, key: &str) -> Option<ObjectManifest> {
    service
        .object_manifests
        .get(&ObjectId::new(bucket.to_string(), key.to_string()))
        .filter(|stored| !stored.deleted)
        .map(|stored| stored.to_manifest())
}

/// The answer to a read.
pub(super) fn read(service: &MatrixMetaService, request: &LogChunkRequest) -> Result<MetaReply, ObjectError> {
    request.validate()?;
    if request.version == 2 {
        return super::unified_log_writer_commands::read(service, request);
    }
    require_legacy_catalog(service, request)?;
    Ok(reply(
        service.writer_leases.get(&request.bucket, &request.key),
        stored_manifest(service, &request.bucket, &request.key).as_ref(),
    ))
}

/// A decision: nothing to commit, or an outcome and what to answer once it commits.
pub(super) enum Decision {
    Unchanged(MetaReply),
    Change(PlacementOutcome, MetaReply),
}

fn require_legacy_catalog(service: &MatrixMetaService, request: &LogChunkRequest) -> Result<(), ObjectError> {
    if service.chunk_catalog.get(&ObjectId::new(&request.bucket, &request.key)).is_some() {
        return Err(refused("this object uses the unified chunk catalog"));
    }
    Ok(())
}

fn require_feature(service: &MatrixMetaService) -> Result<(), ObjectError> {
    // both: a build that reads format 3 manifests may still not read the outcomes that make them
    if !service.cluster_features.has(FEATURE_CHUNK_MANIFEST | FEATURE_LOG_CHUNK_LEASES) {
        return Err(refused("chunk-granular manifests are not enabled for this cluster"));
    }
    Ok(())
}

/// An object is a page object or a log object, fixed when it is created: a log chunk is never added
/// after page extents, and nothing converts one kind into the other. An object that holds no extent
/// yet has not chosen. Refused before any state is decided, so a refusal changes nothing.
fn require_log_object(manifest: Option<&ObjectManifest>) -> Result<(), ObjectError> {
    if manifest.is_some_and(|manifest| manifest.extents.iter().any(|extent| extent.kind != EXTENT_KIND_LOG_CHUNK)) {
        return Err(refused("the object is not a log object: it holds page extents, and an object keeps the kind it was created with"));
    }
    Ok(())
}

fn new_tail(group: &str, index: u64, offset: u64) -> ObjectExtent {
    let (volume_id, chunk_id) = chunk_identity(index);
    ObjectExtent {
        group: group.to_string(),
        volume_id,
        chunk_id,
        object_offset: offset,
        length: 0,
        write_sequence: index,
        kind: EXTENT_KIND_LOG_CHUNK,
        flags: EXTENT_FLAG_TAIL,
        ..Default::default()
    }
}

/// Decide a command against `service`, at the leader's clock reading `now_ms`.
pub(super) fn prepare(
    service: &MatrixMetaService,
    groups: &super::chunk_groups::ChunkGroupCatalog,
    request: &LogChunkRequest,
    now_ms: u64,
) -> Result<Decision, ObjectError> {
    request.validate()?;
    if request.version == 2 {
        return super::unified_log_writer_commands::prepare(service, groups, request, now_ms);
    }
    if request.is_read() {
        return Ok(Decision::Unchanged(read(service, request)?));
    }
    if now_ms == 0 {
        return Err(refused("the metadata clock has no reading"));
    }
    match request.action {
        ACTION_GRANT => grant(service, groups, request, now_ms),
        ACTION_SEAL_SWITCH => switch(service, groups, request, now_ms),
        ACTION_RENEW => renew(service, request, now_ms),
        _ => sweep(service, request, now_ms),
    }
}

fn next_index(service: &MatrixMetaService) -> Result<u64, ObjectError> {
    service
        .placement_commit_index
        .checked_add(1)
        .ok_or_else(|| refused("placement index exhausted"))
}

fn grant(
    service: &MatrixMetaService,
    groups: &super::chunk_groups::ChunkGroupCatalog,
    request: &LogChunkRequest,
    now_ms: u64,
) -> Result<Decision, ObjectError> {
    require_feature(service)?;
    require_legacy_catalog(service, request)?;
    let index = next_index(service)?;
    let held = service.writer_leases.get(&request.bucket, &request.key);
    let before = stored_manifest(service, &request.bucket, &request.key);
    require_log_object(before.as_ref())?;
    // a deleted key keeps its tombstone: the object created again builds on it (its generation goes on
    // from the tombstone's) and is published against it, not against nothing
    let tombstone = service
        .object_manifests
        .get(&ObjectId::new(request.bucket.clone(), request.key.clone()))
        .filter(|stored| stored.deleted)
        .map(|stored| stored.to_manifest());
    let open = before.as_ref().is_some_and(|manifest| manifest.extents.last().is_some_and(crate::log_chunk_outcome::is_open_tail));
    if let Some(lease) = &held {
        if lease.holder != request.holder && lease.expires_at_ms > now_ms {
            return Err(refused("the object is leased to another writer"));
        }
        // the holder asking again while its lease is live and a chunk is open: nothing changes, so
        // a retry of a reply that was lost costs no entry
        if lease.holder == request.holder && lease.expires_at_ms > now_ms && open {
            return Ok(Decision::Unchanged(reply(held.clone(), before.as_ref())));
        }
    }
    let lease = WriterLease {
        holder: request.holder.clone(),
        epoch: index,
        expires_at_ms: now_ms.checked_add(request.ttl_ms).ok_or_else(|| refused("lease term overflow"))?,
    };
    let change = LogChunkChange {
        action: ACTION_GRANT,
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        lease_before: held,
        lease_after: Some(lease.clone()),
        now_ms,
    };
    let manifest = if open {
        None
    } else {
        if let Some(prior) = &before {
            if !(2..=MANIFEST_FORMAT_CHUNKED).contains(&prior.format_version) {
                return Err(refused("a chunk cannot follow a manifest that predates content digests"));
            }
            if prior.extents.len() + 1 > MAX_OBJECT_ENTRIES {
                return Err(refused("the object holds the most entries it may; end it"));
            }
        }
        let tail = new_tail(&request.group, index, before.as_ref().map_or(0, |prior| prior.length));
        groups.check_extents(std::slice::from_ref(&tail))?;
        let mut after = before.clone().unwrap_or_else(|| ObjectManifest {
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            ..Default::default()
        });
        after.format_version = MANIFEST_FORMAT_CHUNKED;
        after.generation = before.as_ref().or(tombstone.as_ref()).map_or(1, |prior| prior.generation + 1);
        after.extents.push(tail);
        Some(ManifestOutcome::decide_chunked(before.clone().or(tombstone), after)?)
    };
    let outcome = PlacementOutcome::log_chunk_decision(service, change, manifest)?;
    let published = match &outcome.manifest {
        Some(manifest) => manifest.after.clone(),
        None => before,
    };
    Ok(Decision::Change(outcome, reply(Some(lease), published.as_ref())))
}

fn switch(
    service: &MatrixMetaService,
    groups: &super::chunk_groups::ChunkGroupCatalog,
    request: &LogChunkRequest,
    now_ms: u64,
) -> Result<Decision, ObjectError> {
    require_feature(service)?;
    require_legacy_catalog(service, request)?;
    let index = next_index(service)?;
    let held = service.writer_leases.get(&request.bucket, &request.key);
    let lease = held
        .clone()
        .filter(|lease| lease.holder == request.holder && lease.epoch == request.epoch && lease.expires_at_ms > now_ms)
        .ok_or_else(|| refused("the writer does not hold the live lease of this object"))?;
    let prior = stored_manifest(service, &request.bucket, &request.key)
        .filter(|manifest| manifest.format_version == MANIFEST_FORMAT_CHUNKED)
        .ok_or_else(|| refused("the object has no chunk being written"))?;
    require_log_object(Some(&prior))?;
    let open = prior
        .extents
        .last()
        .filter(|extent| crate::log_chunk_outcome::is_open_tail(extent))
        .cloned()
        .ok_or_else(|| refused("the object has no chunk being written"))?;
    if prior.extents.len() + 1 > MAX_OBJECT_ENTRIES {
        return Err(refused("the object holds the most entries it may; end it"));
    }
    let mut sealed = open.clone();
    sealed.length = request.sealed_length;
    sealed.flags = EXTENT_FLAG_SEALED
        | if request.footer_sha256.is_empty() { 0 } else { EXTENT_FLAG_FOOTER_SHA256 };
    sealed.footer_crc = request.footer_crc;
    sealed.footer_sha256 = request.footer_sha256.clone();
    let end = open
        .object_offset
        .checked_add(request.sealed_length)
        .ok_or_else(|| refused("object length overflow"))?;
    let tail = new_tail(&request.group, index, end);
    groups.check_extents(std::slice::from_ref(&tail))?;
    let mut after = prior.clone();
    after.generation = prior.generation + 1;
    after.length = end;
    let last = after.extents.len() - 1;
    after.extents[last] = sealed;
    after.extents.push(tail);
    // the switch is also the renewal: the lease runs to at least now + ttl, never less than it did
    let renewed = WriterLease {
        expires_at_ms: lease
            .expires_at_ms
            .max(now_ms.checked_add(request.ttl_ms).ok_or_else(|| refused("lease term overflow"))?),
        ..lease.clone()
    };
    let change = LogChunkChange {
        action: ACTION_SEAL_SWITCH,
        bucket: request.bucket.clone(),
        key: request.key.clone(),
        lease_before: held,
        lease_after: Some(renewed.clone()),
        now_ms,
    };
    let manifest = ManifestOutcome::decide_chunked(Some(prior), after)?;
    let outcome = PlacementOutcome::log_chunk_decision(service, change, Some(manifest))?;
    let published = outcome.manifest.as_ref().and_then(|manifest| manifest.after.clone());
    Ok(Decision::Change(outcome, reply(Some(renewed), published.as_ref())))
}

fn renew(service: &MatrixMetaService, request: &LogChunkRequest, now_ms: u64) -> Result<Decision, ObjectError> {
    let expires = now_ms.checked_add(request.ttl_ms).ok_or_else(|| refused("lease term overflow"))?;
    let (mut renewed, mut lost, mut kept) = (Vec::new(), Vec::new(), 0u64);
    for lease_ref in &request.renewals {
        match service.writer_leases.get(&lease_ref.bucket, &lease_ref.key) {
            Some(lease) if lease.holder == request.holder && lease.epoch == lease_ref.epoch => {
                if lease.expires_at_ms >= expires {
                    kept += 1; // already runs at least that long: a renewal would shorten it
                } else {
                    renewed.push(lease_ref.clone());
                }
            }
            _ => lost.push(lease_ref.clone()),
        }
    }
    let answer = LogChunkReply { version: 1, renewed: renewed.len() as u64 + kept, lost, ..Default::default() };
    if renewed.is_empty() {
        return Ok(Decision::Unchanged(MetaReply::LogChunk(answer)));
    }
    let index = next_index(service)?;
    let outcome = PlacementOutcome {
        version: 16,
        before_index: service.placement_commit_index,
        after_index: index,
        lease_batch: Some(LeaseBatch {
            holder: request.holder.clone(),
            now_ms,
            expires_at_ms: expires,
            renewed,
            dropped: Vec::new(),
        }),
        ..Default::default()
    };
    outcome.to_payload()?;
    Ok(Decision::Change(outcome, MetaReply::LogChunk(answer)))
}

fn sweep(service: &MatrixMetaService, request: &LogChunkRequest, now_ms: u64) -> Result<Decision, ObjectError> {
    let over: Vec<LeaseRef> = request
        .drops
        .iter()
        .filter(|lease_ref| {
            service
                .writer_leases
                .get(&lease_ref.bucket, &lease_ref.key)
                .is_some_and(|lease| lease.epoch == lease_ref.epoch && lease.expires_at_ms < now_ms)
        })
        .cloned()
        .collect();
    let answer = LogChunkReply { version: 1, dropped: over.len() as u64, ..Default::default() };
    if over.is_empty() {
        return Ok(Decision::Unchanged(MetaReply::LogChunk(answer)));
    }
    let index = next_index(service)?;
    let outcome = PlacementOutcome {
        version: 16,
        before_index: service.placement_commit_index,
        after_index: index,
        lease_batch: Some(LeaseBatch { holder: String::new(), now_ms, expires_at_ms: 0, renewed: Vec::new(), dropped: over }),
        ..Default::default()
    };
    outcome.to_payload()?;
    Ok(Decision::Change(outcome, MetaReply::LogChunk(answer)))
}

/// The cleanup command for the leases whose term is over, at most one batch: the sweeper calls
/// this on the leader and proposes what it returns. The deadline index stops at
/// the first live lease; only the selected batch is sorted into outcome key order.
pub(super) fn expired(service: &MatrixMetaService, now_ms: u64) -> Vec<super::MetaRequest> {
    let mut drops: Vec<LeaseRef> = service
        .writer_leases
        .expired_before(now_ms)
        .take(SWEEP_BATCH)
        .map(|(bucket, key, epoch)| LeaseRef { bucket: bucket.to_string(), key: key.to_string(), epoch })
        .collect();
    drops.sort_unstable_by(|left, right| (&left.bucket, &left.key).cmp(&(&right.bucket, &right.key)));
    if drops.is_empty() {
        Vec::new()
    } else {
        vec![super::MetaRequest::LogChunk(LogChunkRequest::sweep(drops))]
    }
}

#[cfg(test)]
#[path = "log_chunk_expiry_tests.rs"]
mod log_chunk_expiry_tests;
