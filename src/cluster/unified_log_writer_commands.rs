// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Version 2 writer commands operate exclusively on the unified chunk catalog.
use super::log_chunk_commands::{Decision, LogChunkReply, LogChunkRequest};
use super::{ChunkGroupCatalog, MetaReply};
use crate::chunk_catalog::ChunkOutcome;
use crate::chunk_manifest::{
    ChunkExtent, ChunkManifest, CHUNK_FLAG_FOOTER_SHA256, CHUNK_FLAG_SEALED, CHUNK_FLAG_TAIL,
    CHUNK_KIND_LOG, CHUNK_MANIFEST_FORMAT, MAX_CHUNK_EXTENTS,
};
use crate::chunk_writer_outcome::ChunkWriterChange;
use crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS;
use crate::log_chunk_outcome::{LogChunkChange, WriterLease, ACTION_GRANT, ACTION_SEAL_SWITCH};
use crate::placement_outcome::PlacementOutcome;
use crate::{MatrixMetaService, ObjectError, ObjectId};

fn refused(message: &str) -> ObjectError {
    ObjectError::PreconditionFailed(message.into())
}
fn check_catalog(
    service: &MatrixMetaService,
    request: &LogChunkRequest,
) -> Result<(), ObjectError> {
    if !service.cluster_features.has(FEATURE_UNIFIED_LOG_WRITERS) {
        return Err(refused(
            "unified LOG writers are not enabled for this cluster",
        ));
    }
    // A retained tombstone is also owned by its catalog. Crossing catalogs is a migration,
    // not a grant, and must not restart the object's generation counter.
    if service
        .object_manifests
        .get(&ObjectId::new(&request.bucket, &request.key))
        .is_some()
    {
        return Err(refused("this object uses the earlier manifest catalog"));
    }
    Ok(())
}
fn open(manifest: &ChunkManifest) -> Option<&ChunkExtent> {
    (!manifest.deleted && manifest.kind == CHUNK_KIND_LOG)
        .then(|| {
            manifest
                .extents
                .last()
                .filter(|e| e.flags == CHUNK_FLAG_TAIL)
        })
        .flatten()
}
fn reply(lease: Option<WriterLease>, manifest: Option<&ChunkManifest>) -> MetaReply {
    let live = manifest.filter(|m| !m.deleted);
    MetaReply::LogChunk(LogChunkReply {
        version: 2,
        lease,
        chunk_open: live.and_then(open).cloned(),
        generation: live.map_or(0, |m| m.generation),
        length: live.map_or(0, |m| m.length),
        ..Default::default()
    })
}
pub(super) fn read(
    service: &MatrixMetaService,
    request: &LogChunkRequest,
) -> Result<MetaReply, ObjectError> {
    check_catalog(service, request)?;
    let id = ObjectId::new(&request.bucket, &request.key);
    let manifest = service.chunk_catalog.get(&id).map(|m| m.as_ref());
    if manifest.is_some_and(|m| !m.deleted && m.kind != CHUNK_KIND_LOG) {
        return Err(refused("the object is not a LOG object"));
    }
    Ok(reply(
        service.writer_leases.get(&request.bucket, &request.key),
        manifest,
    ))
}
fn tail(group: &str, index: u64, offset: u64) -> ChunkExtent {
    let (volume_id, chunk_id) = crate::object_manifest::chunk_identity_of_index(index);
    ChunkExtent {
        group: group.into(),
        volume_id,
        chunk_id,
        kind: CHUNK_KIND_LOG,
        object_offset: offset,
        flags: CHUNK_FLAG_TAIL,
        ..Default::default()
    }
}
pub(super) fn prepare(
    service: &MatrixMetaService,
    groups: &ChunkGroupCatalog,
    request: &LogChunkRequest,
    now_ms: u64,
) -> Result<Decision, ObjectError> {
    check_catalog(service, request)?;
    if request.is_read() {
        return Ok(Decision::Unchanged(read(service, request)?));
    }
    if now_ms == 0 {
        return Err(refused("the metadata clock has no reading"));
    }
    if groups.get(&request.group).is_none() {
        return Err(refused("writer command names an unconfigured chunk group"));
    }
    let before_index = service.placement_commit_index;
    let index = before_index
        .checked_add(1)
        .ok_or_else(|| refused("placement index exhausted"))?;
    let id = ObjectId::new(&request.bucket, &request.key);
    let stored = service.chunk_catalog.get(&id).map(|m| (**m).clone());
    let live = stored.as_ref().filter(|m| !m.deleted);
    if live.is_some_and(|m| m.kind != CHUNK_KIND_LOG) {
        return Err(refused("the object is not a LOG object"));
    }
    let held = service.writer_leases.get(&request.bucket, &request.key);
    let expires = now_ms
        .checked_add(request.ttl_ms)
        .ok_or_else(|| refused("lease term overflow"))?;
    let (lease, publication, unchanged) = match request.action {
        ACTION_GRANT => {
            let has_tail = live.and_then(open).is_some();
            if let Some(lease) = &held {
                if lease.holder != request.holder && lease.expires_at_ms > now_ms {
                    return Err(refused("the object is leased to another writer"));
                }
                if lease.holder == request.holder && lease.expires_at_ms > now_ms && has_tail {
                    return Ok(Decision::Unchanged(reply(held, live)));
                }
            }
            let lease = WriterLease {
                holder: request.holder.clone(),
                epoch: index,
                expires_at_ms: expires,
            };
            if has_tail {
                (lease, None, live.cloned())
            } else {
                let mut after = live.cloned().unwrap_or_else(|| ChunkManifest {
                    format_version: CHUNK_MANIFEST_FORMAT,
                    bucket: request.bucket.clone(),
                    key: request.key.clone(),
                    kind: CHUNK_KIND_LOG,
                    ..Default::default()
                });
                if after.extents.len() >= MAX_CHUNK_EXTENTS {
                    return Err(refused("the object holds the most chunks it may"));
                }
                after.generation = stored
                    .as_ref()
                    .map_or(Some(1), |m| m.generation.checked_add(1))
                    .ok_or_else(|| refused("object generation exhausted"))?;
                after
                    .extents
                    .push(tail(&request.group, index, after.length));
                (
                    lease,
                    Some(ChunkOutcome::decide(stored.clone(), after)?),
                    None,
                )
            }
        }
        ACTION_SEAL_SWITCH => {
            let lease = held
                .as_ref()
                .filter(|l| {
                    l.holder == request.holder
                        && l.epoch == request.epoch
                        && l.expires_at_ms > now_ms
                })
                .ok_or_else(|| refused("the writer does not hold the live lease of this object"))?;
            let prior = live
                .filter(|m| open(m).is_some())
                .ok_or_else(|| refused("the object has no LOG tail"))?;
            if prior.extents.len() >= MAX_CHUNK_EXTENTS {
                return Err(refused("the object holds the most chunks it may"));
            }
            let mut after = prior.clone();
            after.generation = prior
                .generation
                .checked_add(1)
                .ok_or_else(|| refused("object generation exhausted"))?;
            let last = after.extents.last_mut().unwrap();
            if request.sealed_length < last.length {
                return Err(refused("a switch cannot shorten its published tail"));
            }
            last.length = request.sealed_length;
            last.flags = CHUNK_FLAG_SEALED
                | if request.footer_sha256.is_empty() {
                    0
                } else {
                    CHUNK_FLAG_FOOTER_SHA256
                };
            last.footer_crc = request.footer_crc;
            last.footer_sha256 = request.footer_sha256.clone();
            after.length = last
                .object_offset
                .checked_add(last.length)
                .ok_or_else(|| refused("object length overflow"))?;
            after
                .extents
                .push(tail(&request.group, index, after.length));
            let renewed = WriterLease {
                expires_at_ms: lease.expires_at_ms.max(expires),
                ..lease.clone()
            };
            (
                renewed,
                Some(ChunkOutcome::decide(stored.clone(), after)?),
                None,
            )
        }
        _ => return Err(refused("unsupported unified writer action")),
    };
    let published = publication
        .as_ref()
        .and_then(|p| p.after.as_ref())
        .or(unchanged.as_ref());
    let answer = reply(Some(lease.clone()), published);
    let change = ChunkWriterChange {
        lease: Some(LogChunkChange {
            action: request.action,
            bucket: request.bucket.clone(),
            key: request.key.clone(),
            lease_before: held,
            lease_after: Some(lease),
            now_ms,
        }),
        publication,
        unchanged,
    };
    change.check(
        service,
        before_index,
        index,
        Some(crate::object_manifest::admission_cap()),
    )?;
    let outcome = PlacementOutcome {
        version: 17,
        before_index,
        after_index: index,
        chunk_writer: Some(change),
        ..Default::default()
    };
    outcome.to_payload()?;
    Ok(Decision::Change(outcome, answer))
}
