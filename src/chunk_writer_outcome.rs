// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Atomic writer-lease and LOG chunk-catalog changes carried by placement outcome version 17.
//! The expected catalog entry is bound even when taking over an already-open tail.
use crate::chunk_catalog::ChunkOutcome;
use crate::chunk_manifest::{
    ChunkAttributes, ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_FLAG_TAIL, CHUNK_KIND_LOG,
};
use crate::log_chunk_outcome::{LogChunkChange, ACTION_GRANT, ACTION_SEAL_SWITCH};
use crate::{MatrixMetaService, ObjectError, ObjectId};
use prost::Message;

fn invalid(message: &str) -> ObjectError {
    ObjectError::Corruption(message.into())
}
fn open(manifest: &ChunkManifest) -> bool {
    !manifest.deleted
        && manifest.kind == CHUNK_KIND_LOG
        && manifest
            .extents
            .last()
            .is_some_and(|e| e.flags == CHUNK_FLAG_TAIL)
}
fn same_chunk(a: &ChunkExtent, b: &ChunkExtent) -> bool {
    a.group == b.group && a.volume_id == b.volume_id && a.chunk_id == b.chunk_id
}
/// Exactly one lease transition and either a publication or an unchanged expected manifest.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct ChunkWriterChange {
    #[prost(message, optional, tag = "1")]
    pub lease: Option<LogChunkChange>,
    /// Present only for a grant to an existing open tail; captures its complete prior state.
    #[prost(message, optional, tag = "2")]
    pub unchanged: Option<ChunkManifest>,
    #[prost(message, optional, tag = "3")]
    pub publication: Option<ChunkOutcome>,
}
impl ChunkWriterChange {
    pub fn lease(&self) -> Result<&LogChunkChange, ObjectError> {
        self.lease
            .as_ref()
            .ok_or_else(|| invalid("chunk writer change lacks its lease transition"))
    }
    pub fn prior(&self) -> Option<&ChunkManifest> {
        match &self.publication {
            Some(p) => p.before.as_ref(),
            None => self.unchanged.as_ref(),
        }
    }
    pub fn validate(&self, before_index: u64, after_index: u64) -> Result<(), ObjectError> {
        let change = self.lease()?;
        change.validate(before_index, after_index)?;
        match &self.publication {
            None => {
                let prior = self
                    .unchanged
                    .as_ref()
                    .ok_or_else(|| invalid("lease-only grant lacks its expected chunk manifest"))?;
                prior.validate()?;
                if change.action != ACTION_GRANT
                    || !open(prior)
                    || prior.bucket != change.bucket
                    || prior.key != change.key
                {
                    return Err(invalid("lease-only grant needs the named live LOG tail"));
                }
            }
            Some(publication) => {
                if self.unchanged.is_some() {
                    return Err(invalid("chunk writer change repeats its expected manifest"));
                }
                publication.validate()?;
                let after = publication.after()?;
                if after.bucket != change.bucket
                    || after.key != change.key
                    || after.deleted
                    || after.kind != CHUNK_KIND_LOG
                    || after.page_bytes != 0
                {
                    return Err(invalid(
                        "chunk writer publication must name its leased LOG object",
                    ));
                }
                let prior = publication.before.as_ref().filter(|p| !p.deleted);
                if let Some(prior) = prior {
                    if prior.kind != CHUNK_KIND_LOG
                        || ChunkAttributes::of(prior) != ChunkAttributes::of(after)
                    {
                        return Err(invalid(
                            "chunk writer change preserves object kind and attributes",
                        ));
                    }
                }
                let kept = prior.map_or(&[][..], |p| p.extents.as_slice());
                match change.action {
                    ACTION_GRANT => {
                        if prior.is_some_and(open)
                            || after.extents.len() != kept.len() + 1
                            || !after.extents.starts_with(kept)
                            || after.length != prior.map_or(0, |p| p.length)
                        {
                            return Err(invalid("chunk writer grant adds one empty tail only"));
                        }
                        Self::check_tail(
                            &after.extents[kept.len()],
                            after_index,
                            after.length,
                            kept,
                        )?;
                    }
                    ACTION_SEAL_SWITCH => {
                        let prior = prior
                            .filter(|p| open(p))
                            .ok_or_else(|| invalid("chunk writer switch needs its open tail"))?;
                        let n = prior.extents.len();
                        let old = &prior.extents[n - 1];
                        if after.extents.len() != n + 1
                            || after.extents[..n - 1] != prior.extents[..n - 1]
                        {
                            return Err(invalid(
                                "chunk writer switch changes only the last extent",
                            ));
                        }
                        let sealed = &after.extents[n - 1];
                        if !same_chunk(old, sealed)
                            || sealed.kind != CHUNK_KIND_LOG
                            || sealed.chunk_offset != old.chunk_offset
                            || sealed.object_offset != old.object_offset
                            || sealed.flags & CHUNK_FLAG_SEALED == 0
                            || sealed.flags & CHUNK_FLAG_TAIL != 0
                            || sealed.length < old.length
                        {
                            return Err(invalid("chunk writer switch must seal the prior tail without shortening it"));
                        }
                        let end = old
                            .object_offset
                            .checked_add(sealed.length)
                            .ok_or_else(|| invalid("chunk writer length overflow"))?;
                        if after.length != end {
                            return Err(invalid(
                                "chunk writer switch length differs from sealed end",
                            ));
                        }
                        Self::check_tail(&after.extents[n], after_index, end, &prior.extents)?;
                    }
                    _ => return Err(invalid("unknown chunk writer action")),
                }
            }
        }
        Ok(())
    }
    fn check_tail(
        tail: &ChunkExtent,
        index: u64,
        offset: u64,
        others: &[ChunkExtent],
    ) -> Result<(), ObjectError> {
        let identity = crate::object_manifest::chunk_identity_of_index(index);
        if tail.kind != CHUNK_KIND_LOG
            || tail.flags != CHUNK_FLAG_TAIL
            || tail.length != 0
            || tail.chunk_offset != 0
            || tail.object_offset != offset
            || (tail.volume_id, tail.chunk_id) != identity
            || tail.footer_crc != 0
            || !tail.footer_sha256.is_empty()
            || !tail.digest.is_empty()
            || others.iter().any(|old| same_chunk(old, tail))
        {
            return Err(invalid(
                "chunk writer tail must be empty and owned by its outcome index",
            ));
        }
        Ok(())
    }
    /// Check the complete replicated prefix before changing either table.
    pub(crate) fn check(
        &self,
        service: &MatrixMetaService,
        before: u64,
        after: u64,
        admission: Option<usize>,
    ) -> Result<(), ObjectError> {
        self.validate(before, after)?;
        if service.placement_commit_index != before {
            return Err(invalid("chunk writer outcome index mismatch"));
        }
        if !service
            .cluster_features
            .has(crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS)
        {
            return Err(invalid(
                "chunk writer outcome precedes its replicated reader activation",
            ));
        }
        let change = self.lease()?;
        change.check_leases(&service.writer_leases)?;
        let id = ObjectId::new(change.bucket.clone(), change.key.clone());
        if service.chunk_catalog.get(&id).map(|p| p.as_ref()) != self.prior() {
            return Err(invalid("chunk writer prior catalog state mismatch"));
        }
        if let Some(publication) = &self.publication {
            if !service.chunk_catalog.check(publication, admission)? {
                return Err(invalid(
                    "chunk writer outcome would replay only its manifest half",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn apply(
        &self,
        service: &mut MatrixMetaService,
        before: u64,
        after: u64,
    ) -> Result<(), ObjectError> {
        self.check(service, before, after, None)?;
        // Catalog apply has no fallible step after its state checks; leases are changed only after it succeeds.
        if let Some(publication) = &self.publication {
            service.chunk_catalog.apply(publication)?;
        }
        self.lease()?.apply_leases(&mut service.writer_leases);
        service.placement_commit_index = after;
        service.invalidate_snapshot_export_cache();
        Ok(())
    }
}
