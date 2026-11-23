// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The metadata-plane request that publishes one chunk manifest.
//!
//! The client decides an outcome from the manifest it read (the generation it read and the manifest
//! it wants, one generation later) and sends it. The leader checks the outcome against its own
//! state, adding the one limit only a leader applies (how many objects the group may name), and
//! proposes it as an entry of its own. Every voter checks the same outcome against its own state
//! when it applies the entry, with the same rules, so a failing apply is a defect and never a
//! normal answer: whatever [`prepare`] accepts, [`apply`] accepts on every voter that applied the
//! same prefix.

#[cfg(feature = "metadata-consensus")]
use std::sync::Arc;

use crate::chunk_catalog::ChunkOutcome;
use crate::chunk_manifest::ChunkManifest;
#[cfg(feature = "metadata-consensus")]
use crate::ObjectId;
use crate::{MatrixMetaService, ObjectError};

/// What the leader decides for one publication.
pub(super) enum Decision {
    /// The catalog changes: the outcome is proposed, and the reply is the manifest it publishes.
    Change(ChunkOutcome, ChunkManifest),
    /// The catalog already holds this manifest, so this is a replay: nothing is proposed.
    Unchanged(ChunkManifest),
}

/// Decide one publication against `service`.
///
/// This runs every check [`apply`] runs (the generation it was decided from, the manifest's own
/// validity, the object and image bounds) and, on top of them, the leader's admission limit on new
/// objects, which a voter never applies to a committed entry.
pub(super) fn prepare(service: &MatrixMetaService, outcome: &ChunkOutcome) -> Result<Decision, ObjectError> {
    reject_initialized(outcome)?;
    let after = outcome.after()?.clone();
    if service.chunk_catalog.check(outcome, Some(crate::object_manifest::admission_cap()))? {
        Ok(Decision::Change(outcome.clone(), after))
    } else {
        Ok(Decision::Unchanged(after))
    }
}

/// Apply a committed outcome. Returns whether the catalog changed.
pub(super) fn apply(service: &mut MatrixMetaService, outcome: &ChunkOutcome) -> Result<bool, ObjectError> {
    reject_initialized(outcome)?;
    let changed = service.chunk_catalog.apply(outcome)?;
    if changed {
        service.invalidate_snapshot_export_cache();
    }
    Ok(changed)
}

// Initialized objects need the staged publication and read-pin protocol. Older publication
// commands cannot create them, mutate them or drop their storage identities.
fn reject_initialized(outcome: &ChunkOutcome) -> Result<(), ObjectError> {
    if outcome.before.iter().chain(outcome.after.iter()).any(|m| m.initialized_log.is_some()) {
        return Err(ObjectError::PreconditionFailed("initialized LOG publication requires native staging".into()));
    }
    Ok(())
}

/// A leader applies each outcome of a batch to the live catalog so the next is prepared against
/// it, and puts the catalog back, object by object and in the reverse order, before anything is
/// committed. Dropping the draft is what restores it, so no early return can leave it applied.
#[cfg(feature = "metadata-consensus")]
pub(super) struct ChunkDraft<'a> {
    service: &'a mut MatrixMetaService,
    undo: Vec<(ObjectId, Option<Arc<ChunkManifest>>, u64)>,
}

#[cfg(feature = "metadata-consensus")]
impl<'a> ChunkDraft<'a> {
    pub(super) fn new(service: &'a mut MatrixMetaService) -> Self {
        Self { service, undo: Vec::new() }
    }

    pub(super) fn service(&self) -> &MatrixMetaService {
        self.service
    }

    /// Apply `outcome` to the draft. An outcome that fails changes nothing.
    pub(super) fn apply(&mut self, outcome: &ChunkOutcome) -> Result<bool, ObjectError> {
        let id = outcome.object_id()?;
        let entry = self.service.chunk_catalog.entry(&id);
        let revision = self.service.chunk_catalog.revision();
        let changed = apply(self.service, outcome)?;
        if changed {
            self.undo.push((id, entry, revision));
        }
        Ok(changed)
    }
}

#[cfg(feature = "metadata-consensus")]
impl Drop for ChunkDraft<'_> {
    fn drop(&mut self) {
        if self.undo.is_empty() {
            return;
        }
        while let Some((id, entry, revision)) = self.undo.pop() {
            self.service.chunk_catalog.restore_entry(&id, entry, revision);
        }
        self.service.invalidate_snapshot_export_cache();
    }
}

#[cfg(test)]
#[path = "initialized_manifest_admission_tests.rs"]
mod initialized_admission_tests;
