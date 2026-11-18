// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Checkpoint-worker state is independent of the live service's copy-on-write
//! maps. Historical mutations fold as metadata; only the final pages hydrate.
use super::{replica_checkpoint_catalog::LoadedCheckpoint, replica_checkpoint_spans::PageCatalog};
use crate::durable_payload::{
    outcome_log::{StoredOutcomePosition, StoredRecordMetadata},
    outcome_segments::SegmentedOutcomeLog,
};
use crate::{MatrixReplicaService, ReplicaServiceSnapshot};
use std::{collections::BTreeSet, io, sync::Arc};
fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
/// Copy only map handles. A persistent empty template avoids allocating seven
/// empty receipt maps for every foreground mutation.
fn copy_maps(destination: &mut ReplicaServiceSnapshot, source: &ReplicaServiceSnapshot) {
    destination.namespace_fences = source.namespace_fences.clone();
    destination.write_request_receipts = source.write_request_receipts.clone();
    destination.discard_request_receipts = source.discard_request_receipts.clone();
    destination.batch_write_request_receipts = source.batch_write_request_receipts.clone();
    destination.batch_discard_request_receipts = source.batch_discard_request_receipts.clone();
    destination.state_install_request_receipts = source.state_install_request_receipts.clone();
    destination.reclaim_request_receipts = source.reclaim_request_receipts.clone();
    destination.repair_request_receipts = source.repair_request_receipts.clone();
}
fn has_maps(snapshot: &ReplicaServiceSnapshot) -> bool {
    !snapshot.namespace_fences.is_empty()
        || !snapshot.write_request_receipts.is_empty()
        || !snapshot.discard_request_receipts.is_empty()
        || !snapshot.batch_write_request_receipts.is_empty()
        || !snapshot.batch_discard_request_receipts.is_empty()
        || !snapshot.state_install_request_receipts.is_empty()
        || !snapshot.reclaim_request_receipts.is_empty()
        || !snapshot.repair_request_receipts.is_empty()
}
pub(super) fn scalar_template() -> ReplicaServiceSnapshot {
    MatrixReplicaService::new().export_snapshot_unsealed_without_pages()
}
/// No payload-map walk, payload pin, or retained live catalog/receipt handle.
pub(super) fn capture_scalars(
    service: &MatrixReplicaService,
    empty: &ReplicaServiceSnapshot,
) -> ReplicaServiceSnapshot {
    debug_assert!(empty.pages.is_empty() && !has_maps(empty));
    let mut snapshot = service.export_snapshot_unsealed_without_pages();
    copy_maps(&mut snapshot, empty);
    snapshot
}
pub(super) fn encode_scalars(mut snapshot: ReplicaServiceSnapshot) -> io::Result<Vec<u8>> {
    if !snapshot.pages.is_empty() || has_maps(&snapshot) {
        return Err(invalid("replica scalar context contains maps"));
    }
    snapshot.seal_checksum();
    snapshot.to_bytes().map_err(invalid)
}
pub(super) struct CheckpointState {
    pub catalog: PageCatalog,
    pub metadata: ReplicaServiceSnapshot,
    pub cut: Option<StoredOutcomePosition>,
    pub index: Option<super::replica_checkpoint_index::IndexState>,
    pub dirty_pages: BTreeSet<crate::PageId>,
    pub publication: Option<(
        super::replica_checkpoint_catalog::CheckpointCommit,
        StoredOutcomePosition,
    )>,
    pub next_collection_segments: usize,
    pub next_collection_attempt: std::time::Instant,
}
impl CheckpointState {
    pub fn new(metadata: ReplicaServiceSnapshot) -> io::Result<Self> {
        if !metadata.pages.is_empty() {
            return Err(invalid("checkpoint metadata contains payloads"));
        }
        // Decode, instead of cloning, so a live service can never share these maps.
        let mut metadata = metadata;
        metadata.seal_checksum();
        let metadata = ReplicaServiceSnapshot::from_bytes(&metadata.to_bytes().map_err(invalid)?)
            .map_err(invalid)?;
        Ok(Self {
            catalog: PageCatalog::default(),
            metadata,
            cut: None,
            index: None,
            dirty_pages: BTreeSet::new(),
            publication: None,
            next_collection_segments: 16,
            next_collection_attempt: std::time::Instant::now(),
        })
    }
    pub fn from_checkpoint(loaded: LoadedCheckpoint) -> io::Result<Self> {
        let metadata =
            ReplicaServiceSnapshot::from_bytes(&loaded.header.metadata).map_err(invalid)?;
        if !metadata.pages.is_empty() {
            return Err(invalid("checkpoint metadata contains payloads"));
        }
        let publication = loaded
            .publication_position
            .clone()
            .map(|position| (loaded.commit.clone(), position));
        Ok(Self {
            catalog: loaded.catalog,
            metadata,
            cut: loaded.commit.through,
            index: loaded.index,
            dirty_pages: BTreeSet::new(),
            publication,
            next_collection_segments: 16,
            next_collection_attempt: std::time::Instant::now(),
        })
    }
    pub fn fold_record(
        &mut self,
        record: &StoredRecordMetadata,
        max_pages: usize,
    ) -> io::Result<()> {
        if record.batch.version == 2 {
            self.cut = Some(record.position.clone());
            return Ok(());
        }
        if record.batch.version != 3 {
            return Err(invalid("replica outcome lacks committed scalar context"));
        }
        let mut scalars =
            ReplicaServiceSnapshot::from_bytes(&record.batch.replica_metadata).map_err(invalid)?;
        if !scalars.pages.is_empty() || has_maps(&scalars) {
            return Err(invalid("replica scalar context contains maps"));
        }
        let mut old_extents = BTreeSet::new();
        for decision in &record.batch.decisions {
            if let Some(page) = &decision.page {
                let id = page.page_id().map_err(invalid)?;
                if self.index.is_some() {
                    if let Some(old) = self.catalog.pages.get(&id) {
                        for span in &old.spans {
                            old_extents.insert(super::replica_checkpoint_catalog::extent_key(
                                &span.extent,
                            )?);
                        }
                    }
                }
                self.dirty_pages.insert(id);
            }
        }
        self.catalog.fold(record)?;
        if let Some(index) = &mut self.index {
            index.prune(&old_extents);
        }
        if self.catalog.pages.len() > max_pages {
            return Err(invalid("replica page count exceeds configured limit"));
        }
        for decision in &record.batch.decisions {
            if let Some(fence) = &decision.namespace_fence {
                fence
                    .apply_to_catalog(&mut self.metadata.namespace_fences)
                    .map_err(invalid)?;
            }
        }
        copy_maps(&mut scalars, &self.metadata);
        // The old handles are dropped immediately, leaving worker maps exclusive.
        self.metadata = scalars;
        self.cut = Some(record.position.clone());
        Ok(())
    }
    pub fn fold_through(
        &mut self,
        log: &SegmentedOutcomeLog,
        through: &StoredOutcomePosition,
        max_pages: usize,
    ) -> io::Result<()> {
        let mut cursor = log.metadata_cursor_after_through(self.cut.as_ref(), through)?;
        while let Some(record) = cursor.next_record()? {
            self.fold_record(&record, max_pages)?;
        }
        Ok(())
    }
    pub fn fold_available(
        &mut self,
        log: &SegmentedOutcomeLog,
        max_pages: usize,
    ) -> io::Result<u64> {
        let mut cursor = log.metadata_cursor_after(self.cut.as_ref())?;
        let mut bytes = 0u64;
        while let Some(record) = cursor.next_record()? {
            self.fold_record(&record, max_pages)?;
            if record.batch.version != 2 {
                bytes = bytes
                    .checked_add(record.position.length)
                    .ok_or_else(|| invalid("checkpoint accounting overflow"))?;
            }
        }
        Ok(bytes)
    }
    /// Non-journaled service counters/modes are captured at checkpoint start.
    /// Keep the independently replayed maps, rather than pinning live maps.
    pub fn set_scalars(&mut self, mut scalars: ReplicaServiceSnapshot) -> io::Result<()> {
        if !scalars.pages.is_empty() || has_maps(&scalars) {
            return Err(invalid("replica scalar context contains maps"));
        }
        copy_maps(&mut scalars, &self.metadata);
        self.metadata = scalars;
        Ok(())
    }
    pub fn metadata_bytes(&self) -> io::Result<Vec<u8>> {
        let mut metadata = self.metadata.clone();
        metadata.seal_checksum();
        metadata.to_bytes().map_err(invalid)
    }
    /// Startup work: materialize each surviving page once, independent of the
    /// number or payload size of its obsolete historical mutations.
    pub fn restore(
        &self,
        log: &SegmentedOutcomeLog,
        service: &mut MatrixReplicaService,
    ) -> io::Result<()> {
        let mut snapshot =
            ReplicaServiceSnapshot::from_bytes(&self.metadata_bytes()?).map_err(invalid)?;
        for (id, page) in &self.catalog.pages {
            let state = page.materialize(|extent, offset, length| {
                log.read_payload_range(extent, offset, length)
            })?;
            snapshot.pages.insert(id.clone(), Arc::new(state));
        }
        snapshot.seal_for_checkpoint();
        service.install_snapshot(snapshot).map_err(invalid)
    }
}
#[cfg(all(test, unix))]
#[path = "replica_checkpoint_recovery_tests.rs"]
mod tests;
