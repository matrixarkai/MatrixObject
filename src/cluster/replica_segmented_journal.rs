// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Opt-in immutable replica journal. Foreground groups sync once; checkpoint
//! payloads stay immutable and catalog work runs outside the replica mutex.
use super::{
    replica_checkpoint_catalog::{self},
    replica_checkpoint_recovery::{self, CheckpointState},
    replica_checkpoint_spans::{CatalogPage, CatalogPageMetadata, PageCatalog, PayloadSpan},
    replica_journal_backend::ReplicaJournalBackend,
    replica_page_backing::LivePageBacking,
};
use crate::durable_payload::{
    outcome_log::{CheckpointFragment, DecidedMutation, StoredOutcomePosition},
    outcome_segments::SegmentedOutcomeLog,
    MAX_BATCH_BYTES, MAX_BATCH_EXTENTS, MAX_EXTENT_BYTES,
};
use crate::{
    chunk_namespace_fence::NamespaceFenceOutcome, outcome::PageOutcome, MatrixReplicaService,
    ReplicaServiceSnapshot,
};
use prost::Message;
use std::{
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
fn cancelled() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "checkpoint epoch was superseded",
    )
}
enum Pending {
    Page(PageOutcome),
    Namespace(NamespaceFenceOutcome),
}
impl Pending {
    fn decided(&self) -> DecidedMutation<'_> {
        match self {
            Self::Page(p) => DecidedMutation::Page(p),
            Self::Namespace(n) => DecidedMutation::NamespaceFence(n),
        }
    }
    fn length(&self) -> usize {
        12 + match self {
            Self::Page(p) => p.encoded_len(),
            Self::Namespace(n) => n.encoded_len(),
        }
    }
}
struct Completed {
    epoch: u64,
    published: bool,
    state: CheckpointState,
    covered_bytes: u64,
}
#[derive(Clone)]
struct PublishedBoundary {
    position: StoredOutcomePosition,
    revision: Option<u64>,
}
#[derive(Clone, Copy)]
enum CheckpointMode {
    Checkpoint,
    Manual,
    Maintenance { collection_due: bool },
}
type Job = JoinHandle<io::Result<Option<Completed>>>;
pub(super) struct SegmentedReplicaJournal {
    log: Arc<SegmentedOutcomeLog>,
    backing: Option<Arc<LivePageBacking>>,
    metadata_only: bool,
    log_authority: super::log_page_catalog::LogPageCatalog,
    empty: ReplicaServiceSnapshot,
    state: Option<CheckpointState>,
    cut: Option<StoredOutcomePosition>,
    pending: Vec<Pending>,
    pending_bytes: usize,
    context: Option<ReplicaServiceSnapshot>,
    revision: u64,
    started_epoch: Arc<AtomicU64>,
    epoch_sequence: u64,
    queued_checkpoint: Option<u64>,
    published_epoch: u64,
    bytes_since_checkpoint: u64,
    publication: Arc<Mutex<u64>>,
    published_boundary: Arc<Mutex<Option<PublishedBoundary>>>,
    stopping: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
    job: Option<Job>,
    reapers: Vec<JoinHandle<()>>,
    max_pages: usize,
    retention_threshold: usize,
    cleanup: Option<JoinHandle<io::Result<()>>>,
    next_cleanup: Instant,
    #[cfg(test)]
    syncs: u64,
}
impl SegmentedReplicaJournal {
    #[cfg(all(test, unix))]
    pub fn open(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> io::Result<Self> {
        Self::open_with_settings(
            path,
            service,
            max_pages,
            crate::durable_payload::DEFAULT_CAPACITY_BYTES,
            16,
        )
    }
    pub fn open_with_settings(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
        capacity: u64,
        retention_threshold: usize,
    ) -> io::Result<Self> {
        Self::open_with_recovery(
            path,
            service,
            max_pages,
            capacity,
            retention_threshold,
            false,
        )
    }
    pub fn open_with_page_backing(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
        capacity: u64,
        retention_threshold: usize,
    ) -> io::Result<Self> {
        Self::open_with_recovery(
            path,
            service,
            max_pages,
            capacity,
            retention_threshold,
            true,
        )
    }
    fn open_with_recovery(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
        capacity: u64,
        retention_threshold: usize,
        metadata_only: bool,
    ) -> io::Result<Self> {
        if !(2..=1_000_000).contains(&retention_threshold) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "retention threshold must be 2..1000000 segments",
            ));
        }
        if max_pages == 0 {
            return Err(invalid("zero replica page limit"));
        }
        let log = Arc::new(SegmentedOutcomeLog::open(path, capacity)?);
        let loaded = replica_checkpoint_catalog::latest(&log, max_pages)?;
        let (mut state, epoch, bytes_since_checkpoint) = match loaded {
            Some(loaded) => {
                let epoch = loaded.header.epoch;
                let mut state = CheckpointState::from_checkpoint(loaded)?;
                let bytes = state.fold_available(&log, max_pages)?;
                if metadata_only {
                    state.restore_metadata(service)?;
                } else {
                    state.restore(&log, service)?;
                }
                (Some(state), epoch, bytes)
            }
            None => {
                let mut cursor = log.metadata_cursor_after(None)?;
                while let Some(record) = cursor.next_record()? {
                    if record.batch.version != 2 {
                        return Err(invalid(
                            "replica outcomes have no published bootstrap checkpoint",
                        ));
                    }
                }
                (None, 0, 0)
            }
        };
        if let Some(state) = &mut state {
            state.next_collection_segments = retention_threshold;
        }
        let cut = state.as_ref().and_then(|s| s.cut.clone());
        let boundary = state
            .as_ref()
            .and_then(|state| state.publication.as_ref())
            .map(|(_, position)| PublishedBoundary {
                position: position.clone(),
                revision: (bytes_since_checkpoint == 0).then_some(0),
            });
        let log_authority = state
            .as_ref()
            .map(|state| state.catalog.logs.clone())
            .unwrap_or_default();
        let mut journal = Self {
            log,
            backing: None,
            metadata_only,
            log_authority,
            empty: replica_checkpoint_recovery::scalar_template(),
            state,
            cut,
            pending: Vec::with_capacity(MAX_BATCH_EXTENTS),
            pending_bytes: 0,
            context: None,
            revision: 0,
            started_epoch: Arc::new(AtomicU64::new(epoch)),
            epoch_sequence: epoch,
            queued_checkpoint: None,
            published_epoch: epoch,
            bytes_since_checkpoint,
            publication: Arc::new(Mutex::new(epoch)),
            published_boundary: Arc::new(Mutex::new(boundary)),
            stopping: Arc::new(AtomicBool::new(false)),
            failure: Arc::new(Mutex::new(None)),
            job: None,
            reapers: Vec::new(),
            max_pages,
            retention_threshold,
            cleanup: None,
            next_cleanup: Instant::now(),
            #[cfg(test)]
            syncs: 0,
        };
        if epoch == 0 {
            journal.install(service)?;
        }
        if metadata_only {
            journal.enable_page_backing()?;
        }
        Ok(journal)
    }
    fn health(&mut self) -> io::Result<()> {
        self.log.check_health()?;
        if let Some(error) = self
            .failure
            .lock()
            .map_err(|_| invalid("journal failure state is poisoned"))?
            .as_ref()
        {
            return Err(invalid(error));
        }
        if self.job.as_ref().is_some_and(|job| job.is_finished()) {
            match self
                .job
                .take()
                .unwrap()
                .join()
                .map_err(|_| invalid("checkpoint worker panicked"))??
            {
                Some(done) => {
                    if done.published {
                        self.published_epoch = done.epoch;
                        // Without a newer durable mutation, the captured physical
                        // boundary supersedes the previous foreground position,
                        // whose segment may now have been retired.
                        if self.bytes_since_checkpoint == done.covered_bytes {
                            self.cut = done.state.cut.clone();
                        }
                    }
                    self.state = Some(done.state);
                    self.bytes_since_checkpoint = self
                        .bytes_since_checkpoint
                        .saturating_sub(done.covered_bytes);
                }
                None => return Err(invalid("current checkpoint worker was superseded")),
            }
        }
        let mut at = 0;
        while at < self.reapers.len() {
            if self.reapers[at].is_finished() {
                self.reapers
                    .swap_remove(at)
                    .join()
                    .map_err(|_| invalid("checkpoint reaper panicked"))?;
            } else {
                at += 1;
            }
        }
        if self.cleanup.as_ref().is_some_and(|job| job.is_finished()) {
            self.cleanup
                .take()
                .unwrap()
                .join()
                .map_err(|_| invalid("retirement worker panicked"))??;
        }
        let now = Instant::now();
        if self.cleanup.is_none() && now >= self.next_cleanup {
            self.next_cleanup = now + Duration::from_secs(1);
            if self.log.retired_segment_count()? != 0 || self.log.retirement_snapshot_needed()? {
                let log = self.log.clone();
                let failure = self.failure.clone();
                self.cleanup = Some(
                    thread::Builder::new()
                        .name("replica-retirement".into())
                        .spawn(move || {
                            let result = retention::finish_retirements(&log);
                            if let Err(error) = &result {
                                if let Ok(mut failed) = failure.lock() {
                                    *failed = Some(error.to_string());
                                }
                            }
                            result
                        })?,
                );
            }
        }
        Ok(())
    }
    fn page_namespace_available(&self, chunk: &crate::ChunkId) -> Result<(), crate::ObjectError> {
        if self.log_authority.chunks.contains_key(chunk) {
            return Err(crate::ObjectError::PreconditionFailed(
                "page-backed log chunks require log-aware operations".into(),
            ));
        }
        Ok(())
    }
    fn enqueue(
        &mut self,
        pending: Pending,
        context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<()> {
        self.health()?;
        match &pending {
            Pending::Page(page) => self
                .page_namespace_available(&page.page_id().map_err(invalid)?.chunk_id)
                .map_err(invalid)?,
            Pending::Namespace(fence) => {
                if let Some(after) = &fence.after {
                    self.page_namespace_available(
                        &crate::ChunkId::new(after.volume_id, after.chunk_id).map_err(invalid)?,
                    )
                    .map_err(invalid)?;
                }
            }
        }
        let context = context.ok_or_else(|| invalid("replica outcome has no scalar context"))?;
        let length = pending.length();
        if length > MAX_BATCH_BYTES {
            return Err(invalid("replica outcome exceeds group byte limit"));
        }
        if self.pending.len() == MAX_BATCH_EXTENTS || self.pending_bytes + length > MAX_BATCH_BYTES
        {
            self.flush()?;
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("replica mutation revision overflow"))?;
        self.pending_bytes += length;
        self.pending.push(pending);
        self.context = Some(context);
        if sync {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.health()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let context = self
            .context
            .take()
            .ok_or_else(|| invalid("replica group has no scalar context"))?;
        let metadata = replica_checkpoint_recovery::encode_scalars(context)?;
        let mutations: Vec<_> = self.pending.iter().map(Pending::decided).collect();
        let record = self
            .log
            .append_decided_batch_with_metadata(&mutations, &metadata)?;
        if let Some(backing) = &self.backing {
            if let Err(error) = backing.fold(&record) {
                *self
                    .failure
                    .lock()
                    .map_err(|_| invalid("journal failure state is poisoned"))? =
                    Some(error.to_string());
                return Err(error);
            }
        }
        self.bytes_since_checkpoint = self
            .bytes_since_checkpoint
            .checked_add(record.position.length)
            .ok_or_else(|| invalid("checkpoint accounting overflow"))?;
        self.cut = Some(record.position);
        self.pending.clear();
        self.pending_bytes = 0;
        #[cfg(test)]
        {
            self.syncs += 1;
        }
        Ok(())
    }
    fn retire_job(&mut self) {
        if let Some(job) = self.job.take() {
            // Any old catalog is destroyed here, outside the replica mutex.
            self.reapers.push(thread::spawn(move || {
                let _ = job.join();
            }));
        }
        if let Some(state) = self.state.take() {
            self.reapers.push(thread::spawn(move || drop(state)));
        }
    }
    fn reserve_epoch(&mut self) -> io::Result<u64> {
        self.epoch_sequence = self
            .epoch_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint epoch overflow"))?;
        Ok(self.epoch_sequence)
    }
    fn collection_due(&self, now: Instant) -> bool {
        self.state.as_ref().is_some_and(|state| {
            now >= state.next_collection_attempt
                && self
                    .log
                    .retained_segment_count()
                    .is_ok_and(|count| count >= state.next_collection_segments)
        })
    }
    fn compaction_due(&self, now: Instant) -> bool {
        self.state.as_ref().is_some_and(|state| {
            now >= state.next_compaction_attempt
                && self
                    .log
                    .retained_segment_count()
                    .is_ok_and(|count| count >= self.retention_threshold)
        })
    }
    fn install(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.install_catalog(service, None)
    }
    fn install_catalog(
        &mut self,
        service: &MatrixReplicaService,
        page: Option<(&crate::PageId, &crate::PageState)>,
    ) -> io::Result<()> {
        if let Some((page, _)) = page {
            self.page_namespace_available(&page.chunk_id)
                .map_err(invalid)?;
        } else if !self.log_authority.chunks.is_empty() {
            return Err(invalid(
                "whole replica installation cannot replace log authority",
            ));
        }
        self.flush()?;
        let epoch = self.reserve_epoch()?;
        let token = {
            let mut epoch = self
                .publication
                .lock()
                .map_err(|_| invalid("checkpoint publication is poisoned"))?;
            *epoch = epoch
                .checked_add(1)
                .ok_or_else(|| invalid("checkpoint epoch overflow"))?;
            *epoch
        };
        self.started_epoch.fetch_max(epoch, Ordering::Release);
        self.queued_checkpoint = None;
        let boundary = self
            .published_boundary
            .lock()
            .map_err(|_| invalid("published checkpoint boundary is poisoned"))?
            .clone();
        let captured_cut = match boundary.as_ref() {
            Some(boundary) if boundary.revision == Some(self.revision) => {
                self.log.checkpoint_cut(None, Some(&boundary.position))?
            }
            _ => self.log.checkpoint_cut(
                self.cut.as_ref(),
                boundary.as_ref().map(|boundary| &boundary.position),
            )?,
        };
        // Invalidate previous publishers before capturing the canonical catalog.
        // Keep its sources reachable until the replacement root is durable.
        let pinned_log = self.log.clone();
        let _pin = pinned_log.checkpoint_read_pin()?;
        let catalog = if page.is_some() {
            Some(if let Some(backing) = &self.backing {
                backing.catalog_snapshot()?
            } else {
                let loaded = replica_checkpoint_catalog::latest(&self.log, self.max_pages)?
                    .ok_or_else(|| invalid("page installation has no bootstrap checkpoint"))?;
                let mut recovered = CheckpointState::from_checkpoint(loaded)?;
                if let Some(cut) = &captured_cut {
                    recovered.fold_through(&self.log, cut, self.max_pages, &|| Ok(()))?;
                }
                recovered.catalog
            })
        } else {
            None
        };
        self.retire_job();
        let mut state = CheckpointState::new(service.export_snapshot_unsealed_without_pages())?;
        state.next_collection_segments = self.retention_threshold;
        if let Some(catalog) = catalog {
            state.catalog = catalog;
        }
        let snapshot;
        let pages: Vec<(&crate::PageId, &crate::PageState)> = if let Some((id, value)) = page {
            let old = state
                .catalog
                .pages
                .get(id)
                .map_or(0, |p| p.metadata.data_length);
            let total = state.catalog.pages.values().try_fold(0u64, |sum, p| {
                sum.checked_add(p.metadata.data_length)
                    .ok_or_else(|| invalid("replica byte count overflow"))
            })?;
            let total = total
                .checked_sub(old)
                .and_then(|n| n.checked_add(value.data.len() as u64))
                .ok_or_else(|| invalid("replica byte count overflow"))?;
            if (service.capacity_bytes() != 0 && total > service.capacity_bytes())
                || (!state.catalog.pages.contains_key(id)
                    && state.catalog.pages.len() >= self.max_pages)
            {
                return Err(invalid("page installation exceeds replica capacity"));
            }
            vec![(id, value)]
        } else {
            snapshot = service.export_snapshot_unsealed();
            if snapshot.pages.len() > self.max_pages {
                return Err(invalid("replica page count exceeds configured limit"));
            }
            snapshot
                .pages
                .iter()
                .map(|(id, state)| (id, state.as_ref()))
                .collect()
        };
        let mut payload_ordinal = 0;
        for (id, page) in pages {
            let metadata = CatalogPageMetadata::from_state(id, page)?;
            let mut spans = Vec::new();
            for (index, bytes) in page.data.chunks(MAX_EXTENT_BYTES).enumerate() {
                if bytes.iter().all(|b| *b == 0) {
                    continue;
                }
                let fragment = CheckpointFragment {
                    version: 1,
                    checkpoint_id: self.log.store_id().to_vec(),
                    kind: CheckpointFragment::PAYLOAD,
                    ordinal: payload_ordinal,
                    data: bytes.to_vec(),
                };
                payload_ordinal += 1;
                let record = self
                    .log
                    .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])?;
                let mut logical = index as u64 * MAX_EXTENT_BYTES as u64;
                for extent in record.batch.payloads {
                    let length = extent.length;
                    spans.push(PayloadSpan {
                        offset: logical,
                        length,
                        extent: Arc::new(extent),
                        extent_offset: 0,
                    });
                    logical += length;
                }
            }
            state
                .catalog
                .pages
                .insert(id.clone(), CatalogPage { metadata, spans });
        }
        let replacement = if self.backing.is_some() {
            Some(LivePageBacking::new(
                self.log.clone(),
                PageCatalog {
                    pages: state.catalog.pages.clone(),
                    logs: state.catalog.logs.clone(),
                },
            )?)
        } else {
            None
        };
        let metadata = state.metadata_bytes()?;
        let prepared = super::replica_checkpoint_index::prepare(
            &self.log,
            &state.catalog,
            &mut state.index,
            &mut state.dirty_pages,
            metadata,
            captured_cut,
            epoch,
            true,
            &|| Ok(()),
        )?;
        let publication = self
            .publication
            .lock()
            .map_err(|_| invalid("checkpoint publication is poisoned"))?;
        if *publication != token {
            return Err(cancelled());
        }
        let (commit, position) = prepared.publish_record(&self.log)?;
        if let (Some(backing), Some(replacement)) = (&self.backing, replacement) {
            if let Err(error) = backing.replace_from(replacement) {
                *self
                    .failure
                    .lock()
                    .map_err(|_| invalid("journal failure state is poisoned"))? =
                    Some(error.to_string());
                return Err(error);
            }
        }
        *self
            .published_boundary
            .lock()
            .map_err(|_| invalid("published checkpoint boundary is poisoned"))? =
            Some(PublishedBoundary {
                position: position.clone(),
                revision: Some(self.revision),
            });
        state.publication = Some((commit.clone(), position));
        drop(publication);
        self.published_epoch = epoch;
        self.cut = commit.through.clone();
        state.cut = commit.through;
        self.state = Some(state);
        self.bytes_since_checkpoint = 0;
        Ok(())
    }
    fn start_checkpoint(
        &mut self,
        service: &MatrixReplicaService,
        mode: CheckpointMode,
    ) -> io::Result<()> {
        self.flush()?;
        if self.job.is_some() {
            return Ok(());
        }
        let cut = self.log.checkpoint_cut(
            self.cut.as_ref(),
            self.state
                .as_ref()
                .and_then(|state| state.publication.as_ref().map(|(_, position)| position)),
        )?;
        let mode = if self.queued_checkpoint.is_some() {
            CheckpointMode::Manual
        } else {
            mode
        };
        let epoch = match self.queued_checkpoint.take() {
            Some(epoch) => epoch,
            None => self.reserve_epoch()?,
        };
        let Some(mut state) = self.state.take() else {
            return Err(invalid("checkpoint worker state missing"));
        };
        let scalars = replica_checkpoint_recovery::capture_scalars(service, &self.empty);
        let covered_bytes = self.bytes_since_checkpoint;
        let captured_revision = self.revision;
        let log = self.log.clone();
        let backing = self.backing.clone();
        let publication = self.publication.clone();
        let failure = self.failure.clone();
        let started_epoch = self.started_epoch.clone();
        let published_boundary = self.published_boundary.clone();
        let stopping = self.stopping.clone();
        let max_pages = self.max_pages;
        let allow_collection = self.reapers.is_empty() && self.cleanup.is_none();
        let retention_threshold = self.retention_threshold;
        let token = {
            let mut epoch = publication
                .lock()
                .map_err(|_| invalid("checkpoint publication is poisoned"))?;
            *epoch = epoch
                .checked_add(1)
                .ok_or_else(|| invalid("checkpoint epoch overflow"))?;
            *epoch
        };
        if !matches!(mode, CheckpointMode::Maintenance { .. }) {
            self.started_epoch.fetch_max(epoch, Ordering::Release);
        }
        self.job = Some(
            thread::Builder::new()
                .name("replica-checkpoint".into())
                .spawn(move || {
                    let run = (|| -> io::Result<Option<Completed>> {
                        let check = || -> io::Result<()> {
                            if stopping.load(Ordering::Acquire)
                                || *publication
                                    .lock()
                                    .map_err(|_| invalid("checkpoint publication is poisoned"))?
                                    != token
                            {
                                Err(cancelled())
                            } else {
                                Ok(())
                            }
                        };
                        check()?;
                        if let Some(cut) = &cut {
                            state.fold_through(&log, cut, max_pages, &check)?;
                        }
                        let now = Instant::now();
                        let manual = matches!(mode, CheckpointMode::Manual);
                        let maintenance = matches!(mode, CheckpointMode::Maintenance { .. });
                        if manual {
                            state.next_collection_segments = retention_threshold;
                            state.next_collection_attempt = now;
                        }
                        let count_ready = log.retained_segment_count()? >= retention_threshold;
                        let attempt =
                            count_ready && (manual || now >= state.next_compaction_attempt);
                        let readers_busy = log.retention_readers_active();
                        let can_collect = allow_collection && !readers_busy;
                        let needs_index = state.index.is_none();
                        let compaction_pin = if attempt && can_collect {
                            Some(log.checkpoint_read_pin()?)
                        } else {
                            None
                        };
                        let plan = if compaction_pin.is_some() {
                            if let Some(cut) = &cut {
                                Some(super::replica_checkpoint_compaction::plan(
                                    &log, &mut state, cut, epoch, &check,
                                )?)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let work = plan.as_ref().is_some_and(|plan| plan.has_work());
                        let collection_due = matches!(
                            mode,
                            CheckpointMode::Maintenance {
                                collection_due: true
                            }
                        );
                        if maintenance && !work && !(can_collect && (collection_due || needs_index))
                        {
                            if attempt {
                                state.next_compaction_attempt = now + Duration::from_secs(30);
                            }
                            if collection_due && !can_collect {
                                state.next_collection_attempt = now + Duration::from_secs(30);
                            }
                            return Ok(Some(Completed {
                                epoch,
                                published: false,
                                state,
                                covered_bytes: 0,
                            }));
                        }
                        check()?;
                        started_epoch.fetch_max(epoch, Ordering::Release);
                        let moved = if let Some(plan) = plan {
                            super::replica_checkpoint_compaction::apply(
                                &log, &mut state, plan, &check,
                            )?
                        } else {
                            std::collections::BTreeMap::new()
                        };
                        if work {
                            state.next_collection_segments = retention_threshold;
                            state.next_collection_attempt = Instant::now();
                        }
                        state.set_scalars(scalars)?;
                        let metadata = state.metadata_bytes()?;
                        let prepared = super::replica_checkpoint_index::prepare(
                            &log,
                            &state.catalog,
                            &mut state.index,
                            &mut state.dirty_pages,
                            metadata,
                            cut,
                            epoch,
                            false,
                            &check,
                        )?;
                        let guard = publication
                            .lock()
                            .map_err(|_| invalid("checkpoint publication is poisoned"))?;
                        if *guard != token || stopping.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        let (commit, position) = prepared.publish_record(&log)?;
                        if let Some(backing) = &backing {
                            backing.rebase(&moved)?;
                        }
                        drop(moved);
                        *published_boundary
                            .lock()
                            .map_err(|_| invalid("published checkpoint boundary is poisoned"))? =
                            Some(PublishedBoundary {
                                position: position.clone(),
                                revision: Some(captured_revision),
                            });
                        state.publication = Some((commit, position));
                        drop(guard);
                        drop(compaction_pin);
                        if allow_collection {
                            retention::collect_checkpoint(
                                &log,
                                &mut state,
                                retention_threshold,
                                &publication,
                                backing.as_deref(),
                                token,
                                &check,
                            )?;
                        }
                        if attempt {
                            state.next_compaction_attempt = if work {
                                (Instant::now() + Duration::from_secs(1))
                                    .max(state.next_collection_attempt)
                            } else {
                                Instant::now() + Duration::from_secs(30)
                            };
                        }
                        Ok(Some(Completed {
                            epoch,
                            published: true,
                            state,
                            covered_bytes,
                        }))
                    })();
                    match run {
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(None),
                        Err(error) => {
                            if let Ok(mut failed) = failure.lock() {
                                *failed = Some(error.to_string());
                            }
                            Err(error)
                        }
                        result => result,
                    }
                })?,
        );
        Ok(())
    }
}
impl ReplicaJournalBackend for SegmentedReplicaJournal {
    fn check_page_namespace(&self, chunk: &crate::ChunkId) -> Result<(), crate::ObjectError> {
        self.page_namespace_available(chunk)
    }

    fn enable_page_backing(&mut self) -> io::Result<Arc<LivePageBacking>> {
        self.flush()?;
        if let Some(backing) = &self.backing {
            return Ok(backing.clone());
        }
        if self.job.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "checkpoint is in flight",
            ));
        }
        let state = self
            .state
            .as_mut()
            .ok_or_else(|| invalid("checkpoint state is missing"))?;
        // Use the same validated boundary as the next checkpoint. Scanning the
        // raw tail here could advance past unpublished maintenance fragments,
        // leaving the worker cursor ahead of its next captured cut.
        if let Some(through) = self.log.checkpoint_cut(
            self.cut.as_ref(),
            state.publication.as_ref().map(|(_, position)| position),
        )? {
            state.fold_through(&self.log, &through, self.max_pages, &|| Ok(()))?;
        }
        let backing = Arc::new(LivePageBacking::new(
            self.log.clone(),
            PageCatalog {
                pages: state.catalog.pages.clone(),
                logs: state.catalog.logs.clone(),
            },
        )?);
        self.backing = Some(backing.clone());
        Ok(backing)
    }
    fn status(&self) -> io::Result<super::replica_journal_control::ReplicaJournalStatus> {
        use super::replica_journal_control::ReplicaJournalStatus;
        let started_epoch = self.started_epoch.load(Ordering::Acquire);
        Ok(ReplicaJournalStatus {
            version: 1,
            layout: ReplicaJournalStatus::SEGMENTED,
            journal_id: self.log.store_id().to_vec(),
            started_epoch,
            published_epoch: self.published_epoch,
            checkpoint_in_flight: started_epoch > self.published_epoch,
            pending_records: self.pending.len() as u32,
            bytes_since_checkpoint: self.bytes_since_checkpoint,
            journal_bytes: self.log.logical_length()?,
            segments: self.log.segment_count()? as u64,
            ..Default::default()
        })
    }
    fn maintenance_status(
        &self,
    ) -> io::Result<super::replica_journal_control::ReplicaJournalStatus> {
        let mut status = self.status()?;
        status.version = 2;
        let managed = self.log.managed_status()?;
        status.segments = managed.segments as u64;
        status.journal_bytes = managed.journal_bytes;
        status.retired_segments = managed.retired_segments as u64;
        status.retired_journal_bytes = managed.retired_journal_bytes;
        status.retirement_in_flight = self.cleanup.is_some()
            || !self.reapers.is_empty()
            || managed.snapshot_pending
            || (self.job.is_some() && !status.checkpoint_in_flight);
        Ok(status)
    }
    fn write_statistics_status(
        &self,
    ) -> io::Result<super::replica_journal_control::ReplicaJournalStatus> {
        let mut status = self.maintenance_status()?;
        status.version = 3;
        status.write_statistics = Some(self.log.write_statistics()?);
        Ok(status)
    }
    fn requires_context(&self) -> bool {
        true
    }
    fn capture_context(&self, service: &MatrixReplicaService) -> Option<ReplicaServiceSnapshot> {
        Some(replica_checkpoint_recovery::capture_scalars(
            service,
            &self.empty,
        ))
    }
    fn check_health(&mut self) -> io::Result<()> {
        self.health()
    }
    fn background_maintenance(&self) -> bool {
        true
    }
    fn poll_background(&mut self, service: &MatrixReplicaService, interval: u64) -> io::Result<()> {
        self.health()?;
        if self.stopping.load(Ordering::Acquire) || self.job.is_some() || !self.pending.is_empty() {
            return Ok(());
        }
        if self.queued_checkpoint.is_some() {
            return self.start_checkpoint(service, CheckpointMode::Manual);
        }
        if interval == 0 {
            return Ok(());
        }
        if self.bytes_since_checkpoint >= interval {
            return self.start_checkpoint(service, CheckpointMode::Checkpoint);
        }
        let now = Instant::now();
        let collection_due = self.collection_due(now);
        if self.reapers.is_empty()
            && self.cleanup.is_none()
            && (collection_due || self.compaction_due(now))
        {
            self.start_checkpoint(service, CheckpointMode::Maintenance { collection_due })?;
        }
        Ok(())
    }
    fn cancel_background(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        self.queued_checkpoint = None;
        Ok(())
    }
    fn failure_handle(&self) -> Option<Arc<Mutex<Option<String>>>> {
        Some(Arc::clone(&self.failure))
    }
    fn append_owned(
        &mut self,
        outcome: PageOutcome,
        context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<()> {
        self.enqueue(Pending::Page(outcome), context, sync)
    }
    fn append_namespace_owned(
        &mut self,
        outcome: NamespaceFenceOutcome,
        context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<NamespaceFenceOutcome> {
        let reply = outcome.clone();
        self.enqueue(Pending::Namespace(outcome), context, sync)?;
        Ok(reply)
    }
    fn sync_deferred(&mut self) -> io::Result<()> {
        self.flush()
    }
    fn logical_length(&self) -> u64 {
        self.revision
    }
    fn checkpoint_due(&self, interval: u64) -> bool {
        if self.job.is_some() {
            return false;
        }
        self.queued_checkpoint.is_some()
            || (interval != 0
                && (self.bytes_since_checkpoint >= interval
                    || (self.reapers.is_empty()
                        && self.cleanup.is_none()
                        && self.collection_due(Instant::now()))))
    }
    fn checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.start_checkpoint(service, CheckpointMode::Checkpoint)
    }
    fn checkpoint_requested(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.flush()?;
        if self.job.is_some() {
            if self.queued_checkpoint.is_none() {
                let epoch = self.reserve_epoch()?;
                self.queued_checkpoint = Some(epoch);
                self.started_epoch.fetch_max(epoch, Ordering::Release);
            }
            return Ok(());
        }
        if let Some(state) = self.state.as_mut() {
            // A manual barrier gets a fresh collection attempt after writers drain.
            state.next_collection_attempt = Instant::now();
            state.next_collection_segments = self.retention_threshold;
        }
        self.start_checkpoint(service, CheckpointMode::Manual)
    }
    fn checkpoint_for_install(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        if self.metadata_only {
            return Err(invalid(
                "metadata-only recovery requires page-scoped installation",
            ));
        }
        self.install(service)
    }
    fn checkpoint_page_install(
        &mut self,
        service: &MatrixReplicaService,
        page: &crate::PageId,
        state: &crate::PageState,
    ) -> io::Result<()> {
        self.install_catalog(service, Some((page, state)))
    }
    #[cfg(test)]
    fn sync_count(&self) -> u64 {
        self.syncs
    }
}
impl Drop for SegmentedReplicaJournal {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        // Shutdown waits for every owner of the log before releasing its file lock.
        if let Some(job) = self.job.take() {
            let _ = job.join();
        }
        for reaper in self.reapers.drain(..) {
            let _ = reaper.join();
        }
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.join();
        }
    }
}

#[cfg(all(test, unix))]
#[path = "replica_segmented_journal_tests.rs"]
mod tests;

#[path = "replica_checkpoint_retention.rs"]
mod retention;
