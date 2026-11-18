// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Opt-in immutable replica journal. Foreground groups sync once; checkpoint
//! payloads stay immutable and catalog work runs outside the replica mutex.
use super::{
    replica_checkpoint_catalog::{self},
    replica_checkpoint_recovery::{self, CheckpointState},
    replica_checkpoint_spans::{CatalogPage, CatalogPageMetadata, PayloadSpan},
    replica_journal_backend::ReplicaJournalBackend,
};
use crate::durable_payload::{
    outcome_log::{CheckpointFragment, DecidedMutation, StoredOutcomePosition},
    outcome_segments::SegmentedOutcomeLog,
    DEFAULT_CAPACITY_BYTES, MAX_BATCH_BYTES, MAX_BATCH_EXTENTS, MAX_EXTENT_BYTES,
};
use crate::{
    chunk_namespace_fence::NamespaceFenceOutcome, outcome::PageOutcome, MatrixReplicaService,
    ReplicaServiceSnapshot,
};
use prost::Message;
use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
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
    state: CheckpointState,
    covered_bytes: u64,
}
type Job = JoinHandle<io::Result<Option<Completed>>>;
pub(super) struct SegmentedReplicaJournal {
    log: Arc<SegmentedOutcomeLog>,
    empty: ReplicaServiceSnapshot,
    state: Option<CheckpointState>,
    cut: Option<StoredOutcomePosition>,
    pending: Vec<Pending>,
    pending_bytes: usize,
    context: Option<ReplicaServiceSnapshot>,
    revision: u64,
    started_epoch: u64,
    published_epoch: u64,
    bytes_since_checkpoint: u64,
    publication: Arc<Mutex<u64>>,
    failure: Arc<Mutex<Option<String>>>,
    job: Option<Job>,
    reapers: Vec<JoinHandle<()>>,
    max_pages: usize,
    #[cfg(test)]
    syncs: u64,
}
impl SegmentedReplicaJournal {
    pub fn open(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> io::Result<Self> {
        if max_pages == 0 {
            return Err(invalid("zero replica page limit"));
        }
        let log = Arc::new(SegmentedOutcomeLog::open(path, DEFAULT_CAPACITY_BYTES)?);
        let loaded = replica_checkpoint_catalog::latest(&log, max_pages)?;
        let (state, epoch, bytes_since_checkpoint) = match loaded {
            Some(loaded) => {
                let epoch = loaded.header.epoch;
                let mut state = CheckpointState::from_checkpoint(loaded)?;
                let bytes = state.fold_available(&log, max_pages)?;
                state.restore(&log, service)?;
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
        let cut = state.as_ref().and_then(|s| s.cut.clone());
        let mut journal = Self {
            log,
            empty: replica_checkpoint_recovery::scalar_template(),
            state,
            cut,
            pending: Vec::with_capacity(MAX_BATCH_EXTENTS),
            pending_bytes: 0,
            context: None,
            revision: 0,
            started_epoch: epoch,
            published_epoch: epoch,
            bytes_since_checkpoint,
            publication: Arc::new(Mutex::new(epoch)),
            failure: Arc::new(Mutex::new(None)),
            job: None,
            reapers: Vec::new(),
            max_pages,
            #[cfg(test)]
            syncs: 0,
        };
        if epoch == 0 {
            journal.install(service)?;
        }
        Ok(journal)
    }
    fn health(&mut self) -> io::Result<()> {
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
                    self.published_epoch = done.epoch;
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
        Ok(())
    }
    fn enqueue(
        &mut self,
        pending: Pending,
        context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<()> {
        self.health()?;
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
    fn install(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.flush()?;
        let epoch = {
            let mut epoch = self
                .publication
                .lock()
                .map_err(|_| invalid("checkpoint publication is poisoned"))?;
            *epoch = epoch
                .checked_add(1)
                .ok_or_else(|| invalid("checkpoint epoch overflow"))?;
            *epoch
        };
        self.started_epoch = epoch;
        self.retire_job();
        let mut state = CheckpointState::new(service.export_snapshot_unsealed_without_pages())?;
        let snapshot = service.export_snapshot_unsealed();
        if snapshot.pages.len() > self.max_pages {
            return Err(invalid("replica page count exceeds configured limit"));
        }
        let mut payload_ordinal = 0;
        for (id, page) in &snapshot.pages {
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
        let metadata = state.metadata_bytes()?;
        let prepared = super::replica_checkpoint_index::prepare(
            &self.log,
            &state.catalog,
            &mut state.index,
            &mut state.dirty_pages,
            metadata,
            self.cut.clone(),
            epoch,
            true,
            &|| Ok(()),
        )?;
        let publication = self
            .publication
            .lock()
            .map_err(|_| invalid("checkpoint publication is poisoned"))?;
        if *publication != epoch {
            return Err(cancelled());
        }
        let commit = prepared.publish(&self.log)?;
        drop(publication);
        self.published_epoch = epoch;
        self.cut = commit.through.clone();
        state.cut = commit.through;
        self.state = Some(state);
        self.bytes_since_checkpoint = 0;
        Ok(())
    }
    fn start_checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.flush()?;
        if self.job.is_some() {
            return Ok(());
        }
        let Some(mut state) = self.state.take() else {
            return Err(invalid("checkpoint worker state missing"));
        };
        let cut = self.cut.clone();
        let scalars = replica_checkpoint_recovery::capture_scalars(service, &self.empty);
        let covered_bytes = self.bytes_since_checkpoint;
        let log = self.log.clone();
        let publication = self.publication.clone();
        let failure = self.failure.clone();
        let max_pages = self.max_pages;
        let epoch = {
            let mut epoch = publication
                .lock()
                .map_err(|_| invalid("checkpoint publication is poisoned"))?;
            *epoch = epoch
                .checked_add(1)
                .ok_or_else(|| invalid("checkpoint epoch overflow"))?;
            *epoch
        };
        self.started_epoch = epoch;
        self.job = Some(
            thread::Builder::new()
                .name("replica-checkpoint".into())
                .spawn(move || {
                    let run = (|| -> io::Result<Option<Completed>> {
                        let check = || -> io::Result<()> {
                            if *publication
                                .lock()
                                .map_err(|_| invalid("checkpoint publication is poisoned"))?
                                != epoch
                            {
                                Err(cancelled())
                            } else {
                                Ok(())
                            }
                        };
                        check()?;
                        if let Some(cut) = &cut {
                            state.fold_through(&log, cut, max_pages)?;
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
                        if *guard != epoch {
                            return Err(cancelled());
                        }
                        prepared.publish(&log)?;
                        drop(guard);
                        Ok(Some(Completed {
                            epoch,
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
    fn status(&self) -> io::Result<super::replica_journal_control::ReplicaJournalStatus> {
        use super::replica_journal_control::ReplicaJournalStatus;
        Ok(ReplicaJournalStatus {
            version: 1,
            layout: ReplicaJournalStatus::SEGMENTED,
            journal_id: self.log.store_id().to_vec(),
            started_epoch: self.started_epoch,
            published_epoch: self.published_epoch,
            checkpoint_in_flight: self.job.is_some(),
            pending_records: self.pending.len() as u32,
            bytes_since_checkpoint: self.bytes_since_checkpoint,
            journal_bytes: self.log.logical_length()?,
            segments: self.log.segment_count()? as u64,
        })
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
        interval != 0 && self.job.is_none() && self.bytes_since_checkpoint >= interval
    }
    fn checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.start_checkpoint(service)
    }
    fn checkpoint_for_install(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.install(service)
    }
    #[cfg(test)]
    fn sync_count(&self) -> u64 {
        self.syncs
    }
}
impl Drop for SegmentedReplicaJournal {
    fn drop(&mut self) {
        // Shutdown waits for every owner of the log before releasing its file lock.
        if let Some(job) = self.job.take() {
            let _ = job.join();
        }
        for reaper in self.reapers.drain(..) {
            let _ = reaper.join();
        }
    }
}

#[cfg(all(test, unix))]
#[path = "replica_segmented_journal_tests.rs"]
mod tests;
