// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::replica_journal::ReplicaJournal;
use super::replica_journal_control::ReplicaJournalStatus;
use crate::{
    chunk_namespace_fence::NamespaceFenceOutcome, outcome::PageOutcome, MatrixReplicaService,
    ReplicaServiceSnapshot,
};
use std::io;
pub(super) trait ReplicaJournalBackend: Send {
    fn status(&self) -> io::Result<ReplicaJournalStatus>;
    fn maintenance_status(&self) -> io::Result<ReplicaJournalStatus> {
        let mut status = self.status()?;
        status.version = 2;
        Ok(status)
    }
    fn requires_context(&self) -> bool {
        false
    }
    fn capture_context(&self, _service: &MatrixReplicaService) -> Option<ReplicaServiceSnapshot> {
        None
    }
    fn check_health(&mut self) -> io::Result<()> {
        Ok(())
    }
    /// Where this journal records a failure of its background work, for a reader that must not
    /// take the journal's lock. `None` when the journal has no background work.
    fn failure_handle(&self) -> Option<std::sync::Arc<std::sync::Mutex<Option<String>>>> {
        None
    }
    fn append_owned(
        &mut self,
        outcome: PageOutcome,
        context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<()>;
    fn append_namespace_owned(
        &mut self,
        outcome: NamespaceFenceOutcome,
        context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<NamespaceFenceOutcome>;
    fn sync_deferred(&mut self) -> io::Result<()>;
    /// Changes for queued foreground mutations, never for checkpoint fragments.
    fn logical_length(&self) -> u64;
    fn checkpoint_due(&self, interval: u64) -> bool;
    fn checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()>;
    fn checkpoint_requested(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.checkpoint(service)
    }
    fn checkpoint_for_install(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.checkpoint(service)
    }
    #[cfg(test)]
    fn sync_count(&self) -> u64;
}
impl ReplicaJournalBackend for ReplicaJournal {
    fn status(&self) -> io::Result<ReplicaJournalStatus> {
        Ok(ReplicaJournalStatus {
            version: 1,
            journal_id: self.journal_id().to_vec(),
            layout: ReplicaJournalStatus::LEGACY,
            started_epoch: self.checkpoint_epoch(),
            published_epoch: self.checkpoint_epoch(),
            checkpoint_in_flight: false,
            pending_records: 0,
            bytes_since_checkpoint: self.checkpoint_bytes(),
            journal_bytes: self.logical_length(),
            segments: 1,
            ..Default::default()
        })
    }
    fn append_owned(
        &mut self,
        outcome: PageOutcome,
        _context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<()> {
        if sync {
            self.append_outcome(&outcome)
        } else {
            self.append_outcome_deferred(&outcome)
        }
    }
    fn append_namespace_owned(
        &mut self,
        outcome: NamespaceFenceOutcome,
        _context: Option<ReplicaServiceSnapshot>,
        sync: bool,
    ) -> io::Result<NamespaceFenceOutcome> {
        self.append_namespace_fence_deferred(&outcome)?;
        if sync {
            self.sync_deferred()?;
        }
        Ok(outcome)
    }
    fn sync_deferred(&mut self) -> io::Result<()> {
        ReplicaJournal::sync_deferred(self)
    }
    fn logical_length(&self) -> u64 {
        ReplicaJournal::logical_length(self)
    }
    fn checkpoint_due(&self, interval: u64) -> bool {
        ReplicaJournal::checkpoint_due(self, interval)
    }
    fn checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        ReplicaJournal::checkpoint(self, service)
    }
    #[cfg(test)]
    fn sync_count(&self) -> u64 {
        ReplicaJournal::sync_count(self)
    }
}
