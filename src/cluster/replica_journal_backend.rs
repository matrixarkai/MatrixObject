// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::replica_journal::ReplicaJournal;
use crate::{
    chunk_namespace_fence::NamespaceFenceOutcome, outcome::PageOutcome, MatrixReplicaService,
    ReplicaServiceSnapshot,
};
use std::io;
pub(super) trait ReplicaJournalBackend: Send {
    fn capture_context(&self, _service: &MatrixReplicaService) -> Option<ReplicaServiceSnapshot> {
        None
    }
    fn check_health(&mut self) -> io::Result<()> {
        Ok(())
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
    ) -> io::Result<()>;
    fn sync_deferred(&mut self) -> io::Result<()>;
    /// Changes for queued foreground mutations, never for checkpoint fragments.
    fn logical_length(&self) -> u64;
    fn checkpoint_due(&self, interval: u64) -> bool;
    fn checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()>;
    fn checkpoint_for_install(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        self.checkpoint(service)
    }
    #[cfg(test)]
    fn sync_count(&self) -> u64;
}
impl ReplicaJournalBackend for ReplicaJournal {
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
    ) -> io::Result<()> {
        self.append_namespace_fence_deferred(&outcome)?;
        if sync {
            self.sync_deferred()
        } else {
            Ok(())
        }
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
