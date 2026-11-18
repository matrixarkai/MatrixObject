// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! What a replica's heartbeat reports, readable without the replica lock.
//!
//! The replica lock is held across the journal fsync of every write (about 95% of the time it is
//! held), and under a steady write load it is held most of the wall-clock. A heartbeat that must
//! take it with `try_lock` therefore fails about one try in four, whatever retry it is given, and
//! a node that cannot report for a heartbeat TTL drops out of placement. This holds the four
//! things a report needs, each written where it changes (under the replica lock, as before) and
//! read by the heartbeat without it:
//!
//! * `failed`: a replica that failed stays failed until restart.
//! * the bytes in use, and the membership view the consensus layer last applied, together under
//!   one small mutex that is only ever held to copy a value, never across I/O, so that a report
//!   never pairs a view with the usage of a later state.
//! * whether the node is in maintenance, fixed for the server's lifetime.
//! * the capacity, fixed for the server's lifetime.
//!
//! The journal's own background failure is read through a handle to its shared failure cell.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Published {
    used_bytes: u64,
    #[cfg(all(feature = "chunk-consensus", feature = "meta-server"))]
    membership: Option<(crate::cluster::MembershipStatus, std::time::Instant)>,
}

pub(super) struct ReplicaHealth {
    failed: AtomicBool,
    maintenance: bool,
    capacity_bytes: u64,
    published: Mutex<Published>,
    journal_failure: Mutex<Option<Arc<Mutex<Option<String>>>>>,
}

impl ReplicaHealth {
    pub(super) fn new(maintenance: bool, capacity_bytes: u64, used_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            failed: AtomicBool::new(false),
            maintenance,
            capacity_bytes,
            published: Mutex::new(Published { used_bytes, ..Default::default() }),
            journal_failure: Mutex::new(None),
        })
    }

    /// Mark the replica failed. It stays failed.
    pub(super) fn fail(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }

    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    pub(super) fn maintenance(&self) -> bool {
        self.maintenance
    }

    pub(super) fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    fn published(&self) -> std::sync::MutexGuard<'_, Published> {
        // A panic cannot leave this half-written: it is only ever assigned whole values.
        self.published.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(super) fn publish_used(&self, used_bytes: u64) {
        self.published().used_bytes = used_bytes;
    }

    /// Where the journal's background failure is recorded, for a journal that has one.
    pub(super) fn watch_journal(&self, failure: Option<Arc<Mutex<Option<String>>>>) {
        *self.journal_failure.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = failure;
    }

    /// Whether the journal has recorded a background failure. A poisoned cell counts as one.
    pub(super) fn journal_failed(&self) -> bool {
        let handle = self
            .journal_failure
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        handle.is_some_and(|cell| cell.lock().map_or(true, |failure| failure.is_some()))
    }

    /// The bytes in use, and the membership view with how long ago it was last confirmed,
    /// as of one moment.
    #[cfg(all(feature = "chunk-consensus", feature = "meta-server"))]
    pub(super) fn used_and_membership(
        &self,
    ) -> (u64, Option<(crate::cluster::MembershipStatus, std::time::Instant)>) {
        let published = self.published();
        (published.used_bytes, published.membership.clone())
    }

    pub(super) fn used_bytes(&self) -> u64 {
        self.published().used_bytes
    }

    /// Record the membership the consensus layer has applied: a view already held is only
    /// refreshed when the term and index are unchanged, and a view that does not validate is
    /// dropped.
    #[cfg(all(feature = "chunk-consensus", feature = "meta-server"))]
    pub(super) fn observe_membership(&self, term: u64, applied_index: u64, status: impl FnOnce() -> crate::cluster::MembershipStatus) {
        let mut published = self.published();
        match &mut published.membership {
            Some((held, refreshed)) if held.term == term && held.applied_index == applied_index => {
                *refreshed = std::time::Instant::now();
            }
            _ => {
                let status = status();
                published.membership = if status.validate().is_ok() {
                    Some((status, std::time::Instant::now()))
                } else {
                    None
                };
            }
        }
    }
}

/// The bytes a replica holds, kept where the heartbeat can read them: every change is published.
pub(super) struct Used {
    value: u64,
    health: Arc<ReplicaHealth>,
}

impl Used {
    pub(super) fn new(value: u64, health: Arc<ReplicaHealth>) -> Self {
        Self { value, health }
    }

    pub(super) fn get(&self) -> u64 {
        self.value
    }

    pub(super) fn set(&mut self, value: u64) {
        self.value = value;
        self.health.publish_used(value);
    }
}

/// A health record and the usage cell that publishes to it, for a test that builds a replica.
#[cfg(test)]
pub(super) fn test_parts() -> (Used, Arc<ReplicaHealth>) {
    let health = ReplicaHealth::new(false, 0, 0);
    (Used::new(0, Arc::clone(&health)), health)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_journal_failure_is_seen_through_its_cell_without_the_replica() {
        let health = ReplicaHealth::new(false, 10, 0);
        assert!(!health.journal_failed(), "no journal, no failure");
        let cell = Arc::new(Mutex::new(None));
        health.watch_journal(Some(Arc::clone(&cell)));
        assert!(!health.journal_failed());
        *cell.lock().unwrap() = Some("checkpoint worker failed".to_string());
        assert!(health.journal_failed());
        // A cell poisoned by a panicking worker counts as a failure.
        let poisoned = Arc::new(Mutex::new(None));
        let held = Arc::clone(&poisoned);
        let _ = std::thread::spawn(move || {
            let _guard = held.lock().unwrap();
            panic!("worker panicked");
        })
        .join();
        health.watch_journal(Some(poisoned));
        assert!(health.journal_failed());
    }

    #[test]
    fn usage_and_failure_are_published_where_they_change() {
        let health = ReplicaHealth::new(true, 99, 7);
        assert_eq!((health.used_bytes(), health.capacity_bytes(), health.maintenance()), (7, 99, true));
        let mut used = Used::new(7, Arc::clone(&health));
        used.set(4096);
        assert_eq!((used.get(), health.used_bytes()), (4096, 4096));
        assert!(!health.failed());
        health.fail();
        assert!(health.failed());
    }
}
