// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The reclaim sweep, run on a timer by a door.
//!
//! A chunk object's pages are written before its manifest is published, so the pages of a deleted
//! object, of an overwritten generation and of a writer that died before publishing are named by
//! no manifest. [`RemoteMetaClient::reclaim_sweep`] frees them; this module is what calls it, and
//! the settings that say how often and how long a page must stay unnamed first.
//!
//! # WHAT MAKES RUNNING IT SAFE
//!
//! * **Several doors may sweep at once.** Nothing elects one. A discard is conditional on the page
//!   still having its first-write sequence, and the catalog is read again just before each
//!   discard, so two doors that find the same page unnamed free it once and a page one of them
//!   saw unnamed but a writer has published since is kept. The price of not electing is that N
//!   doors make N inventory scans per interval.
//! * **A restart is safe.** What the sweep remembers between rounds is in memory only. A door
//!   that restarts starts every grace clock again, so it frees later and never earlier.
//! * **It never frees a page of a page-granular object.** The live set is built from the chunk
//!   catalog alone, so a page that belongs to an object published the page-granular way would
//!   look unnamed. A round therefore refuses, and forgets what it had seen, while the metadata
//!   plane holds any such object that is not deleted. A door must still be able to start, so
//!   this is a refusal per round and not a failure to start, and it is LOUD: a warning with the
//!   count the first time and then every tenth round, and [`ReclaimStatus::line`]
//!   (`reclaim_refused_page_objects=N`) so that a sweep that never runs can be seen.
//! * **A failure ends the round, not the loop.** The connection is dropped and the next round
//!   opens a new one.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::chunk_reclaim::Candidates;
use crate::cluster::{ObjectWriteOptions, ReclaimReport, RemoteMetaClient};
use crate::ObjectError;

/// How long a page must stay unnamed, over two scans, before it may be freed.
pub const DEFAULT_GRACE_MS: u64 = 3_600_000;
/// The least grace accepted: a write puts its pages down before it publishes, and a shorter
/// grace could free the pages of a write that is still going to publish.
pub const MIN_GRACE_MS: u64 = 60_000;
/// The most grace accepted (seven days): beyond it the setting is a typing mistake.
pub const MAX_GRACE_MS: u64 = 604_800_000;
/// A refused round is reported the first time and then once in this many.
pub const REFUSAL_LOG_EVERY: u64 = 10;
/// The least interval accepted between two rounds.
pub const MIN_INTERVAL_MS: u64 = 1_000;

/// When and how a door sweeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReclaimSettings {
    /// A page is freed once it has been unnamed for this long.
    pub grace_ms: u64,
    /// Time between two rounds.
    pub interval_ms: u64,
}

fn present(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn number(name: &str, value: &str, low: u64, high: u64) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(parsed) if (low..=high).contains(&parsed) => Ok(parsed),
        _ => Err(format!("{name}={value:?} must be a number from {low} to {high}")),
    }
}

/// The settings a door sweeps with, from its environment. The sweep always runs.
///
/// A value that does not parse or is out of range is REFUSED, never replaced by the default.
///
/// * `grace`: `MATRIXOBJECT_RECLAIM_GRACE_MS`, [`MIN_GRACE_MS`] to [`MAX_GRACE_MS`], default
///   [`DEFAULT_GRACE_MS`].
/// * `interval`: `MATRIXOBJECT_RECLAIM_INTERVAL_MS`, [`MIN_INTERVAL_MS`] to the grace, default a
///   quarter of the grace.
pub fn resolve_reclaim(grace: Option<&str>, interval: Option<&str>) -> Result<ReclaimSettings, String> {
    let grace_ms = match present(grace) {
        None => DEFAULT_GRACE_MS,
        Some(value) => number("MATRIXOBJECT_RECLAIM_GRACE_MS", value, MIN_GRACE_MS, MAX_GRACE_MS)?,
    };
    let interval_ms = match present(interval) {
        None => (grace_ms / 4).max(MIN_INTERVAL_MS),
        Some(value) => number("MATRIXOBJECT_RECLAIM_INTERVAL_MS", value, MIN_INTERVAL_MS, grace_ms)?,
    };
    Ok(ReclaimSettings { grace_ms, interval_ms })
}

/// What a sweeper has done, readable from any thread, so "the sweep never runs" is visible.
#[derive(Debug, Default)]
pub struct ReclaimStatus {
    rounds: AtomicU64,
    refused: AtomicU64,
    page_objects: AtomicU64,
    freed: AtomicU64,
}

impl ReclaimStatus {
    /// Rounds that did not run because a page-granular object exists.
    pub fn refused_page_objects(&self) -> u64 {
        self.refused.load(Ordering::SeqCst)
    }

    /// Rounds attempted, whatever their outcome.
    pub fn rounds(&self) -> u64 {
        self.rounds.load(Ordering::SeqCst)
    }

    /// Pages freed since this door started.
    pub fn freed_pages(&self) -> u64 {
        self.freed.load(Ordering::SeqCst)
    }

    /// The page-granular objects the last round found, 0 once none was left.
    pub fn page_objects(&self) -> u64 {
        self.page_objects.load(Ordering::SeqCst)
    }

    /// One line an operator or a probe can read.
    pub fn line(&self) -> String {
        format!(
            "reclaim_refused_page_objects={} page_objects={} rounds={} freed_pages={}",
            self.refused_page_objects(),
            self.page_objects(),
            self.rounds(),
            self.freed_pages()
        )
    }
}

/// The message of the refusal to start over a data directory written by an earlier build.
pub fn older_data_directory_message(page_objects: u64) -> String {
    format!(
        "older data directory not supported: the metadata plane holds {page_objects} page-granular objects that are not \
         deleted, and this build stores and reads chunk objects only; data directories from earlier builds are not supported"
    )
}

/// Refuse to start over objects this build cannot read or reclaim: any page-granular object that is not
/// deleted means the data directory was written by an earlier build, and there is no migration.
pub fn refuse_older_data_directory(client: &mut RemoteMetaClient) -> Result<(), String> {
    match client.count_page_objects() {
        Ok(0) => Ok(()),
        Ok(count) => Err(older_data_directory_message(count)),
        Err(error) => Err(format!("the metadata plane could not be checked for objects from an earlier build: {error}")),
    }
}

/// One door's sweeper: the connection it reuses and what it has seen so far.
pub struct ReclaimLoop<F> {
    open: F,
    options: ObjectWriteOptions,
    settings: ReclaimSettings,
    client: Option<RemoteMetaClient>,
    candidates: Candidates,
    status: Arc<ReclaimStatus>,
}

impl<F> ReclaimLoop<F>
where
    F: FnMut() -> Result<RemoteMetaClient, ObjectError>,
{
    pub fn new(open: F, options: ObjectWriteOptions, settings: ReclaimSettings) -> Self {
        Self { open, options, settings, client: None, candidates: Candidates::new(), status: Arc::new(ReclaimStatus::default()) }
    }

    /// The counters of this sweeper, shared with whoever reports them.
    pub fn status(&self) -> Arc<ReclaimStatus> {
        Arc::clone(&self.status)
    }

    /// One round at `now_ms`. Any failure drops the connection so the next round opens a new one.
    pub fn round(&mut self, now_ms: u64) -> Result<ReclaimReport, ObjectError> {
        if self.client.is_none() {
            self.client = Some((self.open)()?);
        }
        self.status.rounds.fetch_add(1, Ordering::SeqCst);
        let result = Self::sweep(
            self.client.as_mut().expect("the connection was opened above"),
            &mut self.candidates,
            now_ms,
            self.settings.grace_ms,
            &self.options,
            &self.status,
        );
        if result.is_err() {
            self.client = None;
        }
        result
    }

    fn sweep(
        client: &mut RemoteMetaClient,
        candidates: &mut Candidates,
        now_ms: u64,
        grace_ms: u64,
        options: &ObjectWriteOptions,
        status: &ReclaimStatus,
    ) -> Result<ReclaimReport, ObjectError> {
        let page_objects = client.count_page_objects()?;
        status.page_objects.store(page_objects, Ordering::SeqCst);
        if page_objects > 0 {
            *candidates = Candidates::new();
            status.refused.fetch_add(1, Ordering::SeqCst);
            return Err(ObjectError::InvalidArgument(format!(
                "the reclaim sweep is refused while {page_objects} page-granular objects exist: their pages are named by no chunk manifest"
            )));
        }
        let report = client.reclaim_sweep(candidates, now_ms, grace_ms, options)?;
        status.freed.fetch_add(report.freed.len() as u64, Ordering::SeqCst);
        Ok(report)
    }

    /// Sweep until `stop` is set, one round per interval. A failed round is reported and the loop
    /// goes on.
    pub fn run(mut self, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::SeqCst) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis() as u64)
                .unwrap_or(0);
            let refused_before = self.status.refused_page_objects();
            match self.round(now_ms) {
                Ok(report) if !report.freed.is_empty() => eprintln!(
                    "reclaim: freed {} pages ({} in the inventory, {} unnamed) {}",
                    report.freed.len(),
                    report.inventory_pages,
                    report.unnamed_pages,
                    self.status.line()
                ),
                Ok(_) => {}
                // the first refusal and then one in ten, so a sweep that never runs is loud and not a flood
                Err(error) if self.status.refused_page_objects() > refused_before => {
                    if (self.status.refused_page_objects() - 1) % REFUSAL_LOG_EVERY == 0 {
                        eprintln!("WARNING: {error}; {}", self.status.line());
                    }
                }
                Err(error) => eprintln!("reclaim round failed: {error}"),
            }
            let mut waited = 0;
            while waited < self.settings.interval_ms && !stop.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(100));
                waited += 100;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sweep_runs_by_default_with_an_hour_of_grace_and_a_quarter_of_it_between_rounds() {
        assert_eq!(resolve_reclaim(None, None), Ok(ReclaimSettings { grace_ms: 3_600_000, interval_ms: 900_000 }));
        assert_eq!(resolve_reclaim(Some(" 120000 "), None), Ok(ReclaimSettings { grace_ms: 120_000, interval_ms: 30_000 }));
        assert_eq!(resolve_reclaim(Some("60000"), Some("1000")), Ok(ReclaimSettings { grace_ms: 60_000, interval_ms: 1_000 }));
        assert_eq!(resolve_reclaim(Some(""), Some(" ")), Ok(ReclaimSettings { grace_ms: 3_600_000, interval_ms: 900_000 }));
    }

    #[test]
    fn a_setting_that_is_out_of_range_or_not_a_number_is_refused_not_defaulted() {
        for grace in ["59999", "604800001", "x", "-1", "1.5"] {
            assert!(resolve_reclaim(Some(grace), None).is_err(), "grace {grace}");
        }
        for interval in ["999", "0", "x"] {
            assert!(resolve_reclaim(None, Some(interval)).is_err(), "interval {interval}");
        }
        // the interval may not exceed the grace
        assert!(resolve_reclaim(Some("60000"), Some("60001")).is_err());
    }

    #[test]
    fn the_refusal_over_an_older_data_directory_names_it_and_says_there_is_no_migration() {
        let message = older_data_directory_message(3);
        assert!(message.contains("older data directory not supported"), "{message}");
        assert!(message.contains("3 page-granular objects"), "{message}");
        assert!(message.contains("data directories from earlier builds are not supported"), "{message}");
    }
}
