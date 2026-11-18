// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Amortized checkpoint-root discovery and bounded retirement retries.
use super::*;
use std::time::{Duration, Instant};

fn busy(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
}

pub(super) fn finish_retirements(log: &SegmentedOutcomeLog) -> io::Result<()> {
    let stats = match log.cleanup_retired() {
        Ok(stats) => stats,
        Err(error) if busy(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    if stats.pending == 0 && (stats.removed != 0 || log.retirement_snapshot_needed()?) {
        match log.prepare_retirement_snapshot(&|| Ok(())) {
            Ok(Some(prepared)) => match prepared.commit() {
                Ok(committed) => {
                    committed.finish()?;
                }
                Err(error) if busy(&error) => {}
                Err(error) => return Err(error),
            },
            Ok(None) => {}
            Err(error) if busy(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(super) fn collect_checkpoint(
    log: &SegmentedOutcomeLog,
    state: &mut CheckpointState,
    minimum: usize,
    publication: &Mutex<u64>,
    epoch: u64,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<()> {
    let count = log.retained_segment_count()?;
    if count < state.next_collection_segments || Instant::now() < state.next_collection_attempt {
        return Ok(());
    }
    if log.retention_readers_active() {
        state.next_collection_attempt = Instant::now() + Duration::from_secs(30);
        return Ok(());
    }
    check()?;
    let index = state
        .index
        .as_ref()
        .ok_or_else(|| invalid("collection needs a published checkpoint index"))?;
    let (commit, position) = state
        .publication
        .as_ref()
        .ok_or_else(|| invalid("collection has no published root"))?;
    let keep = index.protected_segments(log, &state.catalog, commit, position, check)?;
    let cut = commit
        .through
        .as_ref()
        .ok_or_else(|| invalid("collection has no replay cut"))?;
    let attempt = (|| {
        let Some(prepared) = log.prepare_retention(&keep, cut, check)? else {
            return Ok(None);
        };
        let guard = publication
            .lock()
            .map_err(|_| invalid("checkpoint publication is poisoned"))?;
        if *guard != epoch {
            return Err(cancelled());
        }
        let committed = prepared.commit()?;
        drop(guard);
        // Neither catalog destruction nor physical unlinking holds the epoch gate.
        let stats = committed.finish()?;
        if stats.pending == 0 && stats.removed != 0 {
            finish_retirements(log)?;
        }
        Ok(Some(()))
    })();
    match attempt {
        Err(error) if busy(&error) => {
            state.next_collection_attempt = Instant::now() + Duration::from_secs(30)
        }
        Err(error) => return Err(error),
        Ok(_) => {
            let count = log.retained_segment_count()?;
            state.next_collection_segments = minimum.max(count.saturating_mul(2)).min(1_000_000);
            state.next_collection_attempt = Instant::now()
                + if state.next_collection_segments <= count {
                    Duration::from_secs(30)
                } else {
                    Duration::ZERO
                };
        }
    }
    Ok(())
}
