// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The decision of which pages of chunk objects may be freed, with no network in it.
//!
//! A chunk object's pages are written first and published after, and the catalog keeps one
//! manifest per object, so the pages of a deleted object, of an overwritten generation and of a
//! writer that died before publishing are all pages that **no manifest names**. A page is live
//! exactly when some manifest of the catalog names it: a data page of one of its extents, or the
//! digest page of a multi-page extent.
//!
//! Freeing a page that is merely not named *yet* would destroy a write in progress, and nothing in
//! the metadata plane says how old an allocation is (an allocation is a position in the log, and a
//! client may write into one long after it was made). So a page is freed only when this planner has
//! seen it unnamed in two scans at least a grace period apart, and the answer is checked against the
//! catalog once more, just before the discard, by the caller. The first time a page is seen is
//! remembered here, in memory: a restart forgets it and starts the clock again, which is the safe
//! direction.

use std::collections::{BTreeMap, BTreeSet};

use crate::chunk_catalog::ChunkCatalog;
use crate::chunk_integrity;
use crate::chunk_manifest::CHUNK_KIND_PAGED;

/// One page of one chunk on one group.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageKey {
    pub group: String,
    pub volume_id: u64,
    pub chunk_id: u64,
    pub page_index: u64,
}

/// A page's first write has sequence 1; a discard moves it past that. A page past 1 has already been
/// swept (or rewritten, which only a writer that owns it may do), so it is not a candidate.
pub const FIRST_WRITE_SEQUENCE: u64 = 1;

/// Every page the catalog names: the data pages of each live paged extent and its digest page.
pub fn live_pages(catalog: &ChunkCatalog) -> BTreeSet<PageKey> {
    live_pages_of(catalog.iter().map(|(_, manifest)| manifest.as_ref()))
}

/// The same, from any run of manifests (what a client reads from the catalog, a page at a time).
pub fn live_pages_of<'a>(manifests: impl IntoIterator<Item = &'a crate::chunk_manifest::ChunkManifest>) -> BTreeSet<PageKey> {
    let mut live = BTreeSet::new();
    for manifest in manifests {
        if manifest.deleted || manifest.kind != CHUNK_KIND_PAGED || manifest.page_bytes == 0 {
            continue;
        }
        for extent in &manifest.extents {
            let first = extent.chunk_offset / manifest.page_bytes;
            let stored = chunk_integrity::stored_pages(extent.length, manifest.page_bytes);
            for page in first..first + stored {
                live.insert(PageKey {
                    group: extent.group.clone(),
                    volume_id: extent.volume_id,
                    chunk_id: extent.chunk_id,
                    page_index: page,
                });
            }
        }
    }
    live
}

/// The pages seen unnamed, and when each was first seen.
#[derive(Debug, Default)]
pub struct Candidates {
    first_seen_ms: BTreeMap<PageKey, u64>,
}

impl Candidates {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.first_seen_ms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.first_seen_ms.is_empty()
    }

    /// Record one scan: `inventory` is what the groups hold (each page with its write sequence) and
    /// `live` is what the catalog names, read AFTER the inventory. A page that is unnamed and was
    /// never swept starts its clock the first time it is seen; one that is named now, or was swept,
    /// or is gone, leaves the table. A page whose manifest was published between two scans is
    /// therefore never freed.
    pub fn scan(&mut self, inventory: &[(PageKey, u64)], live: &BTreeSet<PageKey>, now_ms: u64) {
        let unnamed: BTreeSet<&PageKey> = inventory
            .iter()
            .filter(|(page, sequence)| *sequence == FIRST_WRITE_SEQUENCE && !live.contains(page))
            .map(|(page, _)| page)
            .collect();
        self.first_seen_ms.retain(|page, _| unnamed.contains(page));
        for page in unnamed {
            self.first_seen_ms.entry(page.clone()).or_insert(now_ms);
        }
    }

    /// The pages unnamed since at least `grace_ms` ago, in page order.
    pub fn due(&self, now_ms: u64, grace_ms: u64) -> Vec<PageKey> {
        self.first_seen_ms
            .iter()
            .filter(|(_, first)| now_ms.saturating_sub(**first) >= grace_ms)
            .map(|(page, _)| page.clone())
            .collect()
    }
}

/// The last check before a discard: `due` against the catalog read again after it was computed. A
/// manifest published in between names pages that were unnamed a moment ago, and they stay.
pub fn confirm(due: Vec<PageKey>, live_now: &BTreeSet<PageKey>) -> Vec<PageKey> {
    due.into_iter().filter(|page| !live_now.contains(page)).collect()
}

#[cfg(test)]
#[path = "chunk_reclaim_tests.rs"]
mod tests;
