// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Checkpoint maintenance copies live ranges before replacing their root.
#[cfg(all(test, unix))]
use super::replica_checkpoint_spans::PayloadSpan;
use super::replica_page_backing::PayloadRelocation;
use super::replica_payload_relocation::{push_span, translate, PayloadMovement as Movement};
use super::{
    replica_checkpoint_catalog::extent_key,
    replica_checkpoint_recovery::CheckpointState,
    replica_checkpoint_spans::{CatalogPage, PageCatalog},
};
use crate::durable_payload::{
    outcome_log::{CheckpointFragment, DecidedMutation, StoredOutcomePosition},
    outcome_segments::{CompactionSegment, SegmentedOutcomeLog},
    PayloadExtent,
};
use crate::PageId;
use ring::rand::{SecureRandom, SystemRandom};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};

const SAMPLE_SEGMENTS: usize = 64;
const COPY_BYTES: u64 = 8 * 1024 * 1024;
type ExtentKey = ([u8; 16], u64);
fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
fn normalize(ranges: &mut Vec<(u64, u64)>) -> io::Result<u64> {
    ranges.sort_unstable();
    let mut count = 0;
    for index in 0..ranges.len() {
        let (start, end) = ranges[index];
        if start >= end {
            return Err(invalid("empty or reversed compaction range"));
        }
        if count != 0 && ranges[count - 1].1 >= start {
            ranges[count - 1].1 = ranges[count - 1].1.max(end);
        } else {
            ranges[count] = (start, end);
            count += 1;
        }
    }
    ranges.truncate(count);
    ranges.iter().try_fold(0u64, |sum, (start, end)| {
        sum.checked_add(end - start)
            .ok_or_else(|| invalid("compaction byte count overflow"))
    })
}

struct Source {
    extent: Arc<PayloadExtent>,
    ranges: Vec<(u64, u64)>,
}
struct Pending {
    key: ExtentKey,
    start: u64,
    length: u64,
    offset: u64,
}
struct Copier<'a> {
    log: &'a SegmentedOutcomeLog,
    check: &'a dyn Fn() -> io::Result<()>,
    id: Vec<u8>,
    ordinal: u64,
    data: Vec<u8>,
    pending: Vec<Pending>,
    moved: BTreeMap<ExtentKey, Vec<Movement>>,
    sector: Vec<u8>,
    cached_sector: Option<(ExtentKey, u64)>,
}
impl Copier<'_> {
    fn flush(&mut self) -> io::Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        (self.check)()?;
        let mut fragment = CheckpointFragment {
            version: 1,
            checkpoint_id: self.id.clone(),
            kind: CheckpointFragment::PAYLOAD,
            ordinal: self.ordinal,
            data: std::mem::take(&mut self.data),
        };
        let record = self
            .log
            .append_decided_batch(&[DecidedMutation::CheckpointFragment(&fragment)])?;
        let [extent] = record.batch.payloads.as_slice() else {
            return Err(invalid(
                "compaction fragment did not produce one bounded extent",
            ));
        };
        if extent.length != fragment.data.len() as u64 {
            return Err(invalid("compaction payload length changed"));
        }
        let extent = Arc::new(extent.clone());
        for piece in self.pending.drain(..) {
            self.moved.entry(piece.key).or_default().push(Movement {
                start: piece.start,
                end: piece.start + piece.length,
                extent: extent.clone(),
                offset: piece.offset,
            });
        }
        fragment.data.clear();
        self.data = fragment.data;
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("compaction ordinal overflow"))?;
        Ok(())
    }
    fn copy(&mut self, key: ExtentKey, source: &Source, budget: &mut u64) -> io::Result<()> {
        for &(start, end) in &source.ranges {
            let mut at = start;
            while at < end && *budget != 0 {
                (self.check)()?;
                let mut length = (end - at)
                    .min(*budget)
                    .min((CheckpointFragment::MAX_DATA_BYTES - self.data.len()) as u64);
                let offset = self.data.len() as u64;
                if length < 4096 {
                    let sector = at / 4096 * 4096;
                    length = length.min(4096 - (at - sector));
                    if self.cached_sector != Some((key, sector)) {
                        self.cached_sector = None;
                        self.sector.clear();
                        self.log.read_payload_range_append(
                            &source.extent,
                            sector,
                            (source.extent.length - sector).min(4096),
                            &mut self.sector,
                        )?;
                        self.cached_sector = Some((key, sector));
                    }
                    let from = (at - sector) as usize;
                    self.data
                        .extend_from_slice(&self.sector[from..from + length as usize]);
                } else {
                    self.log.read_payload_range_append(
                        &source.extent,
                        at,
                        length,
                        &mut self.data,
                    )?;
                }
                self.pending.push(Pending {
                    key,
                    start: at,
                    length,
                    offset,
                });
                at += length;
                *budget -= length;
                if self.data.len() == CheckpointFragment::MAX_DATA_BYTES {
                    self.flush()?;
                }
            }
            if *budget == 0 {
                break;
            }
        }
        Ok(())
    }
}
/// Copy only selected physical byte unions, packing small ranges together. A
/// bounded prefix can be moved in each pass; unmoved spans keep their old home.
#[cfg(all(test, unix))]
fn relocate(
    log: &SegmentedOutcomeLog,
    catalog: &mut PageCatalog,
    selected: [u8; 16],
    dirty: &mut BTreeSet<PageId>,
    budget: u64,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<u64> {
    relocate_with_movements(log, catalog, selected, dirty, budget, check).map(|(bytes, _)| bytes)
}

fn relocate_with_movements(
    log: &SegmentedOutcomeLog,
    catalog: &mut PageCatalog,
    selected: [u8; 16],
    dirty: &mut BTreeSet<PageId>,
    budget: u64,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<(u64, BTreeMap<ExtentKey, PayloadRelocation>)> {
    relocate_pages_with_movements(log, catalog, selected, dirty, None, budget, check)
}

fn relocate_pages_with_movements(
    log: &SegmentedOutcomeLog,
    catalog: &mut PageCatalog,
    selected: [u8; 16],
    dirty: &mut BTreeSet<PageId>,
    owners: Option<&[PageId]>,
    mut budget: u64,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<(u64, BTreeMap<ExtentKey, PayloadRelocation>)> {
    if budget == 0 {
        return Ok((0, BTreeMap::new()));
    }
    let initial = budget;
    let mut sources: BTreeMap<ExtentKey, Source> = BTreeMap::new();
    let mut collect = |page: &CatalogPage| -> io::Result<()> {
        check()?;
        page.validate_spans()?;
        for span in &page.spans {
            let key = extent_key(&span.extent)?;
            if key.0 != selected {
                continue;
            }
            let source = sources.entry(key).or_insert_with(|| Source {
                extent: span.extent.clone(),
                ranges: Vec::new(),
            });
            if source.extent != span.extent {
                return Err(invalid("conflicting compaction descriptors"));
            }
            source
                .ranges
                .push((span.extent_offset, span.extent_offset + span.length));
        }
        Ok(())
    };
    if let Some(owners) = owners {
        for id in owners {
            collect(
                catalog
                    .pages
                    .get(id)
                    .ok_or_else(|| invalid("compaction owner is missing"))?,
            )?;
        }
    } else {
        for page in catalog.pages.values() {
            collect(page)?;
        }
    }
    if sources.is_empty() {
        return Ok((0, BTreeMap::new()));
    }
    let mut id = vec![0; 16];
    SystemRandom::new()
        .fill(&mut id)
        .map_err(|_| invalid("compaction identity generation failed"))?;
    let mut copier = Copier {
        log,
        check,
        id,
        ordinal: 0,
        data: Vec::with_capacity(CheckpointFragment::MAX_DATA_BYTES + 2 * 4096),
        pending: Vec::new(),
        moved: BTreeMap::new(),
        sector: Vec::with_capacity(4096),
        cached_sector: None,
    };
    for (key, source) in &mut sources {
        check()?;
        normalize(&mut source.ranges)?;
        copier.copy(*key, source, &mut budget)?;
        if budget == 0 {
            break;
        }
    }
    copier.flush()?;
    let mut update =
        |id: &PageId, page: &mut CatalogPage| -> io::Result<()> {
            check()?;
            if !page.spans.iter().any(|span| {
                extent_key(&span.extent).is_ok_and(|key| copier.moved.contains_key(&key))
            }) {
                return Ok(());
            }
            let mut spans = Vec::new();
            let mut changed = false;
            for span in &page.spans {
                match copier.moved.get(&extent_key(&span.extent)?) {
                    Some(moved) => changed |= translate(span, moved, &mut spans),
                    None => push_span(&mut spans, span.clone()),
                }
            }
            if changed {
                page.spans = spans;
                page.validate_spans()?;
                dirty.insert(id.clone());
            }
            Ok(())
        };
    if let Some(owners) = owners {
        for id in owners {
            update(
                id,
                catalog
                    .pages
                    .get_mut(id)
                    .ok_or_else(|| invalid("compaction owner is missing"))?,
            )?;
        }
    } else {
        for (id, page) in &mut catalog.pages {
            update(id, page)?;
        }
    }
    let moved = copier
        .moved
        .into_iter()
        .map(|(key, ranges)| {
            let source = sources[&key].extent.as_ref().clone();
            (key, PayloadRelocation { source, ranges })
        })
        .collect();
    Ok((initial - budget, moved))
}

pub(super) struct Plan {
    selected: Option<CompactionSegment>,
    unreferenced: usize,
}
impl Plan {
    pub fn has_work(&self) -> bool {
        self.selected.is_some() || self.unreferenced != 0
    }
}

/// Classify a rotating candidate batch without writing journal records. The
/// caller holds a read pin through analysis and any replacement publication.
pub(super) fn plan(
    log: &SegmentedOutcomeLog,
    state: &mut CheckpointState,
    cut: &StoredOutcomePosition,
    epoch: u64,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<Plan> {
    plan_with_cache(log, state, cut, epoch, true, check)
}
fn plan_with_cache(
    log: &SegmentedOutcomeLog,
    state: &mut CheckpointState,
    cut: &StoredOutcomePosition,
    epoch: u64,
    use_cache: bool,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<Plan> {
    check()?;
    if state.index.is_none() {
        return Ok(Plan {
            selected: None,
            unreferenced: 0,
        });
    }
    let candidates = log.compaction_candidates(cut, state.compaction_cursor, SAMPLE_SEGMENTS)?;
    state.compaction_cursor = candidates.last().map(|candidate| candidate.number);
    let cached = use_cache && state.index.as_ref().unwrap().ordinary_coverage();
    let mut ranges: BTreeMap<_, Vec<_>> = BTreeMap::new();
    state.index.as_mut().unwrap().refresh_planned_payloads(
        &state.catalog, &state.dirty_pages, check,
    )?;
    let payload_bytes = state.index.as_ref().unwrap()
        .planned_payload_lower_bound(state.catalog.pages.len())?;
    let within_payload_budget = u128::from(log.logical_length()?) <= u128::from(payload_bytes) * 2;
    if !cached {
        ranges = candidates
            .iter()
            .map(|candidate| (candidate.store_id, Vec::new()))
            .collect();
        state.index.as_ref().unwrap().compaction_metadata_ranges(
            log,
            &candidates,
            epoch,
            &mut ranges,
            check,
        )?;
        for page in state.catalog.pages.values() {
            check()?;
            page.validate_spans()?;
            for span in &page.spans {
                if let Some(ranges) = ranges.get_mut(&extent_key(&span.extent)?.0) {
                    let start = span
                        .extent
                        .offset
                        .checked_add(span.extent_offset)
                        .ok_or_else(|| invalid("compaction payload offset overflow"))?;
                    ranges.push((
                        start,
                        start
                            .checked_add(span.length)
                            .ok_or_else(|| invalid("compaction payload end overflow"))?,
                    ));
                }
            }
        }
    }
    let mut selected: Option<(CompactionSegment, u64)> = None;
    let mut unreferenced = 0;
    for candidate in candidates {
        check()?;
        let live = if cached {
            state
                .index
                .as_ref()
                .unwrap()
                .planned_live_bytes(&candidate.store_id, candidate.length)?
        } else {
            let mut spans = ranges.remove(&candidate.store_id).unwrap();
            let live = normalize(&mut spans)?;
            if spans
                .iter()
                .any(|(start, end)| *start < 4096 || *end > candidate.length)
            {
                return Err(invalid("compaction coverage exceeds its segment"));
            }
            live
        };
        let bytes = candidate.length - 4096;
        // Conservative whole-record metadata charges must leave at least half
        // the file obsolete before maintenance spends copying I/O.
        if live == 0 {
            unreferenced += 1;
            continue;
        }
        if u128::from(live) * 2 > u128::from(bytes) {
            continue;
        }
        if selected.as_ref().is_none_or(|(old, old_live)| {
            u128::from(live) * u128::from(old.length - 4096)
                < u128::from(*old_live) * u128::from(bytes)
        }) {
            selected = Some((candidate, live));
        }
    }
    // Whole-file retirement still proceeds. Copy live bytes only when the
    // journal exceeds the same two-to-one byte budget as segment eligibility.
    // The payload bound excludes metadata and cannot exceed its physical union.
    Ok(Plan {
        selected: if within_payload_budget { None } else { selected.map(|(segment, _)| segment) },
        unreferenced,
    })
}

pub(super) fn apply(
    log: &SegmentedOutcomeLog,
    state: &mut CheckpointState,
    plan: Plan,
    check: &dyn Fn() -> io::Result<()>,
) -> io::Result<BTreeMap<ExtentKey, PayloadRelocation>> {
    let Some(selected) = plan.selected else {
        return Ok(BTreeMap::new());
    };
    let index = state.index.as_mut().unwrap();
    let owners = if index.ordinary_coverage() {
        // Apply may follow a full-scan plan or additional dirty page updates.
        index.refresh_planned_payloads(&state.catalog, &state.dirty_pages, check)?;
        Some(index.planned_payload_pages(&selected.store_id, state.catalog.pages.len())?)
    } else {
        None
    };
    let (_, moved) = if let Some(owners) = &owners {
        relocate_pages_with_movements(
            log,
            &mut state.catalog,
            selected.store_id,
            &mut state.dirty_pages,
            Some(owners),
            COPY_BYTES,
            check,
        )?
    } else {
        relocate_with_movements(
            log,
            &mut state.catalog,
            selected.store_id,
            &mut state.dirty_pages,
            COPY_BYTES,
            check,
        )?
    };
    // Unmoved extents still have catalog owners, so pruning them cannot release
    // anything. Partial moves keep their remaining descriptor owners as well.
    let old_keys = moved.keys().copied().collect();
    let index = state.index.as_mut().unwrap();
    index.prune(&old_keys)?;
    index.evacuate_metadata(log, &[selected], &mut state.dirty_pages, check)?;
    Ok(moved)
}

#[cfg(all(test, unix))]
#[path = "replica_checkpoint_compaction_tests.rs"]
mod tests;
