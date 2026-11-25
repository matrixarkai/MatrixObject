// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable page locators, independent of the replica's resident payload cache.
use super::{
    replica_checkpoint_catalog::extent_key,
    replica_checkpoint_spans::{CatalogPage, CatalogPageMetadata, PageCatalog},
    replica_payload_relocation::{push_span, translate, PayloadMovement},
};
use crate::durable_payload::{
    outcome_log::StoredRecordMetadata,
    outcome_segments::{PayloadRangeReader, SegmentedOutcomeLog},
    PayloadExtent,
};
use crate::{PageId, PageState};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{Arc, Mutex, MutexGuard},
};

type ExtentKey = ([u8; 16], u64);
fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
fn segments(page: &CatalogPage) -> io::Result<BTreeSet<Vec<u8>>> {
    page.spans
        .iter()
        .map(|span| Ok(extent_key(&span.extent)?.0.to_vec()))
        .collect()
}

// Extent keys sort by segment first, so adjacent keys share one ownership scan.
fn relocation_segments(
    moved: &BTreeMap<ExtentKey, PayloadRelocation>,
) -> impl Iterator<Item = &[u8; 16]> {
    let mut previous = None;
    moved.keys().filter_map(move |key| {
        if previous == Some(&key.0) {
            None
        } else {
            previous = Some(&key.0);
            Some(&key.0)
        }
    })
}

fn local_descriptor(
    descriptor: &PayloadExtent,
    local: &mut BTreeMap<ExtentKey, Arc<PayloadExtent>>,
) -> io::Result<Arc<PayloadExtent>> {
    let key = extent_key(descriptor)?;
    if let Some(existing) = local.get(&key) {
        if existing.as_ref() != descriptor {
            return Err(invalid("conflicting backing payload descriptors"));
        }
        return Ok(existing.clone());
    }
    let owned = Arc::new(descriptor.clone());
    local.insert(key, owned.clone());
    Ok(owned)
}

struct State {
    catalog: PageCatalog,
    by_segment: BTreeMap<Vec<u8>, BTreeSet<PageId>>,
    bytes: u64,
    failed: Option<String>,
}
impl State {
    fn fold_append_batch(&mut self, record: &StoredRecordMetadata) -> io::Result<bool> {
        // Track successive lengths for repeated decisions on one page. The
        // classification completes before any catalog metadata is changed.
        let mut originals = BTreeMap::new();
        for decision in &record.batch.decisions {
            let Some(page) = &decision.page else { continue };
            let id = page.page_id().map_err(invalid)?;
            let entry = originals.entry(id.clone()).or_insert_with(|| {
                self.catalog.pages.get(&id).map_or((0, 0, 0), |old| {
                    (
                        old.spans.len(),
                        old.metadata.data_length,
                        old.metadata.data_length,
                    )
                })
            });
            if page.data_length < entry.2 || page.patches.iter().any(|patch| patch.offset < entry.2)
            {
                return Ok(false);
            }
            entry.2 = page.data_length;
        }
        // Normal fold validation, including each prior checksum, still applies.
        // Failure fences the backing, so partially applied metadata is hidden.
        self.catalog.fold(record)?;
        for (id, (first, before, after)) in originals {
            let page = self
                .catalog
                .pages
                .get(&id)
                .ok_or_else(|| invalid("appended page is missing"))?;
            if page.metadata.data_length != after {
                return Err(invalid("appended page length differs"));
            }
            page.validate_span_suffix(first)?;
            let added: BTreeSet<_> = page.spans[first..]
                .iter()
                .map(|span| Ok(extent_key(&span.extent)?.0.to_vec()))
                .collect::<io::Result<_>>()?;
            let bytes = self
                .bytes
                .checked_sub(before)
                .and_then(|n| n.checked_add(after))
                .ok_or_else(|| invalid("backing byte accounting overflow"))?;
            for segment in added {
                self.by_segment
                    .entry(segment)
                    .or_default()
                    .insert(id.clone());
            }
            self.bytes = bytes;
        }
        Ok(true)
    }

    fn replace(&mut self, id: PageId, page: CatalogPage) -> io::Result<()> {
        if page.metadata.volume_id != id.chunk_id.volume_id()
            || page.metadata.chunk_id != id.chunk_id.id()
            || page.metadata.page_index != id.page_index
        {
            return Err(invalid("backing page identity differs"));
        }
        let old = self.catalog.pages.get(&id);
        let previous = old.map_or(0, |page| page.metadata.data_length);
        let bytes = self
            .bytes
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(page.metadata.data_length))
            .ok_or_else(|| invalid("backing byte accounting overflow"))?;
        let before = old.map(segments).transpose()?.unwrap_or_default();
        let after = segments(&page)?;
        for segment in before.difference(&after) {
            let pages = self
                .by_segment
                .get_mut(segment)
                .ok_or_else(|| invalid("backing segment membership is missing"))?;
            if !pages.remove(&id) {
                return Err(invalid("backing page membership is missing"));
            }
            if pages.is_empty() {
                self.by_segment.remove(segment);
            }
        }
        for segment in after.difference(&before) {
            self.by_segment
                .entry(segment.clone())
                .or_default()
                .insert(id.clone());
        }
        self.catalog.pages.insert(id, page);
        self.bytes = bytes;
        Ok(())
    }
}

/// Physical sources copied before a checkpoint publication. The source
/// descriptor must still agree with each live span being translated.
pub(super) struct PayloadRelocation {
    pub source: PayloadExtent,
    pub ranges: Vec<PayloadMovement>,
}

pub(super) struct LivePageBacking {
    log: Arc<SegmentedOutcomeLog>,
    state: Mutex<State>,
}

/// Own every selected file before releasing the catalog's publication pin.
/// This plan performs no later lookup through a possibly retired manifest.
pub(super) struct PageReadPlan {
    page: CatalogPage,
    readers: BTreeMap<Vec<u8>, PayloadRangeReader>,
    offset: u64,
    length: u64,
}
impl PageReadPlan {
    pub fn metadata(&self) -> &CatalogPageMetadata {
        &self.page.metadata
    }
    pub fn read(&self) -> io::Result<Vec<u8>> {
        self.page
            .read_data_range(self.offset, self.length, |extent, offset, length| {
                self.readers
                    .get(&extent.store_id)
                    .ok_or_else(|| invalid("backing read source was not captured"))?
                    .read(extent, offset, length)
            })
    }
    pub fn materialize(&self) -> io::Result<PageState> {
        if self.offset != 0 || self.length != self.page.metadata.data_length {
            return Err(invalid("partial backing plan cannot materialize a page"));
        }
        self.page.materialize(|extent, offset, length| {
            self.readers
                .get(&extent.store_id)
                .ok_or_else(|| invalid("backing read source was not captured"))?
                .read(extent, offset, length)
        })
    }
}

impl LivePageBacking {
    pub fn new(log: Arc<SegmentedOutcomeLog>, catalog: PageCatalog) -> io::Result<Self> {
        catalog.validate_log_pages()?;
        let mut state = State {
            catalog: PageCatalog::default(),
            by_segment: BTreeMap::new(),
            bytes: 0,
            failed: None,
        };
        state.catalog.logs = catalog.logs;
        // The index prunes using descriptor ownership within its checkpoint
        // worker. Keep backing/read-plan handles in a separate descriptor
        // domain, while interning aliases within this catalog.
        let mut local = BTreeMap::new();
        for (id, mut page) in catalog.pages {
            for span in &mut page.spans {
                span.extent = local_descriptor(&span.extent, &mut local)?;
            }
            page.validate_spans()?;
            state.replace(id, page)?;
        }
        Ok(Self {
            log,
            state: Mutex::new(state),
        })
    }

    /// The replacement is built before durable publication. Install it under
    /// the publication gate, preserving the shared backing identity and any
    /// previously captured file owners.
    pub fn replace_from(&self, replacement: Self) -> io::Result<()> {
        if !Arc::ptr_eq(&self.log, &replacement.log) {
            return Err(invalid("backing replacement uses another log"));
        }
        let next = replacement
            .state
            .into_inner()
            .map_err(|_| invalid("replacement backing is poisoned"))?;
        *self.lock()? = next;
        Ok(())
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid("backing catalog is poisoned"))?;
        if let Some(message) = &state.failed {
            return Err(invalid(format!("backing catalog failed: {message}")));
        }
        Ok(state)
    }

    /// Called only with a synchronized foreground record, before its pages can
    /// leave the resident cache. Failure fences this catalog until recovery.
    pub fn fold(&self, record: &StoredRecordMetadata) -> io::Result<()> {
        let mut state = self.lock()?;
        let result: io::Result<()> = (|| {
            if state.fold_append_batch(record)? {
                return Ok(());
            }
            let mut changed = BTreeSet::new();
            for decision in &record.batch.decisions {
                if let Some(page) = &decision.page {
                    changed.insert(page.page_id().map_err(invalid)?);
                }
            }
            // Only touched page metadata is staged; never clone the corpus.
            let mut working = PageCatalog {
                logs: state.catalog.logs.clone(),
                ..PageCatalog::default()
            };
            for id in &changed {
                if let Some(page) = state.catalog.pages.get(id) {
                    working.pages.insert(id.clone(), page.clone());
                }
            }
            working.fold(record)?;
            for page in working.pages.values() {
                page.validate_spans()?;
                segments(page)?;
            }
            state.catalog.logs = working.logs;
            for (id, page) in working.pages {
                state.replace(id, page)?;
            }
            Ok(())
        })();
        if let Err(error) = &result {
            state.failed = Some(error.to_string());
        }
        result
    }

    /// The caller pins retention before this capture and through replacement
    /// publication, since this metadata alone does not own physical files.
    pub fn catalog_snapshot(&self) -> io::Result<PageCatalog> {
        let state = self.lock()?;
        let mut pages = state.catalog.pages.clone();
        let logs = state.catalog.logs.clone();
        drop(state);
        let mut local = BTreeMap::new();
        for page in pages.values_mut() {
            for span in &mut page.spans {
                span.extent = local_descriptor(&span.extent, &mut local)?;
            }
        }
        Ok(PageCatalog { pages, logs })
    }
    pub fn sequences_after(
        &self,
        after: Option<&PageId>,
        limit: usize,
    ) -> io::Result<Vec<(PageId, u64)>> {
        use std::ops::Bound::{Excluded, Unbounded};
        let state = self.lock()?;
        Ok(state
            .catalog
            .pages
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .take(limit)
            .map(|(id, page)| (id.clone(), page.metadata.write_sequence))
            .collect())
    }
    pub fn totals(&self) -> io::Result<(usize, u64)> {
        let state = self.lock()?;
        Ok((state.catalog.pages.len(), state.bytes))
    }
    pub fn metadata(&self, id: &PageId) -> io::Result<Option<CatalogPageMetadata>> {
        Ok(self
            .lock()?
            .catalog
            .pages
            .get(id)
            .map(|page| page.metadata.clone()))
    }
    pub fn protected_segments(&self) -> io::Result<BTreeSet<Vec<u8>>> {
        Ok(self.lock()?.by_segment.keys().cloned().collect())
    }
    pub fn capture_range(
        &self,
        id: &PageId,
        offset: u64,
        length: u64,
    ) -> io::Result<Option<PageReadPlan>> {
        self.capture(id, Some((offset, length)))
    }
    pub fn capture_page(&self, id: &PageId) -> io::Result<Option<PageReadPlan>> {
        self.capture(id, None)
    }

    fn capture(&self, id: &PageId, range: Option<(u64, u64)>) -> io::Result<Option<PageReadPlan>> {
        let _pin = self.log.checkpoint_read_pin()?;
        let state = self.lock()?;
        let Some(page) = state.catalog.pages.get(id) else {
            return Ok(None);
        };
        let (offset, length) = range.unwrap_or((0, page.metadata.data_length));
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("backing range overflow"))?;
        if end > page.metadata.data_length {
            return Err(invalid("backing range exceeds page data"));
        }
        let first = page
            .spans
            .partition_point(|span| span.offset + span.length <= offset);
        let last = page
            .spans
            .partition_point(|span| span.offset < end)
            .max(first);
        let spans = if length == 0 {
            Vec::new()
        } else {
            page.spans[first..last].to_vec()
        };
        let mut readers = BTreeMap::new();
        for span in &spans {
            if !readers.contains_key(&span.extent.store_id) {
                readers.insert(
                    span.extent.store_id.clone(),
                    self.log.payload_range_reader(&span.extent.store_id)?,
                );
            }
        }
        Ok(Some(PageReadPlan {
            page: CatalogPage {
                metadata: page.metadata.clone(),
                spans,
            },
            readers,
            offset,
            length,
        }))
    }

    /// Translate current locators after replacement publication and before
    /// collection. A newer partial mutation still retains old untouched spans.
    pub fn rebase(&self, moved: &BTreeMap<ExtentKey, PayloadRelocation>) -> io::Result<usize> {
        let mut state = self.lock()?;
        let result: io::Result<usize> = (|| {
            let mut affected = BTreeSet::new();
            let mut local = BTreeMap::new();
            let mut localized = BTreeMap::new();
            for (key, movement) in moved {
                if extent_key(&movement.source)? != *key {
                    return Err(invalid("backing relocation source identity differs"));
                }
                let mut previous = 0;
                for range in &movement.ranges {
                    if range.start < previous
                        || range.start >= range.end
                        || range.end > movement.source.length
                        || range
                            .offset
                            .checked_add(range.end - range.start)
                            .is_none_or(|end| end > range.extent.length)
                    {
                        return Err(invalid("invalid backing relocation bounds"));
                    }
                    extent_key(&range.extent)?;
                    previous = range.end;
                }
                let ranges = movement
                    .ranges
                    .iter()
                    .map(|range| {
                        Ok(PayloadMovement {
                            start: range.start,
                            end: range.end,
                            extent: local_descriptor(&range.extent, &mut local)?,
                            offset: range.offset,
                        })
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                localized.insert(*key, ranges);
            }
            for segment in relocation_segments(moved) {
                if let Some(pages) = state.by_segment.get(segment.as_slice()) {
                    affected.extend(pages.iter().cloned());
                }
            }
            let mut changed = Vec::new();
            for id in affected {
                let page = state
                    .catalog
                    .pages
                    .get(&id)
                    .ok_or_else(|| invalid("backing page is missing"))?;
                let mut spans = Vec::new();
                let mut replaced = false;
                for span in &page.spans {
                    if let Some(movement) = moved.get(&extent_key(&span.extent)?) {
                        if span.extent.as_ref() != &movement.source {
                            return Err(invalid("backing relocation source descriptor differs"));
                        }
                        replaced |=
                            translate(span, &localized[&extent_key(&span.extent)?], &mut spans);
                    } else {
                        push_span(&mut spans, span.clone());
                    }
                }
                if replaced {
                    let next = CatalogPage {
                        metadata: page.metadata.clone(),
                        spans,
                    };
                    next.validate_spans()?;
                    changed.push((id, next));
                }
            }
            let count = changed.len();
            for (id, page) in changed {
                state.replace(id, page)?;
            }
            Ok(count)
        })();
        if let Err(error) = &result {
            state.failed = Some(error.to_string());
        }
        result
    }
}

#[cfg(all(test, unix))]
#[path = "replica_page_backing_tests.rs"]
mod tests;
