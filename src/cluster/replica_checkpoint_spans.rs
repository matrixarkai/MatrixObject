// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Metadata-only page reconstruction from immutable stored outcome payloads.
//! The checkpoint worker owns this catalog; the append path must not clone it.
use crate::durable_payload::{outcome_log::StoredRecordMetadata, PayloadExtent};
use crate::{
    outcome::{DiscardRange, PageOutcome},
    DataRange, PageId, PageState,
};
use prost::Message;
use std::{collections::BTreeMap, io, sync::Arc};

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Clone, Debug)]
pub(super) struct PayloadSpan {
    pub offset: u64,
    pub length: u64,
    pub extent: Arc<PayloadExtent>,
    pub extent_offset: u64,
}
impl PayloadSpan {
    fn slice(&self, start: u64, end: u64) -> Self {
        Self {
            offset: start,
            length: end - start,
            extent: self.extent.clone(),
            extent_offset: self.extent_offset + start - self.offset,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct CatalogPageMetadata {
    #[prost(uint64, tag = "1")]
    pub volume_id: u64,
    #[prost(uint64, tag = "2")]
    pub chunk_id: u64,
    #[prost(uint64, tag = "3")]
    pub page_index: u64,
    #[prost(uint64, tag = "4")]
    pub logical_length: u64,
    #[prost(uint64, tag = "5")]
    pub write_sequence: u64,
    #[prost(fixed64, tag = "6")]
    pub data_checksum: u64,
    #[prost(uint64, tag = "7")]
    pub data_length: u64,
    #[prost(message, repeated, tag = "8")]
    pub discarded_ranges: Vec<DiscardRange>,
    #[prost(string, tag = "9")]
    pub state_checksum: String,
}
impl CatalogPageMetadata {
    pub fn head(&self, id: &PageId) -> crate::ReplicaHead {
        let mut checksum = String::with_capacity(24);
        crate::append_etag_of_digest(&mut checksum, self.data_checksum);
        let discarded_ranges: Vec<_> = self
            .discarded_ranges
            .iter()
            .map(|r| DataRange {
                offset: r.offset,
                length: r.length,
            })
            .collect();
        crate::ReplicaHead {
            logical_length: self.logical_length,
            write_sequence: self.write_sequence,
            state_checksum: crate::chunk_state_handoff_checksum_from_parts(
                id,
                self.logical_length,
                self.write_sequence,
                &checksum,
                &discarded_ranges,
            ),
            checksum,
            discarded_ranges,
        }
    }
    pub fn from_state(id: &PageId, state: &PageState) -> io::Result<Self> {
        let etag = crate::validate_chunk_state_with_data_etag(state)
            .map_err(|e| invalid(&e.to_string()))?;
        Ok(Self {
            volume_id: id.chunk_id.volume_id(),
            chunk_id: id.chunk_id.id(),
            page_index: id.page_index,
            logical_length: state.logical_length,
            write_sequence: state.write_sequence,
            data_checksum: state.checksum_fnv1a64,
            data_length: state.data.len() as u64,
            discarded_ranges: state
                .discarded_ranges
                .iter()
                .map(|r| DiscardRange {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
            state_checksum: crate::chunk_local_state_checksum_with_data_etag(state, &etag),
        })
    }
}
impl From<&PageOutcome> for CatalogPageMetadata {
    fn from(page: &PageOutcome) -> Self {
        Self {
            volume_id: page.volume_id,
            chunk_id: page.chunk_id,
            page_index: page.page_index,
            logical_length: page.logical_length,
            write_sequence: page.write_sequence,
            data_checksum: page.data_checksum,
            data_length: page.data_length,
            discarded_ranges: page.discarded_ranges.clone(),
            state_checksum: page.after_checksum.clone(),
        }
    }
}
#[derive(Clone)]
pub(super) struct CatalogPage {
    pub metadata: CatalogPageMetadata,
    pub spans: Vec<PayloadSpan>,
}

/// Owned exclusively by a checkpoint/recovery worker, never by an RPC snapshot.
#[derive(Default)]
pub(super) struct PageCatalog {
    pub pages: BTreeMap<PageId, CatalogPage>,
    pub logs: super::log_page_catalog::LogPageCatalog,
}

impl PageCatalog {
    pub fn validate_log_pages(&self) -> io::Result<()> {
        for (chunk, entry) in &self.logs.chunks {
            entry.validate()?;
            let bytes = u64::from(entry.state.page_bytes);
            let length = entry.state.length;
            let first = PageId {
                chunk_id: chunk.clone(),
                page_index: 0,
            };
            let mut expected = 0;
            for (id, page) in self
                .pages
                .range(first..)
                .take_while(|(id, _)| &id.chunk_id == chunk)
            {
                let start = id
                    .page_index
                    .checked_mul(bytes)
                    .ok_or_else(|| invalid("log page index overflow"))?;
                let want = length.saturating_sub(start).min(bytes);
                if page.metadata.data_length != want
                    || page.metadata.logical_length != want
                    || !page.metadata.discarded_ranges.is_empty()
                    || (want != 0 && id.page_index != expected)
                {
                    return Err(invalid(
                        "log checkpoint page coverage differs from authority",
                    ));
                }
                if want != 0 {
                    expected += 1;
                }
            }
            if expected != length.div_ceil(bytes) {
                return Err(invalid("log checkpoint omits a committed page"));
            }
        }
        Ok(())
    }
    /// Fold a validated stored record without loading any payload. On error the
    /// caller discards this working catalog, just as it discards partial replay.
    pub fn fold(&mut self, record: &StoredRecordMetadata) -> io::Result<()> {
        if !matches!(record.batch.version, 1 | 3 | 4) {
            return Err(invalid("unsupported checkpoint outcome batch"));
        }
        for decision in &record.batch.decisions {
            if let Some(log) = &decision.log_page {
                log.validate().map_err(|e| invalid(&e.to_string()))?;
                if log.kind == crate::log_page_outcome::LOG_PAGE_INITIALIZE {
                    let chunk = crate::ChunkId::new(log.volume_id, log.chunk_id)
                        .map_err(|e| invalid(&e.to_string()))?;
                    let first = PageId {
                        chunk_id: chunk.clone(),
                        page_index: 0,
                    };
                    if self
                        .pages
                        .range(first..)
                        .next()
                        .is_some_and(|(id, _)| id.chunk_id == chunk)
                    {
                        return Err(invalid(
                            "log initialization reuses a populated page namespace",
                        ));
                    }
                }
            }
            if let Some(page) = &decision.page {
                let id = page.page_id().map_err(|e| invalid(&e.to_string()))?;
                let transition = record
                    .batch
                    .decisions
                    .iter()
                    .filter_map(|decision| decision.log_page.as_ref())
                    .find(|log| log.volume_id == page.volume_id && log.chunk_id == page.chunk_id);
                if self.logs.chunks.contains_key(&id.chunk_id) || transition.is_some() {
                    let log = transition
                        .ok_or_else(|| invalid("log page mutation omits chunk transition"))?;
                    match log.kind {
                        crate::log_page_outcome::LOG_PAGE_APPEND => {}
                        crate::log_page_outcome::LOG_PAGE_FENCE
                            if page.patches.is_empty()
                                && self.pages.get(&id).is_some_and(|old| {
                                    page.data_length <= old.metadata.data_length
                                }) => {}
                        _ => return Err(invalid("log transition cannot replace page contents")),
                    }
                }
            }
            if let Some(fence) = &decision.namespace_fence {
                if let Some(after) = &fence.after {
                    let id = crate::ChunkId::new(after.volume_id, after.chunk_id)
                        .map_err(|e| invalid(&e.to_string()))?;
                    if self.logs.chunks.contains_key(&id) {
                        return Err(invalid("page namespace fence cannot replace log authority"));
                    }
                }
            }
        }
        let extents: Vec<_> = record
            .batch
            .payloads
            .iter()
            .cloned()
            .map(Arc::new)
            .collect();
        for decision in &record.batch.decisions {
            if let Some(log) = &decision.log_page {
                self.logs.fold(log)?;
            }
            let Some(page) = &decision.page else { continue };
            let id = page.page_id().map_err(|e| invalid(&e.to_string()))?;
            if page.version != 1
                || page.data_length > 256 * 1024 * 1024
                || page.patches.len() != decision.patch_lengths.len()
                || page.physical_deltas.len() != 4
            {
                return Err(invalid("invalid checkpoint page metadata"));
            }
            let previous = self.pages.get(&id);
            let before = previous.map_or("", |page| page.metadata.state_checksum.as_str());
            if before != page.before_checksum {
                return Err(invalid("checkpoint page prior state mismatch"));
            }
            let mut patch_ranges = Vec::with_capacity(page.patches.len());
            let mut replacements = Vec::with_capacity(usize::from(!page.patches.is_empty()));
            let mut virtual_offset = decision.data_offset;
            let mut prior_end = 0;
            for (patch, &length) in page.patches.iter().zip(&decision.patch_lengths) {
                let end = patch
                    .offset
                    .checked_add(u64::from(length))
                    .ok_or_else(|| invalid("checkpoint patch overflow"))?;
                if length == 0
                    || !patch.data.is_empty()
                    || patch.offset < prior_end
                    || end > page.data_length
                {
                    return Err(invalid("invalid checkpoint patch range"));
                }
                patch_ranges.push((patch.offset, end));
                map_payload(
                    &extents,
                    virtual_offset,
                    patch.offset,
                    u64::from(length),
                    &mut replacements,
                )?;
                virtual_offset = virtual_offset
                    .checked_add(u64::from(length))
                    .ok_or_else(|| invalid("checkpoint payload overflow"))?;
                prior_end = end;
            }
            if previous.is_some_and(|old| {
                page.data_length >= old.metadata.data_length
                    && patch_ranges
                        .iter()
                        .all(|(start, _)| *start >= old.metadata.data_length)
            }) {
                let current = self.pages.get_mut(&id).unwrap();
                // This record owns fresh descriptors. Coalescing can join its
                // own adjacent spans, but cannot change an older span prefix.
                for span in replacements {
                    super::replica_payload_relocation::push_span(&mut current.spans, span);
                }
                current.metadata = CatalogPageMetadata::from(page);
                continue;
            }
            let old = previous.map_or(&[][..], |page| page.spans.as_slice());
            let kept = retain_unmodified(old, &patch_ranges, page.data_length);
            let spans = merge_spans(kept, replacements);
            self.pages.insert(
                id,
                CatalogPage {
                    metadata: CatalogPageMetadata::from(page),
                    spans,
                },
            );
        }
        Ok(())
    }
}

/// Map a patch through the batch's contiguous virtual payload stream. Descriptor
/// Arcs are shared across pages/patches, including their sector digest vectors.
fn map_payload(
    extents: &[Arc<PayloadExtent>],
    from: u64,
    logical: u64,
    length: u64,
    out: &mut Vec<PayloadSpan>,
) -> io::Result<()> {
    let end = from
        .checked_add(length)
        .ok_or_else(|| invalid("checkpoint payload overflow"))?;
    let mut base = 0u64;
    let mut copied = 0;
    for extent in extents {
        let next = base
            .checked_add(extent.length)
            .ok_or_else(|| invalid("checkpoint extent overflow"))?;
        let start = from.max(base);
        let finish = end.min(next);
        if start < finish {
            out.push(PayloadSpan {
                offset: logical + start - from,
                length: finish - start,
                extent: extent.clone(),
                extent_offset: start - base,
            });
            copied += finish - start;
        }
        base = next;
        if base >= end {
            break;
        }
    }
    if copied != length {
        return Err(invalid("checkpoint patch exceeds stored payloads"));
    }
    Ok(())
}

/// Single ordered walk; a fragmented page must not cost a full scan per patch.
fn retain_unmodified(old: &[PayloadSpan], patches: &[(u64, u64)], length: u64) -> Vec<PayloadSpan> {
    let mut out = Vec::new();
    let mut patch = 0;
    for span in old {
        let end = (span.offset + span.length).min(length);
        let mut at = span.offset.min(end);
        while patch < patches.len() && patches[patch].1 <= at {
            patch += 1;
        }
        let mut cursor = patch;
        while cursor < patches.len() && patches[cursor].0 < end {
            let (start, finish) = patches[cursor];
            if start > at {
                out.push(span.slice(at, start.min(end)));
            }
            at = at.max(finish).min(end);
            if at == end {
                break;
            }
            cursor += 1;
        }
        if at < end {
            out.push(span.slice(at, end));
        }
        patch = cursor;
    }
    out
}

fn merge_spans(old: Vec<PayloadSpan>, updates: Vec<PayloadSpan>) -> Vec<PayloadSpan> {
    if old.is_empty() && updates.len() <= 1 {
        return updates;
    }
    let capacity = usize::from(!old.is_empty() || !updates.is_empty());
    let mut old = old.into_iter().peekable();
    let mut updates = updates.into_iter().peekable();
    let mut out: Vec<PayloadSpan> = Vec::with_capacity(capacity);
    while old.peek().is_some() || updates.peek().is_some() {
        let take_old = match (old.peek(), updates.peek()) {
            (Some(a), Some(b)) => a.offset < b.offset,
            (Some(_), None) => true,
            _ => false,
        };
        let span = if take_old {
            old.next().unwrap()
        } else {
            updates.next().unwrap()
        };
        if let Some(last) = out.last_mut() {
            if last.offset + last.length == span.offset
                && last.extent_offset + last.length == span.extent_offset
                && Arc::ptr_eq(&last.extent, &span.extent)
            {
                last.length += span.length;
                continue;
            }
        }
        out.push(span);
    }
    out
}

impl CatalogPage {
    /// Validate once when constructing or decoding an immutable catalog page.
    pub fn validate_spans(&self) -> io::Result<()> {
        self.validate_span_suffix(0)
    }

    /// An append preserves a previously validated prefix. Check its boundary
    /// and the new suffix without walking that prefix again.
    pub fn validate_span_suffix(&self, first: usize) -> io::Result<()> {
        if first > self.spans.len() {
            return Err(invalid("checkpoint span suffix exceeds page"));
        }
        if self.metadata.data_length > 256 * 1024 * 1024 {
            return Err(invalid("checkpoint page exceeds limit"));
        }
        let mut previous = if first == 0 {
            0
        } else {
            self.spans[first - 1]
                .offset
                .checked_add(self.spans[first - 1].length)
                .ok_or_else(|| invalid("checkpoint span overflow"))?
        };
        if previous > self.metadata.data_length {
            return Err(invalid("checkpoint span prefix exceeds page"));
        }
        for span in &self.spans[first..] {
            let end = span
                .offset
                .checked_add(span.length)
                .ok_or_else(|| invalid("checkpoint span overflow"))?;
            let physical_end = span
                .extent_offset
                .checked_add(span.length)
                .ok_or_else(|| invalid("checkpoint extent range overflow"))?;
            if span.length == 0
                || span.offset < previous
                || end > self.metadata.data_length
                || physical_end > span.extent.length
            {
                return Err(invalid("invalid checkpoint span bounds or order"));
            }
            previous = end;
        }
        Ok(())
    }

    /// Read a raw data range from a validated catalog page. The provider verifies
    /// immutable extent sectors. Zero-filled holes need no physical reads, and
    /// locating a small range does not walk every fragment of the page.
    pub fn read_data_range(
        &self,
        offset: u64,
        length: u64,
        mut read: impl FnMut(&PayloadExtent, u64, u64) -> io::Result<Vec<u8>>,
    ) -> io::Result<Vec<u8>> {
        if self.metadata.data_length > 256 * 1024 * 1024 || offset > self.metadata.data_length {
            return Err(invalid("checkpoint read offset exceeds page"));
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("checkpoint read overflow"))?
            .min(self.metadata.data_length);
        if offset == end {
            return Ok(Vec::new());
        }
        let first = self
            .spans
            .partition_point(|span| span.offset.saturating_add(span.length) <= offset);
        if let Some(span) = self.spans.get(first).filter(|span| span.offset <= offset) {
            let span_end = span
                .offset
                .checked_add(span.length)
                .ok_or_else(|| invalid("checkpoint span overflow"))?;
            if end <= span_end {
                let physical = span
                    .extent_offset
                    .checked_add(offset - span.offset)
                    .ok_or_else(|| invalid("checkpoint extent offset overflow"))?;
                let data = read(&span.extent, physical, end - offset)?;
                if data.len() as u64 != end - offset {
                    return Err(invalid("short checkpoint payload read"));
                }
                // Keep the old tightly sized result even if the provider read
                // padded sectors or reused a much larger allocation.
                return Ok(if data.capacity() == data.len() {
                    data
                } else {
                    data.into_boxed_slice().into_vec()
                });
            }
        }
        let mut bytes = vec![0; (end - offset) as usize];
        for span in &self.spans[first..] {
            if span.offset >= end {
                break;
            }
            let span_end = span
                .offset
                .checked_add(span.length)
                .ok_or_else(|| invalid("checkpoint span overflow"))?;
            let start = offset.max(span.offset);
            let finish = end.min(span_end);
            if start < finish {
                let physical = span
                    .extent_offset
                    .checked_add(start - span.offset)
                    .ok_or_else(|| invalid("checkpoint extent offset overflow"))?;
                let data = read(&span.extent, physical, finish - start)?;
                if data.len() as u64 != finish - start {
                    return Err(invalid("short checkpoint payload read"));
                }
                bytes[(start - offset) as usize..(finish - offset) as usize].copy_from_slice(&data);
            }
        }
        Ok(bytes)
    }

    /// Payload allocation is recovery/read work, never checkpoint capture work.
    pub fn materialize(
        &self,
        read: impl FnMut(&PayloadExtent, u64, u64) -> io::Result<Vec<u8>>,
    ) -> io::Result<PageState> {
        self.validate_spans()?;
        let bytes = self.read_data_range(0, self.metadata.data_length, read)?;
        let state = PageState {
            data: Arc::new(bytes),
            logical_length: self.metadata.logical_length,
            write_sequence: self.metadata.write_sequence,
            checksum_fnv1a64: self.metadata.data_checksum,
            ec_parity_fragment: None,
            ec_fragment_digests: None,
            discarded_ranges: self
                .metadata
                .discarded_ranges
                .iter()
                .map(|r| DataRange {
                    offset: r.offset,
                    length: r.length,
                })
                .collect(),
        };
        let data_etag = crate::validate_chunk_state_with_data_etag(&state)
            .map_err(|e| invalid(&e.to_string()))?;
        if crate::chunk_local_state_checksum_with_data_etag(&state, &data_etag)
            != self.metadata.state_checksum
        {
            return Err(invalid("checkpoint page resulting state mismatch"));
        }
        Ok(state)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        durable_payload::outcome_log::{DecidedMutation, DurableOutcomeLog},
        ChunkId,
    };
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Dir(PathBuf);
    impl Dir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "replica-catalog-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn id(page: u64) -> PageId {
        PageId {
            chunk_id: ChunkId::new(1, 1).unwrap(),
            page_index: page,
        }
    }
    fn state(data: Vec<u8>, sequence: u64) -> PageState {
        PageState {
            logical_length: data.len() as u64,
            write_sequence: sequence,
            checksum_fnv1a64: crate::fnv1a64(&data),
            data: Arc::new(data),
            ec_parity_fragment: None,
            ec_fragment_digests: None,
            discarded_ranges: Vec::new(),
        }
    }
    fn check(catalog: &PageCatalog, log: &DurableOutcomeLog, page: &PageId, expected: &PageState) {
        let recovered = catalog.pages[page]
            .materialize(|extent, offset, length| log.read_payload_range(extent, offset, length))
            .unwrap();
        assert_eq!(&recovered, expected);
    }

    #[test]
    fn catalog_reconstructs_cross_extent_overwrites_truncation_and_zero_extensions() {
        let dir = Dir::new();
        let log = DurableOutcomeLog::open(dir.0.join("outcomes"), 64 * 1024 * 1024).unwrap();
        let mut catalog = PageCatalog::default();
        let page = id(0);
        let mut previous: Option<PageState> = None;
        for step in 0..7 {
            let mut data = previous
                .as_ref()
                .map(|s| s.data.as_ref().clone())
                .unwrap_or_default();
            match step {
                0 => data.resize(2 * 1024 * 1024 + 137, 11),
                1 => data[900_000..1_500_000].fill(77),
                2 => data.truncate(1500),
                3 => data.resize(1_100_000, 0),
                4 => data[500_000..501_024].fill(91),
                5 => data[17..48].fill(0),
                _ => data.clear(),
            }
            let mut next = state(data, step + 1);
            if step == 5 {
                next.discarded_ranges = vec![DataRange {
                    offset: 17,
                    length: 31,
                }];
            }
            let outcome = PageOutcome::between(&page, previous.as_ref(), &next).unwrap();
            let record = log.append_page(&outcome).unwrap();
            catalog.fold(&record).unwrap();
            check(&catalog, &log, &page, &next);
            assert_eq!(
                Arc::strong_count(&next.data),
                1,
                "catalog retained live page bytes"
            );
            previous = Some(next);
        }
    }

    #[test]
    fn catalog_shares_packed_extent_descriptors_across_pages_without_retaining_payload_buffers() {
        let dir = Dir::new();
        let log = DurableOutcomeLog::open(dir.0.join("outcomes"), 64 * 1024 * 1024).unwrap();
        let states: Vec<_> = (0..32).map(|n| state(vec![n as u8 + 1; 64], 1)).collect();
        let outcomes: Vec<_> = states
            .iter()
            .enumerate()
            .map(|(n, s)| PageOutcome::between(&id(n as u64), None, s).unwrap())
            .collect();
        let decisions: Vec<_> = outcomes.iter().map(DecidedMutation::Page).collect();
        let record = log.append_decided_batch(&decisions).unwrap();
        assert_eq!(record.batch.payloads.len(), 1);
        let mut catalog = PageCatalog::default();
        catalog.fold(&record).unwrap();
        let first = &catalog.pages[&id(0)].spans[0].extent;
        for (n, expected) in states.iter().enumerate() {
            let page = &catalog.pages[&id(n as u64)];
            assert!(Arc::ptr_eq(first, &page.spans[0].extent));
            assert_eq!(Arc::strong_count(&expected.data), 1);
            check(&catalog, &log, &id(n as u64), expected);
            assert!(page.metadata.encoded_len() < 128);
        }
    }

    #[test]
    fn catalog_matches_deterministic_mixed_history_with_zero_filled_holes() {
        let dir = Dir::new();
        let log = DurableOutcomeLog::open(dir.0.join("outcomes"), 64 * 1024 * 1024).unwrap();
        let mut catalog = PageCatalog::default();
        let mut previous: Option<PageState> = None;
        let mut seed = 0x6a09e667u64;
        for step in 0..240 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut data = previous
                .as_ref()
                .map(|s| s.data.as_ref().clone())
                .unwrap_or_default();
            let size = (seed as usize >> 16) % 2048;
            match step % 4 {
                0 => data.resize(size, 0),
                1 => {
                    let at = size.min(data.len());
                    let end = (at + 37).min(data.len());
                    data[at..end].fill((seed >> 32) as u8);
                }
                2 => data.extend(std::iter::repeat((seed >> 32) as u8).take(29)),
                _ => data.truncate(size.min(data.len())),
            }
            let next = state(data, step + 1);
            let outcome = PageOutcome::between(&id(0), previous.as_ref(), &next).unwrap();
            catalog.fold(&log.append_page(&outcome).unwrap()).unwrap();
            check(&catalog, &log, &id(0), &next);
            previous = Some(next);
        }
    }

    #[test]
    fn catalog_refuses_out_of_order_decisions_and_payload_shape_damage() {
        let dir = Dir::new();
        let log = DurableOutcomeLog::open(dir.0.join("outcomes"), 64 * 1024 * 1024).unwrap();
        let first = state(vec![3; 64], 1);
        let second = state(vec![5; 80], 2);
        let a = log
            .append_page(&PageOutcome::between(&id(0), None, &first).unwrap())
            .unwrap();
        let b = log
            .append_page(&PageOutcome::between(&id(0), Some(&first), &second).unwrap())
            .unwrap();
        assert!(PageCatalog::default().fold(&b).is_err());
        let mut bad = StoredRecordMetadata {
            position: a.position.clone(),
            batch: a.batch.clone(),
        };
        bad.batch.decisions[0].patch_lengths[0] += 1;
        assert!(PageCatalog::default().fold(&bad).is_err());
        let mut bad = StoredRecordMetadata {
            position: a.position.clone(),
            batch: a.batch.clone(),
        };
        bad.batch.decisions[0].data_offset = 1000;
        assert!(PageCatalog::default().fold(&bad).is_err());
        let mut catalog = PageCatalog::default();
        catalog.fold(&a).unwrap();
        assert!(catalog.fold(&a).is_err());
    }

    #[test]
    fn catalog_materialization_refuses_short_or_wrong_payload_bytes() {
        let dir = Dir::new();
        let log = DurableOutcomeLog::open(dir.0.join("outcomes"), 64 * 1024 * 1024).unwrap();
        let after = state(vec![19; 128], 1);
        let mut catalog = PageCatalog::default();
        catalog
            .fold(
                &log.append_page(&PageOutcome::between(&id(0), None, &after).unwrap())
                    .unwrap(),
            )
            .unwrap();
        let page = &catalog.pages[&id(0)];
        assert!(page
            .materialize(|_, _, n| Ok(vec![19; n as usize - 1]))
            .is_err());
        assert!(page
            .materialize(|_, _, n| Ok(vec![20; n as usize]))
            .is_err());
        check(&catalog, &log, &id(0), &after);
    }
}

#[cfg(all(test, unix))]
mod range_tests {
    use super::*;
    fn covered_page() -> CatalogPage {
        CatalogPage {
            metadata: CatalogPageMetadata {
                volume_id: 1,
                chunk_id: 1,
                page_index: 0,
                logical_length: 64,
                write_sequence: 1,
                data_checksum: 0,
                data_length: 64,
                discarded_ranges: Vec::new(),
                state_checksum: String::new(),
            },
            spans: vec![PayloadSpan {
                offset: 0,
                length: 64,
                extent_offset: 19,
                extent: Arc::new(PayloadExtent {
                    version: 1,
                    store_id: vec![7; 16],
                    offset: 4096,
                    length: 128,
                    sha256: vec![1; 32],
                    sector_sha256: vec![2; 32],
                }),
            }],
        }
    }
    #[test]
    fn catalog_covered_ranges_reuse_the_provider_buffer_and_clamp_reads() {
        let page = covered_page();
        page.validate_spans().unwrap();
        for (offset, length, expected) in [(0, 64, 64), (7, 9, 9), (60, 100, 4), (64, 9, 0)] {
            let mut pointer = std::ptr::null();
            let mut calls = 0;
            let data = page
                .read_data_range(offset, length, |_, physical, n| {
                    calls += 1;
                    assert_eq!(physical, 19 + offset);
                    assert_eq!(n, expected);
                    let bytes = vec![23; n as usize];
                    pointer = bytes.as_ptr();
                    Ok(bytes)
                })
                .unwrap();
            assert_eq!(data, vec![23; expected as usize]);
            assert_eq!(data.capacity(), data.len());
            assert_eq!(calls, usize::from(expected != 0));
            if expected != 0 {
                assert_eq!(data.as_ptr(), pointer);
            }
        }
    }
    #[test]
    fn catalog_covered_ranges_do_not_retain_provider_padding_capacity() {
        let page = covered_page();
        let mut calls = 0;
        let data = page
            .read_data_range(7, 9, |_, physical, n| {
                calls += 1;
                assert_eq!((physical, n), (26, 9));
                let mut bytes = Vec::with_capacity(8192);
                bytes.resize(n as usize, 31);
                Ok(bytes)
            })
            .unwrap();
        assert_eq!(data, vec![31; 9]);
        assert_eq!(data.capacity(), 9);
        assert_eq!(calls, 1);
    }
    #[test]
    fn catalog_covered_ranges_refuse_wrong_lengths_and_propagate_io_errors() {
        let page = covered_page();
        for count in [8, 10] {
            let error = page
                .read_data_range(7, 9, |_, _, _| Ok(vec![1; count]))
                .unwrap_err();
            assert_eq!(error.to_string(), "short checkpoint payload read");
        }
        let error = page
            .read_data_range(7, 9, |_, _, _| {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "source ended"))
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "source ended");
    }
    #[test]
    fn sparse_ranges_read_only_intersecting_payload_bytes() {
        let extent = Arc::new(PayloadExtent {
            version: 1,
            store_id: vec![7; 16],
            offset: 4096,
            length: 64,
            sha256: vec![1; 32],
            sector_sha256: vec![2; 32],
        });
        let page = CatalogPage {
            metadata: CatalogPageMetadata {
                volume_id: 1,
                chunk_id: 1,
                page_index: 0,
                logical_length: 1_000_064,
                write_sequence: 1,
                data_checksum: 0,
                data_length: 1_000_064,
                discarded_ranges: Vec::new(),
                state_checksum: String::new(),
            },
            spans: vec![
                PayloadSpan {
                    offset: 0,
                    length: 64,
                    extent: extent.clone(),
                    extent_offset: 0,
                },
                PayloadSpan {
                    offset: 1_000_000,
                    length: 64,
                    extent,
                    extent_offset: 0,
                },
            ],
        };
        page.validate_spans().unwrap();
        let mut calls = Vec::new();
        let data = page
            .read_data_range(1_000_019, 4, |_, offset, length| {
                calls.push((offset, length));
                Ok(vec![17; length as usize])
            })
            .unwrap();
        assert_eq!(data, vec![17; 4]);
        assert_eq!(calls, vec![(19, 4)]);
        let hole = page
            .read_data_range(500_000, 4096, |_, _, _| panic!("zero hole read storage"))
            .unwrap();
        assert_eq!(hole, vec![0; 4096]);
        let crossing = page
            .read_data_range(999_998, 4, |_, offset, length| {
                assert_eq!((offset, length), (0, 2));
                Ok(vec![23; length as usize])
            })
            .unwrap();
        assert_eq!(crossing, vec![0, 0, 23, 23]);
        assert!(page
            .read_data_range(1_000_065, 1, |_, _, _| unreachable!())
            .is_err());
        assert!(page
            .read_data_range(1, u64::MAX, |_, _, _| unreachable!())
            .is_err());
        assert_eq!(
            page.read_data_range(1_000_064, 5, |_, _, _| unreachable!())
                .unwrap(),
            Vec::<u8>::new()
        );
    }
}
