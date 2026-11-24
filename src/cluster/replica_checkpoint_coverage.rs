// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Incremental physical byte unions for checkpoint maintenance planning.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cell {
    end: u64,
    count: u64,
}
#[derive(Default, Debug)]
struct Coverage {
    cells: BTreeMap<u64, Cell>,
    bytes: u64,
}
impl Coverage {
    fn split(&mut self, point: u64) {
        let Some((&start, &cell)) = self.cells.range(..=point).next_back() else {
            return;
        };
        if start < point && point < cell.end {
            self.cells.get_mut(&start).unwrap().end = point;
            self.cells.insert(point, cell);
        }
    }
    fn merge(&mut self, start: u64, end: u64) {
        let mut at = self
            .cells
            .range(..start)
            .next_back()
            .map_or(start, |(&key, _)| key);
        loop {
            let Some((&key, &cell)) = self.cells.range(at..).next() else {
                break;
            };
            if key > end {
                break;
            }
            let Some((&next, &value)) = self.cells.range((Excluded(key), Unbounded)).next() else {
                break;
            };
            if cell.end == next && cell.count == value.count {
                self.cells.get_mut(&key).unwrap().end = value.end;
                self.cells.remove(&next);
                at = key;
            } else {
                at = next;
            }
        }
    }
    fn change(&mut self, start: u64, end: u64, add: bool) -> io::Result<()> {
        if start >= end {
            return Err(invalid("invalid coverage range"));
        }
        let first = self
            .cells
            .range(..=start)
            .next_back()
            .filter(|(_, cell)| cell.end > start)
            .map_or(start, |(&key, _)| key);
        let mut reached = start;
        for (&key, cell) in self.cells.range(first..end) {
            if cell.end <= start {
                continue;
            }
            if !add && key > reached {
                return Err(invalid("unowned coverage bytes"));
            }
            if add && cell.count == u64::MAX {
                return Err(invalid("coverage count overflow"));
            }
            reached = reached.max(cell.end.min(end));
        }
        if !add && reached < end {
            return Err(invalid("unowned coverage bytes"));
        }
        self.split(start);
        self.split(end);
        let mut at = start;
        while at < end {
            if let Some(cell) = self.cells.get(&at).copied() {
                if add {
                    self.cells.get_mut(&at).unwrap().count += 1;
                } else if cell.count > 1 {
                    self.cells.get_mut(&at).unwrap().count -= 1;
                } else {
                    self.cells.remove(&at);
                    self.bytes = self
                        .bytes
                        .checked_sub(cell.end - at)
                        .ok_or_else(|| invalid("coverage byte count underflow"))?;
                }
                at = cell.end;
            } else {
                if !add {
                    return Err(invalid("unowned coverage bytes"));
                }
                let next = self
                    .cells
                    .range(at..end)
                    .next()
                    .map_or(end, |(&key, _)| key);
                self.cells.insert(
                    at,
                    Cell {
                        end: next,
                        count: 1,
                    },
                );
                self.bytes = self
                    .bytes
                    .checked_add(next - at)
                    .ok_or_else(|| invalid("coverage byte count overflow"))?;
                at = next;
            }
        }
        self.merge(start, end);
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PhysicalRange {
    segment: [u8; 16],
    start: u64,
    end: u64,
}
impl PhysicalRange {
    fn record(record: ExtentRecord) -> io::Result<Self> {
        Ok(Self {
            segment: record.store_id,
            start: record.offset,
            end: record
                .offset
                .checked_add(record.length)
                .ok_or_else(|| invalid("coverage record overflow"))?,
        })
    }
    fn span(span: &PayloadSpan) -> io::Result<Self> {
        let segment = catalog::extent_key(&span.extent)?.0;
        let start = span
            .extent
            .offset
            .checked_add(span.extent_offset)
            .ok_or_else(|| invalid("coverage payload offset overflow"))?;
        Ok(Self {
            segment,
            start,
            end: start
                .checked_add(span.length)
                .ok_or_else(|| invalid("coverage payload end overflow"))?,
        })
    }
}
#[derive(Default)]
pub(super) struct PlannerCoverage {
    segments: BTreeMap<[u8; 16], Coverage>,
    records: BTreeMap<ExtentRecord, u64>,
    sources: BTreeMap<[u8; 16], u64>,
    payloads: BTreeMap<PageId, Vec<PhysicalRange>>,
    owners: BTreeMap<[u8; 16], BTreeSet<PageId>>,
    legacy_values: usize,
    covered_bytes: u64,
    metadata_record_bytes: u64,
}
impl PlannerCoverage {
    pub(super) fn add_source(&mut self, source: [u8; 16]) -> io::Result<()> {
        let count = self.sources.entry(source).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| invalid("extent source count overflow"))?;
        Ok(())
    }
    pub(super) fn remove_source(&mut self, source: &[u8; 16]) -> io::Result<()> {
        let count = self
            .sources
            .get_mut(source)
            .ok_or_else(|| invalid("extent source is not tracked"))?;
        if *count == 1 {
            self.sources.remove(source);
        } else {
            *count = count
                .checked_sub(1)
                .ok_or_else(|| invalid("extent source count underflow"))?;
        }
        Ok(())
    }
    pub(super) fn protect(
        &self,
        pages: usize,
        keep: &mut BTreeSet<Vec<u8>>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        if self.payloads.len() != pages || !self.ordinary() {
            return Err(invalid("protected coverage does not match indexed catalog"));
        }
        for id in self.segments.keys().chain(self.sources.keys()) {
            check()?;
            if !keep.contains(id.as_slice()) {
                keep.insert(id.to_vec());
            }
        }
        Ok(())
    }
    fn physical(&mut self, range: PhysicalRange, add: bool) -> io::Result<()> {
        if range.start < 4096 || range.start >= range.end {
            return Err(invalid("invalid physical coverage bounds"));
        }
        let before = self.segments.get(&range.segment).map_or(0, |value| value.bytes);
        if add {
            self.segments.entry(range.segment).or_default()
                .change(range.start, range.end, true)?;
        } else {
            let coverage = self.segments.get_mut(&range.segment)
                .ok_or_else(|| invalid("coverage segment is missing"))?;
            coverage.change(range.start, range.end, false)?;
            if coverage.cells.is_empty() { self.segments.remove(&range.segment); }
        }
        let after = self.segments.get(&range.segment).map_or(0, |value| value.bytes);
        self.covered_bytes = self.covered_bytes.checked_sub(before)
            .and_then(|bytes| bytes.checked_add(after))
            .ok_or_else(|| invalid("total coverage byte count overflow"))?;
        Ok(())
    }
    fn add_record(&mut self, record: ExtentRecord) -> io::Result<()> {
        if let Some(count) = self.records.get_mut(&record) {
            *count = count
                .checked_add(1)
                .ok_or_else(|| invalid("saved record count overflow"))?;
        } else {
            let bytes = self.metadata_record_bytes.checked_add(record.length)
                .ok_or_else(|| invalid("metadata coverage byte count overflow"))?;
            self.physical(PhysicalRange::record(record)?, true)?;
            self.records.insert(record, 1);
            self.metadata_record_bytes = bytes;
        }
        Ok(())
    }
    fn remove_record(&mut self, record: ExtentRecord) -> io::Result<()> {
        let count = *self
            .records
            .get(&record)
            .ok_or_else(|| invalid("saved record is not tracked"))?;
        if count > 1 {
            *self.records.get_mut(&record).unwrap() -= 1;
        } else {
            let bytes = self.metadata_record_bytes.checked_sub(record.length)
                .ok_or_else(|| invalid("metadata coverage byte count underflow"))?;
            self.physical(PhysicalRange::record(record)?, false)?;
            self.records.remove(&record);
            self.metadata_record_bytes = bytes;
        }
        Ok(())
    }
    pub(super) fn add_saved(&mut self, saved: Option<&BlobRef>) -> io::Result<()> {
        if let Some(saved) = saved.filter(|saved| saved.position.is_some()) {
            self.add_record(ExtentRecord::new(saved.position()?)?)?;
        }
        Ok(())
    }
    pub(super) fn remove_saved(&mut self, saved: Option<&BlobRef>) -> io::Result<()> {
        if let Some(position) = saved.and_then(|saved| saved.position.as_ref()) {
            self.remove_record(ExtentRecord::new(position)?)?;
        }
        Ok(())
    }
    pub(super) fn validate_value(value: &Value, records: &[ExtentRecord]) -> io::Result<()> {
        match (&value.page, value.catalog.is_empty()) {
            (Some(page), true) => {
                page.plain()?;
                page.position()?;
            }
            (None, false) if records.is_empty() => {}
            _ => return Err(invalid("invalid coverage page binding")),
        }
        Ok(())
    }
    pub(super) fn add_value(&mut self, value: &Value, records: &[ExtentRecord]) -> io::Result<()> {
        Self::validate_value(value, records)?;
        if value.page.is_some() {
            self.add_saved(value.page.as_ref())?;
            for record in records {
                self.add_record(*record)?;
            }
        } else {
            self.legacy_values = self
                .legacy_values
                .checked_add(1)
                .ok_or_else(|| invalid("catalog binding count overflow"))?;
        }
        Ok(())
    }
    pub(super) fn remove_value(
        &mut self,
        value: &Value,
        records: &[ExtentRecord],
    ) -> io::Result<()> {
        Self::validate_value(value, records)?;
        if value.page.is_some() {
            self.remove_saved(value.page.as_ref())?;
            for record in records {
                self.remove_record(*record)?;
            }
        } else {
            self.legacy_values = self
                .legacy_values
                .checked_sub(1)
                .ok_or_else(|| invalid("catalog binding count underflow"))?;
        }
        Ok(())
    }
    pub(super) fn refresh_payloads<'a>(
        &mut self,
        pages: &PageCatalog,
        keys: impl IntoIterator<Item = &'a PageId>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<()> {
        for key in keys {
            check()?;
            let page = pages
                .pages
                .get(key)
                .ok_or_else(|| invalid("coverage page is missing"))?;
            page.validate_spans()?;
            let mut ranges = Vec::with_capacity(page.spans.len());
            for span in &page.spans {
                ranges.push(PhysicalRange::span(span)?);
            }
            if self.payloads.get(key).is_some_and(|old| *old == ranges) {
                continue;
            }
            // Store page keys only: descriptor ownership still controls pruning.
            let old_segments: BTreeSet<_> = self
                .payloads
                .get(key)
                .into_iter()
                .flatten()
                .map(|range| range.segment)
                .collect();
            let new_segments: BTreeSet<_> = ranges.iter().map(|range| range.segment).collect();
            for segment in &old_segments {
                if !self
                    .owners
                    .get(segment)
                    .is_some_and(|owners| owners.contains(key))
                {
                    return Err(invalid("payload owner is not tracked"));
                }
            }
            for segment in new_segments.difference(&old_segments) {
                if self
                    .owners
                    .get(segment)
                    .is_some_and(|owners| owners.contains(key))
                {
                    return Err(invalid("payload owner was already tracked"));
                }
            }
            for segment in old_segments.difference(&new_segments) {
                let owners = self.owners.get_mut(segment).unwrap();
                owners.remove(key);
                if owners.is_empty() {
                    self.owners.remove(segment);
                }
            }
            for segment in new_segments.difference(&old_segments) {
                self.owners.entry(*segment).or_default().insert(key.clone());
            }
            if let Some(old) = self.payloads.remove(key) {
                for range in old {
                    self.physical(range, false)?;
                }
            }
            for range in &ranges {
                self.physical(*range, true)?;
            }
            self.payloads.insert(key.clone(), ranges);
        }
        if self.payloads.len() != pages.pages.len() {
            return Err(invalid("payload coverage does not match page count"));
        }
        Ok(())
    }
    pub(super) fn payload_pages(
        &self,
        segment: &[u8; 16],
        pages: usize,
    ) -> io::Result<Vec<PageId>> {
        if self.payloads.len() != pages || !self.ordinary() {
            return Err(invalid("payload ownership does not match indexed catalog"));
        }
        Ok(self
            .owners
            .get(segment)
            .into_iter()
            .flatten()
            .cloned()
            .collect())
    }
    #[cfg(test)]
    pub(super) fn payload_owner_sets(&self) -> &BTreeMap<[u8; 16], BTreeSet<PageId>> {
        &self.owners
    }
    #[cfg(test)]
    pub(super) fn retained_payload_range_slots(&self) -> usize {
        self.payloads.values().map(|ranges| ranges.capacity()).sum()
    }
    #[cfg(test)]
    pub(super) fn totals(&self) -> BTreeMap<[u8; 16], u64> {
        self.segments
            .iter()
            .map(|(id, coverage)| (*id, coverage.bytes))
            .collect()
    }
    /// Subtracting complete distinct metadata records from the combined union
    /// cannot overstate live payload, including overlapping or shared records.
    pub(super) fn payload_lower_bound(&self, pages: usize) -> io::Result<u64> {
        if self.payloads.len() != pages {
            return Err(invalid("payload coverage does not match page count"));
        }
        Ok(self.covered_bytes.saturating_sub(self.metadata_record_bytes))
    }
    pub(super) fn has_metadata(&self, segment: &[u8; 16]) -> bool {
        let start = ExtentRecord {
            store_id: *segment,
            offset: 0,
            length: 0,
        };
        self.records
            .range(start..)
            .next()
            .is_some_and(|(record, _)| record.store_id == *segment)
    }
    pub(super) fn ordinary(&self) -> bool {
        self.legacy_values == 0
    }
    pub(super) fn live_bytes(&self, segment: &[u8; 16], length: u64) -> io::Result<u64> {
        let Some(coverage) = self.segments.get(segment) else {
            return Ok(0);
        };
        if coverage
            .cells
            .first_key_value()
            .is_some_and(|(&start, _)| start < 4096)
            || coverage
                .cells
                .last_key_value()
                .is_some_and(|(_, cell)| cell.end > length)
        {
            return Err(invalid("cached coverage exceeds its segment"));
        }
        Ok(coverage.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_coverage_preserves_reusable_sources_until_last_release() {
        let mut coverage = PlannerCoverage::default();
        let source = [0x31; 16];
        coverage.add_source(source).unwrap();
        coverage.add_source(source).unwrap();
        coverage.remove_source(&source).unwrap();
        let mut keep = BTreeSet::new();
        coverage.protect(0, &mut keep, &|| Ok(())).unwrap();
        assert_eq!(keep, BTreeSet::from([source.to_vec()]));
        coverage.remove_source(&source).unwrap();
        keep.clear();
        coverage.protect(0, &mut keep, &|| Ok(())).unwrap();
        assert!(keep.is_empty());
        assert!(coverage.remove_source(&source).is_err());
        coverage.sources.insert(source, u64::MAX);
        assert!(coverage.add_source(source).is_err());
        assert_eq!(coverage.sources[&source], u64::MAX);
        assert!(coverage.protect(1, &mut keep, &|| Ok(())).is_err());
        assert!(coverage
            .protect(0, &mut keep, &|| Err(invalid("cancelled")))
            .is_err());
    }
    #[test]
    fn checkpoint_coverage_matches_dense_union_during_shared_and_overlapping_updates() {
        let mut coverage = Coverage::default();
        let mut counts = [0u32; 256];
        let mut active = Vec::new();
        let mut random = 0x6a09e667f3bcc909u64;
        for step in 0..4096 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            if active.is_empty() || random & 3 != 0 {
                let start = (random as usize >> 8) % 256;
                let length = 1 + ((random as usize >> 24) % (256 - start));
                coverage
                    .change(start as u64, (start + length) as u64, true)
                    .unwrap();
                for count in &mut counts[start..start + length] {
                    *count += 1;
                }
                active.push((start, length));
            } else {
                let at = (random as usize >> 8) % active.len();
                let (start, length) = active.swap_remove(at);
                coverage
                    .change(start as u64, (start + length) as u64, false)
                    .unwrap();
                for count in &mut counts[start..start + length] {
                    *count -= 1;
                }
            }
            if step % 32 == 0 {
                assert_eq!(
                    coverage.bytes,
                    counts.iter().filter(|&&n| n != 0).count() as u64
                );
                for (at, &expected) in counts.iter().enumerate() {
                    let actual = coverage
                        .cells
                        .range(..=at as u64)
                        .next_back()
                        .filter(|(_, cell)| cell.end > at as u64)
                        .map_or(0, |(_, cell)| cell.count);
                    assert_eq!(actual, u64::from(expected));
                }
                let mut previous = None;
                for (&start, cell) in &coverage.cells {
                    assert!(start < cell.end && cell.count != 0);
                    if let Some((end, count)) = previous {
                        assert!(end <= start);
                        assert!(end != start || count != cell.count);
                    }
                    previous = Some((cell.end, cell.count));
                }
            }
        }
        for (start, length) in active {
            coverage
                .change(start as u64, (start + length) as u64, false)
                .unwrap();
        }
        assert_eq!(coverage.bytes, 0);
        assert!(coverage.cells.is_empty());
    }
    #[test]
    fn checkpoint_coverage_rejects_gaps_and_overflow_before_changing_membership() {
        let mut coverage = Coverage::default();
        coverage.change(10, 20, true).unwrap();
        coverage.change(30, 40, true).unwrap();
        let before = coverage.cells.clone();
        assert!(coverage.change(15, 35, false).is_err());
        assert_eq!(coverage.cells, before);
        assert_eq!(coverage.bytes, 20);
        coverage.cells.get_mut(&10).unwrap().count = u64::MAX;
        let before = coverage.cells.clone();
        assert!(coverage.change(12, 18, true).is_err());
        assert_eq!(coverage.cells, before);
        assert_eq!(coverage.bytes, 20);
        assert!(coverage.change(9, 9, true).is_err());
    }
    #[test]
    fn checkpoint_coverage_keeps_shared_records_and_overlapping_payload_bytes_until_last_owner() {
        let mut cache = PlannerCoverage::default();
        let record = ExtentRecord {
            store_id: [3; 16],
            offset: 4096,
            length: 8192,
        };
        let payload = PhysicalRange {
            segment: [3; 16],
            start: 8192,
            end: 16384,
        };
        for _ in 0..64 {
            cache.add_record(record).unwrap();
        }
        cache.physical(payload, true).unwrap();
        assert!(cache.has_metadata(&[3; 16]));
        assert!(!cache.has_metadata(&[2; 16]));
        assert!(!cache.has_metadata(&[4; 16]));
        assert_eq!(cache.live_bytes(&[3; 16], 16384).unwrap(), 12288);
        assert!(cache.live_bytes(&[3; 16], 16383).is_err());
        for _ in 0..63 {
            cache.remove_record(record).unwrap();
        }
        assert_eq!(cache.live_bytes(&[3; 16], 16384).unwrap(), 12288);
        cache.remove_record(record).unwrap();
        assert!(!cache.has_metadata(&[3; 16]));
        assert_eq!(cache.live_bytes(&[3; 16], 16384).unwrap(), 8192);
        assert!(cache.remove_record(record).is_err());
        cache.physical(payload, false).unwrap();
        assert_eq!(cache.live_bytes(&[3; 16], 16384).unwrap(), 0);
        assert!(cache.segments.is_empty());
    }
}

#[cfg(test)]
#[path = "replica_checkpoint_payload_coverage_tests.rs"]
mod payload_budget_tests;
