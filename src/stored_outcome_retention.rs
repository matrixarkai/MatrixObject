// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable retirement of unreachable sealed segments; readers pin publication.
use super::*;
use std::{collections::BTreeSet, sync::MutexGuard};

#[cfg(feature = "replica-server")]
pub(crate) struct CompactionSegment {
    pub number: u64,
    pub store_id: [u8; 16],
    pub length: u64,
}

pub(super) struct ReadPin<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for ReadPin<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

pub(crate) struct PreparedRetention<'a> {
    log: &'a SegmentedOutcomeLog,
    _collection: Option<MutexGuard<'a, ()>>,
    pending: PathBuf,
    next_number: u64,
    index: Option<Index>,
    length: u64,
    hash: [u8; 32],
}
impl Drop for PreparedRetention<'_> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.pending);
    }
}

impl SegmentedOutcomeLog {
    #[cfg(feature = "replica-server")]
    pub(crate) fn checkpoint_read_pin(&self) -> io::Result<impl Drop + '_> {
        self.pin()
    }

    /// Sample a bounded batch before the replay tail. This runs in maintenance,
    /// never while capturing foreground replica state.
    #[cfg(feature = "replica-server")]
    pub(crate) fn compaction_candidates(
        &self,
        through: &StoredOutcomePosition,
        after: Option<u64>,
        limit: usize,
    ) -> io::Result<Vec<CompactionSegment>> {
        self.read_metadata(through)?;
        let index = self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?;
        let cut = *index
            .by_id
            .get(&through.store_id)
            .ok_or_else(|| invalid("compaction cut is not owned"))?;
        let mut entries = Vec::new();
        if after.is_none_or(|number| number < cut) {
            entries.extend(
                index
                    .entries
                    .range((
                        after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                        std::ops::Bound::Excluded(cut),
                    ))
                    .take(limit)
                    .map(|(_, entry)| entry),
            );
        }
        if entries.is_empty() && after.is_some() {
            entries.extend(
                index
                    .entries
                    .range(..cut)
                    .take(limit)
                    .map(|(_, entry)| entry),
            );
        }
        entries
            .into_iter()
            .map(|entry| {
                if entry.sealed_length <= SECTOR_BYTES as u64 {
                    return Err(invalid("invalid sealed compaction segment"));
                }
                Ok(CompactionSegment {
                    number: entry.number,
                    store_id: entry
                        .store_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| invalid("invalid compaction segment identity"))?,
                    length: entry.sealed_length,
                })
            })
            .collect()
    }

    /// Intersect a validated catalog interval with a small candidate batch.
    #[cfg(feature = "replica-server")]
    pub(crate) fn compaction_catalog_ranges(
        &self,
        first: &StoredOutcomePosition,
        last: &StoredOutcomePosition,
        candidates: &[CompactionSegment],
    ) -> io::Result<Vec<([u8; 16], u64, u64)>> {
        self.read_metadata(first)?;
        self.read_metadata(last)?;
        let index = self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?;
        let start = index.by_id[&first.store_id];
        let end = index.by_id[&last.store_id];
        if start > end || (start == end && first.offset > last.offset) {
            return Err(invalid("reversed compaction catalog interval"));
        }
        let mut ranges = Vec::new();
        for candidate in candidates {
            if candidate.number < start || candidate.number > end {
                continue;
            }
            let from = if candidate.number == start {
                first.offset
            } else {
                SECTOR_BYTES as u64
            };
            let to = if candidate.number == end {
                last.offset
                    .checked_add(last.length)
                    .ok_or_else(|| invalid("catalog end overflow"))?
            } else {
                candidate.length
            };
            if from >= to || to > candidate.length {
                return Err(invalid("invalid compaction catalog bounds"));
            }
            ranges.push((candidate.store_id, from, to));
        }
        Ok(ranges)
    }

    pub(crate) fn check_health(&self) -> io::Result<()> {
        self.healthy()
    }
    pub(crate) fn retention_readers_active(&self) -> bool {
        self.readers.load(Ordering::Acquire) != 0
    }
    pub(super) fn pin(&self) -> io::Result<ReadPin<'_>> {
        let _gate = self
            .reader_gate
            .lock()
            .map_err(|_| invalid("reader gate is poisoned"))?;
        self.healthy()?;
        self.readers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| invalid("reader pin count overflow"))?;
        Ok(ReadPin(&self.readers))
    }

    pub(crate) fn retained_segment_count(&self) -> io::Result<usize> {
        self.healthy()?;
        Ok(self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .entries
            .len())
    }
    pub(crate) fn managed_status(&self) -> io::Result<ManagedSegmentStatus> {
        self.healthy()?;
        let index = self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?;
        Ok(ManagedSegmentStatus {
            segments: index.entries.len() + index.retired.len(),
            journal_bytes: index.sealed_bytes + index.retired_bytes + index.active.length()
                - SECTOR_BYTES as u64,
            retired_segments: index.retired.len(),
            retired_journal_bytes: index.retired_bytes,
            snapshot_pending: index.retirement_changed,
        })
    }
    pub(crate) fn retired_segment_count(&self) -> io::Result<usize> {
        self.healthy()?;
        Ok(self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .retired
            .len())
    }

    /// Protect every physical segment in an inclusive catalog fragment interval.
    pub(crate) fn protect_interval(
        &self,
        first: &StoredOutcomePosition,
        last: &StoredOutcomePosition,
        keep: &mut BTreeSet<Vec<u8>>,
    ) -> io::Result<()> {
        let _pin = self.pin()?;
        self.read_metadata(first)?;
        self.read_metadata(last)?;
        let (start, end) = {
            let index = self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?;
            (index.by_id[&first.store_id], index.by_id[&last.store_id])
        };
        if start > end || (start == end && first.offset > last.offset) {
            return Err(invalid("reversed protected segment interval"));
        }
        for entry in
            self.entry_snapshot(std::ops::Bound::Included(start), end, false, &|| Ok(()))?
        {
            keep.insert(entry.store_id);
        }
        Ok(())
    }

    fn entry_snapshot(
        &self,
        start: std::ops::Bound<u64>,
        end: u64,
        retired: bool,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<Vec<SegmentEntry>> {
        let mut result = Vec::new();
        let mut after = start;
        loop {
            check()?;
            let batch: Vec<_> = {
                let index = self
                    .index
                    .read()
                    .map_err(|_| invalid("segment index is poisoned"))?;
                let entries = if retired {
                    &index.retired
                } else {
                    &index.entries
                };
                entries
                    .range((after, std::ops::Bound::Included(end)))
                    .take(64)
                    .map(|(_, e)| e.clone())
                    .collect()
            };
            if batch.is_empty() {
                break;
            }
            after = std::ops::Bound::Excluded(batch.last().unwrap().number);
            result.extend(batch);
        }
        Ok(result)
    }

    /// Prepare a durable sparse manifest without holding the append lock while
    /// walking the catalog or writing the manifest body. Publication can refuse
    /// a rollover or an active reader; no retained file is deleted on refusal.
    pub(crate) fn prepare_retention(
        &self,
        keep: &BTreeSet<Vec<u8>>,
        through: &StoredOutcomePosition,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<Option<PreparedRetention<'_>>> {
        self.prepare_snapshot(Some(keep), Some(through), check)
    }
    pub(crate) fn prepare_retirement_snapshot(
        &self,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<Option<PreparedRetention<'_>>> {
        self.prepare_snapshot(None, None, check)
    }
    pub(crate) fn retirement_snapshot_needed(&self) -> io::Result<bool> {
        self.healthy()?;
        Ok(self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .retirement_changed)
    }
    fn prepare_snapshot(
        &self,
        keep: Option<&BTreeSet<Vec<u8>>>,
        through: Option<&StoredOutcomePosition>,
        check: &dyn Fn() -> io::Result<()>,
    ) -> io::Result<Option<PreparedRetention<'_>>> {
        self.healthy()?;
        let collection = self.collection.try_lock().map_err(|e| match e {
            std::sync::TryLockError::WouldBlock => {
                io::Error::new(io::ErrorKind::WouldBlock, "segment collection is busy")
            }
            _ => invalid("segment collection is poisoned"),
        })?;
        check()?;
        if let Some(through) = through {
            self.read_metadata(through)?;
        }
        if self.readers.load(Ordering::Acquire) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "segment readers are active",
            ));
        }
        let (next_number, active) = {
            let writer = self
                .writer
                .lock()
                .map_err(|_| invalid("segment writer is poisoned"))?;
            self.healthy()?;
            (writer.count, writer.active.clone())
        };
        let cut = if let Some(through) = through {
            let index = self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?;
            *index
                .by_id
                .get(&through.store_id)
                .ok_or_else(|| invalid("retention cut is not owned"))?
        } else {
            0
        };
        for id in keep.into_iter().flatten() {
            check()?;
            if !self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?
                .by_id
                .contains_key(id)
            {
                return Err(invalid("retained segment is not owned"));
            }
        }
        let entries =
            self.entry_snapshot(std::ops::Bound::Unbounded, next_number - 1, false, check)?;
        let mut retired =
            self.entry_snapshot(std::ops::Bound::Unbounded, next_number - 1, true, check)?;
        let mut retained = Vec::new();
        let mut removed = 0;
        for entry in entries {
            check()?;
            if entry.number >= cut || keep.is_some_and(|keep| keep.contains(&entry.store_id)) {
                retained.push(entry);
            } else {
                retired.push(entry);
                removed += 1;
            }
        }
        if self
            .writer
            .lock()
            .map_err(|_| invalid("segment writer is poisoned"))?
            .count
            != next_number
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "rollover changed the prepared segment manifest",
            ));
        }
        if removed == 0 && !self.retirement_snapshot_needed()? {
            return Ok(None);
        }
        retired.sort_unstable_by_key(|e| e.number);
        if retired.len() > MAX_SEGMENTS {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retirement cleanup is behind",
            ));
        }
        let manifest = SegmentManifest {
            version: 2,
            store_id: self.store_id().to_vec(),
            capacity: self._anchor.capacity,
            entries: retained,
            retired,
        };
        validate(&manifest, self.store_id(), self._anchor.capacity)?;
        let body = manifest.encode_to_vec();
        let length = ENVELOPE as u64 + body.len() as u64;
        if length + LINK_BYTES as u64 > MAX_MANIFEST_BYTES as u64 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "retained manifest exceeds byte limit",
            ));
        }
        let hash: [u8; 32] = digest::digest(&digest::SHA256, &body)
            .as_ref()
            .try_into()
            .unwrap();
        let pending = sibling(&self.path, ".segments.retaining");
        let mut prepared = PreparedRetention {
            log: self,
            _collection: Some(collection),
            pending,
            next_number,
            index: None,
            length,
            hash,
        };
        check()?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&prepared.pending)?;
        file.write_all(RETAINED_MAGIC)?;
        file.write_all(&(body.len() as u32).to_be_bytes())?;
        file.write_all(&hash)?;
        // Keep cancellation responsive during large manifest preparation.
        for bytes in body.chunks(1024 * 1024) {
            check()?;
            file.write_all(bytes)?;
        }
        file.sync_all()?;
        check()?;
        prepared.index = Some(Index {
            by_id: manifest
                .entries
                .iter()
                .map(|e| (e.store_id.clone(), e.number))
                .collect(),
            sealed_bytes: manifest
                .entries
                .iter()
                .filter(|e| e.sealed_length != 0)
                .map(|e| e.sealed_length - SECTOR_BYTES as u64)
                .sum(),
            retired_bytes: manifest
                .retired
                .iter()
                .map(|e| e.sealed_length - SECTOR_BYTES as u64)
                .sum(),
            entries: manifest
                .entries
                .into_iter()
                .map(|e| (e.number, e))
                .collect(),
            retired: manifest
                .retired
                .into_iter()
                .map(|e| (e.number, e))
                .collect(),
            retirement_changed: false,
            active,
            cache: VecDeque::new(),
        });
        Ok(Some(prepared))
    }

    /// Retry at most 64 retired files. Failed ownership checks or unlink attempts
    /// remain recorded as retirement debt; they cannot damage the live journal.
    pub(crate) fn cleanup_retired(&self) -> io::Result<RetirementStats> {
        let _collection = self.collection.try_lock().map_err(|e| match e {
            std::sync::TryLockError::WouldBlock => {
                io::Error::new(io::ErrorKind::WouldBlock, "segment collection is busy")
            }
            _ => invalid("segment collection is poisoned"),
        })?;
        self.cleanup_retired_inner()
    }
    fn cleanup_retired_inner(&self) -> io::Result<RetirementStats> {
        let candidates: Vec<_> = self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .retired
            .values()
            .take(64)
            .cloned()
            .collect();
        let mut removed = Vec::new();
        for entry in candidates {
            if remove_owned_retired(&self.path, &entry, self._anchor.capacity).is_ok() {
                removed.push(entry);
            }
        }
        if !removed.is_empty() && directory_sync(&self.path).is_ok() {
            for entry in &removed {
                let mut index = self
                    .index
                    .write()
                    .map_err(|_| invalid("segment index is poisoned"))?;
                if index.retired.remove(&entry.number).is_some() {
                    index.retired_bytes -= entry.sealed_length - SECTOR_BYTES as u64;
                    index.retirement_changed = true;
                }
            }
        } else {
            removed.clear();
        }
        Ok(RetirementStats {
            removed: removed.len(),
            pending: self.retired_segment_count()?,
        })
    }
}

#[derive(Debug)]
pub(crate) struct RetirementStats {
    pub removed: usize,
    pub pending: usize,
}

pub(crate) struct CommittedRetention<'a> {
    log: &'a SegmentedOutcomeLog,
    old: Option<Index>,
    _collection: MutexGuard<'a, ()>,
}
impl CommittedRetention<'_> {
    pub(crate) fn finish(mut self) -> io::Result<RetirementStats> {
        drop(self.old.take());
        self.log.cleanup_retired_inner()
    }
}

impl<'a> PreparedRetention<'a> {
    /// Call only under the replica checkpoint publication epoch gate.
    #[cfg(all(test, unix))]
    pub(crate) fn publish(self) -> io::Result<RetirementStats> {
        self.commit()?.finish()
    }
    pub(crate) fn commit(self) -> io::Result<CommittedRetention<'a>> {
        self.commit_with(|source, destination| {
            std::fs::rename(source, destination).and_then(|_| directory_sync(destination))
        })
    }
    #[cfg(all(test, unix))]
    fn publish_with(
        self,
        publish: impl FnOnce(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<RetirementStats> {
        self.commit_with(publish)?.finish()
    }
    fn commit_with(
        mut self,
        publish: impl FnOnce(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<CommittedRetention<'a>> {
        let mut writer = self
            .log
            .writer
            .lock()
            .map_err(|_| invalid("segment writer is poisoned"))?;
        self.log.healthy()?;
        if writer.count != self.next_number {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "rollover changed the prepared segment manifest",
            ));
        }
        let gate = self
            .log
            .reader_gate
            .lock()
            .map_err(|_| invalid("reader gate is poisoned"))?;
        if self.log.readers.load(Ordering::Acquire) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "segment readers are active",
            ));
        }
        let destination = sibling(&self.log.path, ".segments");
        if let Err(error) = publish(&self.pending, &destination) {
            self.log.failed.store(true, Ordering::Release);
            return Err(error);
        }
        let old = {
            let mut index = self
                .log
                .index
                .write()
                .map_err(|_| invalid("segment index is poisoned"))?;
            std::mem::replace(&mut *index, self.index.take().unwrap())
        };
        writer.manifest_length = self.length;
        writer.tail_hash = self.hash;
        drop(gate);
        drop(writer);
        // Release the checkpoint epoch gate before dropping large maps or files.
        Ok(CommittedRetention {
            log: self.log,
            old: Some(old),
            _collection: self._collection.take().unwrap(),
        })
    }
}

fn remove_owned_retired(anchor: &Path, entry: &SegmentEntry, capacity: u64) -> io::Result<()> {
    let path = segment_path(anchor, entry.number);
    let marker_path = super::super::sibling(&path);
    let data_exists = path.try_exists()?;
    let marker_exists = marker_path.try_exists()?;
    if !data_exists && !marker_exists {
        return Ok(());
    }
    // A surviving marker binds cleanup even after a crash between the two unlinks.
    let marker = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&marker_path)?;
    fs2::FileExt::try_lock_exclusive(&marker)?;
    let mut bytes = [0; super::super::MARKER_BYTES + 1];
    if marker.metadata()?.len() != bytes.len() as u64 {
        return Err(invalid("retired identity marker length changed"));
    }
    read_at(&marker, &mut bytes, 0)?;
    if &bytes[..8] != super::super::LOCK_MAGIC
        || bytes[8..24] != entry.store_id
        || digest::digest(&digest::SHA256, &bytes[..24]).as_ref() != &bytes[24..56]
        || bytes[super::super::MARKER_BYTES] != READY
    {
        return Err(invalid("retired segment identity changed"));
    }
    let data = if data_exists {
        let mut file = OpenOptions::new().read(true).open(&path)?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        super::super::verify_header(&mut file, &entry.store_id, capacity, 2)?;
        if file.metadata()?.len() != entry.sealed_length {
            return Err(invalid("retired segment length changed"));
        }
        std::fs::remove_file(&path)?;
        Some(file)
    } else {
        None
    };
    std::fs::remove_file(&marker_path)?;
    drop(data);
    drop(marker);
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "stored_outcome_retention_tests.rs"]
mod tests;

pub(crate) struct ManagedSegmentStatus {
    pub segments: usize,
    pub journal_bytes: u64,
    pub retired_segments: usize,
    pub retired_journal_bytes: u64,
    pub snapshot_pending: bool,
}
