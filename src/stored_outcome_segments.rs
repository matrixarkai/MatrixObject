// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Bounded-handle rollover for immutable binary outcome segments.
use super::outcome_log::{
    DecidedMutation, DurableOutcomeLog, RecoveredMutation, StoredOutcomePosition,
    StoredRecordMetadata,
};
use super::{
    argument, directory_sync, invalid, read_at, PayloadExtent, PayloadExtentStore,
    MAX_CAPACITY_BYTES, READY, SECTOR_BYTES,
};
use crate::{outcome::PageOutcome, MatrixReplicaService};
use prost::Message;
use ring::digest;
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
};
/// Every bounded outcome batch fits an empty segment at this capacity.
pub const MIN_SEGMENT_BYTES: u64 = 24 * 1024 * 1024;
pub const MAX_CACHED_SEALED_SEGMENTS: usize = 16;
const MAX_SEGMENTS: usize = 1_000_000;
const MAX_MANIFEST_BYTES: usize = 256 * 1024 * 1024;
const LINK_BYTES: usize = 128;
const LINK_MAGIC: &[u8; 8] = b"MOSEGL01";
const MAGIC: &[u8; 8] = b"MOSEGM01";
const ENVELOPE: usize = 44;

#[derive(Clone, PartialEq, Message)]
struct SegmentEntry {
    #[prost(uint64, tag = "1")]
    number: u64,
    #[prost(bytes = "vec", tag = "2")]
    store_id: Vec<u8>,
    // Zero only on the active segment. Sealed files have an exact immutable end.
    #[prost(uint64, tag = "3")]
    sealed_length: u64,
}
#[derive(Clone, PartialEq, Message)]
struct SegmentManifest {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    store_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    capacity: u64,
    #[prost(message, repeated, tag = "4")]
    entries: Vec<SegmentEntry>,
}
struct Writer {
    count: u64,
    manifest_length: u64,
    tail_hash: [u8; 32],
    active: Arc<DurableOutcomeLog>,
}
struct Index {
    entries: BTreeMap<u64, SegmentEntry>,
    by_id: BTreeMap<Vec<u8>, u64>,
    active: Arc<DurableOutcomeLog>,
    sealed_bytes: u64,
    cache: VecDeque<(u64, Arc<DurableOutcomeLog>)>,
}
struct ManifestLog {
    metadata: SegmentManifest,
    length: u64,
    tail_hash: [u8; 32],
}
struct ManifestAppend {
    offset: u64,
    bytes: [u8; LINK_BYTES],
}
/// One owner, multiple immutable segments, and a bounded cache of sealed readers.
///
/// The anchor is exclusively locked for the lifetime of this object. Ordinary
/// appends synchronize the selected segment once. Rollover also synchronizes a
/// new empty segment and persists a fixed-size chained manifest record before
/// any mutation is written there. Old positions remain valid across rollover.
/// This does not reclaim files or replace the server's journal/checkpoint path.
pub struct SegmentedOutcomeLog {
    path: PathBuf,
    _anchor: PayloadExtentStore,
    writer: Mutex<Writer>,
    index: RwLock<Index>,
    failed: AtomicBool,
    cache_limit: usize,
}
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}
fn segment_path(path: &Path, number: u64) -> PathBuf {
    sibling(path, &format!(".segment-{number:016x}"))
}
fn canonical_path(path: &Path) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| argument("segment anchor name is required"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(std::fs::canonicalize(parent)?.join(name))
}
fn validate(manifest: &SegmentManifest, id: &[u8], capacity: u64) -> io::Result<()> {
    if manifest.version != 1
        || manifest.store_id != id
        || manifest.capacity != capacity
        || manifest.entries.is_empty()
        || manifest.entries.len() > MAX_SEGMENTS
    {
        return Err(invalid("invalid segment manifest identity or bounds"));
    }
    let mut seen = HashSet::with_capacity(manifest.entries.len());
    for (number, entry) in manifest.entries.iter().enumerate() {
        if entry.number != number as u64
            || entry.store_id.len() != 16
            || !seen.insert(&entry.store_id)
            || (number + 1 == manifest.entries.len() && entry.sealed_length != 0)
            || (number + 1 < manifest.entries.len()
                && (entry.sealed_length <= SECTOR_BYTES as u64
                    || entry.sealed_length > capacity + SECTOR_BYTES as u64
                    || entry.sealed_length % SECTOR_BYTES as u64 != 0))
        {
            return Err(invalid("invalid segment manifest entry"));
        }
    }
    Ok(())
}
fn preflight(mut bytes: &[u8]) -> io::Result<()> {
    let mut entries = 0;
    while !bytes.is_empty() {
        let key = prost::encoding::decode_varint(&mut bytes)
            .map_err(|_| invalid("invalid segment manifest field"))?;
        match (key >> 3, key & 7) {
            (1 | 3, 0) => {
                prost::encoding::decode_varint(&mut bytes)
                    .map_err(|_| invalid("invalid segment manifest scalar"))?;
            }
            (field @ (2 | 4), 2) => {
                let length = usize::try_from(
                    prost::encoding::decode_varint(&mut bytes)
                        .map_err(|_| invalid("invalid segment manifest length"))?,
                )
                .map_err(|_| invalid("segment manifest length overflow"))?;
                if length > bytes.len()
                    || (field == 2 && length != 16)
                    || (field == 4 && length > 64)
                {
                    return Err(invalid("segment manifest field exceeds bounds"));
                }
                if field == 4 {
                    entries += 1;
                    if entries > MAX_SEGMENTS {
                        return Err(invalid("too many segments"));
                    }
                }
                bytes = &bytes[length..];
            }
            _ => return Err(invalid("unsupported segment manifest field")),
        }
    }
    Ok(())
}

fn repair_manifest_tail(
    file: &File,
    at: u64,
    anchor: Option<&Path>,
    next: u64,
    capacity: u64,
) -> io::Result<()> {
    let anchor = anchor.ok_or_else(|| invalid("incomplete segment manifest tail"))?;
    let path = segment_path(anchor, next);
    // A link is attempted only after its empty segment is durably initialized.
    // Never discard a link tail if that segment contains data or lost its identity.
    let lock = File::open(super::sibling(&path))?;
    let mut marker = [0; 57];
    read_at(&lock, &mut marker, 0)?;
    let empty = DurableOutcomeLog::open_existing(
        &path,
        capacity,
        &marker[8..24],
        Some(SECTOR_BYTES as u64),
    )?;
    let mut ready = [0];
    read_at(&File::open(&path)?, &mut ready, SECTOR_BYTES as u64 - 1)?;
    if ready[0] != 0 {
        return Err(invalid("pending manifest segment has held records"));
    }
    drop(empty);
    file.set_len(at)?;
    file.sync_all()
}
fn read_manifest(
    path: &Path,
    id: &[u8],
    capacity: u64,
    anchor: Option<&Path>,
) -> io::Result<ManifestLog> {
    let file = OpenOptions::new()
        .read(true)
        .write(anchor.is_some())
        .open(path)?;
    let length = file.metadata()?.len();
    if length < ENVELOPE as u64 || length > MAX_MANIFEST_BYTES as u64 {
        return Err(invalid("segment manifest file length exceeds bounds"));
    }
    let mut header = [0; ENVELOPE];
    read_at(&file, &mut header, 0)?;
    let size = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    let initial = ENVELOPE + size;
    if &header[..8] != MAGIC || size > 1024 || initial as u64 > length {
        return Err(invalid("segment manifest envelope damaged"));
    }
    let mut body = vec![0; size];
    read_at(&file, &mut body, ENVELOPE as u64)?;
    if digest::digest(&digest::SHA256, &body).as_ref() != &header[12..44] {
        return Err(invalid("segment manifest checksum damaged"));
    }
    preflight(&body)?;
    let mut manifest = SegmentManifest::decode(body.as_slice())
        .map_err(|_| invalid("invalid segment manifest protobuf"))?;
    if manifest.encode_to_vec() != body || manifest.entries.len() != 1 {
        return Err(invalid("noncanonical initial segment manifest"));
    }
    validate(&manifest, id, capacity)?;
    let mut hash: [u8; 32] = header[12..44].try_into().unwrap();
    let mut at = initial as u64;
    while at < length {
        let remaining = length - at;
        if remaining < LINK_BYTES as u64 {
            let mut tail = vec![0; remaining as usize];
            read_at(&file, &mut tail, at)?;
            let n = tail.len().min(8);
            if tail[..n] != LINK_MAGIC[..n] {
                return Err(invalid("unknown segment manifest tail"));
            }
            repair_manifest_tail(&file, at, anchor, manifest.entries.len() as u64, capacity)?;
            break;
        }
        let mut record = [0; LINK_BYTES];
        read_at(&file, &mut record, at)?;
        let number = u64::from_be_bytes(record[8..16].try_into().unwrap());
        let sealed = u64::from_be_bytes(record[48..56].try_into().unwrap());
        if &record[..8] != LINK_MAGIC
            || number != manifest.entries.len() as u64
            || &record[16..32] != id
            || record[56..88] != hash
            || digest::digest(&digest::SHA256, &record[..88]).as_ref() != &record[88..120]
            || record[120..127].iter().any(|n| *n != 0)
            || ![0, READY].contains(&record[127])
            || sealed <= SECTOR_BYTES as u64
            || sealed > capacity + SECTOR_BYTES as u64
            || sealed % SECTOR_BYTES as u64 != 0
            || manifest.entries.len() >= MAX_SEGMENTS
        {
            return Err(invalid("segment manifest link damaged"));
        }
        if record[127] != READY {
            if anchor.is_none() {
                return Err(invalid("unpublished segment manifest link"));
            }
            let mut output = &file;
            output.seek(SeekFrom::Start(at + 127))?;
            output.write_all(&[READY])?;
            output.sync_all()?;
        }
        manifest.entries.last_mut().unwrap().sealed_length = sealed;
        manifest.entries.push(SegmentEntry {
            number,
            store_id: record[32..48].to_vec(),
            sealed_length: 0,
        });
        hash = record[88..120].try_into().unwrap();
        at += LINK_BYTES as u64;
    }
    validate(&manifest, id, capacity)?;
    Ok(ManifestLog {
        metadata: manifest,
        length: at,
        tail_hash: hash,
    })
}
fn publish_rollover(path: &Path, append: &ManifestAppend) -> io::Result<()> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    if file.metadata()?.len() != append.offset {
        return Err(invalid("segment manifest append position changed"));
    }
    file.seek(SeekFrom::Start(append.offset))?;
    file.write_all(&append.bytes)?;
    file.seek(SeekFrom::Start(append.offset + 127))?;
    file.write_all(&[READY])?;
    file.sync_all()
}
fn publish_manifest(path: &Path, manifest: &SegmentManifest) -> io::Result<()> {
    let body = manifest.encode_to_vec();
    if body.len() > 1024 || manifest.entries.len() != 1 {
        return Err(argument("segment manifest exceeds byte limit"));
    }
    let pending = sibling(path, ".pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pending)?;
    file.write_all(MAGIC)?;
    file.write_all(&(body.len() as u32).to_be_bytes())?;
    file.write_all(digest::digest(&digest::SHA256, &body).as_ref())?;
    file.write_all(&body)?;
    file.sync_all()?;
    std::fs::rename(&pending, path)?;
    directory_sync(path)
}
fn unpublished_segment(path: &Path, capacity: u64) -> io::Result<Arc<DurableOutcomeLog>> {
    let exists = path.try_exists()?;
    if exists
        && (!super::sibling(path).try_exists()?
            || std::fs::metadata(path)?.len() > SECTOR_BYTES as u64)
    {
        return Err(invalid(
            "unlisted outcome segment contains data or lost its identity",
        ));
    }
    // Only the underlying identity protocol may rebuild an unpublished header.
    // An initialized truncated file, or one that has held records, is refused.
    let log = DurableOutcomeLog::open(path, capacity)?;
    if log.length() != SECTOR_BYTES as u64 {
        return Err(invalid("unlisted outcome segment is not empty"));
    }
    Ok(Arc::new(log))
}
impl SegmentedOutcomeLog {
    pub fn open(path: impl AsRef<Path>, segment_capacity: u64) -> io::Result<Self> {
        Self::open_inner(path.as_ref(), segment_capacity, MAX_CACHED_SEALED_SEGMENTS)
    }

    fn open_inner(path: &Path, capacity: u64, cache_limit: usize) -> io::Result<Self> {
        if !(MIN_SEGMENT_BYTES..=MAX_CAPACITY_BYTES).contains(&capacity)
            || capacity % SECTOR_BYTES as u64 != 0
            || cache_limit == 0
        {
            return Err(argument(
                "segment capacity must be a sector multiple from 24 through 512 MiB",
            ));
        }
        let path = canonical_path(path)?;
        let manifest_path = sibling(&path, ".segments");
        let lock_path = super::sibling(&path);
        let any = path.try_exists()?
            || manifest_path.try_exists()?
            || segment_path(&path, 0).try_exists()?;
        let allow_create = lock_path.try_exists()? || !any;
        let anchor = PayloadExtentStore::open_inner(&path, capacity, false, allow_create, 3)?;
        if anchor.length() != SECTOR_BYTES as u64 {
            return Err(invalid("segment anchor contains unexpected data"));
        }
        let mut ready = [0];
        read_at(&anchor.file, &mut ready, SECTOR_BYTES as u64 - 1)?;
        if !manifest_path.try_exists()? {
            if ready[0] == READY {
                return Err(invalid("initialized segment manifest is missing"));
            }
            let first = unpublished_segment(&segment_path(&path, 0), capacity)?;
            let manifest = SegmentManifest {
                version: 1,
                store_id: anchor.id.clone(),
                capacity,
                entries: vec![SegmentEntry {
                    number: 0,
                    store_id: first.store_id().to_vec(),
                    sealed_length: 0,
                }],
            };
            publish_manifest(&manifest_path, &manifest)?;
            drop(first);
        }
        let recovered = read_manifest(&manifest_path, anchor.store_id(), capacity, Some(&path))?;
        let manifest = recovered.metadata;
        let unlisted = segment_path(&path, manifest.entries.len() as u64);
        if unlisted.try_exists()? || super::sibling(&unlisted).try_exists()? {
            unpublished_segment(&unlisted, capacity)?;
        }
        for entry in &manifest.entries[..manifest.entries.len() - 1] {
            DurableOutcomeLog::open_existing(
                &segment_path(&path, entry.number),
                capacity,
                &entry.store_id,
                Some(entry.sealed_length),
            )?;
        }
        let last = manifest.entries.last().unwrap();
        let active = Arc::new(DurableOutcomeLog::open_existing(
            &segment_path(&path, last.number),
            capacity,
            &last.store_id,
            None,
        )?);
        if ready[0] != READY {
            let mut file = &anchor.file;
            file.seek(SeekFrom::Start(SECTOR_BYTES as u64 - 1))?;
            file.write_all(&[READY])?;
            file.sync_data()?;
        }
        let by_id = manifest
            .entries
            .iter()
            .map(|e| (e.store_id.clone(), e.number))
            .collect();
        let sealed_bytes = manifest
            .entries
            .iter()
            .filter(|e| e.sealed_length != 0)
            .map(|e| e.sealed_length - SECTOR_BYTES as u64)
            .sum();
        let count = manifest.entries.len() as u64;
        let entries = manifest
            .entries
            .into_iter()
            .map(|e| (e.number, e))
            .collect();
        Ok(Self {
            path,
            _anchor: anchor,
            writer: Mutex::new(Writer {
                count,
                manifest_length: recovered.length,
                tail_hash: recovered.tail_hash,
                active: Arc::clone(&active),
            }),
            index: RwLock::new(Index {
                entries,
                by_id,
                active,
                sealed_bytes,
                cache: VecDeque::new(),
            }),
            failed: AtomicBool::new(false),
            cache_limit,
        })
    }
    fn healthy(&self) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire)
            || self.writer.is_poisoned()
            || self.index.is_poisoned()
        {
            return Err(invalid("segmented outcome log is fenced"));
        }
        Ok(())
    }
    pub fn store_id(&self) -> &[u8] {
        self._anchor.store_id()
    }
    /// Committed record bytes, including their metadata/padding and excluding file headers.
    pub fn logical_length(&self) -> io::Result<u64> {
        self.healthy()?;
        let index = self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?;
        Ok(index.sealed_bytes + index.active.length() - SECTOR_BYTES as u64)
    }
    pub fn segment_count(&self) -> io::Result<usize> {
        self.healthy()?;
        Ok(self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .entries
            .len())
    }
    pub fn append_page(&self, outcome: &PageOutcome) -> io::Result<StoredRecordMetadata> {
        self.append_decided_batch(&[DecidedMutation::Page(outcome)])
    }
    pub fn append_decided_batch(
        &self,
        mutations: &[DecidedMutation<'_>],
    ) -> io::Result<StoredRecordMetadata> {
        self.append_with(mutations, File::sync_data, publish_rollover)
    }

    fn append_with(
        &self,
        mutations: &[DecidedMutation<'_>],
        sync: impl FnOnce(&File) -> io::Result<()>,
        publish: impl FnOnce(&Path, &ManifestAppend) -> io::Result<()>,
    ) -> io::Result<StoredRecordMetadata> {
        self.healthy()?;
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| invalid("segment writer is poisoned"))?;
        self.healthy()?;
        let mut sync = Some(sync);
        match writer
            .active
            .append_with_sync(mutations, |f| sync.take().unwrap()(f))
        {
            Ok(record) => return Ok(record),
            Err(error)
                if error
                    .get_ref()
                    .is_some_and(|error| error.is::<super::outcome_log::CapacityExhausted>()) => {}
            Err(error) => {
                if writer.active.healthy().is_err() {
                    self.failed.store(true, Ordering::Release);
                }
                return Err(error);
            }
        }
        if writer.count >= MAX_SEGMENTS as u64
            || writer.manifest_length + LINK_BYTES as u64 > MAX_MANIFEST_BYTES as u64
        {
            return Err(io::Error::other("segment count limit reached"));
        }
        let rollover = (|| {
            let number = writer.count;
            let active =
                unpublished_segment(&segment_path(&self.path, number), self._anchor.capacity)?;
            if self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?
                .by_id
                .contains_key(active.store_id())
            {
                return Err(invalid("new segment identity is already owned"));
            }
            let length = writer.active.length();
            let mut bytes = [0; LINK_BYTES];
            bytes[..8].copy_from_slice(LINK_MAGIC);
            bytes[8..16].copy_from_slice(&number.to_be_bytes());
            bytes[16..32].copy_from_slice(self.store_id());
            bytes[32..48].copy_from_slice(active.store_id());
            bytes[48..56].copy_from_slice(&length.to_be_bytes());
            bytes[56..88].copy_from_slice(&writer.tail_hash);
            let hash = digest::digest(&digest::SHA256, &bytes[..88]);
            bytes[88..120].copy_from_slice(hash.as_ref());
            writer.active.seal()?;
            publish(
                &sibling(&self.path, ".segments"),
                &ManifestAppend {
                    offset: writer.manifest_length,
                    bytes,
                },
            )?;
            let mut index = self
                .index
                .write()
                .map_err(|_| invalid("segment index is poisoned"))?;
            index.entries.get_mut(&(number - 1)).unwrap().sealed_length = length;
            index.sealed_bytes += length - SECTOR_BYTES as u64;
            index.by_id.insert(active.store_id().to_vec(), number);
            index.entries.insert(
                number,
                SegmentEntry {
                    number,
                    store_id: active.store_id().to_vec(),
                    sealed_length: 0,
                },
            );
            index.active = Arc::clone(&active);
            writer.active = active;
            writer.count += 1;
            writer.manifest_length += LINK_BYTES as u64;
            writer.tail_hash = hash.as_ref().try_into().unwrap();
            drop(index);
            writer
                .active
                .append_with_sync(mutations, |f| sync.take().unwrap()(f))
        })();
        if rollover.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        rollover
    }
    fn handle(&self, id: &[u8]) -> io::Result<Arc<DurableOutcomeLog>> {
        self.healthy()?;
        let (number, entry) = {
            let index = self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?;
            let &number = index
                .by_id
                .get(id)
                .ok_or_else(|| argument("outcome segment is not owned by this log"))?;
            if id == index.active.store_id() {
                return Ok(Arc::clone(&index.active));
            }
            if let Some((_, cached)) = index.cache.iter().find(|(n, _)| *n == number) {
                return Ok(Arc::clone(cached));
            }
            (number, index.entries[&number].clone())
        };
        let opened = Arc::new(DurableOutcomeLog::open_existing(
            &segment_path(&self.path, entry.number),
            self._anchor.capacity,
            &entry.store_id,
            Some(entry.sealed_length),
        )?);
        let mut index = self
            .index
            .write()
            .map_err(|_| invalid("segment index is poisoned"))?;
        if let Some((_, cached)) = index.cache.iter().find(|(n, _)| *n == number) {
            return Ok(Arc::clone(cached));
        }
        while index.cache.len() >= self.cache_limit {
            index.cache.pop_front();
        }
        index.cache.push_back((number, Arc::clone(&opened)));
        Ok(opened)
    }
    pub fn read_metadata(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<StoredRecordMetadata> {
        self.handle(&position.store_id)?.read_metadata(position)
    }
    pub fn read_outcomes(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<Vec<RecoveredMutation>> {
        self.handle(&position.store_id)?.read_outcomes(position)
    }
    pub fn read_payload_range(
        &self,
        extent: &PayloadExtent,
        offset: u64,
        length: u64,
    ) -> io::Result<Vec<u8>> {
        self.handle(&extent.store_id)?
            .read_payload_range(extent, offset, length)
    }
    /// Walk a committed prefix captured at entry, excluding a supplied last-applied record.
    /// Metadata is checked; payload integrity is established only by read/replay.
    pub fn scan_metadata_after(
        &self,
        after: Option<&StoredOutcomePosition>,
        mut visit: impl FnMut(&StoredRecordMetadata) -> io::Result<()>,
    ) -> io::Result<()> {
        self.healthy()?;
        let (entries, active_end) = {
            let index = self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?;
            (
                index.entries.values().cloned().collect::<Vec<_>>(),
                index.active.length(),
            )
        };
        let (start, offset) = if let Some(position) = after {
            self.read_metadata(position)?;
            let number = entries
                .iter()
                .position(|e| e.store_id == position.store_id)
                .ok_or_else(|| argument("replay position is outside captured segments"))?;
            (
                number,
                position
                    .offset
                    .checked_add(position.length)
                    .ok_or_else(|| argument("replay position overflow"))?,
            )
        } else {
            (0, SECTOR_BYTES as u64)
        };
        for (number, entry) in entries.iter().enumerate().skip(start) {
            let log = self.handle(&entry.store_id)?;
            let limit = if entry.sealed_length == 0 {
                active_end
            } else {
                entry.sealed_length
            };
            let mut at = if number == start {
                offset
            } else {
                SECTOR_BYTES as u64
            };
            if at > limit {
                return Err(invalid("replay position exceeds committed cut"));
            }
            while at < limit {
                let record = log
                    .next_metadata(at)?
                    .ok_or_else(|| invalid("committed segment prefix is missing"))?;
                at = at
                    .checked_add(record.position.length)
                    .ok_or_else(|| invalid("record end overflow"))?;
                if at > limit {
                    return Err(invalid("record crosses captured replay cut"));
                }
                visit(&record)?;
            }
        }
        Ok(())
    }
    /// The caller must discard startup state on any error, never serve partial replay.
    pub fn replay_after(
        &self,
        after: Option<&StoredOutcomePosition>,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> io::Result<usize> {
        let mut count = 0;
        self.scan_metadata_after(after, |record| {
            for outcome in self.read_outcomes(&record.position)? {
                outcome
                    .apply(service, max_pages)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                count += 1;
            }
            Ok(())
        })?;
        Ok(count)
    }
}
#[cfg(all(test, unix))]
#[path = "stored_outcome_segment_tests.rs"]
mod tests;
