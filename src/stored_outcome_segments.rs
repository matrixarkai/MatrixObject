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
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
const RETAINED_MAGIC: &[u8; 8] = b"MOSEGM02";
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
    #[prost(message, repeated, tag = "5")]
    retired: Vec<SegmentEntry>,
}
struct Writer {
    count: u64,
    manifest_length: u64,
    tail_hash: [u8; 32],
    active: Arc<DurableOutcomeLog>,
}
/// A committed metadata prefix captured without copying the segment index.
/// Later appends and rollovers are excluded. Only one segment handle is pinned.
pub struct StoredMetadataCursor<'a> {
    _pin: retention::ReadPin<'a>,
    owner: &'a SegmentedOutcomeLog,
    number: u64,
    offset: u64,
    last_number: u64,
    last_end: u64,
    current: Option<(Arc<DurableOutcomeLog>, u64)>,
}
impl StoredMetadataCursor<'_> {
    pub fn next_record(&mut self) -> io::Result<Option<StoredRecordMetadata>> {
        self.owner.healthy()?;
        loop {
            if self.number == self.last_number && self.offset == self.last_end {
                return Ok(None);
            }
            if self.current.is_none() {
                let entry = self
                    .owner
                    .index
                    .read()
                    .map_err(|_| invalid("segment index is poisoned"))?
                    .entries
                    .get(&self.number)
                    .cloned()
                    .ok_or_else(|| invalid("captured segment is missing"))?;
                let limit = if self.number == self.last_number {
                    self.last_end
                } else {
                    entry.sealed_length
                };
                if limit < SECTOR_BYTES as u64
                    || (entry.sealed_length != 0 && limit > entry.sealed_length)
                {
                    return Err(invalid("invalid captured segment boundary"));
                }
                let log = self.owner.handle(&entry.store_id)?;
                if limit > log.length() {
                    return Err(invalid("captured segment prefix is missing"));
                }
                self.current = Some((log, limit));
            }
            let limit = self.current.as_ref().unwrap().1;
            if self.offset > limit {
                return Err(invalid("cursor exceeds captured segment"));
            }
            if self.offset == limit {
                self.number = self
                    .owner
                    .index
                    .read()
                    .map_err(|_| invalid("segment index is poisoned"))?
                    .entries
                    .range((
                        std::ops::Bound::Excluded(self.number),
                        std::ops::Bound::Included(self.last_number),
                    ))
                    .next()
                    .map(|(number, _)| *number)
                    .ok_or_else(|| invalid("captured segment successor is missing"))?;
                self.offset = SECTOR_BYTES as u64;
                self.current = None;
                continue;
            }
            let record = self
                .current
                .as_ref()
                .unwrap()
                .0
                .next_metadata(self.offset)?
                .ok_or_else(|| invalid("captured record is missing"))?;
            let end = self
                .offset
                .checked_add(record.position.length)
                .ok_or_else(|| invalid("captured record end overflow"))?;
            if end <= self.offset || end > limit {
                return Err(invalid("record crosses captured replay cut"));
            }
            self.offset = end;
            return Ok(Some(record));
        }
    }
}

struct Index {
    entries: BTreeMap<u64, SegmentEntry>,
    by_id: BTreeMap<Vec<u8>, u64>,
    active: Arc<DurableOutcomeLog>,
    sealed_bytes: u64,
    retired: BTreeMap<u64, SegmentEntry>,
    retired_bytes: u64,
    retirement_changed: bool,
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
    reader_gate: Mutex<()>,
    readers: AtomicUsize,
    collection: Mutex<()>,
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
    if ![1, 2].contains(&manifest.version)
        || manifest.store_id != id
        || manifest.capacity != capacity
        || manifest.entries.is_empty()
        || manifest.entries.len() > MAX_SEGMENTS
        || manifest.retired.len() > MAX_SEGMENTS
        || (manifest.version == 1 && !manifest.retired.is_empty())
    {
        return Err(invalid("invalid segment manifest identity or bounds"));
    }
    let mut seen = HashSet::with_capacity(manifest.entries.len() + manifest.retired.len());
    let sealed = |entry: &SegmentEntry| {
        entry.sealed_length > SECTOR_BYTES as u64
            && entry.sealed_length <= capacity + SECTOR_BYTES as u64
            && entry.sealed_length % SECTOR_BYTES as u64 == 0
    };
    let mut previous = None;
    let mut numbers = HashSet::with_capacity(manifest.entries.len());
    for (at, entry) in manifest.entries.iter().enumerate() {
        if (manifest.version == 1 && entry.number != at as u64)
            || previous.is_some_and(|n| n >= entry.number)
            || entry.store_id.len() != 16
            || !seen.insert(&entry.store_id)
            || (at + 1 == manifest.entries.len() && entry.sealed_length != 0)
            || (at + 1 < manifest.entries.len() && !sealed(entry))
        {
            return Err(invalid("invalid segment manifest entry"));
        }
        previous = Some(entry.number);
        numbers.insert(entry.number);
    }
    let last = manifest.entries.last().unwrap().number;
    let mut previous = None;
    for entry in &manifest.retired {
        if previous.is_some_and(|n| n >= entry.number)
            || entry.number >= last
            || numbers.contains(&entry.number)
            || entry.store_id.len() != 16
            || !seen.insert(&entry.store_id)
            || !sealed(entry)
        {
            return Err(invalid("invalid retired segment entry"));
        }
        previous = Some(entry.number);
    }
    Ok(())
}
fn preflight(mut bytes: &[u8]) -> io::Result<()> {
    let mut entries = [0usize; 2];
    while !bytes.is_empty() {
        let key = prost::encoding::decode_varint(&mut bytes)
            .map_err(|_| invalid("invalid segment manifest field"))?;
        match (key >> 3, key & 7) {
            (1 | 3, 0) => {
                prost::encoding::decode_varint(&mut bytes)
                    .map_err(|_| invalid("invalid segment manifest scalar"))?;
            }
            (field @ (2 | 4 | 5), 2) => {
                let length = usize::try_from(
                    prost::encoding::decode_varint(&mut bytes)
                        .map_err(|_| invalid("invalid segment manifest length"))?,
                )
                .map_err(|_| invalid("segment manifest length overflow"))?;
                if length > bytes.len()
                    || (field == 2 && length != 16)
                    || (field >= 4 && length > 64)
                {
                    return Err(invalid("segment manifest field exceeds bounds"));
                }
                if field >= 4 {
                    entries[(field - 4) as usize] += 1;
                    if entries[(field - 4) as usize] > MAX_SEGMENTS {
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
    let retained = &header[..8] == RETAINED_MAGIC;
    if (!retained && &header[..8] != MAGIC)
        || (!retained && size > 1024)
        || size > MAX_MANIFEST_BYTES - ENVELOPE
        || initial as u64 > length
    {
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
    if manifest.encode_to_vec() != body
        || manifest.version != if retained { 2 } else { 1 }
        || (!retained && manifest.entries.len() != 1)
    {
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
            let next = manifest
                .entries
                .last()
                .unwrap()
                .number
                .checked_add(1)
                .ok_or_else(|| invalid("segment number overflow"))?;
            repair_manifest_tail(&file, at, anchor, next, capacity)?;
            break;
        }
        let mut record = [0; LINK_BYTES];
        read_at(&file, &mut record, at)?;
        let number = u64::from_be_bytes(record[8..16].try_into().unwrap());
        let sealed = u64::from_be_bytes(record[48..56].try_into().unwrap());
        if &record[..8] != LINK_MAGIC
            || Some(number) != manifest.entries.last().unwrap().number.checked_add(1)
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
                retired: Vec::new(),
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
        let count = manifest
            .entries
            .last()
            .unwrap()
            .number
            .checked_add(1)
            .ok_or_else(|| invalid("segment number overflow"))?;
        let unlisted = segment_path(&path, count);
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
        let retired_bytes = manifest
            .retired
            .iter()
            .map(|e| e.sealed_length - SECTOR_BYTES as u64)
            .sum();
        let retired = manifest
            .retired
            .into_iter()
            .map(|e| (e.number, e))
            .collect();
        let entries = manifest
            .entries
            .into_iter()
            .map(|e| (e.number, e))
            .collect();
        let log = Self {
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
                retired,
                retired_bytes,
                retirement_changed: false,
                cache: VecDeque::new(),
            }),
            failed: AtomicBool::new(false),
            cache_limit,
            reader_gate: Mutex::new(()),
            readers: AtomicUsize::new(0),
            collection: Mutex::new(()),
        };
        log.cleanup_retired()?;
        Ok(log)
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
        Ok(index.sealed_bytes + index.retired_bytes + index.active.length() - SECTOR_BYTES as u64)
    }
    pub fn segment_count(&self) -> io::Result<usize> {
        self.healthy()?;
        let index = self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?;
        Ok(index.entries.len() + index.retired.len())
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
        self.append_with_metadata(mutations, None, sync, publish)
    }
    pub fn append_decided_batch_with_metadata(
        &self,
        mutations: &[DecidedMutation<'_>],
        metadata: &[u8],
    ) -> io::Result<StoredRecordMetadata> {
        self.append_with_metadata(mutations, Some(metadata), File::sync_data, publish_rollover)
    }
    fn append_with_metadata(
        &self,
        mutations: &[DecidedMutation<'_>],
        metadata: Option<&[u8]>,
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
            .append_with_sync_metadata(mutations, metadata, |f| sync.take().unwrap()(f))
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
        if self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .entries
            .len()
            >= MAX_SEGMENTS
            || writer.manifest_length + LINK_BYTES as u64 > MAX_MANIFEST_BYTES as u64
        {
            return Err(io::Error::other("segment count limit reached"));
        }
        let rollover = (|| {
            let number = writer.count;
            let next = number
                .checked_add(1)
                .ok_or_else(|| invalid("segment number overflow"))?;
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
            writer.count = next;
            writer.manifest_length += LINK_BYTES as u64;
            writer.tail_hash = hash.as_ref().try_into().unwrap();
            drop(index);
            writer
                .active
                .append_with_sync_metadata(mutations, metadata, |f| sync.take().unwrap()(f))
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
        let _pin = self.pin()?;
        self.handle(&position.store_id)?.read_metadata(position)
    }
    pub fn read_outcomes(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<Vec<RecoveredMutation>> {
        let _pin = self.pin()?;
        self.handle(&position.store_id)?.read_outcomes(position)
    }
    pub fn read_outcomes_with_metadata(
        &self,
        position: &StoredOutcomePosition,
    ) -> io::Result<(Vec<RecoveredMutation>, Vec<u8>)> {
        let _pin = self.pin()?;
        self.handle(&position.store_id)?
            .read_outcomes_with_metadata(position)
    }
    pub fn read_payload_range(
        &self,
        extent: &PayloadExtent,
        offset: u64,
        length: u64,
    ) -> io::Result<Vec<u8>> {
        let _pin = self.pin()?;
        self.handle(&extent.store_id)?
            .read_payload_range(extent, offset, length)
    }
    /// Walk a committed prefix captured at entry, excluding a supplied last-applied record.
    /// Metadata is checked; payload integrity is established only by read/replay.
    pub fn metadata_cursor_after(
        &self,
        after: Option<&StoredOutcomePosition>,
    ) -> io::Result<StoredMetadataCursor<'_>> {
        let pin = self.pin()?;
        let (first, last_number, last_end, start_number) = {
            let index = self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?;
            let first = *index
                .entries
                .keys()
                .next()
                .ok_or_else(|| invalid("empty segment index"))?;
            let last = *index
                .entries
                .keys()
                .next_back()
                .ok_or_else(|| invalid("empty segment index"))?;
            let start = match after {
                Some(position) => *index
                    .by_id
                    .get(&position.store_id)
                    .ok_or_else(|| argument("replay position is outside this log"))?,
                None => first,
            };
            (first, last, index.active.length(), start)
        };
        let offset = match after {
            Some(position) => {
                self.read_metadata(position)?;
                position
                    .offset
                    .checked_add(position.length)
                    .ok_or_else(|| argument("replay position overflow"))?
            }
            None => SECTOR_BYTES as u64,
        };
        if start_number < first
            || start_number > last_number
            || (start_number == last_number && offset > last_end)
        {
            return Err(argument("replay position is beyond the captured prefix"));
        }
        Ok(StoredMetadataCursor {
            _pin: pin,
            owner: self,
            number: start_number,
            offset,
            last_number,
            last_end,
            current: None,
        })
    }

    /// Capture records after a prior cut through an explicit committed cut.
    pub fn metadata_cursor_after_through(
        &self,
        after: Option<&StoredOutcomePosition>,
        through: &StoredOutcomePosition,
    ) -> io::Result<StoredMetadataCursor<'_>> {
        let mut cursor = self.metadata_cursor_after(after)?;
        self.read_metadata(through)?;
        let number = *self
            .index
            .read()
            .map_err(|_| invalid("segment index is poisoned"))?
            .by_id
            .get(&through.store_id)
            .ok_or_else(|| argument("end record is outside this log"))?;
        let end = through
            .offset
            .checked_add(through.length)
            .ok_or_else(|| argument("replay position overflow"))?;
        if number < cursor.number
            || (number == cursor.number && end < cursor.offset)
            || number > cursor.last_number
            || (number == cursor.last_number && end > cursor.last_end)
        {
            return Err(argument("metadata cut is outside the captured range"));
        }
        cursor.last_number = number;
        cursor.last_end = end;
        Ok(cursor)
    }

    /// Capture an inclusive, already committed record range. This permits a
    /// streaming checkpoint reader to skip interleaved foreground decisions.
    pub fn metadata_cursor_between(
        &self,
        first: &StoredOutcomePosition,
        last: &StoredOutcomePosition,
    ) -> io::Result<StoredMetadataCursor<'_>> {
        let pin = self.pin()?;
        self.read_metadata(first)?;
        self.read_metadata(last)?;
        let (number, last_number) = {
            let index = self
                .index
                .read()
                .map_err(|_| invalid("segment index is poisoned"))?;
            (
                *index
                    .by_id
                    .get(&first.store_id)
                    .ok_or_else(|| argument("first record is outside this log"))?,
                *index
                    .by_id
                    .get(&last.store_id)
                    .ok_or_else(|| argument("last record is outside this log"))?,
            )
        };
        let last_end = last
            .offset
            .checked_add(last.length)
            .ok_or_else(|| argument("replay position overflow"))?;
        if number > last_number || (number == last_number && first.offset > last.offset) {
            return Err(argument("reversed metadata record range"));
        }
        Ok(StoredMetadataCursor {
            _pin: pin,
            owner: self,
            number,
            offset: first.offset,
            last_number,
            last_end,
            current: None,
        })
    }

    pub fn scan_metadata_after(
        &self,
        after: Option<&StoredOutcomePosition>,
        mut visit: impl FnMut(&StoredRecordMetadata) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut cursor = self.metadata_cursor_after(after)?;
        while let Some(record) = cursor.next_record()? {
            visit(&record)?;
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

#[path = "stored_outcome_retention.rs"]
mod retention;
