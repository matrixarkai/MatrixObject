// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Immutable sector-aligned payloads with small binary protobuf descriptors.
//!
//! A successful append synchronizes payload bytes before returning descriptors.
//! Callers must durably publish those descriptors in their metadata journal before
//! acknowledging an object mutation. This module does not perform that publication.
use prost::Message;
use ring::{
    digest,
    rand::{SecureRandom, SystemRandom},
};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};

pub const SECTOR_BYTES: usize = 4096;
pub const MAX_EXTENT_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_EXTENTS: usize = 64;
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CAPACITY_BYTES: u64 = 512 * 1024 * 1024;
const DATA_MAGIC: &[u8; 8] = b"MOEXDATA";
const LOCK_MAGIC: &[u8; 8] = b"MOEXLOCK";
const MARKER_BYTES: usize = 56;
const READY: u8 = 0xa5;
const ENVELOPE_BYTES: usize = 44;

/// A journal/checkpoint value; it contains no payload or machine-local path.
#[derive(Clone, PartialEq, Message)]
pub struct PayloadExtent {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub store_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(uint64, tag = "4")]
    pub length: u64,
    /// SHA-256 of exactly the logical payload, excluding sector padding.
    #[prost(bytes = "vec", tag = "5")]
    pub sha256: Vec<u8>,
    /// Concatenated SHA-256 digests of complete padded sectors, in order.
    #[prost(bytes = "vec", tag = "6")]
    pub sector_sha256: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
struct FileHeader {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "vec", tag = "2")]
    store_id: Vec<u8>,
    #[prost(uint32, tag = "3")]
    sector_bytes: u32,
    #[prost(uint64, tag = "4")]
    capacity_bytes: u64,
}

/// One exclusively owned append-only payload file. Capacity excludes its header.
///
/// Existing payloads are never rewritten by append. File identity is also kept
/// in a stable lock file so replacement/missing initialized files are refused.
/// Range reads use positional I/O and verify only the sectors they touch.
/// A write or synchronization error fences this handle until it is reopened.
pub struct PayloadExtentStore {
    file: File,
    _lock: File,
    id: Vec<u8>,
    capacity: u64,
    end: AtomicU64,
    writer: Mutex<()>,
    failed: AtomicBool,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn argument(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn padded(length: u64) -> io::Result<u64> {
    length
        .checked_add(SECTOR_BYTES as u64 - 1)
        .map(|n| n / SECTOR_BYTES as u64 * SECTOR_BYTES as u64)
        .ok_or_else(|| argument("payload length overflow"))
}
fn sibling(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    name.into()
}
fn directory_sync(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path.parent().unwrap())?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "payload initialization requires Unix directory synchronization",
        ))
    }
}

fn encode_header(id: &[u8], capacity: u64) -> [u8; SECTOR_BYTES] {
    encode_profile_header(id, capacity, 1)
}
fn encode_profile_header(id: &[u8], capacity: u64, profile: u32) -> [u8; SECTOR_BYTES] {
    let body = FileHeader {
        version: profile,
        store_id: id.to_vec(),
        sector_bytes: SECTOR_BYTES as u32,
        capacity_bytes: capacity,
    }
    .encode_to_vec();
    let mut header = [0; SECTOR_BYTES];
    header[..8].copy_from_slice(DATA_MAGIC);
    header[8..12].copy_from_slice(&(body.len() as u32).to_be_bytes());
    header[12..44].copy_from_slice(digest::digest(&digest::SHA256, &body).as_ref());
    header[44..44 + body.len()].copy_from_slice(&body);
    header
}
fn verify_header(file: &mut File, id: &[u8], capacity: u64, profile: u32) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = [0; SECTOR_BYTES];
    file.read_exact(&mut bytes)?;
    if &bytes[..8] != DATA_MAGIC {
        return Err(invalid("unknown payload file format"));
    }
    let length = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let padding_end = SECTOR_BYTES - usize::from(profile != 1);
    if length == 0 || length > padding_end - ENVELOPE_BYTES {
        return Err(invalid("invalid payload header length"));
    }
    let body = &bytes[ENVELOPE_BYTES..ENVELOPE_BYTES + length];
    if digest::digest(&digest::SHA256, body).as_ref() != &bytes[12..44]
        || bytes[ENVELOPE_BYTES + length..padding_end]
            .iter()
            .any(|n| *n != 0)
        || (profile != 1 && ![0, READY].contains(&bytes[SECTOR_BYTES - 1]))
    {
        return Err(invalid("payload header checksum or padding damaged"));
    }
    let header =
        FileHeader::decode(body).map_err(|_| invalid("invalid payload header protobuf"))?;
    if header.encode_to_vec() != body {
        return Err(invalid("noncanonical payload file header"));
    }
    if header.version != profile
        || header.store_id != id
        || header.sector_bytes != SECTOR_BYTES as u32
        || header.capacity_bytes != capacity
    {
        return Err(invalid(
            "payload file identity, capacity or version mismatch",
        ));
    }
    Ok(())
}
fn marker(lock: &mut File, path: &Path) -> io::Result<(Vec<u8>, bool)> {
    let length = lock.metadata()?.len();
    if length < MARKER_BYTES as u64 {
        if path.try_exists()? {
            return Err(invalid(
                "uninitialized payload identity has an existing data file",
            ));
        }
        let mut id = [0; 16];
        SystemRandom::new()
            .fill(&mut id)
            .map_err(|_| io::Error::other("payload identity generation failed"))?;
        let mut bytes = Vec::from(LOCK_MAGIC.as_slice());
        bytes.extend_from_slice(&id);
        let hash = digest::digest(&digest::SHA256, &bytes);
        bytes.extend_from_slice(hash.as_ref());
        lock.set_len(0)?;
        lock.seek(SeekFrom::Start(0))?;
        lock.write_all(&bytes)?;
        lock.sync_all()?;
        directory_sync(path)?;
        return Ok((id.to_vec(), false));
    }
    if length != MARKER_BYTES as u64 && length != MARKER_BYTES as u64 + 1 {
        return Err(invalid("invalid payload identity marker length"));
    }
    let mut bytes = vec![0; length as usize];
    lock.seek(SeekFrom::Start(0))?;
    lock.read_exact(&mut bytes)?;
    if &bytes[..8] != LOCK_MAGIC
        || digest::digest(&digest::SHA256, &bytes[..24]).as_ref() != &bytes[24..56]
    {
        return Err(invalid("payload identity marker checksum mismatch"));
    }
    let ready = length == MARKER_BYTES as u64 + 1;
    if ready && bytes[MARKER_BYTES] != READY {
        return Err(invalid("invalid payload initialization marker"));
    }
    Ok((bytes[8..24].to_vec(), ready))
}
impl PayloadExtentStore {
    /// Open or initialize a file with a fixed, sector-multiple payload capacity.
    /// Unaligned interrupted tails are refused; use complete published roots with
    /// open_recovered to reclaim unpublished bytes explicitly.
    pub fn open(path: impl AsRef<Path>, capacity: u64) -> io::Result<Self> {
        Self::open_inner(path.as_ref(), capacity, false, true, 1)
    }
    fn open_inner(
        path: &Path,
        capacity: u64,
        allow_partial_tail: bool,
        allow_create: bool,
        profile: u32,
    ) -> io::Result<Self> {
        if capacity < SECTOR_BYTES as u64
            || capacity > MAX_CAPACITY_BYTES
            || capacity % SECTOR_BYTES as u64 != 0
        {
            return Err(argument(
                "payload capacity must be a sector multiple between 4096 and 536870912 bytes",
            ));
        }
        let name = path
            .file_name()
            .ok_or_else(|| argument("payload file name is required"))?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let path = std::fs::canonicalize(parent)?.join(name);
        let mut lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(allow_create)
            .truncate(false)
            .open(sibling(&path))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let (id, ready) = marker(&mut lock, &path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(!ready)
            .truncate(false)
            .open(&path)?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        if !ready {
            // No caller can append before READY is durable. Only an unpublished
            // header may be rebuilt after interrupted initialization.
            if file.metadata()?.len() > SECTOR_BYTES as u64 {
                return Err(invalid("uninitialized payload file contains data"));
            }
            file.set_len(SECTOR_BYTES as u64)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&if profile == 1 {
                encode_header(&id, capacity)
            } else {
                encode_profile_header(&id, capacity, profile)
            })?;
            file.sync_all()?;
            directory_sync(&path)?;
            lock.seek(SeekFrom::End(0))?;
            lock.write_all(&[READY])?;
            lock.sync_all()?;
        }
        verify_header(&mut file, &id, capacity, profile)?;
        let end = file.metadata()?.len();
        if end < SECTOR_BYTES as u64
            || end > SECTOR_BYTES as u64 + capacity
            || (!allow_partial_tail && end % SECTOR_BYTES as u64 != 0)
        {
            return Err(invalid(
                "payload file length exceeds bounds or has an interrupted tail",
            ));
        }
        Ok(Self {
            file,
            _lock: lock,
            id,
            capacity,
            end: AtomicU64::new(end),
            writer: Mutex::new(()),
            failed: AtomicBool::new(false),
        })
    }
    /// Reclaim only bytes after the highest complete published extent.
    ///
    /// The caller MUST supply the complete published and retained metadata roots for this file.
    /// Every root is validated before any truncation. An empty set explicitly
    /// declares that no payload in the file is published. Interior holes remain.
    pub fn open_recovered(
        path: impl AsRef<Path>,
        capacity: u64,
        roots: &[PayloadExtent],
    ) -> io::Result<Self> {
        let store = Self::open_inner(path.as_ref(), capacity, true, false, 1)?;
        let mut unique = std::collections::BTreeMap::new();
        for extent in roots {
            store.validate_extent(extent)?;
            if let Some(previous) = unique.insert(extent.offset, extent) {
                if previous != extent {
                    return Err(argument("conflicting payload roots at one offset"));
                }
            }
        }
        let mut end = SECTOR_BYTES as u64;
        for extent in unique.values() {
            if extent.offset < end {
                return Err(argument("overlapping payload roots"));
            }
            end = extent.offset + padded(extent.length)?;
        }
        for extent in unique.values() {
            store.read(extent)?;
        }
        if end < store.length() {
            store.file.set_len(end)?;
            store.file.sync_all()?;
            store.end.store(end, Ordering::Release);
        }
        Ok(store)
    }
    pub fn store_id(&self) -> &[u8] {
        &self.id
    }
    /// Physical end including the fixed header and padded extents.
    pub fn length(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }
    pub fn remaining_bytes(&self) -> u64 {
        self.capacity - (self.length() - SECTOR_BYTES as u64)
    }
    fn healthy(&self) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) || self.writer.is_poisoned() {
            Err(invalid(
                "payload file is fenced after failed persistence; reopen required",
            ))
        } else {
            Ok(())
        }
    }
    pub fn append(&self, payload: &[u8]) -> io::Result<PayloadExtent> {
        Ok(self.append_batch(&[payload])?.pop().unwrap())
    }
    /// Append at most 64 extents / 8 MiB of logical input, with one data sync.
    /// Validation/capacity refusals perform no writes. Persistence failures can
    /// leave an unpublished tail, return no descriptors, and fence this handle.
    pub fn append_batch(&self, payloads: &[&[u8]]) -> io::Result<Vec<PayloadExtent>> {
        self.append_with_sync(payloads, File::sync_data)
    }
    fn append_with_sync(
        &self,
        payloads: &[&[u8]],
        sync: impl FnOnce(&File) -> io::Result<()>,
    ) -> io::Result<Vec<PayloadExtent>> {
        self.healthy()?;
        if payloads.len() > MAX_BATCH_EXTENTS {
            return Err(argument("too many payload extents in batch"));
        }
        let mut logical = 0usize;
        let mut span = 0u64;
        for bytes in payloads {
            if bytes.is_empty() || bytes.len() > MAX_EXTENT_BYTES {
                return Err(argument("payload extent must contain 1..1048576 bytes"));
            }
            logical = logical
                .checked_add(bytes.len())
                .ok_or_else(|| argument("payload batch size overflow"))?;
            if logical > MAX_BATCH_BYTES {
                return Err(argument("payload batch exceeds byte bound"));
            }
            span = span
                .checked_add(padded(bytes.len() as u64)?)
                .ok_or_else(|| argument("payload file offset overflow"))?;
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let _writer = self.writer.lock().map_err(|_| {
            self.failed.store(true, Ordering::Release);
            invalid("payload writer lock is poisoned")
        })?;
        self.healthy()?;
        let begin = self.length();
        let next = begin
            .checked_add(span)
            .ok_or_else(|| argument("payload file offset overflow"))?;
        if next > SECTOR_BYTES as u64 + self.capacity {
            return Err(io::Error::other("payload file capacity exhausted"));
        }
        let mut descriptors = Vec::with_capacity(payloads.len());
        let result = (|| {
            let mut file = &self.file;
            file.seek(SeekFrom::Start(begin))?;
            let mut offset = begin;
            let zeros = [0; SECTOR_BYTES];
            for bytes in payloads {
                let mut hashes = Vec::with_capacity(bytes.len().div_ceil(SECTOR_BYTES) * 32);
                for sector in bytes.chunks(SECTOR_BYTES) {
                    if sector.len() == SECTOR_BYTES {
                        hashes.extend_from_slice(digest::digest(&digest::SHA256, sector).as_ref());
                    } else {
                        let mut padded = [0; SECTOR_BYTES];
                        padded[..sector.len()].copy_from_slice(sector);
                        hashes.extend_from_slice(digest::digest(&digest::SHA256, &padded).as_ref());
                    }
                }
                let checksum = if bytes.len() == SECTOR_BYTES {
                    hashes[..32].to_vec()
                } else {
                    digest::digest(&digest::SHA256, bytes).as_ref().to_vec()
                };
                file.write_all(bytes)?;
                let span = padded(bytes.len() as u64)?;
                file.write_all(&zeros[..span as usize - bytes.len()])?;
                descriptors.push(PayloadExtent {
                    version: 1,
                    store_id: self.id.clone(),
                    offset,
                    length: bytes.len() as u64,
                    sha256: checksum,
                    sector_sha256: hashes,
                });
                offset += span;
            }
            sync(&self.file)
        })();
        if let Err(error) = result {
            self.failed.store(true, Ordering::Release);
            return Err(error);
        }
        self.end.store(next, Ordering::Release);
        Ok(descriptors)
    }
    fn validate_extent(&self, extent: &PayloadExtent) -> io::Result<()> {
        self.healthy()?;
        if extent.version != 1
            || extent.store_id != self.id
            || extent.length == 0
            || extent.length > MAX_EXTENT_BYTES as u64
            || extent.offset < SECTOR_BYTES as u64
            || extent.offset % SECTOR_BYTES as u64 != 0
            || extent.sha256.len() != 32
            || extent.sector_sha256.len()
                != extent.length.div_ceil(SECTOR_BYTES as u64) as usize * 32
        {
            return Err(argument("invalid payload extent descriptor"));
        }
        let end = extent
            .offset
            .checked_add(padded(extent.length)?)
            .ok_or_else(|| argument("payload extent end overflow"))?;
        if end > self.length() {
            return Err(invalid("payload extent is beyond the stored file"));
        }
        Ok(())
    }
    pub fn read(&self, extent: &PayloadExtent) -> io::Result<Vec<u8>> {
        self.read_range(extent, 0, extent.length)
    }
    /// Verify touched padded sectors; a complete read also verifies the whole
    /// logical payload digest. Padding and at most two edge sectors are read.
    pub fn read_range(
        &self,
        extent: &PayloadExtent,
        offset: u64,
        length: u64,
    ) -> io::Result<Vec<u8>> {
        self.validate_extent(extent)?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| argument("payload read range overflow"))?;
        if end > extent.length {
            return Err(argument("payload read range is outside extent"));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let first = offset / SECTOR_BYTES as u64;
        let last = end.div_ceil(SECTOR_BYTES as u64);
        let mut bytes = vec![0; ((last - first) * SECTOR_BYTES as u64) as usize];
        read_at(
            &self.file,
            &mut bytes,
            extent.offset + first * SECTOR_BYTES as u64,
        )?;
        for (index, sector) in bytes.chunks_exact(SECTOR_BYTES).enumerate() {
            let checksum = (first as usize + index) * 32;
            if digest::digest(&digest::SHA256, sector).as_ref()
                != &extent.sector_sha256[checksum..checksum + 32]
            {
                return Err(invalid("payload sector checksum mismatch"));
            }
        }
        let skip = (offset % SECTOR_BYTES as u64) as usize;
        bytes.copy_within(skip..skip + length as usize, 0);
        bytes.truncate(length as usize);
        if offset == 0
            && length == extent.length
            && digest::digest(&digest::SHA256, &bytes).as_ref() != extent.sha256.as_slice()
        {
            return Err(invalid("payload checksum mismatch"));
        }
        Ok(bytes)
    }
}
fn read_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        #[cfg(unix)]
        let result = std::os::unix::fs::FileExt::read_at(file, bytes, offset);
        #[cfg(windows)]
        let result = std::os::windows::fs::FileExt::seek_read(file, bytes, offset);
        #[cfg(not(any(unix, windows)))]
        let result: io::Result<usize> = Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "positional payload reads require Unix or Windows",
        ));
        match result {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "payload file is truncated",
                ))
            }
            Ok(n) => {
                offset += n as u64;
                bytes = &mut bytes[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

impl PayloadExtentStore {
    // Published segment identities must never create or repair an absent file.
    fn open_existing_bound(
        path: &Path,
        capacity: u64,
        expected: &[u8],
        profile: u32,
        readonly: bool,
        allow_partial_tail: bool,
    ) -> io::Result<Self> {
        let lock = OpenOptions::new()
            .read(true)
            .write(!readonly)
            .open(sibling(path))?;
        if readonly {
            fs2::FileExt::try_lock_shared(&lock)?;
        } else {
            fs2::FileExt::try_lock_exclusive(&lock)?;
        }
        if lock.metadata()?.len() != MARKER_BYTES as u64 + 1 {
            return Err(invalid("published segment identity is incomplete"));
        }
        let mut bytes = [0; MARKER_BYTES + 1];
        read_at(&lock, &mut bytes, 0)?;
        if &bytes[..8] != LOCK_MAGIC
            || &bytes[8..24] != expected
            || digest::digest(&digest::SHA256, &bytes[..24]).as_ref() != &bytes[24..56]
            || bytes[MARKER_BYTES] != READY
        {
            return Err(invalid("published segment identity mismatch"));
        }
        let mut file = OpenOptions::new().read(true).write(!readonly).open(path)?;
        if readonly {
            fs2::FileExt::try_lock_shared(&file)?;
        } else {
            fs2::FileExt::try_lock_exclusive(&file)?;
        }
        verify_header(&mut file, expected, capacity, profile)?;
        let end = file.metadata()?.len();
        if end < SECTOR_BYTES as u64
            || end > SECTOR_BYTES as u64 + capacity
            || (!allow_partial_tail && end % SECTOR_BYTES as u64 != 0)
        {
            return Err(invalid("published segment length is invalid"));
        }
        Ok(Self {
            file,
            _lock: lock,
            id: expected.to_vec(),
            capacity,
            end: AtomicU64::new(end),
            writer: Mutex::new(()),
            failed: AtomicBool::new(false),
        })
    }
}

#[cfg(all(test, unix))]
#[path = "durable_payload_tests.rs"]
mod tests;

#[path = "stored_outcome_records.rs"]
pub mod outcome_log;

#[path = "stored_outcome_segments.rs"]
pub mod outcome_segments;
