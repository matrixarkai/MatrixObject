// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A log chunk: bytes appended to one chunk by one writer at a time, with no length stored
//! anywhere. Every record is self-describing (its offset, length and checksums), so the length of
//! the chunk is DERIVED when the file is replayed, and a write costs one append and one sync.
//!
//! Legacy records have a 40-byte header followed by data. Identified append records use
//! a 56-byte header: the version magic changes, bytes 36..52 hold an opaque attempt tag,
//! and bytes 52..56 checksum the preceding header bytes. Readers accept both versions;
//! production writers continue emitting legacy records during reader deployment.
//! The legacy header is:
//!
//! ```text
//! 0   magic "MOL1"
//! 4   kind      1 append, 2 seal, 3 truncate
//! 5   flags     seal only: bit 0 = the data is a 32-byte SHA-256 of the sealed bytes
//! 6   reserved  zero
//! 8   generation u32   the chunk generation; the same in every record of one file
//! 12  epoch     u64    the writer epoch; never decreases along the file
//! 20  offset    u64    append: where the data starts (= the length before it);
//!                      seal: the final length; truncate: the new length
//! 28  length    u32    bytes of data after the header
//! 32  data_crc  u32    CRC-32C of the data (append); the whole-chunk footer CRC (seal)
//! 36  header_crc u32   CRC-32C of bytes 0..36
//! ```
//!
//! Only an incomplete final header or a valid header with incomplete data may be removed.
//! A complete record with a damaged header or payload is corruption, including the final record:
//! it may already have been acknowledged. Recovery refuses it without changing the file.
//! Any write or sync failure makes this handle unavailable until it is reopened and recovered.
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Bytes read at a time while a file is replayed or a sealed chunk is checksummed: the only
/// buffer an open of a sealed chunk needs, however large the chunk is.
const REPLAY_PIECE: usize = 256 * 1024;

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}
#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        let count =
            std::os::windows::fs::FileExt::seek_read(file, &mut buf[done..], offset + done as u64)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        done += count;
    }
    Ok(())
}

pub const LOG_HEADER_LEN: usize = 40;
const MAGIC: &[u8; 4] = b"MOL1";
/// Identified append records carry sixteen CRC-protected opaque attempt bytes.
/// Ordinary writes continue emitting the legacy forty-byte record header.
pub const IDENTIFIED_LOG_HEADER_LEN: usize = 56;
const IDENTIFIED_MAGIC: &[u8; 4] = b"MOL2";
/// Reader-first header carrying the complete published prefix, protected by the header CRC.
/// Existing append APIs do not emit this format.
pub const PUBLISHED_LOG_HEADER_LEN: usize = 64;
const PUBLISHED_MAGIC: &[u8; 4] = b"MOL3";
const KIND_APPEND: u8 = 1;
const KIND_SEAL: u8 = 2;
const KIND_TRUNCATE: u8 = 3;
const FLAG_SEAL_SHA256: u8 = 1;
/// One record never carries more than this (a single write is atomic up to here).
pub const MAX_RECORD_DATA: usize = 4 * 1024 * 1024;
/// Records that may wait for an earlier one, and the bytes they may hold.
pub const DEFAULT_REORDER_RECORDS: usize = 16;
pub const DEFAULT_REORDER_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub enum LogError {
    /// The writer's epoch is older than one this chunk has seen.
    Fenced {
        current: u64,
    },
    /// The chunk is sealed: nothing more may be appended or truncated.
    Sealed,
    /// The bytes named overlap bytes already held, and differ.
    Overlap,
    /// A record, offset or length outside what one chunk or one record may hold.
    Bounds(&'static str),
    /// The reorder window (records or bytes) is full: the missing earlier record must arrive.
    Window,
    /// A seal whose length or checksum does not match what this chunk holds.
    SealMismatch,
    /// The file is damaged in a way that is not a torn tail.
    Corrupt(String),
    /// A previous write or sync failed; reopen and recover before making further changes.
    Unavailable,
    Io(io::Error),
}
impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LogError {}
impl From<io::Error> for LogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
fn corrupt(message: impl ToString) -> LogError {
    LogError::Corrupt(message.to_string())
}

const fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut n = 0;
    while n < 256 {
        let mut crc = n as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[n] = crc;
        n += 1;
    }
    table
}
static CRC32C: [u32; 256] = crc32c_table();
/// CRC-32C (Castagnoli) of `data`, continuing from `crc` (start from 0).
pub fn crc32c_update(crc: u32, data: &[u8]) -> u32 {
    let mut state = !crc;
    for byte in data {
        state = CRC32C[((state ^ u32::from(*byte)) & 0xff) as usize] ^ (state >> 8);
    }
    !state
}
// Dispatch only while validating persistent bytes; append-time checksums stay portable.
fn crc32c_update_stored(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if data.len() >= 8 && std::arch::is_x86_feature_detected!("sse4.2") {
        // SAFETY: runtime detection establishes the instruction set before entry.
        return unsafe { crc32c_update_sse42(crc, data) };
    }
    crc32c_update(crc, data)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_update_sse42(crc: u32, data: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
    let mut state = u64::from(!crc);
    let mut words = data.chunks_exact(8);
    for word in &mut words {
        state = _mm_crc32_u64(state, u64::from_le_bytes(word.try_into().unwrap()));
    }
    let mut state = state as u32;
    for byte in words.remainder() {
        state = _mm_crc32_u8(state, *byte);
    }
    !state
}

#[cfg(test)]
fn crc32c_update_portable(crc: u32, data: &[u8]) -> u32 {
    crc32c_update(crc, data)
}
fn crc32c_stored_record(data: &[u8]) -> u32 {
    crc32c_update_stored(0, data)
}

pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_update(0, data)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Header {
    kind: u8,
    flags: u8,
    generation: u32,
    epoch: u64,
    offset: u64,
    length: u32,
    data_crc: u32,
    attempt_tag: Option<[u8; 16]>,
    published_length: Option<u64>,
}
impl Header {
    fn encoded_len(&self) -> usize {
        if self.published_length.is_some() {
            PUBLISHED_LOG_HEADER_LEN
        } else if self.attempt_tag.is_some() {
            IDENTIFIED_LOG_HEADER_LEN
        } else {
            LOG_HEADER_LEN
        }
    }
    fn encode(&self) -> [u8; PUBLISHED_LOG_HEADER_LEN] {
        let mut out = [0u8; PUBLISHED_LOG_HEADER_LEN];
        out[..4].copy_from_slice(if self.published_length.is_some() {
            PUBLISHED_MAGIC
        } else if self.attempt_tag.is_some() {
            IDENTIFIED_MAGIC
        } else {
            MAGIC
        });
        out[4] = self.kind;
        out[5] = self.flags;
        out[8..12].copy_from_slice(&self.generation.to_le_bytes());
        out[12..20].copy_from_slice(&self.epoch.to_le_bytes());
        out[20..28].copy_from_slice(&self.offset.to_le_bytes());
        out[28..32].copy_from_slice(&self.length.to_le_bytes());
        out[32..36].copy_from_slice(&self.data_crc.to_le_bytes());
        let crc_at = if let Some(tag) = self.attempt_tag {
            out[36..52].copy_from_slice(&tag);
            if let Some(length) = self.published_length {
                out[52..60].copy_from_slice(&length.to_le_bytes());
                60
            } else {
                52
            }
        } else {
            36
        };
        let crc = crc32c(&out[..crc_at]);
        out[crc_at..crc_at + 4].copy_from_slice(&crc.to_le_bytes());
        out
    }
    /// `None` for an unknown version or a damaged complete header.
    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < LOG_HEADER_LEN {
            return None;
        }
        let published = &bytes[..4] == PUBLISHED_MAGIC;
        let identified = published || &bytes[..4] == IDENTIFIED_MAGIC;
        if !identified && &bytes[..4] != MAGIC {
            return None;
        }
        let crc_at = if published {
            60
        } else if identified {
            52
        } else {
            36
        };
        if bytes.len() < crc_at + 4
            || crc32c_stored_record(&bytes[..crc_at])
                != u32::from_le_bytes(bytes[crc_at..crc_at + 4].try_into().ok()?)
        {
            return None;
        }
        let header = Self {
            kind: bytes[4],
            flags: bytes[5],
            generation: u32::from_le_bytes(bytes[8..12].try_into().ok()?),
            epoch: u64::from_le_bytes(bytes[12..20].try_into().ok()?),
            offset: u64::from_le_bytes(bytes[20..28].try_into().ok()?),
            length: u32::from_le_bytes(bytes[28..32].try_into().ok()?),
            data_crc: u32::from_le_bytes(bytes[32..36].try_into().ok()?),
            attempt_tag: if identified {
                Some(bytes[36..52].try_into().ok()?)
            } else {
                None
            },
            published_length: if published {
                Some(u64::from_le_bytes(bytes[52..60].try_into().ok()?))
            } else {
                None
            },
        };
        let shaped = bytes[6..8] == [0, 0]
            && (!identified || header.kind == KIND_APPEND)
            && match header.kind {
                KIND_APPEND => header.flags == 0 && header.length as usize <= MAX_RECORD_DATA,
                KIND_SEAL => {
                    header.flags & !FLAG_SEAL_SHA256 == 0
                        && header.length
                            == if header.flags & FLAG_SEAL_SHA256 != 0 {
                                32
                            } else {
                                0
                            }
                }
                KIND_TRUNCATE => header.flags == 0 && header.length == 0 && header.data_crc == 0,
                _ => false,
            };
        shaped.then_some(header)
    }
}

/// Where the bytes `[logical, logical + len)` of the chunk sit in the file: the start of one
/// record's data. A truncate can only cut a record short, so the visible bytes are a prefix of it;
/// `record_len` and `data_crc` still describe the whole record, which is what a read checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Extent {
    logical: u64,
    file_pos: u64,
    len: u32,
    record_len: u32,
    data_crc: u32,
    header_len: u8,
}

/// The records of sealed chunks that were read lately, shared by every chunk of one node and
/// bounded in bytes. A sealed file never changes, so an entry never goes stale; the only cost of
/// dropping one is reading and checking the record again.
pub struct RecordCache {
    cap_bytes: usize,
    bytes: usize,
    tick: u64,
    entries: HashMap<(Arc<Path>, u64), (Arc<Vec<u8>>, u64)>,
    order: BTreeMap<u64, (Arc<Path>, u64)>,
}
impl RecordCache {
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            cap_bytes,
            bytes: 0,
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }
    pub fn cap_bytes(&self) -> usize {
        self.cap_bytes
    }
    /// Bytes held now. Never above `cap_bytes`.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn get(&mut self, path: &Arc<Path>, file_pos: u64) -> Option<Arc<Vec<u8>>> {
        self.tick += 1;
        let tick = self.tick;
        let key = (Arc::clone(path), file_pos);
        let (data, used) = self.entries.get_mut(&key)?;
        self.order.remove(used);
        *used = tick;
        self.order.insert(tick, key);
        Some(Arc::clone(data))
    }
    fn insert(&mut self, path: &Arc<Path>, file_pos: u64, data: Arc<Vec<u8>>) {
        if data.len() > self.cap_bytes {
            return;
        }
        while self.bytes + data.len() > self.cap_bytes {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((evicted, _)) = self.entries.remove(&oldest) {
                self.bytes -= evicted.len();
            }
        }
        self.tick += 1;
        let key = (Arc::clone(path), file_pos);
        self.bytes += data.len();
        if let Some((old, used)) = self.entries.insert(key.clone(), (data, self.tick)) {
            self.bytes -= old.len();
            self.order.remove(&used);
        }
        self.order.insert(self.tick, key);
    }
}

/// What a seal recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sealed {
    pub length: u64,
    pub footer_crc: u32,
    pub sha256: Option<[u8; 32]>,
}

/// Identity metadata of one complete durable append record, without its payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendRecordIdentity {
    pub offset: u64,
    pub length: u32,
    /// Opaque context-bound tag; absent on legacy records.
    pub attempt_tag: Option<[u8; 16]>,
}

/// Complete record identity and its optional local publication boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendRecordObservation {
    pub identity: AppendRecordIdentity,
    /// Absent for legacy records; present even when a new record publishes zero bytes.
    pub published_length: Option<u64>,
}

struct PendingRecord {
    data: Vec<u8>,
    attempt_tag: Option<[u8; 16]>,
    epoch: u64,
}

pub struct LogChunk {
    file: File,
    failed: bool,
    path: PathBuf,
    shared_path: Arc<Path>,
    generation: u32,
    max_length: u64,
    /// The bytes of the chunk, held only while the chunk can still change: an open tail. A sealed
    /// chunk holds none and is read from its file.
    data: Vec<u8>,
    /// Every byte `[0, length)` is described by the extents, in order, with nothing missing between them.
    extents: Vec<Extent>,
    length: u64,
    published_length: u64,
    /// Sticky across truncation: old append APIs cannot downgrade a publication-aware file.
    publication_mode: bool,
    /// CRC-32C of the whole chunk, kept running.
    crc: u32,
    epoch: u64,
    sealed: Option<Sealed>,
    /// Bytes of the file that hold whole, valid records.
    file_len: u64,
    /// The length that has been synced.
    durable_length: u64,
    pending: BTreeMap<u64, PendingRecord>,
    pending_bytes: usize,
    max_pending_records: usize,
    max_pending_bytes: usize,
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(ring::digest::digest(&ring::digest::SHA256, data).as_ref());
    out
}

impl LogChunk {
    /// Open or create the chunk file and replay it. The length, the epoch and whether the chunk is
    /// sealed come from the records alone. A torn final record is cut off (and the cut is made
    /// durable) before this returns.
    pub fn open(path: &Path, generation: u32, max_length: u64) -> Result<Self, LogError> {
        if max_length == 0 || max_length > 512 * 1024 * 1024 {
            return Err(LogError::Bounds("chunk limit"));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let file_len = file.metadata()?.len();
        let mut chunk = Self {
            file: file.try_clone()?,
            failed: false,
            path: path.to_path_buf(),
            shared_path: Arc::from(path),
            generation,
            max_length,
            data: Vec::new(),
            extents: Vec::new(),
            length: 0,
            published_length: 0,
            publication_mode: false,
            crc: 0,
            epoch: 0,
            sealed: None,
            file_len: 0,
            durable_length: 0,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            max_pending_records: DEFAULT_REORDER_RECORDS,
            max_pending_bytes: DEFAULT_REORDER_BYTES,
        };
        let mut offset = 0u64;
        let mut piece = vec![0u8; REPLAY_PIECE];
        while offset < file_len {
            let remaining = file_len - offset;
            let mut header_bytes = [0u8; PUBLISHED_LOG_HEADER_LEN];
            if remaining < LOG_HEADER_LEN as u64 {
                // A header cut short: only the beginning of one may be left.
                let mut tail = vec![0u8; remaining as usize];
                read_exact_at(&file, &mut tail, offset)?;
                let prefix = tail.len().min(MAGIC.len());
                if tail[..prefix] != MAGIC[..prefix]
                    && tail[..prefix] != IDENTIFIED_MAGIC[..prefix]
                    && tail[..prefix] != PUBLISHED_MAGIC[..prefix]
                    && tail.iter().any(|byte| *byte != 0)
                {
                    return Err(corrupt("a log chunk ends in bytes that are not a record"));
                }
                break;
            }
            read_exact_at(&file, &mut header_bytes[..LOG_HEADER_LEN], offset)?;
            let header_len = if &header_bytes[..4] == PUBLISHED_MAGIC {
                PUBLISHED_LOG_HEADER_LEN
            } else if &header_bytes[..4] == IDENTIFIED_MAGIC {
                IDENTIFIED_LOG_HEADER_LEN
            } else {
                LOG_HEADER_LEN
            };
            // Read the available extended prefix before deciding whether a short header is torn.
            let available = remaining.min(header_len as u64) as usize;
            if available > LOG_HEADER_LEN {
                read_exact_at(
                    &file,
                    &mut header_bytes[LOG_HEADER_LEN..available],
                    offset + LOG_HEADER_LEN as u64,
                )?;
            }
            if remaining < header_len as u64 {
                // A changed version marker must not turn a complete older header into a torn tail.
                for (magic, length) in [
                    (MAGIC, LOG_HEADER_LEN),
                    (IDENTIFIED_MAGIC, IDENTIFIED_LOG_HEADER_LEN),
                ] {
                    if available >= length {
                        let mut older = header_bytes;
                        older[..4].copy_from_slice(magic);
                        if Header::decode(&older[..length]).is_some() {
                            return Err(corrupt(
                                "short extended header retains an older record checksum",
                            ));
                        }
                    }
                }
                break;
            }
            let Some(header) = Header::decode(&header_bytes[..header_len]) else {
                return Err(corrupt(format!(
                    "log chunk record at {offset} has a damaged header"
                )));
            };
            let end = offset + header_len as u64 + u64::from(header.length);
            if header.generation != generation {
                return Err(corrupt("log chunk record names another generation"));
            }
            if header.epoch < chunk.epoch || chunk.sealed.is_some() {
                return Err(corrupt("log chunk tail follows a seal or a newer epoch"));
            }
            if end > file_len {
                break; // the data was not fully written
            }
            let data_pos = offset + header_len as u64;
            match header.kind {
                KIND_APPEND => {
                    // checked a piece at a time: a record is never held whole
                    let mut record_crc = 0u32;
                    let mut total_crc = chunk.crc;
                    let mut done = 0usize;
                    while done < header.length as usize {
                        let count = piece.len().min(header.length as usize - done);
                        read_exact_at(&file, &mut piece[..count], data_pos + done as u64)?;
                        record_crc = crc32c_update_stored(record_crc, &piece[..count]);
                        total_crc = crc32c_update_stored(total_crc, &piece[..count]);
                        done += count;
                    }
                    if record_crc != header.data_crc {
                        return Err(corrupt(format!(
                            "log chunk record at {offset} has damaged data"
                        )));
                    }
                    chunk.replay_append(&header, data_pos, total_crc)?;
                }
                _ => {
                    let mut seal_data = vec![0u8; header.length as usize];
                    read_exact_at(&file, &mut seal_data, data_pos)?;
                    chunk.replay_record(&header, &seal_data)?;
                }
            }
            offset = end;
        }
        if offset != file_len {
            file.set_len(offset)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::Start(offset))?;
        chunk.file = file;
        chunk.file_len = offset;
        chunk.durable_length = chunk.length;
        if chunk.sealed.is_none() {
            // an open tail keeps its bytes: appends are checked against them
            let mut data = Vec::with_capacity(chunk.length as usize);
            chunk.stream_logical(0, chunk.length, |bytes| data.extend_from_slice(bytes))?;
            chunk.data = data;
        }
        Ok(chunk)
    }

    /// Feed bytes `[start, end)` of the chunk to `each`, read from the file in bounded pieces.
    fn stream_logical(
        &self,
        start: u64,
        end: u64,
        mut each: impl FnMut(&[u8]),
    ) -> Result<(), LogError> {
        let mut piece = vec![0u8; REPLAY_PIECE.min((end - start) as usize).max(1)];
        let mut at = start;
        let first = self
            .extents
            .partition_point(|extent| extent.logical + u64::from(extent.len) <= start);
        for extent in &self.extents[first..] {
            if at >= end {
                break;
            }
            let from = at.max(extent.logical);
            let upto = end.min(extent.logical + u64::from(extent.len));
            let mut pos = from;
            while pos < upto {
                let count = piece.len().min((upto - pos) as usize);
                read_exact_at(
                    &self.file,
                    &mut piece[..count],
                    extent.file_pos + (pos - extent.logical),
                )?;
                each(&piece[..count]);
                pos += count as u64;
            }
            at = upto;
        }
        Ok(())
    }

    /// Drop every byte at and above `length` from the extents (a truncate).
    fn cut_extents(&mut self, length: u64) {
        while let Some(last) = self.extents.last_mut() {
            if last.logical >= length {
                self.extents.pop();
            } else {
                if last.logical + u64::from(last.len) > length {
                    last.len = (length - last.logical) as u32;
                }
                break;
            }
        }
    }

    /// One append record whose data has been checked as it was read; `total_crc` is the running
    /// checksum of the whole chunk with that data added.
    fn replay_append(
        &mut self,
        header: &Header,
        data_pos: u64,
        total_crc: u32,
    ) -> Result<(), LogError> {
        if header.epoch < self.epoch {
            return Err(corrupt("log chunk epoch goes backwards"));
        }
        if self.sealed.is_some() {
            return Err(corrupt("log chunk has a record after its seal"));
        }
        let length = self.length;
        if header.offset != length
            || length + u64::from(header.length) > self.max_length
            || header.length == 0
        {
            return Err(corrupt("log chunk append does not continue the chunk"));
        }
        let end = length + u64::from(header.length);
        let published = match header.published_length {
            Some(value) => {
                if (value != self.published_length && value != end)
                    || (length > self.published_length && header.epoch != self.epoch)
                {
                    return Err(corrupt(
                        "log chunk publication boundary or epoch is invalid",
                    ));
                }
                self.publication_mode = true;
                value
            }
            None if self.publication_mode => {
                return Err(corrupt("legacy append follows publication-aware records"))
            }
            None => end,
        };
        self.extents.push(Extent {
            logical: length,
            file_pos: data_pos,
            len: header.length,
            record_len: header.length,
            data_crc: header.data_crc,
            header_len: header.encoded_len() as u8,
        });
        self.length = end;
        self.published_length = published;
        self.crc = total_crc;
        self.epoch = header.epoch;
        Ok(())
    }

    fn replay_record(&mut self, header: &Header, data: &[u8]) -> Result<(), LogError> {
        if header.epoch < self.epoch {
            return Err(corrupt("log chunk epoch goes backwards"));
        }
        if self.sealed.is_some() {
            return Err(corrupt("log chunk has a record after its seal"));
        }
        let length = self.length;
        match header.kind {
            KIND_TRUNCATE => {
                if header.offset > length {
                    return Err(corrupt("log chunk truncates past its length"));
                }
                if self.publication_mode && !self.is_publication_boundary(header.offset)? {
                    return Err(corrupt("log chunk truncation cuts an object publication"));
                }
                self.cut_extents(header.offset);
                self.length = header.offset;
                self.published_length = header.offset;
                let mut crc = 0u32;
                self.stream_logical(0, header.offset, |bytes| crc = crc32c_update(crc, bytes))?;
                self.crc = crc;
            }
            _ => {
                if header.offset != length
                    || header.data_crc != self.crc
                    || self.published_length != length
                {
                    return Err(corrupt("log chunk seal does not match its bytes"));
                }
                let sha256 = if header.flags & FLAG_SEAL_SHA256 != 0 {
                    let recorded: [u8; 32] =
                        data.try_into().map_err(|_| corrupt("seal digest size"))?;
                    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
                    self.stream_logical(0, length, |bytes| context.update(bytes))?;
                    let mut actual = [0u8; 32];
                    actual.copy_from_slice(context.finish().as_ref());
                    if recorded != actual {
                        return Err(corrupt("log chunk seal digest does not match its bytes"));
                    }
                    Some(recorded)
                } else {
                    None
                };
                self.sealed = Some(Sealed {
                    length,
                    footer_crc: self.crc,
                    sha256,
                });
            }
        }
        self.epoch = header.epoch;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn generation(&self) -> u32 {
        self.generation
    }

    /// The bytes applied so far (durable only after `commit`).
    pub fn length(&self) -> u64 {
        self.length
    }
    /// Whether this file has used publication-aware records, including before truncation.
    pub fn has_publication_records(&self) -> bool {
        self.publication_mode
    }
    /// End of complete object requests represented by publication-aware records.
    /// This is an applied boundary; use `durable_published_length` for durable visibility.
    pub fn published_length(&self) -> u64 {
        self.published_length
    }
    /// Published prefix covered by the captured durable physical length. The legacy-only
    /// path remains a scalar operation; partial syncs validate indexed publication boundaries.
    pub fn durable_published_length(&self) -> Result<u64, LogError> {
        self.check_available()?;
        if !self.publication_mode || self.durable_length >= self.length {
            return Ok(self.published_length.min(self.durable_length));
        }
        let index = self
            .extents
            .partition_point(|e| e.logical + u64::from(e.len) <= self.durable_length);
        if index == 0 {
            return Ok(0);
        }
        let extent = &self.extents[index - 1];
        let header = self.indexed_header(extent)?;
        let published = header
            .published_length
            .unwrap_or(extent.logical + u64::from(extent.len));
        if published > self.durable_length || !self.is_publication_boundary(published)? {
            return Err(corrupt(
                "durable publication does not name a covered complete boundary",
            ));
        }
        Ok(published)
    }
    fn is_publication_boundary(&self, offset: u64) -> Result<bool, LogError> {
        if offset == 0 || offset == self.published_length {
            return Ok(true);
        }
        if offset > self.published_length {
            return Ok(false);
        }
        let index = self.extents.partition_point(|e| e.logical < offset);
        let Some(extent) = index.checked_sub(1).and_then(|i| self.extents.get(i)) else {
            return Ok(false);
        };
        let header = self.indexed_header(extent)?;
        Ok(match header.published_length {
            Some(published) => {
                published == offset && extent.logical + u64::from(extent.len) == offset
            }
            None => offset <= extent.logical + u64::from(extent.len),
        })
    }
    /// Whether the chunk's bytes are held in memory (an open tail) rather than read from its file.
    pub fn is_resident(&self) -> bool {
        self.sealed.is_none()
    }
    /// Bytes of heap this chunk holds for its payload.
    pub fn resident_bytes(&self) -> usize {
        self.data.capacity()
    }
    /// Records the chunk's bytes are spread over.
    pub fn extent_count(&self) -> usize {
        self.extents.len()
    }
    /// The length that has been made durable.
    pub fn durable_length(&self) -> u64 {
        self.durable_length
    }
    /// A second handle on the file and the length it covers right now: syncing the handle makes at
    /// least that many bytes durable, and needs neither the chunk nor any lock that guards it.
    pub fn sync_handle(&self) -> io::Result<(File, u64)> {
        Ok((self.file.try_clone()?, self.length()))
    }
    /// Record that a sync of a handle taken when the chunk was `length` long has finished. A
    /// truncate in between can only have lowered the length, so the value is clamped to it.
    pub fn note_durable(&mut self, length: u64) {
        self.durable_length = self.durable_length.max(length.min(self.length()));
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn sealed(&self) -> Option<&Sealed> {
        self.sealed.as_ref()
    }
    /// Append bytes `[offset, offset+length)` of the chunk to `out`, or fewer if the chunk ends
    /// first. An open tail answers from memory. A sealed chunk is read from its file one whole
    /// record at a time: each record is checked against the checksum written with it, and kept in
    /// `cache` (bounded) for the next read.
    pub fn read_into(
        &self,
        offset: u64,
        length: usize,
        cache: &mut RecordCache,
        out: &mut Vec<u8>,
    ) -> Result<(), LogError> {
        let start = offset.min(self.length);
        let end = start.saturating_add(length as u64).min(self.length);
        if start >= end {
            return Ok(());
        }
        out.reserve((end - start) as usize);
        if self.sealed.is_none() {
            out.extend_from_slice(&self.data[start as usize..end as usize]);
            return Ok(());
        }
        let first = self
            .extents
            .partition_point(|extent| extent.logical + u64::from(extent.len) <= start);
        for extent in &self.extents[first..] {
            if extent.logical >= end {
                break;
            }
            let record = match cache.get(&self.shared_path, extent.file_pos) {
                Some(record) => record,
                None => {
                    let mut bytes = vec![0u8; extent.record_len as usize];
                    read_exact_at(&self.file, &mut bytes, extent.file_pos)?;
                    if crc32c_stored_record(&bytes) != extent.data_crc {
                        return Err(corrupt(format!(
                            "log chunk record at {} has damaged data",
                            extent.file_pos - u64::from(extent.header_len)
                        )));
                    }
                    let record: Arc<Vec<u8>> = Arc::new(bytes);
                    cache.insert(&self.shared_path, extent.file_pos, Arc::clone(&record));
                    record
                }
            };
            let from = start.max(extent.logical) - extent.logical;
            let upto = end.min(extent.logical + u64::from(extent.len)) - extent.logical;
            out.extend_from_slice(&record[from as usize..upto as usize]);
        }
        Ok(())
    }
    /// `read_into` with no cache: the bytes `[offset, offset+length)` as a new vector.
    pub fn read_vec(&self, offset: u64, length: usize) -> Result<Vec<u8>, LogError> {
        let mut out = Vec::new();
        self.read_into(offset, length, &mut RecordCache::new(0), &mut out)?;
        Ok(out)
    }
    /// Running CRC-32C over the whole chunk.
    pub fn footer_crc(&self) -> u32 {
        self.crc
    }
    pub fn set_reorder_window(&mut self, records: usize, bytes: usize) {
        self.max_pending_records = records;
        self.max_pending_bytes = bytes;
    }
    pub fn pending_records(&self) -> usize {
        self.pending.len()
    }

    fn check_available(&self) -> Result<(), LogError> {
        if self.failed {
            Err(LogError::Unavailable)
        } else {
            Ok(())
        }
    }

    fn sync(&mut self) -> Result<(), LogError> {
        self.check_available()?;
        if let Err(error) = self.file.sync_data() {
            self.failed = true;
            return Err(error.into());
        }
        Ok(())
    }

    fn check_epoch(&self, epoch: u64) -> Result<(), LogError> {
        self.check_available()?;
        if epoch < self.epoch {
            return Err(LogError::Fenced {
                current: self.epoch,
            });
        }
        Ok(())
    }
    fn write_record(&mut self, header: Header, data: &[u8]) -> Result<(), LogError> {
        let header_len = header.encoded_len();
        let mut frame = Vec::with_capacity(header_len + data.len());
        frame.extend_from_slice(&header.encode()[..header_len]);
        frame.extend_from_slice(data);
        // one write call per record: a crash leaves a prefix of it, never a hole inside it
        self.check_available()?;
        if let Err(error) = self
            .file
            .seek(SeekFrom::Start(self.file_len))
            .and_then(|_| self.file.write_all(&frame))
        {
            self.failed = true;
            return Err(error.into());
        }
        self.file_len += frame.len() as u64;
        Ok(())
    }

    /// Inspect a complete durable record starting exactly at `offset`.
    /// A truncated record or a position inside a record has no complete identity.
    /// This observation does not fence pending writes or establish replica quorum.
    pub fn append_record_identity(
        &self,
        offset: u64,
    ) -> Result<Option<AppendRecordIdentity>, LogError> {
        self.record_identity(offset, true)
    }

    fn record_identity(
        &self,
        offset: u64,
        require_durable: bool,
    ) -> Result<Option<AppendRecordIdentity>, LogError> {
        Ok(self
            .record_observation(offset, require_durable)?
            .map(|record| record.identity))
    }
    /// Inspect an exact, complete durable record without reading its payload.
    pub fn durable_record_observation(
        &self,
        offset: u64,
    ) -> Result<Option<AppendRecordObservation>, LogError> {
        self.record_observation(offset, true)
    }
    /// The complete record ending at the durable published prefix. A legacy truncate can
    /// leave a partial old record; such a prefix has no complete record observation.
    pub fn durable_publication_record(&self) -> Result<Option<AppendRecordObservation>, LogError> {
        let published = self.durable_published_length()?;
        if published == 0 {
            return Ok(None);
        }
        let next = self.extents.partition_point(|e| e.logical < published);
        let Some(extent) = next.checked_sub(1).and_then(|i| self.extents.get(i)) else {
            return Ok(None);
        };
        if extent.logical + u64::from(extent.len) != published {
            return Ok(None);
        }
        self.record_observation(extent.logical, true)
    }
    fn record_observation(
        &self,
        offset: u64,
        require_durable: bool,
    ) -> Result<Option<AppendRecordObservation>, LogError> {
        self.check_available()?;
        let index = self
            .extents
            .partition_point(|extent| extent.logical < offset);
        let Some(extent) = self
            .extents
            .get(index)
            .filter(|extent| extent.logical == offset)
        else {
            return Ok(None);
        };
        if extent.len != extent.record_len
            || (require_durable && extent.logical + u64::from(extent.len) > self.durable_length)
        {
            return Ok(None);
        }
        let header = self.indexed_header(extent)?;
        Ok(Some(AppendRecordObservation {
            identity: AppendRecordIdentity {
                offset,
                length: header.length,
                attempt_tag: header.attempt_tag,
            },
            published_length: header.published_length,
        }))
    }

    fn indexed_header(&self, extent: &Extent) -> Result<Header, LogError> {
        let length = usize::from(extent.header_len);
        if !matches!(
            length,
            LOG_HEADER_LEN | IDENTIFIED_LOG_HEADER_LEN | PUBLISHED_LOG_HEADER_LEN
        ) {
            return Err(corrupt("invalid indexed record header length"));
        }
        let mut bytes = [0u8; PUBLISHED_LOG_HEADER_LEN];
        read_exact_at(
            &self.file,
            &mut bytes[..length],
            extent.file_pos - length as u64,
        )?;
        let header = Header::decode(&bytes[..length])
            .ok_or_else(|| corrupt("append identity header is damaged"))?;
        if header.kind != KIND_APPEND
            || header.generation != self.generation
            || header.offset != extent.logical
            || header.length != extent.record_len
            || header.data_crc != extent.data_crc
            || header.encoded_len() != length
        {
            return Err(corrupt(
                "append identity header does not match the indexed record",
            ));
        }
        Ok(header)
    }

    /// Accept `data` for `[offset, offset+data.len())` at `epoch`. Records that arrive ahead of
    /// the end wait (bounded) until the bytes before them are applied; bytes already held are
    /// acknowledged again if identical and refused if not. Returns the applied length, which is
    /// the acknowledgement: everything below it is held, and it is durable after `commit`.
    pub fn append(&mut self, epoch: u64, offset: u64, data: &[u8]) -> Result<u64, LogError> {
        self.append_record(epoch, offset, data, None)
    }

    /// Append one context-bound attempt. Retries must have the same complete range, tag and bytes.
    /// Out-of-order records retain their tags within the existing bounded reorder window.
    /// A new writer must truncate and commit its chosen tail before using a different epoch
    /// while identified records are pending. Callers still enforce the writer lease and quorum.
    /// As with `append`, success is not durable until a successful commit.
    pub fn append_identified(
        &mut self,
        epoch: u64,
        offset: u64,
        data: &[u8],
        attempt_tag: [u8; 16],
    ) -> Result<u64, LogError> {
        self.append_record(epoch, offset, data, Some(attempt_tag))
    }

    fn append_record(
        &mut self,
        epoch: u64,
        offset: u64,
        data: &[u8],
        attempt_tag: Option<[u8; 16]>,
    ) -> Result<u64, LogError> {
        self.check_epoch(epoch)?;
        if self.publication_mode {
            return Err(LogError::Bounds(
                "publication-aware mutation protocol required",
            ));
        }
        if self.sealed.is_some() {
            return Err(LogError::Sealed);
        }
        if data.is_empty() || data.len() > MAX_RECORD_DATA {
            return Err(LogError::Bounds("record size"));
        }
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or(LogError::Bounds("offset"))?;
        if end > self.max_length {
            return Err(LogError::Bounds("chunk full"));
        }
        for (&start, pending) in &self.pending {
            let identified = attempt_tag.is_some() || pending.attempt_tag.is_some();
            if identified && pending.epoch != epoch {
                return Err(LogError::Overlap);
            }
            if identified
                && start < end
                && start + pending.data.len() as u64 > offset
                && (start != offset
                    || pending.data.len() != data.len()
                    || pending.attempt_tag != attempt_tag
                    || pending.data != data)
            {
                return Err(LogError::Overlap);
            }
        }
        let length = self.length();
        if attempt_tag.is_none() && offset < length {
            let first = self
                .extents
                .partition_point(|extent| extent.logical + u64::from(extent.len) <= offset);
            if self.extents[first..]
                .iter()
                .take_while(|extent| extent.logical < end)
                .any(|extent| usize::from(extent.header_len) == IDENTIFIED_LOG_HEADER_LEN)
            {
                return Err(LogError::Overlap);
            }
        }
        if end <= length {
            if let Some(tag) = attempt_tag {
                let expected = AppendRecordIdentity {
                    offset,
                    length: data.len() as u32,
                    attempt_tag: Some(tag),
                };
                if self.record_identity(offset, false)? != Some(expected) {
                    return Err(LogError::Overlap);
                }
            }
            let held = &self.data[offset as usize..end as usize];
            return if held == data {
                Ok(length)
            } else {
                Err(LogError::Overlap)
            };
        }
        if offset > length {
            return self.hold(epoch, offset, data, attempt_tag);
        }
        // offset <= length < end: the part below `length` must match, the rest is new
        let overlap = (length - offset) as usize;
        if attempt_tag.is_some() && overlap != 0 {
            return Err(LogError::Overlap);
        }
        if self.data[offset as usize..].get(..overlap) != Some(&data[..overlap]) {
            return Err(LogError::Overlap);
        }
        self.apply_new(epoch, &data[overlap..], attempt_tag)?;
        self.drain_pending(epoch)?;
        Ok(self.length())
    }

    fn hold(
        &mut self,
        epoch: u64,
        offset: u64,
        data: &[u8],
        attempt_tag: Option<[u8; 16]>,
    ) -> Result<u64, LogError> {
        let end = offset + data.len() as u64;
        // overlap with a waiting record of another shape is refused; the same record again is not
        if let Some(existing) = self.pending.get(&offset) {
            return if existing.data == data && existing.attempt_tag == attempt_tag {
                Ok(self.length())
            } else {
                Err(LogError::Overlap)
            };
        }
        if let Some((before, held)) = self.pending.range(..offset).next_back() {
            if before + held.data.len() as u64 > offset {
                return Err(LogError::Overlap);
            }
        }
        if let Some((after, _)) = self.pending.range(offset..).next() {
            if *after < end {
                return Err(LogError::Overlap);
            }
        }
        if self.pending.len() >= self.max_pending_records
            || self.pending_bytes + data.len() > self.max_pending_bytes
        {
            return Err(LogError::Window);
        }
        self.pending_bytes += data.len();
        self.pending.insert(
            offset,
            PendingRecord {
                data: data.to_vec(),
                attempt_tag,
                epoch,
            },
        );
        if attempt_tag.is_some() {
            self.epoch = epoch;
        }
        Ok(self.length())
    }

    fn drain_pending(&mut self, epoch: u64) -> Result<(), LogError> {
        loop {
            let length = self.length();
            let Some((&next, _)) = self.pending.iter().next() else {
                return Ok(());
            };
            if next > length {
                return Ok(());
            }
            let pending = self.pending.remove(&next).unwrap();
            let data = pending.data;
            self.pending_bytes -= data.len();
            let overlap = (length - next) as usize;
            if overlap > data.len()
                || self.data[next as usize..].get(..overlap) != Some(&data[..overlap])
            {
                return Err(LogError::Overlap);
            }
            if overlap < data.len() {
                self.apply_new(epoch, &data[overlap..], pending.attempt_tag)?;
            }
        }
    }

    fn apply_new(
        &mut self,
        epoch: u64,
        data: &[u8],
        attempt_tag: Option<[u8; 16]>,
    ) -> Result<(), LogError> {
        let offset = self.length();
        let header = Header {
            kind: KIND_APPEND,
            flags: 0,
            generation: self.generation,
            epoch,
            offset,
            length: data.len() as u32,
            data_crc: crc32c(data),
            attempt_tag,
            published_length: None,
        };
        let data_pos = self.file_len + header.encoded_len() as u64;
        self.write_record(header, data)?;
        self.extents.push(Extent {
            logical: offset,
            file_pos: data_pos,
            len: data.len() as u32,
            record_len: data.len() as u32,
            data_crc: header.data_crc,
            header_len: header.encoded_len() as u8,
        });
        self.data.extend_from_slice(data);
        self.length += data.len() as u64;
        self.published_length = self.length;
        self.crc = crc32c_update(self.crc, data);
        self.epoch = epoch;
        Ok(())
    }

    /// Make everything applied so far durable (one sync, however many records) and return the
    /// durable length: the length a writer may treat as acknowledged.
    pub fn commit(&mut self) -> Result<u64, LogError> {
        self.sync()?;
        self.durable_length = self.length();
        Ok(self.durable_length)
    }

    /// Cut the chunk back to `length` (a new writer drops bytes only some replicas hold), at an
    /// epoch at least the current one. Waiting records are dropped. Durable after `commit`.
    pub fn truncate(&mut self, epoch: u64, length: u64) -> Result<(), LogError> {
        self.check_epoch(epoch)?;
        if self.publication_mode {
            return Err(LogError::Bounds(
                "publication-aware mutation protocol required",
            ));
        }
        if self.sealed.is_some() {
            return Err(LogError::Sealed);
        }
        if length > self.length() {
            return Err(LogError::Bounds("truncate past the end"));
        }
        let header = Header {
            kind: KIND_TRUNCATE,
            flags: 0,
            generation: self.generation,
            epoch,
            offset: length,
            length: 0,
            data_crc: 0,
            attempt_tag: None,
            published_length: None,
        };
        self.write_record(header, &[])?;
        self.data.truncate(length as usize);
        self.cut_extents(length);
        self.length = length;
        self.published_length = length;
        self.crc = crc32c(&self.data);
        self.epoch = epoch;
        self.pending.clear();
        self.pending_bytes = 0;
        self.durable_length = self.durable_length.min(length);
        Ok(())
    }

    /// Close the chunk at its present length. The footer CRC is the running checksum; the SHA-256
    /// is computed here, once, when asked for. Waiting records must have been applied first.
    pub fn seal(&mut self, epoch: u64, with_sha256: bool) -> Result<Sealed, LogError> {
        self.check_epoch(epoch)?;
        if self.publication_mode {
            return Err(LogError::Bounds(
                "publication-aware mutation protocol required",
            ));
        }
        if self.sealed.is_some() {
            return Err(LogError::Sealed);
        }
        if !self.pending.is_empty() {
            return Err(LogError::SealMismatch);
        }
        let digest = with_sha256.then(|| sha256(&self.data));
        let header = Header {
            kind: KIND_SEAL,
            flags: if with_sha256 { FLAG_SEAL_SHA256 } else { 0 },
            generation: self.generation,
            epoch,
            offset: self.length(),
            length: if with_sha256 { 32 } else { 0 },
            data_crc: self.crc,
            attempt_tag: None,
            published_length: None,
        };
        self.write_record(header, digest.as_ref().map(|d| d.as_slice()).unwrap_or(&[]))?;
        self.sync()?;
        self.durable_length = self.length();
        self.epoch = epoch;
        let sealed = Sealed {
            length: self.length(),
            footer_crc: self.crc,
            sha256: digest,
        };
        self.sealed = Some(sealed.clone());
        // nothing more can change: the bytes now live only in the file
        self.data = Vec::new();
        Ok(sealed)
    }
}

#[cfg(test)]
#[path = "log_chunk_crc32c_tests.rs"]
mod crc32c_tests;

#[cfg(test)]
#[path = "log_chunk_identity_tests.rs"]
mod identity_tests;

#[cfg(test)]
#[path = "log_chunk_identified_write_tests.rs"]
mod identified_write_tests;

#[cfg(test)]
#[path = "log_chunk_publication_tests.rs"]
mod publication_tests;
