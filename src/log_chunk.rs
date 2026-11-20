// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A log chunk: bytes appended to one chunk by one writer at a time, with no length stored
//! anywhere. Every record is self-describing (its offset, length and checksums), so the length of
//! the chunk is DERIVED when the file is replayed, and a write costs one append and one sync.
//!
//! The file is a sequence of records, each a 40-byte header followed by its data:
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
}
impl Header {
    fn encode(&self) -> [u8; LOG_HEADER_LEN] {
        let mut out = [0u8; LOG_HEADER_LEN];
        out[..4].copy_from_slice(MAGIC);
        out[4] = self.kind;
        out[5] = self.flags;
        out[8..12].copy_from_slice(&self.generation.to_le_bytes());
        out[12..20].copy_from_slice(&self.epoch.to_le_bytes());
        out[20..28].copy_from_slice(&self.offset.to_le_bytes());
        out[28..32].copy_from_slice(&self.length.to_le_bytes());
        out[32..36].copy_from_slice(&self.data_crc.to_le_bytes());
        let crc = crc32c(&out[..36]);
        out[36..40].copy_from_slice(&crc.to_le_bytes());
        out
    }
    /// `None` when the bytes are not a well-formed header (bad magic, checksum or shape).
    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < LOG_HEADER_LEN || &bytes[..4] != MAGIC {
            return None;
        }
        if crc32c_stored_record(&bytes[..36]) != u32::from_le_bytes(bytes[36..40].try_into().ok()?)
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
        };
        let shaped = bytes[6..8] == [0, 0]
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
    /// CRC-32C of the whole chunk, kept running.
    crc: u32,
    epoch: u64,
    sealed: Option<Sealed>,
    /// Bytes of the file that hold whole, valid records.
    file_len: u64,
    /// The length that has been synced.
    durable_length: u64,
    pending: BTreeMap<u64, Vec<u8>>,
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
            let mut header_bytes = [0u8; LOG_HEADER_LEN];
            if remaining < LOG_HEADER_LEN as u64 {
                // A header cut short: only the beginning of one may be left.
                let mut tail = vec![0u8; remaining as usize];
                read_exact_at(&file, &mut tail, offset)?;
                let prefix = tail.len().min(MAGIC.len());
                if tail[..prefix] != MAGIC[..prefix] && tail.iter().any(|byte| *byte != 0) {
                    return Err(corrupt("a log chunk ends in bytes that are not a record"));
                }
                break;
            }
            read_exact_at(&file, &mut header_bytes, offset)?;
            let Some(header) = Header::decode(&header_bytes) else {
                return Err(corrupt(format!(
                    "log chunk record at {offset} has a damaged header"
                )));
            };
            let end = offset + LOG_HEADER_LEN as u64 + u64::from(header.length);
            if header.generation != generation {
                return Err(corrupt("log chunk record names another generation"));
            }
            if header.epoch < chunk.epoch || chunk.sealed.is_some() {
                return Err(corrupt("log chunk tail follows a seal or a newer epoch"));
            }
            if end > file_len {
                break; // the data was not fully written
            }
            let data_pos = offset + LOG_HEADER_LEN as u64;
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
        self.extents.push(Extent {
            logical: length,
            file_pos: data_pos,
            len: header.length,
            record_len: header.length,
            data_crc: header.data_crc,
        });
        self.length += u64::from(header.length);
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
                self.cut_extents(header.offset);
                self.length = header.offset;
                let mut crc = 0u32;
                self.stream_logical(0, header.offset, |bytes| crc = crc32c_update(crc, bytes))?;
                self.crc = crc;
            }
            _ => {
                if header.offset != length || header.data_crc != self.crc {
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
                            extent.file_pos - LOG_HEADER_LEN as u64
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
        let mut frame = Vec::with_capacity(LOG_HEADER_LEN + data.len());
        frame.extend_from_slice(&header.encode());
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

    /// Accept `data` for `[offset, offset+data.len())` at `epoch`. Records that arrive ahead of
    /// the end wait (bounded) until the bytes before them are applied; bytes already held are
    /// acknowledged again if identical and refused if not. Returns the applied length, which is
    /// the acknowledgement: everything below it is held, and it is durable after `commit`.
    pub fn append(&mut self, epoch: u64, offset: u64, data: &[u8]) -> Result<u64, LogError> {
        self.check_epoch(epoch)?;
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
        let length = self.length();
        if end <= length {
            let held = &self.data[offset as usize..end as usize];
            return if held == data {
                Ok(length)
            } else {
                Err(LogError::Overlap)
            };
        }
        if offset > length {
            return self.hold(epoch, offset, data);
        }
        // offset <= length < end: the part below `length` must match, the rest is new
        let overlap = (length - offset) as usize;
        if self.data[offset as usize..].get(..overlap) != Some(&data[..overlap]) {
            return Err(LogError::Overlap);
        }
        self.apply_new(epoch, &data[overlap..])?;
        self.drain_pending(epoch)?;
        Ok(self.length())
    }

    fn hold(&mut self, epoch: u64, offset: u64, data: &[u8]) -> Result<u64, LogError> {
        let end = offset + data.len() as u64;
        // overlap with a waiting record of another shape is refused; the same record again is not
        if let Some(existing) = self.pending.get(&offset) {
            return if existing == data {
                Ok(self.length())
            } else {
                Err(LogError::Overlap)
            };
        }
        if let Some((before, held)) = self.pending.range(..offset).next_back() {
            if before + held.len() as u64 > offset {
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
        let _ = epoch;
        self.pending_bytes += data.len();
        self.pending.insert(offset, data.to_vec());
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
            let data = self.pending.remove(&next).unwrap();
            self.pending_bytes -= data.len();
            let overlap = (length - next) as usize;
            if overlap > data.len()
                || self.data[next as usize..].get(..overlap) != Some(&data[..overlap])
            {
                return Err(LogError::Overlap);
            }
            if overlap < data.len() {
                self.apply_new(epoch, &data[overlap..])?;
            }
        }
    }

    fn apply_new(&mut self, epoch: u64, data: &[u8]) -> Result<(), LogError> {
        let offset = self.length();
        let header = Header {
            kind: KIND_APPEND,
            flags: 0,
            generation: self.generation,
            epoch,
            offset,
            length: data.len() as u32,
            data_crc: crc32c(data),
        };
        let data_pos = self.file_len + LOG_HEADER_LEN as u64;
        self.write_record(header, data)?;
        self.extents.push(Extent {
            logical: offset,
            file_pos: data_pos,
            len: data.len() as u32,
            record_len: data.len() as u32,
            data_crc: header.data_crc,
        });
        self.data.extend_from_slice(data);
        self.length += data.len() as u64;
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
        };
        self.write_record(header, &[])?;
        self.data.truncate(length as usize);
        self.cut_extents(length);
        self.length = length;
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
