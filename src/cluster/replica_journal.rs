// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::message::ReplicaRequest;
use super::wire::{RequestCode, MAX_PAYLOAD_BYTES};
use crate::{fnv1a64, MatrixReplicaService};

const HEADER_LEN: usize = 24;
const MAGIC: &[u8; 4] = b"MOR1";
/// Frame magic of a journal laid out for zero-filled preallocation. Its frames end in one
/// `FRAME_END` byte after the payload, so a write cut short (last byte never written) can be told
/// from a record damaged after it was acknowledged. A reader that does not know this magic
/// refuses the file at the first frame.
const MAGIC_ZERO_TAIL: &[u8; 4] = b"MOZ2";
/// Last byte of every zero-tail frame. Never zero.
const FRAME_END: u8 = 0xA5;

pub(super) struct ReplicaJournal {
    file: File,
    // Stable across atomic journal replacement; retain the journal lock too.
    _lock: Option<File>,
    path: PathBuf,
    bytes_since_checkpoint: u64,
    initialized: bool,
    /// The LOGICAL end of the log: where the next record goes. A zero-tail file is longer.
    length: u64,
    /// True when the file is a zero-tail journal (`MAGIC_ZERO_TAIL` frames).
    chain: bool,
    /// Zero-fill chunk for a zero-tail journal; zero fills nothing ahead.
    zero_chunk: u64,
    /// How far the file has been zero-filled or written.
    physical: u64,
    #[cfg(test)]
    sync_count: u64,
}

/// Where a scrub stands: `next` is the offset to pass to the following step, always a frame
/// boundary, and `done` is true once the log has been walked to its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))] // no server walks the journal yet
pub(super) struct ScrubStep {
    pub(super) next: u64,
    pub(super) total: u64,
    pub(super) done: bool,
}

/// One past the last nonzero byte in `[from, to)`, or `from` when all of it is zero.
fn zero_scan(file: &mut File, from: u64, to: u64) -> io::Result<u64> {
    file.seek(SeekFrom::Start(from))?;
    let mut buffer = vec![0u8; 1 << 20];
    let mut at = from;
    let mut last = from;
    while at < to {
        let want = ((to - at) as usize).min(buffer.len());
        file.read_exact(&mut buffer[..want])?;
        if let Some(position) = buffer[..want].iter().rposition(|byte| *byte != 0) {
            last = at + position as u64 + 1;
        }
        at += want as u64;
    }
    Ok(last)
}

/// Write zeros over `[from, to)`, extending the file if it ends earlier, and make them durable.
/// The file cursor does not move.
fn zero_range(file: &File, from: u64, to: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let zeros = vec![0u8; 1 << 20];
        let mut at = from;
        while at < to {
            let want = ((to - at) as usize).min(zeros.len());
            file.write_all_at(&zeros[..want], at)?;
            at += want as u64;
        }
        file.sync_data()
    }
    #[cfg(not(unix))]
    {
        let _ = (file, from, to);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "zero-filled journal preallocation needs a unix file",
        ))
    }
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

fn outcome_header(payload: &[u8], chain: bool) -> io::Result<[u8; HEADER_LEN]> {
    if payload.is_empty() || payload.len() > MAX_PAYLOAD_BYTES + 1 {
        return Err(invalid("replica outcome exceeds its length bound"));
    }
    let mut header = [0; HEADER_LEN];
    header[..4].copy_from_slice(if chain { MAGIC_ZERO_TAIL } else { b"MOR2" });
    header[4..8].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    let header_checksum = fnv1a64(&header[..8]);
    header[8..16].copy_from_slice(&header_checksum.to_be_bytes());
    header[16..24].copy_from_slice(&fnv1a64(payload).to_be_bytes());
    Ok(header)
}

impl ReplicaJournal {
    #[cfg(test)]
    pub(super) fn open(path: &Path, service: &mut MatrixReplicaService) -> io::Result<Self> {
        Self::open_with_limits(path, service, usize::MAX)
    }

    #[cfg(test)]
    pub(super) fn open_with_limits(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> io::Result<Self> {
        Self::open_with_options(path, service, max_pages, 0)
    }

    /// `zero_chunk` nonzero asks for the zero-tail layout for a journal this call creates, and for
    /// the replacement a checkpoint writes. An existing journal keeps the layout its first frame
    /// names.
    pub(super) fn open_with_options(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
        zero_chunk: u64,
    ) -> io::Result<Self> {
        if zero_chunk != 0 {
            super::validate_wal_zero_fill_bytes(Some(zero_chunk))
                .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
        }
        let path = match path.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let parent = path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                parent.canonicalize()?.join(
                    path.file_name()
                        .ok_or_else(|| invalid("journal filename missing"))?,
                )
            }
            Err(error) => return Err(error),
        };
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(sibling(&path, ".lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let initialized = match lock.metadata()?.len() {
            0 => false,
            1 => {
                let mut marker = [0];
                (&lock).read_exact(&mut marker)?;
                if marker != *b"1" {
                    return Err(invalid("replica journal marker is invalid"));
                }
                true
            }
            _ => return Err(invalid("replica journal marker is invalid")),
        };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(!initialized)
            .truncate(false)
            .open(&path)?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        // Sync the file and its directory before this node can acknowledge a write.
        file.sync_all()?;
        #[cfg(unix)]
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        let file_len = file.metadata()?.len();
        if initialized && file_len == 0 {
            return Err(invalid("initialized replica journal is empty"));
        }
        let mut offset = restore_base(&mut file, service, max_pages)?;
        let base_end = offset;
        // A zero-tail journal is named by its first frame, or is a fresh file the host asked to
        // be one and that a crash left zero-filled before its first record.
        let mut chain = zero_chunk != 0 && file_len == 0;
        if file_len >= offset + 4 {
            let mut first = [0u8; 4];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut first)?;
            file.seek(SeekFrom::Start(offset))?;
            chain = &first == MAGIC_ZERO_TAIL || (zero_chunk != 0 && !initialized && first == [0; 4]);
        }
        let tail_bytes = u64::from(chain);
        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < HEADER_LEN as u64 {
                let mut tail = vec![0; remaining as usize];
                file.read_exact(&mut tail)?;
                let prefix = tail.len().min(MAGIC.len());
                let zeros = chain && tail.iter().all(|byte| *byte == 0);
                let named = if chain {
                    tail[..prefix] == MAGIC_ZERO_TAIL[..prefix]
                } else {
                    tail[..prefix] == MAGIC[..prefix] || tail[..prefix] == b"MOR2"[..prefix]
                };
                if !zeros && !named {
                    return Err(invalid("replica journal has a damaged final header"));
                }
                break;
            }
            let mut header = [0; HEADER_LEN];
            file.read_exact(&mut header)?;
            if chain && header == [0u8; HEADER_LEN] {
                // The end of the log. Only zeros may follow; a nonzero byte further on is a
                // record the log cannot reach, which is damage and not an unwritten tail.
                if zero_scan(&mut file, offset + HEADER_LEN as u64, file_len)?
                    != offset + HEADER_LEN as u64
                {
                    return Err(invalid("replica journal has data beyond its end"));
                }
                break;
            }
            let known = if chain {
                &header[..4] == MAGIC_ZERO_TAIL
            } else {
                &header[..4] == MAGIC || &header[..4] == b"MOR2"
            };
            if !known
                || fnv1a64(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
            {
                // A header cut short has only zeros after it. A header damaged after its record
                // was acknowledged has that record's payload and terminator after it.
                if chain
                    && zero_scan(&mut file, offset + HEADER_LEN as u64, file_len)?
                        == offset + HEADER_LEN as u64
                {
                    break;
                }
                return Err(invalid("replica journal header checksum mismatch"));
            }
            let length = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            if length == 0 || length > MAX_PAYLOAD_BYTES + 1 {
                return Err(invalid("replica journal record exceeds its length bound"));
            }
            if remaining - (HEADER_LEN as u64) < length as u64 + tail_bytes {
                break;
            }
            let mut payload = vec![0; length];
            file.read_exact(&mut payload)?;
            let mut terminator = FRAME_END;
            if chain {
                let mut last = [0u8; 1];
                file.read_exact(&mut last)?;
                terminator = last[0];
            }
            if fnv1a64(&payload) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                // Cut short: the frame's last byte was never written and nothing came after it.
                // A frame whose last byte is present, or that has anything after it, was written
                // in full and then damaged.
                let end = offset + HEADER_LEN as u64 + length as u64 + 1;
                if chain && terminator == 0 && zero_scan(&mut file, end, file_len)? == end {
                    break;
                }
                return Err(invalid("replica journal payload checksum mismatch"));
            }
            if chain && terminator != 0 && terminator != FRAME_END {
                return Err(invalid("replica journal frame terminator damaged"));
            }
            if &header[..4] == b"MOR2" || &header[..4] == MAGIC_ZERO_TAIL {
                if payload.starts_with(crate::chunk_namespace_fence::MAGIC) {
                    crate::chunk_namespace_fence::NamespaceFenceOutcome::from_payload(&payload)
                        .and_then(|outcome| outcome.apply(service)).map_err(invalid)?;
                } else {
                    crate::outcome::PageOutcome::from_payload(&payload)
                    .map_err(invalid)?
                    .apply(service, max_pages)
                    .map_err(invalid)?;
                }
            } else {
                let code = RequestCode::from_u8(payload[0])
                    .ok_or_else(|| invalid("unknown replica journal operation"))?;
                if !matches!(code, RequestCode::WriteChunk | RequestCode::DiscardChunk) {
                    return Err(invalid("replica journal contains a non-mutating operation"));
                }
                let request = ReplicaRequest::decode(code, &payload[1..]).map_err(invalid)?;
                super::server::apply_to_replica(service, request, max_pages).map_err(invalid)?;
            }
            offset += HEADER_LEN as u64 + length as u64 + tail_bytes;
        }
        if initialized && offset == 0 {
            return Err(invalid("initialized replica journal lost its first record"));
        }
        if chain {
            // What a cut-short write left past the log is zeroed, so the next record is not
            // followed by the rest of the one that was lost.
            let residue = zero_scan(&mut file, offset, file_len)?;
            if residue > offset {
                zero_range(&file, offset, residue)?;
            }
            file.seek(SeekFrom::Start(offset))?;
        } else {
            if offset != file_len {
                // Only a trailing incomplete record is discarded; complete damaged records
                // are errors. The shortened file is durable before new appends begin.
                file.set_len(offset)?;
                file.sync_all()?;
            }
            file.seek(SeekFrom::End(0))?;
        }
        let mut journal = Self {
            file,
            _lock: Some(lock),
            path,
            bytes_since_checkpoint: offset - base_end,
            initialized,
            length: offset,
            chain,
            zero_chunk,
            physical: file_len.max(offset),
            #[cfg(test)]
            sync_count: 0,
        };
        // Fill the first chunk now, while the node is not serving, so no request waits on it.
        journal.prefill()?;
        if offset != 0 {
            journal.mark_initialized()?;
        }
        Ok(journal)
    }

    #[cfg(test)]
    pub(super) fn encode(request: &ReplicaRequest) -> io::Result<Vec<u8>> {
        let payload = request.encode_payload();
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(invalid("replica journal request exceeds its length bound"));
        }
        let mut record = Vec::with_capacity(HEADER_LEN + 1 + payload.len());
        record.extend_from_slice(MAGIC);
        record.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
        record.extend_from_slice(&fnv1a64(&record).to_be_bytes());
        record.extend_from_slice(&[0; 8]);
        record.push(request.code() as u8);
        record.extend_from_slice(&payload);
        let checksum = fnv1a64(&record[HEADER_LEN..]);
        record[16..24].copy_from_slice(&checksum.to_be_bytes());
        Ok(record)
    }

    #[cfg(test)]
    pub(super) fn encode_outcome(outcome: &crate::outcome::PageOutcome) -> io::Result<Vec<u8>> {
        let payload = outcome.to_payload().map_err(invalid)?;
        let header = outcome_header(&payload, false)?;
        let mut record = Vec::with_capacity(HEADER_LEN + payload.len());
        record.extend_from_slice(&header);
        record.extend_from_slice(&payload);
        Ok(record)
    }

    pub(super) fn append_outcome(&mut self, outcome: &crate::outcome::PageOutcome) -> io::Result<()> {
        self.append_outcome_deferred(outcome)?;
        self.sync_deferred()
    }

    /// Keep the outcome payload in its serialized buffer while writing its
    /// checksum header and payload as two slices. A bounded batch syncs once
    /// after all of its outcomes have been appended.
    pub(super) fn append_outcome_deferred(
        &mut self,
        outcome: &crate::outcome::PageOutcome,
    ) -> io::Result<()> {
        let payload = outcome.to_payload().map_err(invalid)?;
        self.append_payload_deferred(&payload)
    }

    pub(super) fn append_namespace_fence_deferred(
        &mut self, outcome: &crate::chunk_namespace_fence::NamespaceFenceOutcome,
    ) -> io::Result<()> {
        let payload = outcome.to_payload().map_err(invalid)?;
        self.append_payload_deferred(&payload)
    }

    fn append_payload_deferred(&mut self, payload: &[u8]) -> io::Result<()> {
        let header = outcome_header(payload, self.chain)?;
        let frame = HEADER_LEN as u64 + payload.len() as u64 + u64::from(self.chain);
        let next_bytes = self
            .bytes_since_checkpoint
            .checked_add(frame)
            .ok_or_else(|| invalid("journal length overflow"))?;
        self.reserve(frame)?;
        let end = [FRAME_END];
        let mut slices = [
            IoSlice::new(&header),
            IoSlice::new(payload),
            IoSlice::new(&end[..usize::from(self.chain)]),
        ];
        let mut remaining = &mut slices[..];
        while !remaining.is_empty() {
            match self.file.write_vectored(remaining) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(written) => IoSlice::advance_slices(&mut remaining, written),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        self.bytes_since_checkpoint = next_bytes;
        self.length += frame;
        Ok(())
    }

    /// Zero-fill one chunk ahead of the log if the file does not already hold that much, so the
    /// fill is paid when the journal opens and not by a write.
    fn prefill(&mut self) -> io::Result<()> {
        if self.chain && self.zero_chunk != 0 {
            let target = self.length.saturating_add(self.zero_chunk);
            if target > self.physical {
                zero_range(&self.file, self.physical, target)?;
                self.physical = target;
            }
        }
        Ok(())
    }

    /// Make room for `extra` more bytes of log. A zero-tail journal fills the file with zeros
    /// ahead of the log, so appends overwrite space that already exists and no append changes the
    /// file length.
    fn reserve(&mut self, extra: u64) -> io::Result<()> {
        if !self.chain {
            return Ok(());
        }
        self.physical = self.physical.max(self.length);
        let end = self.length.saturating_add(extra);
        if self.zero_chunk != 0 && end > self.physical {
            let target = self
                .physical
                .saturating_add(end.saturating_sub(self.physical).max(self.zero_chunk));
            zero_range(&self.file, self.physical, target)?;
            self.physical = target;
        }
        Ok(())
    }

    /// Validate the journal from `from` (zero, or the `next` of a previous step) until at least
    /// `max_bytes` have been read or the log ends: each frame's header, payload digest and
    /// terminator, and, from zero, the checkpoint image and its digest. Nothing is applied and the
    /// journal is not changed; reads use positioned I/O, so the append cursor does not move. A
    /// step from zero covers the whole checkpoint image, so it can exceed `max_bytes`. A host
    /// calls this off the write path at a pace of its own choosing.
    #[cfg(unix)]
    #[cfg_attr(not(test), allow(dead_code))] // no server walks the journal yet
    pub(super) fn scrub_step(&self, from: u64, max_bytes: u64) -> io::Result<ScrubStep> {
        use std::os::unix::fs::FileExt;
        if from > self.length {
            return Err(invalid("scrub position is past the end of the journal"));
        }
        let mut at = from;
        if at == 0 {
            let mut prefix = [0u8; 4];
            let count = self.length.min(4) as usize;
            self.file.read_exact_at(&mut prefix[..count], 0)?;
            if count == 4 && &prefix == b"MOSJ" {
                let mut header = [0; BASE_HEADER_LEN];
                self.file.read_exact_at(&mut header, 0)?;
                if header[4..8] != 1u32.to_be_bytes()
                    || fnv1a64(&header[..24])
                        != u64::from_be_bytes(header[24..].try_into().unwrap())
                {
                    return Err(invalid("replica checkpoint header is invalid"));
                }
                let length = u64::from_be_bytes(header[8..16].try_into().unwrap());
                if BASE_HEADER_LEN as u64 + length > self.length {
                    return Err(invalid("replica checkpoint is incomplete or oversized"));
                }
                let mut hash = Digest::new(io::sink());
                let mut buffer = vec![0u8; 1 << 20];
                let mut done = 0;
                while done < length {
                    let want = ((length - done) as usize).min(buffer.len());
                    self.file
                        .read_exact_at(&mut buffer[..want], BASE_HEADER_LEN as u64 + done)?;
                    hash.update(&buffer[..want]);
                    done += want as u64;
                }
                if hash.hash != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                    return Err(invalid("replica checkpoint checksum mismatch"));
                }
                at = BASE_HEADER_LEN as u64 + length;
            }
        }
        let start = at;
        let tail = u64::from(self.chain);
        while at < self.length && at - start < max_bytes.max(1) {
            if self.length - at < HEADER_LEN as u64 {
                return Err(invalid("replica journal has a damaged final header"));
            }
            let mut header = [0; HEADER_LEN];
            self.file.read_exact_at(&mut header, at)?;
            let known = if self.chain {
                &header[..4] == MAGIC_ZERO_TAIL
            } else {
                &header[..4] == MAGIC || &header[..4] == b"MOR2"
            };
            if !known
                || fnv1a64(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
            {
                return Err(invalid("replica journal header checksum mismatch"));
            }
            let length = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            if length == 0
                || length > MAX_PAYLOAD_BYTES + 1
                || self.length - at - (HEADER_LEN as u64) < length as u64 + tail
            {
                return Err(invalid("replica journal record exceeds its length bound"));
            }
            let mut payload = vec![0; length + tail as usize];
            self.file
                .read_exact_at(&mut payload, at + HEADER_LEN as u64)?;
            if self.chain {
                let last = payload.pop().unwrap();
                if last != FRAME_END && last != 0 {
                    return Err(invalid("replica journal frame terminator damaged"));
                }
            }
            if fnv1a64(&payload) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                return Err(invalid("replica journal payload checksum mismatch"));
            }
            at += HEADER_LEN as u64 + length as u64 + tail;
        }
        Ok(ScrubStep {
            next: at,
            total: self.length,
            done: at == self.length,
        })
    }

    /// The log's own length, which in a zero-tail journal is shorter than the file.
    #[cfg(test)]
    pub(super) fn logical_len(&self) -> u64 {
        self.length
    }

    #[cfg(test)]
    pub(super) fn append(&mut self, record: &[u8]) -> io::Result<()> {
        self.append_deferred(record)?;
        self.sync_deferred()
    }

    /// Append a prebuilt record for recovery tests.
    #[cfg(test)]
    pub(super) fn append_deferred(&mut self, record: &[u8]) -> io::Result<()> {
        self.file.write_all(record)?;
        self.length += record.len() as u64;
        self.bytes_since_checkpoint = self
            .bytes_since_checkpoint
            .checked_add(record.len() as u64)
            .ok_or_else(|| invalid("journal length overflow"))?;
        Ok(())
    }

    pub(super) fn sync_deferred(&mut self) -> io::Result<()> {
        // Growing the file requires its new length to survive a crash;
        // sync_data persists the bytes and metadata needed to retrieve them.
        self.file.sync_data()?;
        self.mark_initialized()?;
        #[cfg(test)]
        {
            self.sync_count = self.sync_count.saturating_add(1);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn sync_count(&self) -> u64 {
        self.sync_count
    }

    fn mark_initialized(&mut self) -> io::Result<()> {
        if self.initialized {
            return Ok(());
        }
        if let Some(lock) = &self._lock {
            let mut marker = lock;
            marker.seek(SeekFrom::Start(0))?;
            marker.write_all(b"1")?;
            lock.sync_all()?;
        }
        self.initialized = true;
        Ok(())
    }

    pub(super) fn checkpoint_due(&self, interval: u64) -> bool {
        interval != 0 && self.bytes_since_checkpoint >= interval
    }

    #[cfg(unix)]
    pub(super) fn checkpoint(&mut self, service: &MatrixReplicaService) -> io::Result<()> {
        let mut stream = super::replica_image::capture(service.export_snapshot(), (1, 1))
            .map_err(invalid)?
            .ok_or_else(|| invalid("replica checkpoint exceeds stream bounds"))?;
        let length = stream.length;
        let temporary = sibling(&self.path, ".pending");
        let mut replacement = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        fs2::FileExt::try_lock_exclusive(&replacement)?;
        replacement.write_all(&[0; BASE_HEADER_LEN])?;
        let mut output = Digest::new(&mut replacement);
        if io::copy(&mut stream, &mut output)? != length {
            return Err(invalid("replica checkpoint stream length changed"));
        }
        let checksum = output.hash;
        replacement.seek(SeekFrom::Start(0))?;
        replacement.write_all(&base_header(length, checksum))?;
        replacement.sync_all()?;
        replacement.seek(SeekFrom::End(0))?;
        // The stable lock spans both files. No writer can reopen the old inode
        // between replacement and acquiring the new journal lock.
        std::fs::rename(&temporary, &self.path)?;
        self.file = replacement;
        self.length = BASE_HEADER_LEN as u64 + length;
        self.physical = self.length;
        self.chain = self.zero_chunk != 0;
        self.bytes_since_checkpoint = 0;
        File::open(self.path.parent().unwrap())?.sync_all()?;
        self.mark_initialized()?;
        Ok(())
    }

    #[cfg(not(unix))]
    pub(super) fn checkpoint(&mut self, _service: &MatrixReplicaService) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "journal checkpoints require Unix atomic file replacement",
        ))
    }
}

const BASE_HEADER_LEN: usize = 32;
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}
fn base_header(length: u64, checksum: u64) -> [u8; BASE_HEADER_LEN] {
    let mut header = [0; BASE_HEADER_LEN];
    header[..4].copy_from_slice(b"MOSJ");
    header[4..8].copy_from_slice(&1u32.to_be_bytes());
    header[8..16].copy_from_slice(&length.to_be_bytes());
    header[16..24].copy_from_slice(&checksum.to_be_bytes());
    let digest = fnv1a64(&header[..24]);
    header[24..].copy_from_slice(&digest.to_be_bytes());
    header
}
fn restore_base(
    file: &mut File,
    service: &mut MatrixReplicaService,
    max_pages: usize,
) -> io::Result<u64> {
    let count = file.metadata()?.len().min(4) as usize;
    let mut prefix = [0; 4];
    file.read_exact(&mut prefix[..count])?;
    file.seek(SeekFrom::Start(0))?;
    if count == 0 || prefix[..count] != b"MOSJ"[..count] {
        return Ok(0);
    }
    let mut header = [0; BASE_HEADER_LEN];
    file.read_exact(&mut header)?;
    if header[4..8] != 1u32.to_be_bytes()
        || fnv1a64(&header[..24]) != u64::from_be_bytes(header[24..].try_into().unwrap())
    {
        return Err(invalid("replica checkpoint header is invalid"));
    }
    let length = u64::from_be_bytes(header[8..16].try_into().unwrap());
    if !(2..=super::replica_image::MAX_FILE_CHECKPOINT_BYTES).contains(&length)
        || length
            > file
                .metadata()?
                .len()
                .saturating_sub(BASE_HEADER_LEN as u64)
    {
        return Err(invalid("replica checkpoint is incomplete or oversized"));
    }
    let mut input = Digest::new(file);
    let snapshot = super::replica_image::restore(
        &mut input,
        length,
        (1, 1),
        service.capacity_bytes(),
        max_pages,
    )
    .map_err(invalid)?;
    if input.hash != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
        return Err(invalid("replica checkpoint checksum mismatch"));
    }
    service
        .install_validated_snapshot(snapshot)
        .map_err(invalid)?;
    Ok(BASE_HEADER_LEN as u64 + length)
}

struct Digest<T> {
    inner: T,
    hash: u64,
}
impl<T> Digest<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            hash: 0xcbf29ce484222325,
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(0x100000001b3);
        }
    }
}
impl<T: Read> Read for Digest<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        self.update(&bytes[..count]);
        Ok(count)
    }
}
impl<T: Write> Write for Digest<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
impl ReplicaJournal {
    pub(super) fn read_only_for_test(path: &Path) -> Self {
        Self {
            file: File::open(path).unwrap(),
            _lock: None,
            path: path.to_path_buf(),
            bytes_since_checkpoint: 0,
            initialized: true,
            length: 0,
            chain: false,
            zero_chunk: 0,
            physical: 0,
            sync_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChunkId, PageId};

    fn page(index: u64) -> PageId {
        PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: index,
        }
    }
    fn request(index: u64, bytes: &[u8]) -> ReplicaRequest {
        ReplicaRequest::WriteChunk {
            page_id: page(index),
            offset: 0,
            data: bytes.to_vec(),
        }
    }
    fn path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "replica-journal-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn every_incomplete_final_record_preserves_the_committed_prefix() {
        let path = path("tail");
        let first = ReplicaJournal::encode(&request(1, b"committed")).unwrap();
        let second = ReplicaJournal::encode(&request(2, b"unfinished")).unwrap();
        for cut in 0..second.len() {
            let bytes = [first.as_slice(), &second[..cut]].concat();
            std::fs::write(&path, bytes).unwrap();
            let mut service = MatrixReplicaService::new();
            let mut journal = ReplicaJournal::open(&path, &mut service).unwrap();
            assert_eq!(
                service.read_chunk(&page(1), 0, 9).unwrap().data,
                b"committed"
            );
            assert!(service.head_chunk(&page(2)).is_err());
            assert_eq!(std::fs::metadata(&path).unwrap().len(), first.len() as u64);
            journal.append(&second).unwrap();
            drop(journal);
            let mut recovered = MatrixReplicaService::new();
            drop(ReplicaJournal::open(&path, &mut recovered).unwrap());
            assert_eq!(
                recovered.read_chunk(&page(2), 0, 10).unwrap().data,
                b"unfinished"
            );
        }
        std::fs::remove_file(sibling(&path, ".lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn corruption_in_each_record_byte_is_rejected_without_truncation() {
        let path = path("corrupt");
        let record = ReplicaJournal::encode(&request(1, b"durable")).unwrap();
        for index in 0..record.len() {
            let mut damaged = record.clone();
            damaged[index] ^= 1;
            std::fs::write(&path, &damaged).unwrap();
            assert!(
                ReplicaJournal::open(&path, &mut MatrixReplicaService::new()).is_err(),
                "byte {index}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
        }
        std::fs::remove_file(sibling(&path, ".lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use crate::{outcome::PageOutcome, ChunkId, PageId};
    #[test]
    fn binary_outcome_journal_recovers_mixed_history_and_rejects_damage() {
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        };
        let request = ReplicaRequest::WriteChunk {
            page_id: page.clone(),
            offset: 0,
            data: b"old".to_vec(),
        };
        let first = ReplicaJournal::encode(&request).unwrap();
        let mut source = MatrixReplicaService::new();
        super::super::server::apply_to_replica(&mut source, request, 1).unwrap();
        let before = source.read_chunk_state(&page).unwrap();
        let counts = PageOutcome::counters(&source);
        source.write_chunk(page.clone(), 3, b"new").unwrap();
        let outcome = PageOutcome::between(
            &page,
            Some(&before),
            &source.read_chunk_state(&page).unwrap(),
        )
        .unwrap()
        .with_counters(counts, PageOutcome::counters(&source));
        let second = ReplicaJournal::encode_outcome(&outcome).unwrap();
        let path = std::env::temp_dir().join(format!("binary-journal-{}", std::process::id()));
        for cut in 0..second.len() {
            std::fs::write(&path, [first.as_slice(), &second[..cut]].concat()).unwrap();
            let mut recovered = MatrixReplicaService::new();
            let mut journal = ReplicaJournal::open(&path, &mut recovered).unwrap();
            assert_eq!(recovered.read_chunk_state(&page).unwrap(), before);
            journal.append_outcome(&outcome).unwrap();
            drop(journal);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                [first.as_slice(), second.as_slice()].concat()
            );
            let mut recovered = MatrixReplicaService::new();
            drop(ReplicaJournal::open(&path, &mut recovered).unwrap());
            assert_eq!(
                recovered.read_chunk_state(&page).unwrap(),
                source.read_chunk_state(&page).unwrap()
            );
            assert_eq!(
                PageOutcome::counters(&recovered),
                PageOutcome::counters(&source)
            );
        }
        for index in 0..second.len() {
            let mut damaged = second.clone();
            damaged[index] ^= 1;
            let bytes = [first.as_slice(), damaged.as_slice()].concat();
            std::fs::write(&path, &bytes).unwrap();
            assert!(ReplicaJournal::open(&path, &mut MatrixReplicaService::new()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        std::fs::remove_file(sibling(&path, ".lock")).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(all(test, unix))]
mod checkpoint_tests {
    use super::*;
    use crate::{ChunkId, PageId};
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "journal-checkpoint-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn file(&self) -> PathBuf {
            self.0.join("replica.wal")
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn service(capacity: u64) -> MatrixReplicaService {
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(capacity).unwrap();
        service
    }
    fn page(index: u64) -> PageId {
        PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: index,
        }
    }
    fn append(journal: &mut ReplicaJournal, service: &mut MatrixReplicaService, value: u8) {
        let before = service.read_chunk_state(&page(0)).ok();
        let counters = crate::outcome::PageOutcome::counters(service);
        service.write_chunk(page(0), 0, &[value; 1024]).unwrap();
        let outcome = crate::outcome::PageOutcome::between(
            &page(0),
            before.as_ref(),
            &service.read_chunk_state(&page(0)).unwrap(),
        )
        .unwrap()
        .with_counters(counters, crate::outcome::PageOutcome::counters(service));
        journal.append_outcome(&outcome).unwrap();
    }

    #[test]
    fn journal_checkpoint_reclaims_history_keeps_lock_and_recovers_following_outcomes() {
        let directory = Directory::new();
        let path = directory.file();
        let mut state = service(4096);
        let mut journal = ReplicaJournal::open_with_limits(&path, &mut state, 2).unwrap();
        for value in 1..=20 {
            append(&mut journal, &mut state, value);
        }
        let old_len = std::fs::metadata(&path).unwrap().len();
        assert!(journal.checkpoint_due(1024));
        let expected = state.export_snapshot();
        journal.checkpoint(&state).unwrap();
        let base_len = std::fs::metadata(&path).unwrap().len();
        assert!(base_len < old_len / 2);
        assert!(!journal.checkpoint_due(1));
        assert!(ReplicaJournal::open_with_limits(&path, &mut service(4096), 2).is_err());
        // A leftover candidate is never an authoritative image.
        std::fs::write(sibling(&path, ".pending"), b"unfinished image").unwrap();
        drop(journal);
        let mut recovered = service(4096);
        let mut journal = ReplicaJournal::open_with_limits(&path, &mut recovered, 2).unwrap();
        assert_eq!(recovered.export_snapshot(), expected);
        assert!(!journal.checkpoint_due(1));
        append(&mut journal, &mut recovered, 99);
        let expected = recovered.export_snapshot();
        assert!(journal.checkpoint_due(1024));
        drop(journal);
        // An incomplete later outcome is discarded without discarding the base.
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"MOR2")
            .unwrap();
        let mut recovered = service(4096);
        let mut journal = ReplicaJournal::open_with_limits(&path, &mut recovered, 2).unwrap();
        assert_eq!(recovered.export_snapshot(), expected);
        journal.checkpoint(&recovered).unwrap();
        drop(journal);
        let mut recovered = service(4096);
        drop(ReplicaJournal::open_with_limits(&path, &mut recovered, 2).unwrap());
        assert_eq!(recovered.export_snapshot(), expected);
    }

    #[test]
    fn journal_checkpoint_refuses_missing_truncated_damaged_or_misconfigured_state() {
        let directory = Directory::new();
        let path = directory.file();
        let mut state = service(4096);
        let mut journal = ReplicaJournal::open_with_limits(&path, &mut state, 2).unwrap();
        append(&mut journal, &mut state, 5);
        journal.checkpoint(&state).unwrap();
        drop(journal);
        let bytes = std::fs::read(&path).unwrap();
        for cut in [0, 1, 3, 4, 31, 32, bytes.len() - 1] {
            std::fs::write(&path, &bytes[..cut]).unwrap();
            assert!(
                ReplicaJournal::open_with_limits(&path, &mut service(4096), 2).is_err(),
                "cut {cut}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), bytes[..cut]);
        }
        for index in (0..BASE_HEADER_LEN).chain([bytes.len() / 2, bytes.len() - 1]) {
            let mut damaged = bytes.clone();
            damaged[index] ^= 1;
            std::fs::write(&path, &damaged).unwrap();
            assert!(
                ReplicaJournal::open_with_limits(&path, &mut service(4096), 2).is_err(),
                "byte {index}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
        }
        std::fs::write(&path, &bytes).unwrap();
        assert!(ReplicaJournal::open_with_limits(&path, &mut service(8192), 2).is_err());
        assert!(ReplicaJournal::open_with_limits(&path, &mut service(4096), 0).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::remove_file(&path).unwrap();
        assert!(ReplicaJournal::open_with_limits(&path, &mut service(4096), 2).is_err());
        assert!(
            !path.exists(),
            "missing initialized history must not be recreated"
        );
    }

    #[test]
    #[ignore = "272 MiB standalone durable checkpoint; run explicitly in release mode"]
    fn standalone_journal_streams_large_pages_and_more_than_256_mib() {
        let directory = Directory::new();
        let path = directory.file();
        let mut state = service(300 * 1024 * 1024);
        let mut journal = ReplicaJournal::open_with_limits(&path, &mut state, 2).unwrap();
        state
            .write_chunk(page(0), 0, &vec![7; 256 * 1024 * 1024])
            .unwrap();
        state
            .write_chunk(page(1), 0, &vec![9; 16 * 1024 * 1024])
            .unwrap();
        state.discard_chunk(&page(0), 100, 10).unwrap();
        let expected = state.export_snapshot();
        journal.checkpoint(&state).unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > 256 * 1024 * 1024);
        drop(journal);
        drop(state);
        let mut recovered = service(300 * 1024 * 1024);
        drop(ReplicaJournal::open_with_limits(&path, &mut recovered, 2).unwrap());
        assert_eq!(recovered.export_snapshot(), expected);
    }
}

#[cfg(all(test, unix))]
mod zero_tail_tests {
    use super::*;
    use crate::{ChunkId, PageId};

    fn page(index: u64) -> PageId {
        PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: index,
        }
    }
    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "replica-zero-tail-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    fn service() -> MatrixReplicaService {
        let mut service = MatrixReplicaService::new();
        service.set_capacity_bytes(1 << 20).unwrap();
        service
    }
    fn append(journal: &mut ReplicaJournal, service: &mut MatrixReplicaService, value: u8) {
        let before = service.read_chunk_state(&page(0)).ok();
        let counters = crate::outcome::PageOutcome::counters(service);
        service.write_chunk(page(0), 0, &[value; 300]).unwrap();
        let outcome = crate::outcome::PageOutcome::between(
            &page(0),
            before.as_ref(),
            &service.read_chunk_state(&page(0)).unwrap(),
        )
        .unwrap()
        .with_counters(counters, crate::outcome::PageOutcome::counters(service));
        journal.append_outcome(&outcome).unwrap();
    }
    fn open(path: &Path, service: &mut MatrixReplicaService, zero: bool) -> io::Result<ReplicaJournal> {
        ReplicaJournal::open_with_options(path, service, 4, if zero { 1 << 16 } else { 0 })
    }
    fn state_of(path: &Path) -> io::Result<Option<crate::outcome::PageOutcome>> {
        let _ = path;
        Ok(None)
    }
    fn remove(path: &Path) {
        let _ = std::fs::remove_file(sibling(path, ".lock"));
        let _ = std::fs::remove_file(path);
    }

    /// Three acknowledged writes, then one more. Returns the path, the replica state after the
    /// third and after the fourth write, the log length before and after the fourth write, and the
    /// file image.
    fn fixture(label: &str) -> (PathBuf, Vec<u8>, Vec<u8>, u64, u64, Vec<u8>) {
        let _ = state_of;
        let path = path(label);
        let mut state = service();
        let mut journal = open(&path, &mut state, true).unwrap();
        for value in 1..=3 {
            append(&mut journal, &mut state, value);
        }
        let third = state.read_chunk(&page(0), 0, 300).unwrap().data;
        let before = journal.logical_len();
        append(&mut journal, &mut state, 4);
        let fourth = state.read_chunk(&page(0), 0, 300).unwrap().data;
        let after = journal.logical_len();
        drop(journal);
        (path.clone(), third, fourth, before, after, std::fs::read(&path).unwrap())
    }

    #[test]
    fn a_crash_at_any_byte_of_the_last_write_recovers_every_acknowledged_record() {
        let (path, third, fourth, before, after, full) = fixture("cut");
        assert!(full.len() as u64 > after, "the file must be longer than the log");
        for cut in before..after {
            let mut image = full.clone();
            image[cut as usize..].fill(0);
            std::fs::write(&path, &image).unwrap();
            let mut recovered = service();
            let journal = open(&path, &mut recovered, true)
                .unwrap_or_else(|error| panic!("cut {cut}: a cut-short write refused: {error}"));
            let seen = recovered.read_chunk(&page(0), 0, 300).unwrap().data;
            // Bytes cut off that were zero anyway leave the record whole and verified (it then
            // lacks only its terminator), and a verified record is kept.
            if full[cut as usize..after as usize - 1].iter().any(|byte| *byte != 0) {
                assert_eq!(seen, third, "cut {cut}: a partial record was applied");
            } else {
                assert!(seen == third || seen == fourth, "cut {cut}");
            }
            drop(journal);
            // Write on: nothing of the lost record may follow the next one.
            let mut again = service();
            let mut journal = open(&path, &mut again, true).unwrap();
            append(&mut journal, &mut again, 9);
            drop(journal);
            let mut last = service();
            drop(open(&path, &mut last, true).unwrap());
            assert_eq!(last.read_chunk(&page(0), 0, 300).unwrap().data, vec![9u8; 300], "cut {cut}");
        }
        remove(&path);
    }

    /// A flipped byte anywhere in an acknowledged record, with zeros after the last of them, is
    /// damage and refuses the open; a rule that drops "a bad last record followed by zeros" turns
    /// this red.
    #[test]
    fn damage_to_an_acknowledged_record_is_never_dropped() {
        let (path, _third, _fourth, _before, after, full) = fixture("flip");
        for index in 0..after as usize {
            let mut damaged = full.clone();
            damaged[index] ^= 1;
            std::fs::write(&path, &damaged).unwrap();
            assert!(
                open(&path, &mut service(), true).is_err(),
                "byte {index} of {after}: damage to an acknowledged record was accepted"
            );
            assert_eq!(std::fs::read(&path).unwrap(), damaged, "byte {index}");
        }
        remove(&path);
    }

    #[test]
    fn bytes_beyond_the_end_of_the_log_refuse_the_open() {
        let (path, _third, _fourth, _before, after, full) = fixture("beyond");
        for offset in [after + 24, after + 4096, full.len() as u64 - 1] {
            let mut damaged = full.clone();
            damaged[offset as usize] = 1;
            std::fs::write(&path, &damaged).unwrap();
            assert!(open(&path, &mut service(), true).is_err(), "stray byte at {offset}");
        }
        remove(&path);
    }

    #[test]
    fn a_clean_reopen_keeps_the_zero_tail_and_appends_do_not_grow_the_file() {
        let path = path("clean");
        let mut state = service();
        let mut journal = open(&path, &mut state, true).unwrap();
        append(&mut journal, &mut state, 1);
        let physical = std::fs::metadata(&path).unwrap().len();
        assert!(physical >= 1 << 16);
        for value in 2..=30 {
            append(&mut journal, &mut state, value);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), physical);
        }
        let logical = journal.logical_len();
        assert!(logical < physical);
        let syncs = journal.sync_count();
        assert_eq!(syncs, 30, "one barrier per write");
        drop(journal);
        let mut recovered = service();
        // The layout comes from the file, not from the switch.
        let journal = open(&path, &mut recovered, false).unwrap();
        assert_eq!(journal.logical_len(), logical);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), physical);
        assert_eq!(recovered.read_chunk(&page(0), 0, 300).unwrap().data, vec![30u8; 300]);
        drop(journal);
        remove(&path);
    }

    #[test]
    fn a_checkpoint_replaces_the_journal_with_a_zero_tail_one_of_the_right_length() {
        let path = path("checkpoint");
        let mut state = service();
        let mut journal = open(&path, &mut state, true).unwrap();
        for value in 1..=10 {
            append(&mut journal, &mut state, value);
        }
        journal.checkpoint(&state).unwrap();
        let logical = journal.logical_len();
        append(&mut journal, &mut state, 11);
        assert!(journal.logical_len() > logical);
        assert!(std::fs::metadata(&path).unwrap().len() > journal.logical_len());
        let end = journal.logical_len();
        drop(journal);
        let mut recovered = service();
        let journal = open(&path, &mut recovered, true).unwrap();
        assert_eq!(journal.logical_len(), end);
        assert_eq!(recovered.read_chunk(&page(0), 0, 300).unwrap().data, vec![11u8; 300]);
        drop(journal);
        remove(&path);
    }

    fn deferred(journal: &mut ReplicaJournal, service: &mut MatrixReplicaService, value: u8) {
        let before = service.read_chunk_state(&page(0)).ok();
        let counters = crate::outcome::PageOutcome::counters(service);
        service.write_chunk(page(0), 0, &[value; 300]).unwrap();
        let outcome = crate::outcome::PageOutcome::between(
            &page(0),
            before.as_ref(),
            &service.read_chunk_state(&page(0)).unwrap(),
        )
        .unwrap()
        .with_counters(counters, crate::outcome::PageOutcome::counters(service));
        journal.append_outcome_deferred(&outcome).unwrap();
    }

    /// Group commit: however many outcomes a batch holds, it costs one sync, in both layouts.
    #[test]
    fn a_batch_of_outcomes_costs_one_sync_in_both_layouts() {
        for zero in [false, true] {
            let path = path("batch");
            let mut state = service();
            let mut journal = open(&path, &mut state, zero).unwrap();
            let before = journal.sync_count();
            for value in 1..=8 {
                deferred(&mut journal, &mut state, value);
            }
            assert_eq!(journal.sync_count(), before, "a deferred append must not sync");
            journal.sync_deferred().unwrap();
            assert_eq!(journal.sync_count(), before + 1, "zero={zero}");
            drop(journal);
            remove(&path);
        }
    }

    /// A scrub walks bounded steps to the log's end (never the zero fill) and finds damage in
    /// the checkpoint image or in any frame.
    #[test]
    fn a_scrub_walks_in_steps_and_finds_damage_in_both_layouts() {
        for zero in [false, true] {
            let path = path("scrub");
            let mut state = service();
            let mut journal = open(&path, &mut state, zero).unwrap();
            for value in 1..=6 {
                append(&mut journal, &mut state, value);
            }
            journal.checkpoint(&state).unwrap();
            for value in 7..=20 {
                append(&mut journal, &mut state, value);
            }
            let end = journal.logical_len();
            let (mut at, mut steps) = (0, 0);
            loop {
                let step = journal.scrub_step(at, 500).unwrap();
                assert!(step.next > at);
                at = step.next;
                steps += 1;
                if step.done {
                    break;
                }
            }
            assert_eq!(at, end, "zero={zero}: the scrub covers the log, not the file");
            assert!(steps > 5);
            assert!(journal.scrub_step(end + 1, 10).is_err());
            let image = std::fs::read(&path).unwrap();
            for index in (0..end as usize).step_by(13) {
                let mut damaged = image.clone();
                damaged[index] ^= 1;
                std::fs::write(&path, &damaged).unwrap();
                let mut at = 0;
                let outcome = loop {
                    match journal.scrub_step(at, 500) {
                        Ok(step) if step.done => break Ok(()),
                        Ok(step) => at = step.next,
                        Err(error) => break Err(error),
                    }
                };
                assert!(outcome.is_err(), "zero={zero}: damage at byte {index} went unnoticed");
            }
            drop(journal);
            remove(&path);
        }
    }

    #[test]
    fn the_original_layout_is_unchanged_and_keeps_its_layout_when_the_switch_turns_on() {
        let path = path("legacy");
        let mut state = service();
        let mut journal = open(&path, &mut state, false).unwrap();
        append(&mut journal, &mut state, 1);
        let length = std::fs::metadata(&path).unwrap().len();
        assert_eq!(journal.logical_len(), length);
        drop(journal);
        let mut recovered = service();
        let mut journal = open(&path, &mut recovered, true).unwrap();
        append(&mut journal, &mut recovered, 2);
        assert_eq!(journal.logical_len(), std::fs::metadata(&path).unwrap().len());
        drop(journal);
        assert_eq!(&std::fs::read(&path).unwrap()[..4], b"MOR2");
        assert_ne!(FRAME_END, 0);
        remove(&path);
    }
}
