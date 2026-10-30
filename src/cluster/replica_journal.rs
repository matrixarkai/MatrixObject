// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::message::ReplicaRequest;
use super::wire::{RequestCode, MAX_PAYLOAD_BYTES};
use crate::{fnv1a64, MatrixReplicaService};

const HEADER_LEN: usize = 24;
const MAGIC: &[u8; 4] = b"MOR1";

pub(super) struct ReplicaJournal {
    file: File,
    // Stable across atomic journal replacement; retain the journal lock too.
    _lock: Option<File>,
    path: PathBuf,
    bytes_since_checkpoint: u64,
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

impl ReplicaJournal {
    #[cfg(test)]
    pub(super) fn open(path: &Path, service: &mut MatrixReplicaService) -> io::Result<Self> {
        Self::open_with_limits(path, service, usize::MAX)
    }

    pub(super) fn open_with_limits(
        path: &Path,
        service: &mut MatrixReplicaService,
        max_pages: usize,
    ) -> io::Result<Self> {
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
        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < HEADER_LEN as u64 {
                let mut tail = vec![0; remaining as usize];
                file.read_exact(&mut tail)?;
                let prefix = tail.len().min(MAGIC.len());
                if tail[..prefix] != MAGIC[..prefix] && tail[..prefix] != b"MOR2"[..prefix] {
                    return Err(invalid("replica journal has a damaged final header"));
                }
                break;
            }
            let mut header = [0; HEADER_LEN];
            file.read_exact(&mut header)?;
            if (&header[..4] != MAGIC && &header[..4] != b"MOR2")
                || fnv1a64(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
            {
                return Err(invalid("replica journal header checksum mismatch"));
            }
            let length = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            if length == 0 || length > MAX_PAYLOAD_BYTES + 1 {
                return Err(invalid("replica journal record exceeds its length bound"));
            }
            if remaining - (HEADER_LEN as u64) < length as u64 {
                break;
            }
            let mut payload = vec![0; length];
            file.read_exact(&mut payload)?;
            if fnv1a64(&payload) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                return Err(invalid("replica journal payload checksum mismatch"));
            }
            if &header[..4] == b"MOR2" {
                crate::outcome::PageOutcome::from_payload(&payload)
                    .map_err(invalid)?
                    .apply(service, max_pages)
                    .map_err(invalid)?;
            } else {
                let code = RequestCode::from_u8(payload[0])
                    .ok_or_else(|| invalid("unknown replica journal operation"))?;
                if !matches!(code, RequestCode::WriteChunk | RequestCode::DiscardChunk) {
                    return Err(invalid("replica journal contains a non-mutating operation"));
                }
                let request = ReplicaRequest::decode(code, &payload[1..]).map_err(invalid)?;
                super::server::apply_to_replica(service, request, max_pages).map_err(invalid)?;
            }
            offset += HEADER_LEN as u64 + length as u64;
        }
        if initialized && offset == 0 {
            return Err(invalid("initialized replica journal lost its first record"));
        }
        if offset != file_len {
            // Only a trailing incomplete record is discarded; complete damaged records
            // are errors. The shortened file is durable before new appends begin.
            file.set_len(offset)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        let journal = Self {
            file,
            _lock: Some(lock),
            path,
            bytes_since_checkpoint: offset - base_end,
        };
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

    pub(super) fn encode_outcome(outcome: &crate::outcome::PageOutcome) -> io::Result<Vec<u8>> {
        let payload = outcome.to_payload().map_err(invalid)?;
        let mut record = b"MOR2".to_vec();
        record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        record.extend_from_slice(&fnv1a64(&record).to_be_bytes());
        record.extend_from_slice(&fnv1a64(&payload).to_be_bytes());
        record.extend_from_slice(&payload);
        Ok(record)
    }

    pub(super) fn append(&mut self, record: &[u8]) -> io::Result<()> {
        self.file.write_all(record)?;
        self.file.sync_all()?;
        self.mark_initialized()?;
        self.bytes_since_checkpoint = self
            .bytes_since_checkpoint
            .checked_add(record.len() as u64)
            .ok_or_else(|| invalid("journal length overflow"))?;
        Ok(())
    }

    fn mark_initialized(&self) -> io::Result<()> {
        if let Some(lock) = &self._lock {
            if lock.metadata()?.len() == 0 {
                let mut marker = lock;
                marker.write_all(b"1")?;
                lock.sync_all()?;
            }
        }
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
            journal.append(&second).unwrap();
            drop(journal);
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
        journal
            .append(&ReplicaJournal::encode_outcome(&outcome).unwrap())
            .unwrap();
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
