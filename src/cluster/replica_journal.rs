// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::message::ReplicaRequest;
use super::wire::{RequestCode, MAX_PAYLOAD_BYTES};
use crate::{fnv1a64, MatrixReplicaService};

const HEADER_LEN: usize = 24;
const MAGIC: &[u8; 4] = b"MOR1";

pub(super) struct ReplicaJournal {
    file: File,
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

impl ReplicaJournal {
    #[cfg(test)]
    pub(super) fn open(path: &Path, service: &mut MatrixReplicaService) -> io::Result<Self> {
        Self::open_with_limits(path, service, usize::MAX)
    }

    pub(super) fn open_with_limits(path: &Path, service: &mut MatrixReplicaService, max_pages: usize) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
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
        let mut offset = 0u64;
        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < HEADER_LEN as u64 {
                let mut tail = vec![0; remaining as usize];
                file.read_exact(&mut tail)?;
                let prefix = tail.len().min(MAGIC.len());
                if tail[..prefix] != MAGIC[..prefix] {
                    return Err(invalid("replica journal has a damaged final header"));
                }
                break;
            }
            let mut header = [0; HEADER_LEN];
            file.read_exact(&mut header)?;
            if &header[..4] != MAGIC
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
            let code = RequestCode::from_u8(payload[0])
                .ok_or_else(|| invalid("unknown replica journal operation"))?;
            if !matches!(code, RequestCode::WriteChunk | RequestCode::DiscardChunk) {
                return Err(invalid("replica journal contains a non-mutating operation"));
            }
            let request = ReplicaRequest::decode(code, &payload[1..]).map_err(invalid)?;
            super::server::apply_to_replica(service, request, max_pages).map_err(invalid)?;
            offset += HEADER_LEN as u64 + length as u64;
        }
        if offset != file_len {
            // Only a trailing incomplete record is discarded; complete damaged records
            // are errors. The shortened file is durable before new appends begin.
            file.set_len(offset)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file })
    }

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

    pub(super) fn append(&mut self, record: &[u8]) -> io::Result<()> {
        self.file.write_all(record)?;
        self.file.sync_all()
    }
}

#[cfg(test)]
impl ReplicaJournal {
    pub(super) fn read_only_for_test(path: &Path) -> Self {
        Self {
            file: File::open(path).unwrap(),
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
        std::fs::remove_file(path).unwrap();
    }
}
