// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::{fnv1a64, placement_outcome::PlacementOutcome, MatrixMetaService};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};
const HEADER: usize = 24;
const MAGIC: &[u8; 4] = b"MOMJ";
const CATALOG_MAGIC: &[u8; 4] = b"MOCJ";
const LIMIT: usize = 8 * 1024 * 1024;
pub(super) struct PlacementJournal {
    file: File,
}
fn invalid(e: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}
impl PlacementJournal {
    #[cfg(test)]
    pub(super) fn open(
        path: &Path,
        service: &mut MatrixMetaService,
        initialized: bool,
    ) -> io::Result<Self> {
        Self::open_with_catalog(
            path,
            service,
            &super::ChunkGroupCatalog::default(),
            initialized,
        )
    }
    pub(super) fn open_with_catalog(
        path: &Path,
        service: &mut MatrixMetaService,
        catalog: &super::ChunkGroupCatalog,
        initialized: bool,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(!initialized)
            .truncate(false)
            .open(path)?;
        file.sync_all()?;
        #[cfg(unix)]
        File::open(path.parent().unwrap())?.sync_all()?;
        let length = file.metadata()?.len();
        let mut offset = 0;
        while offset < length {
            let remaining = length - offset;
            if remaining < HEADER as u64 {
                let mut tail = vec![0; remaining as usize];
                file.read_exact(&mut tail)?;
                let prefix = tail.len().min(4);
                if tail[..prefix] != MAGIC[..prefix] && tail[..prefix] != CATALOG_MAGIC[..prefix] {
                    return Err(invalid("damaged placement journal tail"));
                }
                break;
            }
            let mut header = [0; HEADER];
            file.read_exact(&mut header)?;
            if (&header[..4] != MAGIC && &header[..4] != CATALOG_MAGIC)
                || fnv1a64(&header[..8]) != u64::from_be_bytes(header[8..16].try_into().unwrap())
            {
                return Err(invalid("damaged placement journal header"));
            }
            let size = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            if size == 0 || size > LIMIT {
                return Err(invalid("placement journal record exceeds size limit"));
            }
            if remaining - (HEADER as u64) < size as u64 {
                break;
            }
            let mut bytes = vec![0; size];
            file.read_exact(&mut bytes)?;
            if fnv1a64(&bytes) != u64::from_be_bytes(header[16..24].try_into().unwrap()) {
                return Err(invalid("placement journal checksum mismatch"));
            }
            if &header[..4] == MAGIC {
                PlacementOutcome::from_payload(&bytes)
                    .map_err(invalid)?
                    .apply(service)
                    .map_err(invalid)?;
            } else {
                super::chunk_catalog_outcome::ChunkCatalogOutcome::from_payload(&bytes)
                    .map_err(invalid)?
                    .apply(service, catalog)
                    .map_err(invalid)?;
            }
            offset += HEADER as u64 + size as u64;
        }
        if offset != length {
            file.set_len(offset)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self { file })
    }
    pub(super) fn record(outcome: &PlacementOutcome) -> io::Result<Vec<u8>> {
        let bytes = outcome.to_payload().map_err(invalid)?;
        Self::encode_record(MAGIC, &bytes)
    }
    pub(super) fn catalog_record(
        outcome: &super::chunk_catalog_outcome::ChunkCatalogOutcome,
    ) -> io::Result<Vec<u8>> {
        Self::encode_record(CATALOG_MAGIC, &outcome.to_payload().map_err(invalid)?)
    }
    fn encode_record(magic: &[u8; 4], bytes: &[u8]) -> io::Result<Vec<u8>> {
        if bytes.is_empty() || bytes.len() > LIMIT {
            return Err(invalid("journal record exceeds size limit"));
        }
        let mut record = magic.to_vec();
        record.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        record.extend_from_slice(&fnv1a64(&record).to_be_bytes());
        record.extend_from_slice(&fnv1a64(&bytes).to_be_bytes());
        record.extend_from_slice(&bytes);
        Ok(record)
    }
    pub(super) fn append_catalog(
        &mut self,
        outcome: &super::chunk_catalog_outcome::ChunkCatalogOutcome,
    ) -> io::Result<()> {
        self.file.write_all(&Self::catalog_record(outcome)?)?;
        self.file.sync_all()
    }
    pub(super) fn append(&mut self, outcome: &PlacementOutcome) -> io::Result<()> {
        self.file.write_all(&Self::record(outcome)?)?;
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn placement_journal_rejects_corruption_and_recovers_every_torn_tail() {
        let base = MatrixMetaService::new().placement_commit_index;
        let first = PlacementOutcome {
            version: 1,
            before_index: base,
            after_index: base + 1,
            nodes: vec![],
            placements: vec![],
            manifest: None,
        };
        let second = PlacementOutcome {
            before_index: base + 1,
            after_index: base + 2,
            ..first.clone()
        };
        let a = PlacementJournal::record(&first).unwrap();
        let b = PlacementJournal::record(&second).unwrap();
        let path = std::env::temp_dir().join(format!("placement-journal-{}", std::process::id()));
        for cut in 0..b.len() {
            std::fs::write(&path, [a.as_slice(), &b[..cut]].concat()).unwrap();
            let mut service = MatrixMetaService::new();
            let mut journal = PlacementJournal::open(&path, &mut service, true).unwrap();
            assert_eq!(service.placement_commit_index, base + 1);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), a.len() as u64);
            journal.append(&second).unwrap();
            drop(journal);
            let mut service = MatrixMetaService::new();
            drop(PlacementJournal::open(&path, &mut service, true).unwrap());
            assert_eq!(service.placement_commit_index, base + 2);
        }
        for index in 0..b.len() {
            let mut damaged = b.clone();
            damaged[index] ^= 1;
            let bytes = [a.as_slice(), damaged.as_slice()].concat();
            std::fs::write(&path, &bytes).unwrap();
            assert!(PlacementJournal::open(&path, &mut MatrixMetaService::new(), true).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn manifest_journal_recovers_atomic_publication_and_torn_suffixes() {
        let (base, create, replace) = crate::tests::manifest_publication_fixture();
        let first = PlacementJournal::record(&create).unwrap();
        let second = PlacementJournal::record(&replace).unwrap();
        let path = std::env::temp_dir().join(format!("manifest-journal-{}", std::process::id()));
        let mut expected = base.clone();
        create.apply(&mut expected).unwrap();
        for cut in 0..second.len() {
            std::fs::write(&path, [first.as_slice(), &second[..cut]].concat()).unwrap();
            let mut recovered = base.clone();
            let mut journal = PlacementJournal::open(&path, &mut recovered, true).unwrap();
            assert_eq!(recovered.export_snapshot(), expected.export_snapshot());
            journal.append(&replace).unwrap();
            drop(journal);
            let mut recovered = base.clone();
            drop(PlacementJournal::open(&path, &mut recovered, true).unwrap());
            let mut complete = expected.clone();
            replace.apply(&mut complete).unwrap();
            assert_eq!(recovered.export_snapshot(), complete.export_snapshot());
        }
        std::fs::remove_file(path).unwrap();
    }
}
