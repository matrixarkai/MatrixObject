// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::{MatrixMetaService, MetaSnapshot};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct MetaSnapshotFile {
    directory: PathBuf,
    lock: File,
    journal: Option<super::placement_journal::PlacementJournal>,
}

impl MetaSnapshotFile {
    pub(super) fn open(
        directory: &Path,
        service: &mut MatrixMetaService,
    ) -> io::Result<(Self, bool)> {
        let directory = directory.canonicalize()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("metadata.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let mut storage = Self { directory, lock, journal: None };
        let recovered = match File::open(storage.directory.join("metadata.snapshot")) {
            Ok(file) => {
                if file.metadata()?.len() > MAX_SNAPSHOT_BYTES {
                    return Err(invalid("metadata snapshot exceeds 64 MiB"));
                }
                let mut bytes = Vec::new();
                file.take(MAX_SNAPSHOT_BYTES + 1).read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "metadata snapshot exceeds 64 MiB",
                    ));
                }
                let snapshot = MetaSnapshot::from_bytes(&bytes).map_err(invalid)?;
                // Metadata installation and placement restoration are distinct APIs.
                service
                    .install_placement_roster(&snapshot)
                    .map_err(invalid)?;
                service.install_snapshot(snapshot).map_err(invalid)?;
                true
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if storage.lock.metadata()?.len() != 0 {
                    return Err(invalid(
                        "initialized metadata directory is missing its snapshot",
                    ));
                }
                // Bootstrap is persisted by seed_data_nodes before the startup line.
                // Do not turn an interrupted first startup into a recovered empty roster.
                false
            }
            Err(err) => return Err(err),
        };
        let mut marker = Vec::with_capacity(2);
        File::open(storage.directory.join("metadata.lock"))?.take(2).read_to_end(&mut marker)?;
        let initialized = match marker.as_slice() {
            b"" | b"1" => false,
            b"2" => true,
            _ => return Err(invalid("invalid metadata storage marker")),
        };
        let path = storage.directory.join("metadata.outcomes");
        if !recovered && path.exists() && path.metadata()?.len() != 0 {
            return Err(invalid("placement outcomes exist without a base snapshot"));
        }
        storage.journal = Some(super::placement_journal::PlacementJournal::open(&path, service, initialized)?);
        if recovered {
            let mut lock = &storage.lock;
            lock.write_all(b"2")?;
            lock.sync_all()?;
        }
        Ok((storage, recovered))
    }

    pub(super) fn append_outcome(&mut self, outcome: &crate::placement_outcome::PlacementOutcome) -> io::Result<()> {
        self.journal.as_mut().ok_or_else(|| invalid("placement journal is not open"))?.append(outcome)
    }

    pub(super) fn persist(&self, service: &MatrixMetaService) -> io::Result<()> {
        let bytes = service.export_snapshot().to_bytes().map_err(invalid)?;
        if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(invalid("metadata snapshot exceeds 64 MiB"));
        }
        // The directory lock protects this fixed temporary name. A leftover temporary
        // file is an uncommitted snapshot and is overwritten by the next mutation.
        let temporary = self.directory.join("metadata.pending");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, self.directory.join("metadata.snapshot"))?;
        #[cfg(unix)]
        File::open(&self.directory)?.sync_all()?;
        // Record initialization only after a complete snapshot is durable. A missing
        // acknowledged snapshot must never be mistaken for a new empty directory.
        if self.lock.metadata()?.len() == 0 {
            let mut lock = &self.lock;
            lock.write_all(b"2")?;
            lock.sync_all()?;
        }
        Ok(())
    }
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
