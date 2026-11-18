// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::{ChunkId, PageId, PageState};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "segment-retirement-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("journal")
    }
    fn open(&self) -> SegmentedOutcomeLog {
        SegmentedOutcomeLog::open(self.path(), MIN_SEGMENT_BYTES).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn page(n: u64, size: usize) -> PageOutcome {
    let data = vec![n as u8 + 1; size];
    let state = PageState {
        logical_length: size as u64,
        write_sequence: 1,
        checksum_fnv1a64: crate::fnv1a64(&data),
        data: Arc::new(data),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    };
    PageOutcome::between(
        &PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: n,
        },
        None,
        &state,
    )
    .unwrap()
}

// Early sealing is valid: create a real chained manifest with small immutable
// segments so retirement/crash tests need kilobytes rather than gigabytes.
fn fixture(count: usize) -> (Directory, Vec<StoredRecordMetadata>) {
    let dir = Directory::new();
    let log = dir.open();
    let id = log.store_id().to_vec();
    drop(log);
    let manifest_path = sibling(&dir.path(), ".segments");
    let mut hash = read_manifest(&manifest_path, &id, MIN_SEGMENT_BYTES, None)
        .unwrap()
        .tail_hash;
    let mut records: Vec<StoredRecordMetadata> = Vec::new();
    for n in 0..count {
        let log = DurableOutcomeLog::open(segment_path(&dir.path(), n as u64), MIN_SEGMENT_BYTES)
            .unwrap();
        let record = log.append_page(&page(n as u64, 64)).unwrap();
        if n + 1 < count {
            log.seal().unwrap();
        }
        if n != 0 {
            let at = std::fs::metadata(&manifest_path).unwrap().len();
            let mut bytes = [0; LINK_BYTES];
            bytes[..8].copy_from_slice(LINK_MAGIC);
            bytes[8..16].copy_from_slice(&(n as u64).to_be_bytes());
            bytes[16..32].copy_from_slice(&id);
            bytes[32..48].copy_from_slice(log.store_id());
            let previous = &records[n - 1].position;
            bytes[48..56].copy_from_slice(&(previous.offset + previous.length).to_be_bytes());
            bytes[56..88].copy_from_slice(&hash);
            hash = digest::digest(&digest::SHA256, &bytes[..88])
                .as_ref()
                .try_into()
                .unwrap();
            bytes[88..120].copy_from_slice(&hash);
            publish_rollover(&manifest_path, &ManifestAppend { offset: at, bytes }).unwrap();
        }
        records.push(record);
    }
    (dir, records)
}
fn retain<'a>(
    log: &'a SegmentedOutcomeLog,
    records: &[StoredRecordMetadata],
) -> PreparedRetention<'a> {
    log.prepare_retention(
        &BTreeSet::from([records[0].position.store_id.clone()]),
        &records.last().unwrap().position,
        &|| Ok(()),
    )
    .unwrap()
    .unwrap()
}

#[test]
fn outcome_segments_retention_keeps_shared_payloads_skips_deleted_numbers_and_rolls_over() {
    let (dir, records) = fixture(4);
    let log = dir.open();
    let stats = retain(&log, &records).publish().unwrap();
    assert_eq!(stats.removed, 2);
    assert_eq!(stats.pending, 0);
    assert_eq!(log.retained_segment_count().unwrap(), 2);
    assert!(log.read_metadata(&records[1].position).is_err());
    assert_eq!(
        log.read_payload_range(&records[0].batch.payloads[0], 0, 64)
            .unwrap(),
        vec![1; 64]
    );
    let mut cursor = log.metadata_cursor_after(None).unwrap();
    assert_eq!(
        cursor.next_record().unwrap().unwrap().position,
        records[0].position
    );
    assert_eq!(
        cursor.next_record().unwrap().unwrap().position,
        records[3].position
    );
    assert!(cursor.next_record().unwrap().is_none());
    drop(cursor);
    let mut last = records[3].position.clone();
    let value = page(90, 3 * 1024 * 1024);
    for _ in 0..9 {
        last = log.append_page(&value).unwrap().position;
    }
    assert_ne!(last.store_id, records[3].position.store_id);
    assert!(segment_path(&dir.path(), 4).exists());
    assert!(!segment_path(&dir.path(), 1).exists());
    assert!(log.retirement_snapshot_needed().unwrap());
    log.prepare_retirement_snapshot(&|| Ok(()))
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    let id = log.store_id().to_vec();
    drop(log);
    let manifest = read_manifest(
        &sibling(&dir.path(), ".segments"),
        &id,
        MIN_SEGMENT_BYTES,
        None,
    )
    .unwrap();
    assert_eq!(manifest.metadata.version, 2);
    assert!(manifest.metadata.retired.is_empty());
    let log = dir.open();
    assert_eq!(log.read_metadata(&last).unwrap().position, last);
    assert!(log.read_metadata(&records[0].position).is_ok());
}

#[test]
fn outcome_segments_retention_reader_pins_refuse_publication_without_fencing() {
    let (dir, records) = fixture(4);
    let log = dir.open();
    let prepared = retain(&log, &records);
    let mut cursor = log.metadata_cursor_after(None).unwrap();
    assert_eq!(
        prepared.publish().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(log.segment_count().unwrap(), 4);
    for expected in &records {
        assert_eq!(
            cursor.next_record().unwrap().unwrap().position,
            expected.position
        );
    }
    assert_eq!(
        log.prepare_retention(&BTreeSet::new(), &records[3].position, &|| Ok(()))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(cursor);
    retain(&log, &records).publish().unwrap();
    assert!(log.append_page(&page(8, 64)).is_ok());
}

#[test]
fn outcome_segments_retention_interrupted_preparation_and_rollover_leave_old_manifest() {
    let (dir, records) = fixture(4);
    let log = dir.open();
    let original = std::fs::read(sibling(&dir.path(), ".segments")).unwrap();
    let calls = std::cell::Cell::new(0);
    let result = log.prepare_retention(
        &BTreeSet::from([records[0].position.store_id.clone()]),
        &records[3].position,
        &|| {
            calls.set(calls.get() + 1);
            if calls.get() >= 8 {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        },
    );
    assert_eq!(result.err().unwrap().kind(), io::ErrorKind::Interrupted);
    assert_eq!(
        std::fs::read(sibling(&dir.path(), ".segments")).unwrap(),
        original
    );
    let prepared = retain(&log, &records);
    let value = page(11, 3 * 1024 * 1024);
    for _ in 0..9 {
        log.append_page(&value).unwrap();
    }
    assert_eq!(
        prepared.publish().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(log.segment_count().unwrap(), 5);
    assert!(log.read_metadata(&records[1].position).is_ok());
    assert!(!sibling(&dir.path(), ".segments.retaining").exists());
}

#[test]
fn outcome_segments_retention_uncertain_publication_fences_and_restarts_before_cleanup() {
    for after_rename in [false, true] {
        let (dir, records) = fixture(4);
        let log = dir.open();
        let result = retain(&log, &records).publish_with(|source, destination| {
            if after_rename {
                std::fs::rename(source, destination)?;
                directory_sync(destination)?;
            }
            Err(io::Error::other("injected publication failure"))
        });
        assert!(result.is_err());
        assert!(log.append_page(&page(8, 64)).is_err());
        assert!(segment_path(&dir.path(), 1).exists());
        drop(log);
        let log = dir.open();
        assert_eq!(
            log.retained_segment_count().unwrap(),
            if after_rename { 2 } else { 4 }
        );
        assert!(log.read_metadata(&records[0].position).is_ok());
        assert!(log.read_metadata(&records[3].position).is_ok());
        assert_eq!(segment_path(&dir.path(), 1).exists(), !after_rename);
    }
}

#[test]
fn outcome_segments_retention_retries_marker_only_cleanup_and_quarantines_foreign_identity() {
    for foreign in [false, true] {
        let (dir, records) = fixture(4);
        let log = dir.open();
        retain(&log, &records)
            .publish_with(|source, destination| {
                std::fs::rename(source, destination)?;
                directory_sync(destination)?;
                Err(io::Error::other("interrupted before unlink"))
            })
            .unwrap_err();
        drop(log);
        let path = segment_path(&dir.path(), 1);
        let marker = super::super::super::sibling(&path);
        if foreign {
            let mut bytes = std::fs::read(&marker).unwrap();
            bytes[8..24].fill(0x55);
            let hash = digest::digest(&digest::SHA256, &bytes[..24]);
            bytes[24..56].copy_from_slice(hash.as_ref());
            std::fs::write(&marker, bytes).unwrap();
        } else {
            std::fs::remove_file(&path).unwrap();
        }
        let log = dir.open();
        assert!(log.read_metadata(&records[0].position).is_ok());
        assert!(log.read_metadata(&records[3].position).is_ok());
        assert_eq!(log.retired_segment_count().unwrap(), usize::from(foreign));
        assert_eq!(marker.exists(), foreign);
        assert_eq!(path.exists(), foreign);
    }
}
