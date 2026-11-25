// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::log_page_outcome::*;
use crate::{ChunkId, PageId, PageState};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "log-page-outcomes-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("outcomes.data")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn initialized() -> LogPageState {
    LogPageState {
        generation: 3,
        writer_epoch: 7,
        initialization: vec![8; 16],
        storage_incarnation: vec![9; 16],
        length: 0,
        published_length: 0,
        limit: 64 * 1024 * 1024,
        page_bytes: 4096,
        crc32c: 0,
        sealed: false,
        seal_sha256: Vec::new(),
    }
}
fn initialize() -> LogPageOutcome {
    LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_INITIALIZE,
        before: None,
        after: Some(initialized()),
        append: None,
    }
}
fn append(data: &[u8]) -> LogPageOutcome {
    let mut after = initialized();
    after.length = data.len() as u64;
    after.published_length = after.length;
    after.crc32c = crate::log_chunk::crc32c(data);
    LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_APPEND,
        before: Some(initialized()),
        after: Some(after),
        append: Some(LogPageAppendIdentity {
            attempt: vec![17; 16],
            offset: 0,
            length: data.len() as u32,
            crc32c: crate::log_chunk::crc32c(data),
        }),
    }
}
fn pages(data: &[u8]) -> Vec<PageOutcome> {
    data.chunks(4096)
        .enumerate()
        .map(|(index, bytes)| {
            let state = PageState {
                logical_length: bytes.len() as u64,
                write_sequence: 1,
                checksum_fnv1a64: crate::fnv1a64(bytes),
                data: Arc::new(bytes.to_vec()),
                ec_parity_fragment: None,
                ec_fragment_digests: None,
                discarded_ranges: Vec::new(),
            };
            let id = PageId {
                chunk_id: ChunkId::new(1, 2).unwrap(),
                page_index: index as u64,
            };
            PageOutcome::between(&id, None, &state).unwrap()
        })
        .collect()
}
fn mutations<'a>(pages: &'a [PageOutcome], log: &'a LogPageOutcome) -> Vec<DecidedMutation<'a>> {
    pages
        .iter()
        .map(DecidedMutation::Page)
        .chain(std::iter::once(DecidedMutation::LogPage(log)))
        .collect()
}
#[test]
fn log_page_batch_crosses_pages_with_one_sync_and_reopens_exact_identity() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 1024 * 1024).unwrap();
    let init = initialize();
    log.append_decided_batch(&[DecidedMutation::LogPage(&init)])
        .unwrap();
    let data = vec![23; 4096 + 113];
    let pages = pages(&data);
    let outcome = append(&data);
    let mut syncs = 0;
    let record = log
        .append_with_sync(&mutations(&pages, &outcome), |file| {
            syncs += 1;
            file.sync_data()
        })
        .unwrap();
    assert_eq!(syncs, 1);
    assert_eq!(record.batch.version, 4);
    assert_eq!(
        record.batch.payloads.iter().map(|p| p.length).sum::<u64>(),
        data.len() as u64
    );
    assert_eq!(
        record.batch.decisions.last().unwrap().patch_lengths.len(),
        0
    );
    drop(log);
    let log = DurableOutcomeLog::open(dir.path(), 1024 * 1024).unwrap();
    let recovered = log.read_outcomes(&record.position).unwrap();
    assert_eq!(
        recovered,
        pages
            .into_iter()
            .map(RecoveredMutation::Page)
            .chain(std::iter::once(RecoveredMutation::LogPage(outcome.clone())))
            .collect::<Vec<_>>()
    );
    outcome.check_before(Some(&initialized())).unwrap();
    assert!(outcome.check_before(None).is_err());
    let mut service = MatrixReplicaService::new();
    assert!(log
        .replay_from(SECTOR_BYTES as u64, &mut service, 100)
        .is_err());
    assert!(matches!(
        service.read_chunk_state(&PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0
        }),
        Err(ObjectError::NotFound(_))
    ));
}
#[test]
fn log_page_batch_refuses_missing_overlapping_and_foreign_page_coverage_before_io() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 1024 * 1024).unwrap();
    let data = vec![23; 4209];
    let valid = pages(&data);
    let outcome = append(&data);
    let before = log.length();
    let mut duplicate = valid.clone();
    duplicate[1].page_index = 0;
    let mut foreign = valid.clone();
    foreign[1].chunk_id += 1;
    let mut hole = valid.clone();
    hole[1].patches[0].offset += 1;
    hole[1].data_length += 1;
    hole[1].logical_length += 1;
    for pages in [vec![], valid[..1].to_vec(), duplicate, foreign, hole] {
        assert!(log
            .append_decided_batch(&mutations(&pages, &outcome))
            .is_err());
        assert_eq!(log.length(), before);
    }
    log.append_decided_batch(&mutations(&valid, &outcome))
        .unwrap();
}
#[test]
fn log_page_batch_refuses_wrong_payload_or_prefix_crc_without_poisoning_writer() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 1024 * 1024).unwrap();
    let data = vec![23; 64];
    let pages = pages(&data);
    let valid = append(&data);
    let before = log.length();
    for prefix in [false, true] {
        let mut wrong = valid.clone();
        if prefix {
            wrong.after.as_mut().unwrap().crc32c ^= 1;
        } else {
            wrong.append.as_mut().unwrap().crc32c ^= 1;
        }
        assert!(log
            .append_decided_batch(&mutations(&pages, &wrong))
            .is_err());
        assert_eq!(log.length(), before);
    }
    log.append_decided_batch(&mutations(&pages, &valid))
        .unwrap();
}
#[test]
fn log_page_batch_uncertain_sync_reopens_both_pages_and_log_transition() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 1024 * 1024).unwrap();
    let data = vec![23; 4209];
    let pages = pages(&data);
    let outcome = append(&data);
    log.append_decided_batch(&[DecidedMutation::LogPage(&initialize())])
        .unwrap();
    let start = log.length();
    assert!(log
        .append_with_sync(&mutations(&pages, &outcome), |_| Err(io::Error::other(
            "injected sync failure"
        )))
        .is_err());
    assert!(log.healthy().is_err());
    drop(log);
    let reopened = DurableOutcomeLog::open(dir.path(), 1024 * 1024).unwrap();
    let record = reopened.next_metadata(start).unwrap().unwrap();
    let recovered = reopened.read_outcomes(&record.position).unwrap();
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered.last(), Some(&RecoveredMutation::LogPage(outcome)));
}
#[test]
fn log_page_codec_rejects_noncanonical_and_changed_storage_transitions() {
    let data = vec![23; 64];
    let valid = append(&data);
    let encoded = valid.to_payload().unwrap();
    assert_eq!(LogPageOutcome::from_payload(&encoded).unwrap(), valid);
    let mut unknown = encoded;
    unknown.extend_from_slice(&[0x78, 1]);
    assert!(LogPageOutcome::from_payload(&unknown).is_err());
    for field in 0..5 {
        let mut wrong = valid.clone();
        let state = wrong.after.as_mut().unwrap();
        match field {
            0 => state.generation += 1,
            1 => state.writer_epoch += 1,
            2 => state.initialization[0] ^= 1,
            3 => state.storage_incarnation[0] ^= 1,
            _ => state.page_bytes *= 2,
        }
        assert!(wrong.to_payload().is_err());
    }
}
#[test]
fn log_page_codec_fence_preserves_publication_and_seal_refuses_incomplete_tail() {
    let data = vec![23; 64];
    let written = append(&data).after.unwrap();
    let mut after = written.clone();
    after.writer_epoch += 1;
    let mut fence = LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_FENCE,
        before: Some(written.clone()),
        after: Some(after),
        append: None,
    };
    fence.validate().unwrap();
    fence.after.as_mut().unwrap().length -= 1;
    assert!(fence.validate().is_err());
    let mut sealed = written.clone();
    sealed.sealed = true;
    let mut seal = LogPageOutcome {
        kind: LOG_PAGE_SEAL,
        before: Some(written),
        after: Some(sealed),
        ..fence
    };
    seal.validate().unwrap();
    seal.before.as_mut().unwrap().published_length -= 1;
    assert!(seal.validate().is_err());
}

#[test]
fn log_page_fence_can_drop_only_the_incomplete_suffix_in_the_same_epoch() {
    let prefix = b"published";
    let data = b"published-incomplete";
    let mut before = append(data).after.unwrap();
    before.published_length = prefix.len() as u64;
    let mut after = before.clone();
    after.length = before.published_length;
    after.crc32c = crate::log_chunk::crc32c(prefix);
    let mut fence = LogPageOutcome {
        version: 1,
        volume_id: 1,
        chunk_id: 2,
        kind: LOG_PAGE_FENCE,
        before: Some(before),
        after: Some(after),
        append: None,
    };
    fence.validate().unwrap();
    fence.after.as_mut().unwrap().writer_epoch -= 1;
    assert!(fence.validate().is_err());
}

#[test]
fn segmented_log_page_replay_refuses_before_applying_page_mutations() {
    let dir = Directory::new();
    let log = crate::durable_payload::outcome_segments::SegmentedOutcomeLog::open(
        dir.path(),
        32 * 1024 * 1024,
    )
    .unwrap();
    let data = vec![23; 4209];
    let pages = pages(&data);
    let outcome = append(&data);
    log.append_decided_batch(&mutations(&pages, &outcome))
        .unwrap();
    let mut service = MatrixReplicaService::new();
    assert!(log.replay_after(None, &mut service, 100).is_err());
    assert!(matches!(
        service.read_chunk_state(&PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0
        }),
        Err(ObjectError::NotFound(_))
    ));
}
