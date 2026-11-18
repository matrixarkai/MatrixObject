// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "checkpoint-fragments-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> PathBuf {
        self.0.join("stored")
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn fragment(kind: u32, ordinal: u64, byte: u8, length: usize) -> CheckpointFragment {
    CheckpointFragment {
        version: 1,
        checkpoint_id: vec![13; 16],
        kind,
        ordinal,
        data: vec![byte; length],
    }
}

#[test]
fn stored_checkpoint_fragments_preserve_packed_payload_and_canonical_identity() {
    let dir = Dir::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let a = fragment(CheckpointFragment::PAYLOAD, 0, 7, 1024);
    let b = fragment(CheckpointFragment::CATALOG, 0, 9, 512);
    let record = log
        .append_decided_batch(&[
            DecidedMutation::CheckpointFragment(&a),
            DecidedMutation::CheckpointFragment(&b),
        ])
        .unwrap();
    assert_eq!(record.batch.version, 2);
    assert_eq!(record.batch.payloads.len(), 1);
    assert!(record.batch.decisions.iter().all(|d| d
        .checkpoint_fragment
        .as_ref()
        .unwrap()
        .data
        .is_empty()));
    assert_eq!(
        log.read_outcomes(&record.position).unwrap(),
        vec![
            RecoveredMutation::CheckpointFragment(a.clone()),
            RecoveredMutation::CheckpointFragment(b.clone())
        ]
    );
    let cross = log
        .read_payload_range(&record.batch.payloads[0], 1020, 12)
        .unwrap();
    assert_eq!(cross, [vec![7; 4], vec![9; 8]].concat());
    let mut service = MatrixReplicaService::new();
    assert!(RecoveredMutation::CheckpointFragment(a)
        .apply(&mut service, 100)
        .is_err());
    assert_eq!(service.page_count(), 0);
    assert_eq!(crate::outcome::PageOutcome::counters(&service), [0; 4]);
    drop(log);
    let reopened = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    assert_eq!(
        reopened.read_outcomes(&record.position).unwrap()[1],
        RecoveredMutation::CheckpointFragment(b)
    );
}

#[test]
fn stored_checkpoint_fragments_refuse_mixed_state_batches_without_writing() {
    let dir = Dir::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let root = fragment(CheckpointFragment::CATALOG, 0, 11, 512);
    let before = log.length();
    let state = crate::PageState {
        logical_length: 16,
        write_sequence: 1,
        checksum_fnv1a64: crate::fnv1a64(&[7; 16]),
        data: std::sync::Arc::new(vec![7; 16]),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    };
    let page = PageOutcome::between(
        &crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        },
        None,
        &state,
    )
    .unwrap();
    assert!(log
        .append_decided_batch(&[
            DecidedMutation::CheckpointFragment(&root),
            DecidedMutation::Page(&page)
        ])
        .is_err());
    assert_eq!(log.length(), before);
    assert!(log
        .append_decided_batch(&[
            DecidedMutation::Page(&page),
            DecidedMutation::CheckpointFragment(&root)
        ])
        .is_err());
    assert_eq!(log.length(), before);
    let ordinary = log.append_page(&page).unwrap();
    assert_eq!(ordinary.batch.version, 1);
    assert!(ordinary.batch.decisions[0].checkpoint_fragment.is_none());
}

#[test]
fn stored_checkpoint_fragments_bound_payloads_and_refuse_documents_and_unknown_fields() {
    let good = fragment(
        CheckpointFragment::PAYLOAD,
        u64::MAX,
        29,
        CheckpointFragment::MAX_DATA_BYTES,
    );
    let bytes = good.to_payload().unwrap();
    assert_eq!(CheckpointFragment::from_payload(&bytes).unwrap(), good);
    let mut invalid_id = fragment(CheckpointFragment::PAYLOAD, 0, 1, 1);
    invalid_id.checkpoint_id.pop();
    assert!(invalid_id.to_payload().is_err());
    let mut unknown = bytes.clone();
    unknown.extend_from_slice(&[0x30, 1]);
    assert!(CheckpointFragment::from_payload(&unknown).is_err());
    assert!(CheckpointFragment::from_payload(br#"{"data":[1,2,3]}"#).is_err());
    for mut value in [
        fragment(4, 0, 1, 1),
        fragment(1, 0, 1, 0),
        fragment(2, 0, 1, CheckpointFragment::MAX_DATA_BYTES + 1),
    ] {
        assert!(value.to_payload().is_err());
        value.checkpoint_id.clear();
        assert!(value.to_payload().is_err());
    }
}

#[test]
fn stored_checkpoint_fragments_verify_logical_binding_and_fail_closed_on_sync_errors() {
    let dir = Dir::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let value = fragment(CheckpointFragment::CATALOG, 8, 5, 2048);
    let stored = log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&value)])
        .unwrap();
    let mut changed = stored.batch.clone();
    changed.decisions[0]
        .checkpoint_fragment
        .as_mut()
        .unwrap()
        .ordinal += 1;
    assert!(log.hydrate(&changed).is_err());
    let mut downgraded = stored.batch.clone();
    downgraded.version = 1;
    let header = (stored.batch.payloads[0].offset - stored.position.offset) as usize;
    assert!(log
        .validate_batch(
            &downgraded,
            stored.position.offset,
            header,
            stored.position.offset + stored.position.length
        )
        .is_err());
    assert!(log
        .append_with_sync(&[DecidedMutation::CheckpointFragment(&value)], |_| Err(
            io::Error::other("injected sync error")
        ))
        .is_err());
    assert!(log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&value)])
        .is_err());
    drop(log);
    let recovered = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    assert_eq!(
        recovered.read_outcomes(&stored.position).unwrap(),
        vec![RecoveredMutation::CheckpointFragment(value)]
    );
}

#[test]
fn stored_checkpoint_fragments_survive_segment_rollover_and_reopen() {
    let dir = Dir::new();
    let log =
        super::super::outcome_segments::SegmentedOutcomeLog::open(dir.path(), 24 * 1024 * 1024)
            .unwrap();
    let mut positions = Vec::new();
    for index in 0..28 {
        let value = fragment(CheckpointFragment::PAYLOAD, index, index as u8, 1024 * 1024);
        positions.push(
            log.append_decided_batch(&[DecidedMutation::CheckpointFragment(&value)])
                .unwrap()
                .position,
        );
    }
    assert_eq!(log.segment_count().unwrap(), 2);
    drop(log);
    let log =
        super::super::outcome_segments::SegmentedOutcomeLog::open(dir.path(), 24 * 1024 * 1024)
            .unwrap();
    for index in [0usize, 13, 27] {
        let expected = fragment(
            CheckpointFragment::PAYLOAD,
            index as u64,
            index as u8,
            1024 * 1024,
        );
        assert_eq!(
            log.read_outcomes(&positions[index]).unwrap(),
            vec![RecoveredMutation::CheckpointFragment(expected)]
        );
    }
}

#[test]
fn stored_replica_metadata_is_committed_with_data_and_requires_context_aware_replay() {
    let dir = Dir::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let state = crate::PageState {
        logical_length: 4096,
        write_sequence: 1,
        checksum_fnv1a64: crate::fnv1a64(&[23; 4096]),
        data: std::sync::Arc::new(vec![23; 4096]),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    };
    let page = PageOutcome::between(
        &crate::PageId {
            chunk_id: crate::ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        },
        None,
        &state,
    )
    .unwrap();
    let context = b"replica-metadata-proof";
    let mut syncs = 0;
    let record = log
        .append_with_sync_metadata(&[DecidedMutation::Page(&page)], Some(context), |file| {
            syncs += 1;
            file.sync_data()
        })
        .unwrap();
    assert_eq!(syncs, 1);
    assert_eq!(record.batch.version, 3);
    assert_eq!(record.batch.replica_metadata, context);
    assert!(log.read_outcomes(&record.position).is_err());
    assert_eq!(
        log.read_outcomes_with_metadata(&record.position).unwrap(),
        (
            vec![RecoveredMutation::Page(page.clone())],
            context.to_vec()
        )
    );
    let mut service = MatrixReplicaService::new();
    assert!(log
        .replay_from(SECTOR_BYTES as u64, &mut service, 100)
        .is_err());
    assert_eq!(service.page_count(), 0);
    let before = log.length();
    assert!(log
        .append_decided_batch_with_metadata(&[DecidedMutation::Page(&page)], &[])
        .is_err());
    assert!(log
        .append_decided_batch_with_metadata(
            &[DecidedMutation::Page(&page)],
            &vec![0; MAX_REPLICA_METADATA_BYTES + 1]
        )
        .is_err());
    let checkpoint = fragment(CheckpointFragment::CATALOG, 0, 1, 64);
    assert!(log
        .append_decided_batch_with_metadata(
            &[DecidedMutation::CheckpointFragment(&checkpoint)],
            context
        )
        .is_err());
    assert_eq!(log.length(), before);
    let mut bytes = std::fs::read(dir.path()).unwrap();
    let start = record.position.offset as usize + ENVELOPE;
    let at = bytes[start..]
        .windows(context.len())
        .position(|v| v == context)
        .unwrap()
        + start;
    bytes[at] ^= 1;
    std::fs::write(dir.path(), &bytes).unwrap();
    assert!(log.read_outcomes_with_metadata(&record.position).is_err());
}
