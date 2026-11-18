// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::{
    chunk_namespace_fence::{ImmutablePage, NamespaceFence},
    ChunkId, DataRange, PageId, PageState,
};
use std::{
    fs::OpenOptions,
    io::Read,
    path::PathBuf,
    sync::{mpsc, Arc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "outcome-records-{}-{}",
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
fn page() -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 2).unwrap(),
        page_index: 0,
    }
}
fn state(data: Vec<u8>, sequence: u64) -> PageState {
    PageState {
        logical_length: data.len() as u64,
        write_sequence: sequence,
        checksum_fnv1a64: crate::fnv1a64(&data),
        data: Arc::new(data),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    }
}
fn decision(before: Option<&PageState>, after: &PageState) -> PageOutcome {
    PageOutcome::between(&page(), before, after).unwrap()
}
fn service() -> MatrixReplicaService {
    let mut value = MatrixReplicaService::new();
    value.set_keep_ec_sidecar(false);
    value
}
fn flip(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}
#[test]
fn stored_outcomes_share_one_sync_and_recover_identical_binary_decisions() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let a = state(b"base".to_vec(), 1);
    let b = state(b"base-tail".to_vec(), 2);
    let mut c = state(b"bXse-tail".to_vec(), 3);
    c.discarded_ranges = vec![DataRange {
        offset: 3,
        length: 1,
    }];
    let outcomes = [
        decision(None, &a),
        decision(Some(&a), &b),
        decision(Some(&b), &c),
    ];
    let mutations = outcomes
        .iter()
        .map(DecidedMutation::Page)
        .collect::<Vec<_>>();
    let mut syncs = 0;
    let record = log
        .append_with_sync(&mutations, |file| {
            syncs += 1;
            let mut flag = [0];
            read_at(file, &mut flag, SECTOR_BYTES as u64 + COMMIT_OFFSET)?;
            assert_eq!(flag, [READY]);
            assert_eq!(log.length(), SECTOR_BYTES as u64);
            file.sync_data()
        })
        .unwrap();
    assert_eq!(syncs, 1);
    assert_eq!(record.batch.payloads.len(), 1);
    assert!(record.batch.decisions.iter().all(|d| d
        .page
        .as_ref()
        .unwrap()
        .patches
        .iter()
        .all(|p| p.data.is_empty())));
    for (read, original) in log
        .read_outcomes(&record.position)
        .unwrap()
        .iter()
        .zip(&outcomes)
    {
        assert_eq!(read.to_payload().unwrap(), original.to_payload().unwrap());
    }
    let end = log.length();
    drop(log);
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    assert_eq!(log.length(), end);
    let mut recovered = service();
    assert_eq!(log.replay_from(4096, &mut recovered, 100).unwrap(), 3);
    assert_eq!(recovered.read_chunk_state(&page()).unwrap(), c);
}
#[test]
fn stored_outcomes_preserve_namespace_fences_in_order() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let after = state(b"immutable".to_vec(), 1);
    let outcome = decision(None, &after);
    let mut source = service();
    outcome.apply(&mut source, 100).unwrap();
    let fence = NamespaceFenceOutcome::decide(
        &source,
        NamespaceFence {
            version: 1,
            volume_id: 1,
            chunk_id: 2,
            generation: 1,
            pages: vec![ImmutablePage::from_state(0, &after).unwrap()],
        },
    )
    .unwrap();
    let record = log
        .append_decided_batch(&[
            DecidedMutation::Page(&outcome),
            DecidedMutation::NamespaceFence(&fence),
        ])
        .unwrap();
    let restored = log.read_outcomes(&record.position).unwrap();
    assert_eq!(
        restored[1].to_payload().unwrap(),
        fence.to_payload().unwrap()
    );
    let mut replay = service();
    for value in restored {
        value.apply(&mut replay, 100).unwrap();
    }
    assert!(NamespaceFenceOutcome::decide(&replay, fence.after.unwrap()).is_err());
}
#[test]
fn stored_outcomes_pack_sparse_patches_and_split_large_payloads() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let base = state(vec![0; 4096], 1);
    let after = state(
        (0..4096).map(|i| if i % 32 == 1 { 7 } else { 0 }).collect(),
        2,
    );
    let sparse = decision(Some(&base), &after);
    assert!(sparse.patches.len() > 100);
    let record = log.append_page(&sparse).unwrap();
    assert_eq!(record.batch.payloads.len(), 1);
    assert!(record.position.length < 256 * 1024);
    assert_eq!(
        log.read_outcomes(&record.position).unwrap()[0]
            .to_payload()
            .unwrap(),
        sparse.to_payload().unwrap()
    );
    let large = decision(None, &state(vec![0x31; 2 * MAX_EXTENT_BYTES + 17], 1));
    let record = log.append_page(&large).unwrap();
    assert_eq!(record.batch.payloads.len(), 3);
    assert_eq!(
        log.read_outcomes(&record.position).unwrap()[0]
            .to_payload()
            .unwrap(),
        large.to_payload().unwrap()
    );
}
#[test]
fn stored_outcomes_raw_and_record_profiles_refuse_each_other() {
    let dir = Directory::new();
    let raw = PayloadExtentStore::open(dir.path(), 64 * 1024 * 1024).unwrap();
    drop(raw);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    let other = Directory::new();
    let log = DurableOutcomeLog::open(other.path(), 64 * 1024 * 1024).unwrap();
    log.append_page(&decision(None, &state(b"a".to_vec(), 1)))
        .unwrap();
    drop(log);
    let before = std::fs::read(other.path()).unwrap();
    assert!(PayloadExtentStore::open(other.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(other.path()).unwrap(), before);
}
#[test]
fn stored_outcomes_metadata_reads_do_not_admit_corrupt_payloads() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let record = log
        .append_page(&decision(None, &state(vec![7; 4096], 1)))
        .unwrap();
    flip(&dir.path(), record.batch.payloads[0].offset + 9);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(log.read_metadata(&record.position).is_ok());
    assert!(log.read_outcomes(&record.position).is_err());
    drop(log);
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    assert!(log.replay_from(4096, &mut service(), 100).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
}
#[test]
fn stored_outcomes_refusals_do_not_write_and_io_failure_fences() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 4096).unwrap();
    let outcome = decision(None, &state(vec![8; 4096], 1));
    let before = std::fs::read(dir.path()).unwrap();
    assert!(log.append_page(&outcome).is_err());
    assert!(log.append_decided_batch(&[]).is_err());
    assert!(log
        .append_decided_batch(&vec![DecidedMutation::Page(&outcome); 65])
        .is_err());
    let mut bad = outcome.clone();
    bad.physical_deltas.clear();
    assert!(log.append_page(&bad).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    let other = Directory::new();
    let log = DurableOutcomeLog::open(other.path(), 64 * 1024 * 1024).unwrap();
    assert!(log
        .append_with_sync(&[DecidedMutation::Page(&outcome)], |_| Err(
            io::Error::other("injected sync failure")
        ))
        .is_err());
    assert_eq!(log.length(), 4096);
    assert!(log.append_page(&outcome).is_err());
    assert!(log.next_metadata(4096).is_err());
    drop(log);
    let log = DurableOutcomeLog::open(other.path(), 64 * 1024 * 1024).unwrap();
    let record = log.next_metadata(4096).unwrap().unwrap();
    assert_eq!(
        log.read_outcomes(&record.position).unwrap()[0]
            .to_payload()
            .unwrap(),
        outcome.to_payload().unwrap()
    );
}
#[test]
fn stored_outcomes_recover_named_partial_tail_and_refuse_committed_truncation() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let a = state(b"a".to_vec(), 1);
    let first = log.append_page(&decision(None, &a)).unwrap();
    let second = log
        .append_page(&decision(Some(&a), &state(b"ab".to_vec(), 2)))
        .unwrap();
    drop(log);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.path())
        .unwrap();
    file.seek(SeekFrom::Start(second.position.offset + COMMIT_OFFSET))
        .unwrap();
    file.write_all(&[0]).unwrap();
    file.set_len(second.position.offset + ENVELOPE as u64 + 17)
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    assert_eq!(log.length(), first.position.offset + first.position.length);
    let mut replay = service();
    assert_eq!(log.replay_from(4096, &mut replay, 100).unwrap(), 1);
    drop(log);
    let file = OpenOptions::new().write(true).open(dir.path()).unwrap();
    file.set_len(first.position.offset + ENVELOPE as u64 + 17)
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
}
#[test]
fn stored_outcomes_complete_metadata_damage_is_not_truncated() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let record = log
        .append_page(&decision(None, &state(b"kept".to_vec(), 1)))
        .unwrap();
    drop(log);
    flip(&dir.path(), record.position.offset + ENVELOPE as u64 + 3);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
}
#[test]
fn stored_outcomes_reads_remain_available_while_writer_waits_for_sync() {
    let dir = Directory::new();
    let log = Arc::new(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap());
    let a = state(b"old".to_vec(), 1);
    let first = log.append_page(&decision(None, &a)).unwrap();
    let outcome = decision(Some(&a), &state(b"old-new".to_vec(), 2));
    let (ready, started) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let writer = Arc::clone(&log);
    let worker = std::thread::spawn(move || {
        writer.append_with_sync(&[DecidedMutation::Page(&outcome)], |file| {
            ready.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
            file.sync_data()
        })
    });
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let reader = Arc::clone(&log);
    let (done, result) = mpsc::channel();
    let reader_thread =
        std::thread::spawn(move || done.send(reader.read_outcomes(&first.position)).unwrap());
    let read = result.recv_timeout(Duration::from_secs(3));
    release.send(()).unwrap();
    worker.join().unwrap().unwrap();
    reader_thread.join().unwrap();
    assert_eq!(
        read.unwrap().unwrap()[0].to_payload().unwrap(),
        decision(None, &a).to_payload().unwrap()
    );
}

#[test]
fn stored_outcomes_metadata_only_transition_has_no_payload_extent() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let a = state(b"visible".to_vec(), 1);
    log.append_page(&decision(None, &a)).unwrap();
    let mut b = a.clone();
    b.write_sequence = 2;
    b.discarded_ranges = vec![DataRange {
        offset: 1,
        length: 2,
    }];
    let outcome = decision(Some(&a), &b);
    assert!(outcome.patches.is_empty());
    let record = log.append_page(&outcome).unwrap();
    assert!(record.batch.payloads.is_empty());
    assert_eq!(record.position.length, 4096);
    let mut replay = service();
    assert_eq!(log.replay_from(4096, &mut replay, 100).unwrap(), 2);
    assert_eq!(replay.read_chunk_state(&page()).unwrap(), b);
}
struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn stored_outcomes_process_child() {
    let Some(directory) = std::env::var_os("MATRIXOBJECT_STORED_OUTCOME_CHILD_DIRECTORY") else {
        return;
    };
    let path = PathBuf::from(directory).join("outcomes.data");
    let log = DurableOutcomeLog::open(&path, 64 * 1024 * 1024).unwrap();
    let a = state(b"base".to_vec(), 1);
    log.append_page(&decision(None, &a)).unwrap();
    let next = decision(Some(&a), &state(b"base-tail".to_vec(), 2));
    let ready = || {
        println!("STORED_OUTCOME_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    };
    let mode = std::env::var("MATRIXOBJECT_STORED_OUTCOME_CHILD_MODE").unwrap();
    if mode == "committed" {
        log.append_page(&next).unwrap();
        ready();
    }
    let start = log.length();
    let _ = log.append_with_sync(&[DecidedMutation::Page(&next)], |file| {
        if mode == "partial" {
            let mut file = file;
            file.seek(SeekFrom::Start(start + COMMIT_OFFSET))?;
            file.write_all(&[0])?;
            file.set_len(start + ENVELOPE as u64 + 17)?;
            file.sync_all()?;
        } else if mode != "before_sync" {
            panic!("unknown child mode");
        }
        ready();
        Ok(())
    });
}
fn running_writer(mode: &str) -> (Directory, Process) {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let dir = Directory::new();
    let mut child = Process(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "durable_payload::outcome_log::tests::stored_outcomes_process_child",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MATRIXOBJECT_STORED_OUTCOME_CHILD_DIRECTORY", &dir.0)
            .env("MATRIXOBJECT_STORED_OUTCOME_CHILD_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) if line.contains("STORED_OUTCOME_READY") => {
                    let _ = send.send(true);
                    return;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let _ = send.send(false);
    });
    let ready = receive.recv_timeout(Duration::from_secs(20));
    if !matches!(ready, Ok(true)) {
        let _ = child.0.kill();
        let _ = child.0.wait();
        reader.join().unwrap();
        let mut error = String::new();
        child
            .0
            .stderr
            .take()
            .unwrap()
            .take(16384)
            .read_to_string(&mut error)
            .unwrap();
        panic!("stored outcome child failed: {ready:?}; {error}");
    }
    reader.join().unwrap();
    assert!(child.0.try_wait().unwrap().is_none());
    (dir, child)
}
fn killed_writer(mode: &str) -> Directory {
    let (dir, mut child) = running_writer(mode);
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    dir
}

#[test]
#[ignore = "process lifecycle check; run separately with --ignored --test-threads=1"]
fn stored_outcomes_three_process_copies_replay_after_kill() {
    let mut writers = (0..3)
        .map(|_| running_writer("committed"))
        .collect::<Vec<_>>();
    assert_eq!(
        writers
            .iter()
            .map(|(_, child)| child.0.id())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    for (_, child) in &mut writers {
        assert!(child.0.try_wait().unwrap().is_none());
    }
    for (_, child) in &mut writers {
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
    }
    let copies = writers.into_iter().map(|(dir, _)| dir).collect::<Vec<_>>();
    for dir in copies {
        let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
        let mut replay = service();
        assert_eq!(log.replay_from(4096, &mut replay, 100).unwrap(), 2);
        assert_eq!(
            &replay.read_chunk_state(&page()).unwrap().data[..],
            b"base-tail"
        );
    }
}
#[test]
#[ignore = "process lifecycle check; run separately with --ignored --test-threads=1"]
fn stored_outcomes_process_partial_tail_preserves_acknowledged_prefix() {
    let dir = killed_writer("partial");
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let mut replay = service();
    assert_eq!(log.replay_from(4096, &mut replay, 100).unwrap(), 1);
    assert_eq!(&replay.read_chunk_state(&page()).unwrap().data[..], b"base");
}
#[test]
#[ignore = "process lifecycle check; run separately with --ignored --test-threads=1"]
fn stored_outcomes_process_before_sync_can_recover_an_unconfirmed_decision() {
    let dir = killed_writer("before_sync");
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let mut replay = service();
    assert_eq!(log.replay_from(4096, &mut replay, 100).unwrap(), 2);
    assert_eq!(
        &replay.read_chunk_state(&page()).unwrap().data[..],
        b"base-tail"
    );
}

#[test]
fn stored_outcomes_binary_metadata_and_envelope_bytes_are_stable() {
    fn unhex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks_exact(2)
            .map(|bytes| u8::from_str_radix(std::str::from_utf8(bytes).unwrap(), 16).unwrap())
            .collect()
    }
    let fence = NamespaceFenceOutcome {
        version: 1,
        before_generation: 0,
        after: Some(NamespaceFence {
            version: 1,
            volume_id: 1,
            chunk_id: 2,
            generation: 1,
            pages: Vec::new(),
        }),
    };
    let original = fence.to_payload().unwrap();
    let body = StoredOutcomeBatch {
        version: 1,
        decisions: vec![StoredDecision {
            page: None,
            patch_lengths: Vec::new(),
            data_offset: 0,
            original_sha256: digest::digest(&digest::SHA256, &original).as_ref().to_vec(),
            original_length: original.len() as u32,
            namespace_fence: Some(fence),
            checkpoint_fragment: None,
        }],
        payloads: Vec::new(),
        replica_metadata: Vec::new(),
    }
    .encode_to_vec();
    assert_eq!(body,unhex("0801123222209e55fa24164376fa6ee06ba786e33aeb7dd3f3ce9089d8bfb2b289433258b1a52818320c08011a080801100118022001"));
    assert_eq!(
        digest::digest(
            &digest::SHA256,
            &envelope(&body, 4096, 4096, 4096, &[7; 16])
        )
        .as_ref(),
        unhex("7590600c6e6ecfb695c95c804017758d68bc3627ee60320fa1b2b91378deacfc")
    );
}
#[test]
fn stored_outcomes_full_batch_shares_payload_and_bounds_storage_overhead() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let source = decision(None, &state(vec![7; 4096], 1));
    let outcomes = (0..64)
        .map(|index| {
            let mut value = source.clone();
            value.page_index = index;
            value
        })
        .collect::<Vec<_>>();
    let mutations = outcomes
        .iter()
        .map(DecidedMutation::Page)
        .collect::<Vec<_>>();
    let mut syncs = 0;
    let record = log
        .append_with_sync(&mutations, |file| {
            syncs += 1;
            file.sync_data()
        })
        .unwrap();
    assert_eq!(syncs, 1);
    assert_eq!(record.batch.decisions.len(), 64);
    assert_eq!(record.batch.payloads.len(), 1);
    assert!(record.position.length < 320 * 1024);
    let mut replay = service();
    assert_eq!(log.replay_from(4096, &mut replay, 100).unwrap(), 64);
    assert_eq!(replay.page_count(), 64);
}
#[test]
fn stored_outcomes_missing_first_record_and_unknown_tail_are_refused() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let record = log
        .append_page(&decision(None, &state(b"a".to_vec(), 1)))
        .unwrap();
    drop(log);
    let mut file = OpenOptions::new().append(true).open(dir.path()).unwrap();
    file.write_all(b"MOEXREC1-short").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    assert_eq!(
        log.length(),
        record.position.offset + record.position.length
    );
    drop(log);
    let mut file = OpenOptions::new().append(true).open(dir.path()).unwrap();
    file.write_all(b"unknown").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    let file = OpenOptions::new().write(true).open(dir.path()).unwrap();
    file.set_len(4096).unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert!(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::metadata(dir.path()).unwrap().len(), 4096);
}
#[test]
fn stored_outcomes_preflight_rejects_excess_messages_and_unknown_fields() {
    let mut bytes = vec![8, 1];
    for _ in 0..65 {
        bytes.extend_from_slice(&[18, 0]);
    }
    assert!(preflight(&bytes).is_err());
    assert!(preflight(&[8, 1, 32, 1]).is_err());
    assert!(preflight(&[8, 1, 18, 255, 255, 255, 255, 15]).is_err());
}

#[test]
fn stored_outcomes_control_records_bind_file_identity_and_absolute_position() {
    fn fence(chunk: u64) -> NamespaceFenceOutcome {
        NamespaceFenceOutcome {
            version: 1,
            before_generation: 0,
            after: Some(NamespaceFence {
                version: 1,
                volume_id: 1,
                chunk_id: chunk,
                generation: 1,
                pages: Vec::new(),
            }),
        }
    }
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let a = log
        .append_decided_batch(&[DecidedMutation::NamespaceFence(&fence(2))])
        .unwrap();
    let b = log
        .append_decided_batch(&[DecidedMutation::NamespaceFence(&fence(3))])
        .unwrap();
    let mut moved = vec![0; b.position.length as usize];
    read_at(&log.store.file, &mut moved, b.position.offset).unwrap();
    drop(log);
    let mut file = OpenOptions::new().write(true).open(dir.path()).unwrap();
    file.seek(SeekFrom::Start(a.position.offset)).unwrap();
    file.write_all(&moved).unwrap();
    file.set_len(a.position.offset + b.position.length).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = std::fs::read(dir.path()).unwrap();
    assert!(DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    let source = Directory::new();
    let log = DurableOutcomeLog::open(source.path(), 64 * 1024 * 1024).unwrap();
    let record = log
        .append_decided_batch(&[DecidedMutation::NamespaceFence(&fence(2))])
        .unwrap();
    let mut bytes = vec![0; record.position.length as usize];
    read_at(&log.store.file, &mut bytes, record.position.offset).unwrap();
    drop(log);
    let target = Directory::new();
    let log = DurableOutcomeLog::open(target.path(), 64 * 1024 * 1024).unwrap();
    drop(log);
    let mut file = OpenOptions::new().append(true).open(target.path()).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let before = std::fs::read(target.path()).unwrap();
    assert!(DurableOutcomeLog::open(target.path(), 64 * 1024 * 1024).is_err());
    assert_eq!(std::fs::read(target.path()).unwrap(), before);
}

#[test]
fn stored_outcomes_profile_header_bounds_return_errors_without_panicking() {
    let dir = Directory::new();
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    drop(log);
    let mut bytes = std::fs::read(dir.path()).unwrap();
    bytes[8..12].copy_from_slice(&4052u32.to_be_bytes());
    bytes[44..4096].fill(0);
    let checksum = digest::digest(&digest::SHA256, &bytes[44..4096]);
    bytes[12..44].copy_from_slice(checksum.as_ref());
    std::fs::write(dir.path(), &bytes).unwrap();
    let result = std::panic::catch_unwind(|| DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024));
    assert!(result.is_ok());
    assert!(result.unwrap().is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), bytes);
}
#[test]
fn stored_outcomes_write_failure_and_poison_refuse_future_operations() {
    let dir = Directory::new();
    let mut log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let a = state(b"a".to_vec(), 1);
    let record = log.append_page(&decision(None, &a)).unwrap();
    log.store.file = OpenOptions::new().read(true).open(dir.path()).unwrap();
    let before = std::fs::read(dir.path()).unwrap();
    assert!(log
        .append_page(&decision(Some(&a), &state(b"ab".to_vec(), 2)))
        .is_err());
    assert!(log.read_metadata(&record.position).is_err());
    assert_eq!(std::fs::read(dir.path()).unwrap(), before);
    drop(log);
    let log = DurableOutcomeLog::open(dir.path(), 64 * 1024 * 1024).unwrap();
    let outcome = decision(Some(&a), &state(b"ab".to_vec(), 2));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        log.append_with_sync(&[DecidedMutation::Page(&outcome)], |_| {
            panic!("injected outcome writer panic")
        })
    }));
    assert!(result.is_err());
    assert!(log.read_metadata(&record.position).is_err());
    assert!(log.append_page(&outcome).is_err());
}
