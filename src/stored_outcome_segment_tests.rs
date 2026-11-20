// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::{ChunkId, PageId, PageState};
use std::{
    sync::mpsc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "outcome-segments-{}-{}",
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
        self.0.join("outcomes")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
static LARGE: Mutex<()> = Mutex::new(());
fn outcome(index: u64, bytes: usize) -> PageOutcome {
    let data = vec![0x31; bytes];
    let after = PageState {
        logical_length: bytes as u64,
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
            page_index: index,
        },
        None,
        &after,
    )
    .unwrap()
}
fn service() -> MatrixReplicaService {
    let mut value = MatrixReplicaService::new();
    value.set_keep_ec_sidecar(false);
    value
}
fn full(log: &SegmentedOutcomeLog) -> Vec<StoredOutcomePosition> {
    let mut value = outcome(0, 3 * 1024 * 1024);
    (0..7)
        .map(|n| {
            value.page_index = n;
            log.append_page(&value).unwrap().position
        })
        .collect()
}
#[test]
fn outcome_segments_checkpoint_cut_orders_owned_positions_and_refuses_invalid_bounds() {
    let _large = LARGE.lock().unwrap_or_else(|error| error.into_inner());
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let mut positions = full(&log);
    positions.push(
        log.append_page(&outcome(7, 3 * 1024 * 1024))
            .unwrap()
            .position,
    );
    let first = positions.first().unwrap();
    let last = positions.last().unwrap();
    assert_ne!(first.store_id, last.store_id);
    assert_eq!(log.checkpoint_cut(None, None).unwrap(), None);
    assert_eq!(
        log.checkpoint_cut(Some(first), None).unwrap().as_ref(),
        Some(first)
    );
    for (a, b) in [(first, last), (last, first), (last, last)] {
        assert_eq!(
            log.checkpoint_cut(Some(a), Some(b)).unwrap().as_ref(),
            Some(last)
        );
    }
    for change in 0..6 {
        let mut invalid = last.clone();
        match change {
            0 => invalid.version = 2,
            1 => invalid.store_id[0] ^= 1,
            2 => invalid.offset += 1,
            3 => invalid.length = 0,
            4 => invalid.offset += invalid.length,
            _ => invalid.offset = u64::MAX,
        }
        assert!(log.checkpoint_cut(Some(first), Some(&invalid)).is_err());
    }
    let mut conflicting = first.clone();
    conflicting.length += SECTOR_BYTES as u64;
    assert!(log.checkpoint_cut(Some(first), Some(&conflicting)).is_err());
    log.append_page(&outcome(8, 1)).unwrap();
}
#[test]
fn outcome_segments_one_sync_preserves_positions_and_binary_metadata() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(log.logical_length().unwrap(), 0);
    assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
    let value = outcome(0, 4096);
    let mut syncs = 0;
    let record = log
        .append_with(
            &[DecidedMutation::Page(&value)],
            |f| {
                syncs += 1;
                f.sync_data()
            },
            publish_rollover,
        )
        .unwrap();
    assert_eq!(syncs, 1);
    assert_eq!(log.segment_count().unwrap(), 1);
    assert_eq!(log.logical_length().unwrap(), record.position.length);
    assert!(record.batch.decisions[0].page.as_ref().unwrap().patches[0]
        .data
        .is_empty());
    assert_eq!(
        log.read_outcomes(&record.position).unwrap()[0]
            .to_payload()
            .unwrap(),
        value.to_payload().unwrap()
    );
    let id = log.store_id().to_vec();
    drop(log);
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(log.store_id(), id);
    assert_eq!(
        log.read_metadata(&record.position).unwrap().position,
        record.position
    );
    assert!(DurableOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
    assert!(PayloadExtentStore::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
}
#[test]
fn outcome_segments_rollover_replays_in_order_and_retains_old_ranges() {
    let _large = LARGE.lock().unwrap();
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let positions = full(&log);
    let before = log.logical_length().unwrap();
    let value = outcome(7, 3 * 1024 * 1024);
    let mut syncs = 0;
    let record = log
        .append_with(
            &[DecidedMutation::Page(&value)],
            |f| {
                syncs += 1;
                f.sync_data()
            },
            publish_rollover,
        )
        .unwrap();
    assert_eq!(syncs, 1);
    assert_ne!(positions[0].store_id, record.position.store_id);
    assert_eq!(log.segment_count().unwrap(), 2);
    assert_eq!(
        log.logical_length().unwrap(),
        before + record.position.length
    );
    let old = log.read_metadata(&positions[0]).unwrap();
    assert_eq!(
        log.read_payload_range(&old.batch.payloads[0], 0, 4096)
            .unwrap(),
        vec![0x31; 4096]
    );
    let mut expected = service();
    for pos in &positions {
        for value in log.read_outcomes(pos).unwrap() {
            value.apply(&mut expected, 100).unwrap();
        }
    }
    drop(log);
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(
        log.replay_after(Some(positions.last().unwrap()), &mut expected, 100)
            .unwrap(),
        1
    );
    let mut replay = service();
    assert_eq!(log.replay_after(None, &mut replay, 100).unwrap(), 8);
    assert_eq!(replay, expected);
    assert_eq!(
        replay
            .read_chunk_state(&value.page_id().unwrap())
            .unwrap()
            .data
            .len(),
        3 * 1024 * 1024
    );
}
#[test]
fn outcome_segments_cache_is_bounded_and_evicted_readers_keep_shared_locks() {
    let _large = LARGE.lock().unwrap();
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open_inner(&dir.path(), MIN_SEGMENT_BYTES, 1).unwrap();
    let initial = std::fs::read(sibling(&dir.path(), ".segments")).unwrap();
    let mut value = outcome(0, 3 * 1024 * 1024);
    let mut firsts = Vec::new();
    for n in 0..22 {
        value.page_index = n;
        let record = log.append_page(&value).unwrap();
        if firsts.last().map_or(true, |p: &StoredOutcomePosition| {
            p.store_id != record.position.store_id
        }) {
            firsts.push(record.position);
        }
    }
    assert_eq!(firsts.len(), 4);
    let manifest = std::fs::read(sibling(&dir.path(), ".segments")).unwrap();
    assert_eq!(&manifest[..initial.len()], initial);
    assert_eq!(manifest.len(), initial.len() + 3 * LINK_BYTES);
    for n in 0..3 {
        assert_eq!(
            &manifest[initial.len() + n * LINK_BYTES..initial.len() + n * LINK_BYTES + 8],
            LINK_MAGIC
        );
    }
    let retained = log.handle(&firsts[0].store_id).unwrap();

    for pos in &firsts[..3] {
        match log.read_outcomes(pos).unwrap().remove(0) {
            RecoveredMutation::Page(value) => {
                assert_eq!(value.patches[0].data.len(), 3 * 1024 * 1024)
            }
            _ => panic!("expected page outcome"),
        }
        assert!(log.index.read().unwrap().cache.len() <= 1);
    }
    assert_eq!(
        retained.read_metadata(&firsts[0]).unwrap().position,
        firsts[0]
    );
    assert_eq!(log.read_metadata(&firsts[0]).unwrap().position, firsts[0]);
}
#[test]
fn outcome_segments_old_reads_do_not_wait_for_rollover_sync() {
    let _large = LARGE.lock().unwrap();
    let dir = Directory::new();
    let log = Arc::new(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap());
    let positions = full(&log);
    let position = positions[0].clone();
    let retained = log.handle(&position.store_id).unwrap();
    let (ready, receive) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let writer = Arc::clone(&log);
    let task = std::thread::spawn(move || {
        writer.append_with(
            &[DecidedMutation::Page(&outcome(7, 3 * 1024 * 1024))],
            |f| {
                ready.send(()).unwrap();
                wait.recv().unwrap();
                f.sync_data()
            },
            publish_rollover,
        )
    });
    receive.recv_timeout(Duration::from_secs(20)).unwrap();
    let reader = Arc::clone(&log);
    let (send, recv) = mpsc::channel();
    let read = std::thread::spawn(move || {
        send.send(reader.read_outcomes(&position).map(|v| v.len()))
            .unwrap()
    });
    let result = recv.recv_timeout(Duration::from_secs(2));
    release.send(()).unwrap();
    task.join().unwrap().unwrap();
    read.join().unwrap();
    assert_eq!(result.unwrap().unwrap(), 1);
    assert_eq!(
        retained.read_metadata(&positions[0]).unwrap().position,
        positions[0]
    );
}
#[test]
fn outcome_segments_publication_failures_fence_and_recover_before_or_after_sync() {
    let _large = LARGE.lock().unwrap();
    for published in [false, true] {
        let dir = Directory::new();
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        full(&log);
        let result = log.append_with(
            &[DecidedMutation::Page(&outcome(7, 3 * 1024 * 1024))],
            File::sync_data,
            |path, manifest| {
                if published {
                    publish_rollover(path, manifest)?;
                }
                Err(io::Error::other("injected manifest publication failure"))
            },
        );
        assert!(result.is_err());
        assert!(log.logical_length().is_err());
        assert!(log.append_page(&outcome(8, 4)).is_err());
        drop(log);
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 7);
        log.append_page(&outcome(7, 3 * 1024 * 1024)).unwrap();
        assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 8);
    }
}
#[test]
fn outcome_segments_sync_failure_is_uncertain_and_fences_every_operation() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let value = outcome(0, 4096);
    assert!(log
        .append_with(
            &[DecidedMutation::Page(&value)],
            |_| Err(io::Error::other("injected record sync failure")),
            publish_rollover
        )
        .is_err());
    assert!(log.scan_metadata_after(None, |_| Ok(())).is_err());
    drop(log);
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 1);
}
#[test]
fn outcome_segments_invalid_batches_have_no_rollover_or_manifest_side_effect() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let original = std::fs::read(sibling(&dir.path(), ".segments")).unwrap();
    assert!(log.append_decided_batch(&[]).is_err());
    let value = outcome(0, 4096);
    assert!(log
        .append_decided_batch(&vec![DecidedMutation::Page(&value); 65])
        .is_err());
    assert_eq!(log.logical_length().unwrap(), 0);
    assert_eq!(log.segment_count().unwrap(), 1);
    assert_eq!(
        std::fs::read(sibling(&dir.path(), ".segments")).unwrap(),
        original
    );
    log.append_page(&value).unwrap();
    for cap in [0, 4096, MIN_SEGMENT_BYTES - 4096, MAX_CAPACITY_BYTES + 4096] {
        let path = dir.0.join(format!("invalid-{cap}"));
        assert!(SegmentedOutcomeLog::open(&path, cap).is_err());
        assert!(!path.exists());
    }
}
#[test]
fn outcome_segments_missing_manifest_and_active_identity_are_never_recreated() {
    for missing in [
        ".segments",
        ".segment-0000000000000000",
        ".segment-0000000000000000.lock",
    ] {
        let dir = Directory::new();
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        log.append_page(&outcome(0, 4096)).unwrap();
        drop(log);
        let path = sibling(&dir.path(), missing);
        std::fs::remove_file(&path).unwrap();
        assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
        assert!(!path.exists());
    }
}
#[test]
fn outcome_segments_sealed_damage_and_unlisted_data_are_refused_without_repair() {
    let _large = LARGE.lock().unwrap();
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    full(&log);
    log.append_page(&outcome(7, 3 * 1024 * 1024)).unwrap();
    drop(log);
    let first = segment_path(&dir.path(), 0);
    let original = std::fs::metadata(&first).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&first)
        .unwrap()
        .set_len(original - 1)
        .unwrap();
    assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
    assert_eq!(std::fs::metadata(&first).unwrap().len(), original - 1);
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    drop(log);
    let orphan = DurableOutcomeLog::open(segment_path(&dir.path(), 1), MIN_SEGMENT_BYTES).unwrap();
    orphan.append_page(&outcome(0, 4096)).unwrap();
    drop(orphan);
    let bytes = std::fs::read(segment_path(&dir.path(), 1)).unwrap();
    assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
    assert_eq!(std::fs::read(segment_path(&dir.path(), 1)).unwrap(), bytes);
}
#[test]
fn outcome_segments_only_active_named_partial_tails_can_be_removed() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let record = log.append_page(&outcome(0, 4096)).unwrap();
    drop(log);
    let path = segment_path(&dir.path(), 0);
    let end = std::fs::metadata(&path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"MOEX").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), end);
    assert_eq!(log.read_outcomes(&record.position).unwrap().len(), 1);
}
#[test]
fn outcome_segments_scan_cut_excludes_concurrent_appends() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let first = log.append_page(&outcome(0, 4096)).unwrap();
    let second = log.append_page(&outcome(1, 4096)).unwrap();
    let mut seen = Vec::new();
    log.scan_metadata_after(None, |record| {
        seen.push(record.position.clone());
        if seen.len() == 1 {
            log.append_page(&outcome(2, 4096))?;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, vec![first.position, second.position.clone()]);
    let mut count = 0;
    log.scan_metadata_after(Some(&second.position), |_| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 1);
    let mut foreign = second.position;
    foreign.store_id = vec![0; 16];
    assert!(log.read_metadata(&foreign).is_err());
}
#[test]
fn outcome_segments_manifest_encoding_and_identity_are_stable() {
    let dir = Directory::new();
    let path = dir.0.join("manifest");
    let manifest = SegmentManifest {
        retired: Vec::new(),
        version: 1,
        store_id: (0..16).collect(),
        capacity: MIN_SEGMENT_BYTES,
        entries: vec![SegmentEntry {
            number: 0,
            store_id: (16..32).collect(),
            sealed_length: 0,
        }],
    };
    publish_manifest(&path, &manifest).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let expected = vec![
        8, 1, 18, 16, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 24, 128, 128, 128, 12,
        34, 18, 18, 16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ];
    assert_eq!(&bytes[ENVELOPE..], expected);
    assert_eq!(&bytes[..8], b"MOSEGM01");
    assert_eq!(
        read_manifest(&path, &manifest.store_id, MIN_SEGMENT_BYTES, None)
            .unwrap()
            .metadata,
        manifest
    );
    assert!(read_manifest(&path, &[1; 16], MIN_SEGMENT_BYTES, None).is_err());
    let mut damaged = bytes.clone();
    damaged[12] ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    assert!(read_manifest(&path, &manifest.store_id, MIN_SEGMENT_BYTES, None).is_err());
    assert_eq!(std::fs::read(path).unwrap(), damaged);
}

#[test]
fn outcome_segments_poisoned_writer_refuses_reads_and_mutations() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        log.append_with(
            &[DecidedMutation::Page(&outcome(0, 4096))],
            |_| panic!("injected sync panic"),
            publish_rollover,
        )
    }));
    assert!(result.is_err());
    assert!(log.logical_length().is_err());
    assert!(log.append_page(&outcome(1, 4096)).is_err());
    drop(log);
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 1);
}
const CHILD_DIRECTORY: &str = "MATRIXOBJECT_SEGMENT_CHILD_DIRECTORY";
const CHILD_MODE: &str = "MATRIXOBJECT_SEGMENT_CHILD_MODE";
#[test]
fn outcome_segments_process_child() {
    let Some(path) = std::env::var_os(CHILD_DIRECTORY) else {
        return;
    };
    let mode = std::env::var(CHILD_MODE).unwrap();
    let log =
        SegmentedOutcomeLog::open(PathBuf::from(path).join("outcomes"), MIN_SEGMENT_BYTES).unwrap();
    full(&log);
    let signal = || {
        println!("\nREADY");
        std::io::stdout().flush().unwrap();
    };
    let forever = || -> ! {
        loop {
            std::thread::park();
        }
    };
    let value = outcome(7, 3 * 1024 * 1024);
    match mode.as_str() {
        "committed" => {
            log.append_page(&value).unwrap();
            signal();
            forever();
        }
        "before_manifest" => {
            let _ = log.append_with(&[DecidedMutation::Page(&value)], File::sync_data, |_, _| {
                signal();
                forever();
            });
        }
        "after_manifest" => {
            let _ = log.append_with(
                &[DecidedMutation::Page(&value)],
                File::sync_data,
                |path, manifest| {
                    publish_rollover(path, manifest)?;
                    signal();
                    forever();
                },
            );
        }

        "partial_manifest" => {
            let _ = log.append_with(
                &[DecidedMutation::Page(&value)],
                File::sync_data,
                |path, append| {
                    let mut file = OpenOptions::new().append(true).open(path)?;
                    file.write_all(&append.bytes[..64])?;
                    file.sync_all()?;
                    signal();
                    forever();
                },
            );
        }
        "before_sync" => {
            let _ = log.append_with(
                &[DecidedMutation::Page(&value)],
                |_| {
                    signal();
                    forever();
                },
                publish_rollover,
            );
        }
        _ => panic!("unknown segment child mode"),
    }
}
struct Process(std::process::Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn child(mode: &str) -> (Directory, Process) {
    use std::io::{BufRead, BufReader};
    let dir = Directory::new();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable_payload::outcome_segments::tests::outcome_segments_process_child",
            "--nocapture",
        ])
        .env(CHILD_DIRECTORY, &dir.0)
        .env(CHILD_MODE, mode)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut process = Process(child);
    let stdout = process.0.stdout.take().unwrap();
    let (send, recv) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let result = BufReader::new(stdout)
            .lines()
            .any(|line| line.is_ok_and(|line| line == "READY"));
        let _ = send.send(result);
    });
    let ready = recv.recv_timeout(Duration::from_secs(30));
    if !matches!(ready, Ok(true)) {
        let _ = process.0.kill();
        let _ = process.0.wait();
        reader.join().unwrap();
        panic!("child never reached durability boundary");
    }
    (dir, process)
}
#[test]
#[ignore = "process lifecycle check; run separately with --ignored --test-threads=1"]
fn outcome_segments_three_live_writer_processes_recover_after_kill() {
    let _large = LARGE.lock().unwrap();
    let mut children = (0..3).map(|_| child("committed")).collect::<Vec<_>>();
    let ids = children
        .iter()
        .map(|(_, p)| p.0.id())
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 3);
    for (_, process) in &mut children {
        assert!(process.0.try_wait().unwrap().is_none());
        process.0.kill().unwrap();
    }
    for (dir, mut process) in children {
        assert!(!process.0.wait().unwrap().success());
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        assert_eq!(log.segment_count().unwrap(), 2);
        assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 8);
    }
}
#[test]
#[ignore = "process lifecycle check; run separately with --ignored --test-threads=1"]
fn outcome_segments_process_kills_cover_manifest_and_record_publication() {
    let _large = LARGE.lock().unwrap();
    for mode in [
        "before_manifest",
        "after_manifest",
        "partial_manifest",
        "before_sync",
    ] {
        let (dir, mut process) = child(mode);
        assert!(process.0.try_wait().unwrap().is_none());
        process.0.kill().unwrap();
        assert!(!process.0.wait().unwrap().success());
        drop(process);
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        let expected = if mode == "before_sync" { 8 } else { 7 };
        assert_eq!(
            log.replay_after(None, &mut service(), 100).unwrap(),
            expected
        );
        log.append_page(&outcome(8, 4096)).unwrap();
        assert_eq!(
            log.replay_after(None, &mut service(), 100).unwrap(),
            expected + 1
        );
    }
}

#[test]
fn outcome_segments_pending_manifest_links_require_an_empty_initialized_child() {
    let _large = LARGE.lock().unwrap();
    for cut in [1, 64, 127, 128] {
        let dir = Directory::new();
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        full(&log);
        let manifest_path = sibling(&dir.path(), ".segments");
        let initial = std::fs::read(&manifest_path).unwrap();
        assert!(log
            .append_with(
                &[DecidedMutation::Page(&outcome(7, 3 * 1024 * 1024))],
                File::sync_data,
                |path, append| {
                    let mut file = OpenOptions::new().append(true).open(path)?;
                    file.write_all(&append.bytes[..cut])?;
                    file.sync_all()?;
                    Err(io::Error::other("injected manifest tail"))
                }
            )
            .is_err());
        drop(log);
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        let expected = if cut == 128 {
            initial.len() + LINK_BYTES
        } else {
            initial.len()
        };
        assert_eq!(
            std::fs::metadata(&manifest_path).unwrap().len(),
            expected as u64
        );
        assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 7);
        log.append_page(&outcome(7, 3 * 1024 * 1024)).unwrap();
        assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 8);
    }
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    full(&log);
    assert!(log
        .append_with(
            &[DecidedMutation::Page(&outcome(7, 3 * 1024 * 1024))],
            File::sync_data,
            |path, append| {
                let mut file = OpenOptions::new().append(true).open(path)?;
                file.write_all(&append.bytes[..64])?;
                file.sync_all()?;
                Err(io::Error::other("injected manifest tail"))
            }
        )
        .is_err());
    drop(log);
    let orphan = DurableOutcomeLog::open(segment_path(&dir.path(), 1), MIN_SEGMENT_BYTES).unwrap();
    orphan.append_page(&outcome(7, 4096)).unwrap();
    drop(orphan);
    let bytes = std::fs::read(sibling(&dir.path(), ".segments")).unwrap();
    assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
    assert_eq!(
        std::fs::read(sibling(&dir.path(), ".segments")).unwrap(),
        bytes
    );
}
#[test]
fn outcome_segments_chained_links_reject_damage_and_reordering() {
    let dir = Directory::new();
    let path = dir.0.join("manifest");
    let manifest = SegmentManifest {
        retired: Vec::new(),
        version: 1,
        store_id: (0..16).collect(),
        capacity: MIN_SEGMENT_BYTES,
        entries: vec![SegmentEntry {
            number: 0,
            store_id: vec![16; 16],
            sealed_length: 0,
        }],
    };
    publish_manifest(&path, &manifest).unwrap();
    let initial = std::fs::read(&path).unwrap();
    let mut hash: [u8; 32] = initial[12..44].try_into().unwrap();
    for number in 1u64..=3 {
        let mut bytes = [0; LINK_BYTES];
        bytes[..8].copy_from_slice(LINK_MAGIC);
        bytes[8..16].copy_from_slice(&number.to_be_bytes());
        bytes[16..32].copy_from_slice(&manifest.store_id);
        bytes[32..48].copy_from_slice(&[number as u8; 16]);
        bytes[48..56].copy_from_slice(&8192u64.to_be_bytes());
        bytes[56..88].copy_from_slice(&hash);
        let digest = digest::digest(&digest::SHA256, &bytes[..88]);
        bytes[88..120].copy_from_slice(digest.as_ref());
        hash = digest.as_ref().try_into().unwrap();
        publish_rollover(
            &path,
            &ManifestAppend {
                offset: initial.len() as u64 + (number - 1) * LINK_BYTES as u64,
                bytes,
            },
        )
        .unwrap();
    }
    assert_eq!(
        read_manifest(&path, &manifest.store_id, MIN_SEGMENT_BYTES, None)
            .unwrap()
            .metadata
            .entries
            .len(),
        4
    );
    let original = std::fs::read(&path).unwrap();
    for offset in [0, 8, 16, 32, 48, 56, 88, 120, 127] {
        let mut bytes = original.clone();
        bytes[initial.len() + offset] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_manifest(&path, &manifest.store_id, MIN_SEGMENT_BYTES, None).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    let mut moved = original.clone();
    let first = original[initial.len()..initial.len() + LINK_BYTES].to_vec();
    moved[initial.len()..initial.len() + LINK_BYTES]
        .copy_from_slice(&original[initial.len() + LINK_BYTES..initial.len() + 2 * LINK_BYTES]);
    moved[initial.len() + LINK_BYTES..initial.len() + 2 * LINK_BYTES].copy_from_slice(&first);
    std::fs::write(&path, &moved).unwrap();
    assert!(read_manifest(&path, &manifest.store_id, MIN_SEGMENT_BYTES, None).is_err());
}

#[test]
fn outcome_segments_unknown_manifest_tail_is_preserved() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    drop(log);
    let path = sibling(&dir.path(), ".segments");
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"UNKNOWN").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let bytes = std::fs::read(&path).unwrap();
    assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn outcome_segments_unpublished_header_cuts_recover_but_initialized_loss_is_refused() {
    for cut in [0, 43, 4095] {
        let dir = Directory::new();
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        drop(log);
        let path = segment_path(&dir.path(), 1);
        let orphan = DurableOutcomeLog::open(&path, MIN_SEGMENT_BYTES).unwrap();
        drop(orphan);
        let lock = OpenOptions::new()
            .write(true)
            .open(super::super::sibling(&path))
            .unwrap();
        lock.set_len(56).unwrap();
        lock.sync_all().unwrap();
        drop(lock);
        let data = OpenOptions::new().write(true).open(&path).unwrap();
        data.set_len(cut).unwrap();
        data.sync_all().unwrap();
        drop(data);
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        assert_eq!(log.segment_count().unwrap(), 1);
        assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
        assert_eq!(
            std::fs::metadata(super::super::sibling(&path))
                .unwrap()
                .len(),
            57
        );
    }
    for cut in [0, 4095, 4096] {
        let dir = Directory::new();
        let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
        drop(log);
        let path = segment_path(&dir.path(), 1);
        let orphan = DurableOutcomeLog::open(&path, MIN_SEGMENT_BYTES).unwrap();
        orphan.append_page(&outcome(0, 4096)).unwrap();
        drop(orphan);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(cut).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let bytes = std::fs::read(&path).unwrap();
        assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}
#[test]
fn outcome_segments_capacity_configuration_is_bound_to_the_anchor() {
    for capacity in [super::super::DEFAULT_CAPACITY_BYTES, MAX_CAPACITY_BYTES] {
        let dir = Directory::new();
        let log = SegmentedOutcomeLog::open(dir.path(), capacity).unwrap();
        let value = log.append_page(&outcome(0, 4096)).unwrap();
        drop(log);
        let bytes = std::fs::read(dir.path()).unwrap();
        assert!(SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).is_err());
        assert_eq!(std::fs::read(dir.path()).unwrap(), bytes);
        let log = SegmentedOutcomeLog::open(dir.path(), capacity).unwrap();
        assert_eq!(log.read_outcomes(&value.position).unwrap().len(), 1);
    }
}

#[test]
fn outcome_segments_disk_full_is_not_a_configured_capacity_refusal() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let manifest = std::fs::read(sibling(&dir.path(), ".segments")).unwrap();
    let error = log
        .append_with(
            &[DecidedMutation::Page(&outcome(0, 4096))],
            |_| Err(io::Error::from_raw_os_error(28)),
            |_, _| panic!("disk failure must not publish rollover"),
        )
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(28));
    assert_eq!(log.index.read().unwrap().entries.len(), 1);
    assert!(!segment_path(&dir.path(), 1).exists());
    assert_eq!(
        std::fs::read(sibling(&dir.path(), ".segments")).unwrap(),
        manifest
    );
    assert!(log.append_page(&outcome(1, 4096)).is_err());
    drop(log);
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    assert_eq!(log.replay_after(None, &mut service(), 100).unwrap(), 1);
}

#[test]
fn outcome_segments_cursor_captures_exact_cross_segment_bounds_without_later_appends() {
    let _large = LARGE.lock().unwrap();
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open_inner(&dir.path(), MIN_SEGMENT_BYTES, 1).unwrap();
    let mut positions = Vec::new();
    for index in 0..10 {
        positions.push(
            log.append_page(&outcome(index, 3 * 1024 * 1024))
                .unwrap()
                .position,
        );
    }
    let mut bounded = log
        .metadata_cursor_between(&positions[2], &positions[8])
        .unwrap();
    let mut prefix = log.metadata_cursor_after(None).unwrap();
    let later = log.append_page(&outcome(10, 4096)).unwrap().position;
    let mut seen = Vec::new();
    while let Some(record) = bounded.next_record().unwrap() {
        seen.push(record.position);
    }
    assert_eq!(seen, positions[2..=8]);
    assert!(bounded.next_record().unwrap().is_none());
    let mut seen = Vec::new();
    while let Some(record) = prefix.next_record().unwrap() {
        seen.push(record.position);
    }
    assert_eq!(seen, positions);
    let mut after = log.metadata_cursor_after(positions.last()).unwrap();
    assert_eq!(after.next_record().unwrap().unwrap().position, later);
    assert!(after.next_record().unwrap().is_none());
    assert!(log
        .metadata_cursor_between(&positions[8], &positions[2])
        .is_err());
    let mut invalid = positions[2].clone();
    invalid.length += 4096;
    assert!(log
        .metadata_cursor_between(&invalid, &positions[8])
        .is_err());
}

#[test]
fn outcome_segments_cursor_can_read_checkpoint_fragments_interleaved_with_state_changes() {
    let dir = Directory::new();
    let log = SegmentedOutcomeLog::open(dir.path(), MIN_SEGMENT_BYTES).unwrap();
    let a = crate::durable_payload::outcome_log::CheckpointFragment {
        version: 1,
        checkpoint_id: vec![12; 16],
        kind: crate::durable_payload::outcome_log::CheckpointFragment::CATALOG,
        ordinal: 0,
        data: b"head".to_vec(),
    };
    let b = crate::durable_payload::outcome_log::CheckpointFragment {
        ordinal: 1,
        data: b"tail".to_vec(),
        ..a.clone()
    };
    let first = log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&a)])
        .unwrap()
        .position;
    log.append_page(&outcome(2, 4096)).unwrap();
    let last = log
        .append_decided_batch(&[DecidedMutation::CheckpointFragment(&b)])
        .unwrap()
        .position;
    let mut cursor = log.metadata_cursor_between(&first, &last).unwrap();
    log.append_page(&outcome(3, 4096)).unwrap();
    let mut versions = Vec::new();
    let mut data = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        versions.push(record.batch.version);
        if record.batch.version == 2 {
            for value in log.read_outcomes(&record.position).unwrap() {
                match value {
                    RecoveredMutation::CheckpointFragment(value) => data.extend(value.data),
                    _ => panic!("expected checkpoint fragment"),
                }
            }
        }
    }
    assert_eq!(versions, vec![2, 1, 2]);
    assert_eq!(data, b"headtail");
}

#[path = "stored_outcome_write_statistics_tests.rs"]
mod write_statistics_tests;
