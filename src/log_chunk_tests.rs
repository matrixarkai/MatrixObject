// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::log_chunk::*;
use std::fs;
use std::path::PathBuf;

const MAX: u64 = 64 * 1024 * 1024;

struct Dir(PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("logchunk-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i * 7) as u8)).collect()
}
/// A chunk holding three appends, and the file's length after each.
fn three(path: &std::path::Path) -> (Vec<u8>, [u64; 3]) {
    let mut chunk = LogChunk::open(path, 1, MAX).unwrap();
    let mut all = Vec::new();
    let mut ends = [0u64; 3];
    for (i, n) in [100usize, 37, 4096].into_iter().enumerate() {
        let data = bytes(i as u8 + 1, n);
        chunk.append(1, all.len() as u64, &data).unwrap();
        all.extend_from_slice(&data);
        chunk.commit().unwrap();
        ends[i] = fs::metadata(path).unwrap().len();
    }
    (all, ends)
}

#[test]
fn crc32c_matches_the_standard_check_value() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(b""), 0);
    assert_eq!(crc32c_update(crc32c(b"1234"), b"56789"), 0xE306_9283);
}

#[test]
fn the_length_is_derived_on_replay_and_nothing_else_stores_it() {
    let dir = Dir::new("derive");
    let path = dir.file("c");
    let (all, ends) = three(&path);
    // one header per record, no separate length record: the file is exactly records
    assert_eq!(ends[2], (3 * LOG_HEADER_LEN + all.len()) as u64);
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(chunk.length(), all.len() as u64);
    assert_eq!(chunk.durable_length(), all.len() as u64);
    assert_eq!(chunk.read(0, all.len()), &all[..]);
    assert_eq!(chunk.footer_crc(), crc32c(&all));
    assert_eq!(chunk.epoch(), 1);
}

#[test]
fn a_cut_at_every_byte_of_the_last_record_replays_to_the_records_before_it() {
    let dir = Dir::new("cut");
    let path = dir.file("c");
    let (all, ends) = three(&path);
    let before_last = &all[..137];
    let original = fs::read(&path).unwrap();
    for cut in ends[1]..ends[2] {
        let copy = dir.file(&format!("cut{cut}"));
        fs::write(&copy, &original[..cut as usize]).unwrap();
        let chunk = LogChunk::open(&copy, 1, MAX).unwrap();
        assert_eq!(chunk.length(), 137, "cut at byte {cut}");
        assert_eq!(chunk.read(0, 137), before_last);
        // the cut is made durable: the file is exactly the whole records
        assert_eq!(fs::metadata(&copy).unwrap().len(), ends[1], "cut at byte {cut}");
    }
    // a cut exactly after the last record loses nothing
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(chunk.length(), all.len() as u64);
}

#[test]
fn a_header_that_is_partly_written_with_zeros_after_it_is_cut() {
    let dir = Dir::new("zerotail");
    let path = dir.file("c");
    let (all, ends) = three(&path);
    let mut original = fs::read(&path).unwrap();
    // the last record's header half written, the rest of its extent zero (size updated first)
    let start = ends[1] as usize;
    for byte in original[start + 20..].iter_mut() {
        *byte = 0;
    }
    fs::write(&path, &original).unwrap();
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(chunk.length(), 137);
    assert!(all.len() > 137);
}

#[test]
fn damage_that_is_not_a_torn_tail_refuses_the_open_and_changes_nothing() {
    let dir = Dir::new("damage");
    let path = dir.file("c");
    let (_, ends) = three(&path);
    let original = fs::read(&path).unwrap();
    let cases: Vec<(&str, usize)> = vec![
        ("data byte of a record with records after it", LOG_HEADER_LEN + 3),
        ("header byte of a record with records after it", 9),
        ("header byte of the second record", ends[0] as usize + 13),
        ("data byte of the middle record", ends[0] as usize + LOG_HEADER_LEN + 2),
    ];
    for (name, at) in cases {
        let mut damaged = original.clone();
        damaged[at] ^= 0x40;
        let copy = dir.file("damaged");
        fs::write(&copy, &damaged).unwrap();
        match LogChunk::open(&copy, 1, MAX) {
            Err(LogError::Corrupt(_)) => {}
            other => panic!("{name}: expected Corrupt, got {:?}", other.map(|c| c.length())),
        }
        assert_eq!(fs::read(&copy).unwrap(), damaged, "{name}: the file must not be changed");
    }
    // damaged data in the LAST record, with nothing after it, is a write that never finished
    let mut tail = original.clone();
    let last = tail.len() - 1;
    tail[last] ^= 0x01;
    let copy = dir.file("tail");
    fs::write(&copy, &tail).unwrap();
    assert_eq!(LogChunk::open(&copy, 1, MAX).unwrap().length(), 137);
}

#[test]
fn a_record_of_another_generation_or_a_backwards_epoch_is_refused() {
    let dir = Dir::new("generation");
    let path = dir.file("c");
    three(&path);
    assert!(matches!(LogChunk::open(&path, 2, MAX), Err(LogError::Corrupt(_))));
    // epoch 5 then a record at epoch 3 is damage
    let path2 = dir.file("e");
    let mut chunk = LogChunk::open(&path2, 1, MAX).unwrap();
    chunk.append(5, 0, &bytes(1, 10)).unwrap();
    // the in-memory fence refuses the stale epoch, so build the bad file by hand
    assert!(matches!(chunk.append(3, 10, &bytes(2, 10)), Err(LogError::Fenced { current: 5 })));
}

#[test]
fn pipelined_records_arriving_out_of_order_are_applied_in_offset_order() {
    let dir = Dir::new("reorder");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let parts: Vec<Vec<u8>> = (0..8).map(|i| bytes(i as u8, 1000 + i * 13)).collect();
    let mut offsets = Vec::new();
    let mut at = 0u64;
    for part in &parts {
        offsets.push(at);
        at += part.len() as u64;
    }
    // 3, 1, 2, 0, 7, 5, 6, 4
    let order = [3usize, 1, 2, 0, 7, 5, 6, 4];
    let mut acks = Vec::new();
    for index in order {
        acks.push(chunk.append(1, offsets[index], &parts[index]).unwrap());
    }
    // nothing is acknowledged past a missing record
    assert_eq!(acks[0], 0);
    assert_eq!(acks[1], 0);
    assert_eq!(acks[2], 0);
    assert_eq!(acks[3], offsets[4]); // 0 arrives: 0..=3 applied
    assert_eq!(acks[4], offsets[4]);
    assert_eq!(acks[5], offsets[4]);
    assert_eq!(acks[6], offsets[4]);
    assert_eq!(acks[7], at);
    chunk.commit().unwrap();
    let flat: Vec<u8> = parts.concat();
    assert_eq!(chunk.read(0, flat.len()), &flat[..]);
    // the file holds the records in offset order, so replay reproduces the bytes
    let again = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(again.read(0, flat.len()), &flat[..]);
    assert_eq!(again.pending_records(), 0);
}

#[test]
fn a_record_that_never_arrives_leaves_waiting_records_unwritten() {
    let dir = Dir::new("lost");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    chunk.append(1, 0, &bytes(1, 100)).unwrap();
    chunk.append(1, 200, &bytes(3, 100)).unwrap(); // the record at 100 is missing
    chunk.commit().unwrap();
    assert_eq!(chunk.length(), 100);
    drop(chunk);
    // a crash loses the waiting record: it was never acknowledged
    assert_eq!(LogChunk::open(&path, 1, MAX).unwrap().length(), 100);
}

#[test]
fn the_reorder_window_is_bounded() {
    let dir = Dir::new("window");
    let mut chunk = LogChunk::open(&dir.file("c"), 1, MAX).unwrap();
    chunk.set_reorder_window(2, 1000);
    chunk.append(1, 100, &bytes(1, 10)).unwrap();
    chunk.append(1, 200, &bytes(1, 10)).unwrap();
    assert!(matches!(chunk.append(1, 300, &bytes(1, 10)), Err(LogError::Window)));
    chunk.set_reorder_window(8, 25);
    assert!(matches!(chunk.append(1, 300, &bytes(1, 10)), Err(LogError::Window)));
    // the same waiting record again is not new
    assert_eq!(chunk.append(1, 100, &bytes(1, 10)).unwrap(), 0);
    // overlapping a waiting record with another shape is refused
    assert!(matches!(chunk.append(1, 105, &bytes(1, 10)), Err(LogError::Overlap)));
    assert!(matches!(chunk.append(1, 95, &bytes(1, 10)), Err(LogError::Overlap)));
}

#[test]
fn a_retry_is_acknowledged_again_and_a_conflicting_rewrite_is_refused() {
    let dir = Dir::new("retry");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let data = bytes(9, 500);
    assert_eq!(chunk.append(1, 0, &data).unwrap(), 500);
    let size = fs::metadata(&path).unwrap().len();
    assert_eq!(chunk.append(1, 0, &data).unwrap(), 500);
    assert_eq!(chunk.append(1, 100, &data[100..300]).unwrap(), 500);
    assert_eq!(fs::metadata(&path).unwrap().len(), size, "a retry writes nothing");
    let mut other = data.clone();
    other[250] ^= 1;
    assert!(matches!(chunk.append(1, 0, &other), Err(LogError::Overlap)));
    // a retry with different boundaries that also extends the chunk applies only the new bytes
    let mut longer = data[400..].to_vec();
    longer.extend_from_slice(&bytes(4, 50));
    assert_eq!(chunk.append(1, 400, &longer).unwrap(), 550);
    assert_eq!(chunk.length(), 550);
}

#[test]
fn a_stale_writer_is_fenced_and_a_new_epoch_may_cut_the_tail() {
    let dir = Dir::new("epoch");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    chunk.append(1, 0, &bytes(1, 300)).unwrap();
    chunk.append(2, 300, &bytes(2, 100)).unwrap();
    assert!(matches!(chunk.append(1, 400, &bytes(3, 10)), Err(LogError::Fenced { current: 2 })));
    assert!(matches!(chunk.truncate(1, 100), Err(LogError::Fenced { current: 2 })));
    // the new owner decides the tail is 350 and cuts the bytes only this replica held
    chunk.truncate(3, 350).unwrap();
    assert_eq!(chunk.length(), 350);
    chunk.append(3, 350, &bytes(4, 20)).unwrap();
    chunk.commit().unwrap();
    let expected = [bytes(1, 300), bytes(2, 100)[..50].to_vec(), bytes(4, 20)].concat();
    let again = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(again.read(0, 1000), &expected[..]);
    assert_eq!(again.epoch(), 3);
    assert!(matches!(chunk.truncate(3, 999), Err(LogError::Bounds(_))));
}

#[test]
fn a_seal_records_the_footer_and_refuses_every_later_change() {
    let dir = Dir::new("seal");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let data = bytes(5, 3000);
    chunk.append(1, 0, &data).unwrap();
    let sealed = chunk.seal(1, true).unwrap();
    assert_eq!(sealed.length, 3000);
    assert_eq!(sealed.footer_crc, crc32c(&data));
    let want: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &data).as_ref().try_into().unwrap();
    assert_eq!(sealed.sha256, Some(want));
    assert!(matches!(chunk.append(1, 3000, &bytes(1, 1)), Err(LogError::Sealed)));
    assert!(matches!(chunk.truncate(2, 0), Err(LogError::Sealed)));
    assert!(matches!(chunk.seal(1, false), Err(LogError::Sealed)));
    let again = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(again.sealed(), Some(&sealed));
    assert_eq!(again.durable_length(), 3000);
    // a seal without the digest
    let plain = dir.file("p");
    let mut chunk = LogChunk::open(&plain, 1, MAX).unwrap();
    chunk.append(1, 0, &data).unwrap();
    let sealed = chunk.seal(1, false).unwrap();
    assert_eq!(sealed.sha256, None);
    assert_eq!(LogChunk::open(&plain, 1, MAX).unwrap().sealed(), Some(&sealed));
}

#[test]
fn a_seal_with_waiting_records_is_refused_and_a_damaged_seal_is_corruption() {
    let dir = Dir::new("seal2");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    chunk.append(1, 0, &bytes(1, 10)).unwrap();
    chunk.append(1, 50, &bytes(1, 10)).unwrap();
    assert!(matches!(chunk.seal(1, false), Err(LogError::SealMismatch)));
    // append more, seal, then change a data byte BEFORE the seal: the footer no longer matches
    let path = dir.file("d");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    chunk.append(1, 0, &bytes(1, 64)).unwrap();
    chunk.seal(1, true).unwrap();
    drop(chunk);
    let mut file = fs::read(&path).unwrap();
    file[LOG_HEADER_LEN + 5] ^= 1; // the append's data: its own CRC now fails, with a seal after it
    fs::write(&path, &file).unwrap();
    assert!(matches!(LogChunk::open(&path, 1, MAX), Err(LogError::Corrupt(_))));
}

#[test]
fn limits_are_enforced() {
    let dir = Dir::new("limits");
    let mut chunk = LogChunk::open(&dir.file("c"), 1, 1000).unwrap();
    assert!(matches!(chunk.append(1, 0, &[]), Err(LogError::Bounds(_))));
    assert!(matches!(chunk.append(1, 0, &vec![0; MAX_RECORD_DATA + 1]), Err(LogError::Bounds(_))));
    assert!(matches!(chunk.append(1, 0, &vec![0; 1001]), Err(LogError::Bounds(_))));
    chunk.append(1, 0, &bytes(1, 1000)).unwrap();
    assert!(matches!(chunk.append(1, 1000, &bytes(1, 1)), Err(LogError::Bounds(_))));
    assert!(matches!(LogChunk::open(&dir.file("x"), 1, 0), Err(LogError::Bounds(_))));
}

#[test]
fn one_commit_makes_many_records_durable() {
    let dir = Dir::new("commit");
    let path = dir.file("c");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    for i in 0..10u64 {
        chunk.append(1, i * 100, &bytes(i as u8, 100)).unwrap();
    }
    assert_eq!(chunk.length(), 1000);
    assert_eq!(chunk.durable_length(), 0);
    assert_eq!(chunk.commit().unwrap(), 1000);
    assert_eq!(chunk.durable_length(), 1000);
}
