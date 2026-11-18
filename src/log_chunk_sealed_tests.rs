// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! A sealed chunk is read from its file, a record at a time, through a bounded cache: its bytes are
//! not held in memory, and a record is checked against its checksum every time it is read from disk.
use crate::log_chunk::*;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

const MAX: u64 = 64 * 1024 * 1024;

struct Dir(PathBuf);
impl Dir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("logchunksealed-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i * 7) as u8).wrapping_add((i >> 9) as u8)).collect()
}
/// Records of mixed sizes totalling at least `total` bytes, appended and made durable.
fn fill(chunk: &mut LogChunk, total: usize) -> Vec<u8> {
    let mut all = Vec::new();
    let mut i = 0usize;
    while all.len() < total {
        let size = [1usize, 4096, 65_537, 200_000, 1_048_576][i % 5];
        let data = bytes(i as u8, size);
        chunk.append(1, all.len() as u64, &data).unwrap();
        all.extend_from_slice(&data);
        i += 1;
    }
    chunk.commit().unwrap();
    all
}

#[test]
fn a_sealed_chunk_of_eight_mib_is_read_back_byte_identical_from_its_file_and_holds_no_payload() {
    let dir = Dir::new("identical");
    let path = dir.0.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let all = fill(&mut chunk, 8 * 1024 * 1024);
    assert!(chunk.is_resident());
    chunk.seal(1, true).unwrap();
    assert!(!chunk.is_resident());
    assert_eq!(chunk.resident_bytes(), 0, "sealing drops the payload");
    drop(chunk);

    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert!(!chunk.is_resident());
    assert_eq!(chunk.resident_bytes(), 0, "a sealed chunk is opened without its payload");
    assert_eq!(chunk.length(), all.len() as u64);
    let mut cache = RecordCache::new(2 * 1024 * 1024);
    let mut out = Vec::new();
    chunk.read_into(0, all.len(), &mut cache, &mut out).unwrap();
    assert_eq!(out, all);
    assert!(cache.bytes() <= cache.cap_bytes(), "the cache stays inside its bound");
    // ranges that start and end inside records, across record boundaries, and past the end
    for (offset, length) in [(0usize, 1usize), (3, 5000), (4096, 70_000), (all.len() - 10, 100), (all.len(), 5), (1_000_000, 3_000_000)] {
        let mut part = Vec::new();
        chunk.read_into(offset as u64, length, &mut cache, &mut part).unwrap();
        let end = (offset + length).min(all.len());
        assert_eq!(part, all[offset.min(all.len())..end], "range {offset}+{length}");
        assert!(cache.bytes() <= cache.cap_bytes());
    }
}

#[test]
fn the_record_cache_never_goes_above_its_bound_however_much_is_read() {
    let dir = Dir::new("bound");
    let path = dir.0.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let all = fill(&mut chunk, 6 * 1024 * 1024);
    chunk.seal(1, false).unwrap();
    drop(chunk);
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let mut cache = RecordCache::new(1_200_000);
    for pass in 0..3 {
        let mut at = 0usize;
        while at < all.len() {
            let mut out = Vec::new();
            chunk.read_into(at as u64, 50_000, &mut cache, &mut out).unwrap();
            assert_eq!(out, all[at..(at + 50_000).min(all.len())]);
            assert!(cache.bytes() <= 1_200_000, "pass {pass}: {} bytes cached", cache.bytes());
            at += 50_000;
        }
    }
    assert!(cache.len() >= 1, "the cache holds something");
}

#[test]
fn a_record_damaged_after_the_chunk_was_opened_is_refused_when_it_is_read() {
    let dir = Dir::new("damaged");
    let path = dir.0.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let all = fill(&mut chunk, 600_000);
    chunk.seal(1, false).unwrap();
    drop(chunk);
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    // flip one byte inside the data of the second record, behind the open handle's back
    let first = 1usize;
    let target = LOG_HEADER_LEN + first + LOG_HEADER_LEN + 100;
    let mut file = fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(target as u64)).unwrap();
    file.write_all(&[0xA5]).unwrap();
    file.sync_all().unwrap();
    let mut cache = RecordCache::new(0);
    let mut out = Vec::new();
    // the first record is intact
    chunk.read_into(0, 1, &mut cache, &mut out).unwrap();
    assert_eq!(out, all[..1]);
    // the second is not
    let error = chunk.read_into(first as u64, 200, &mut cache, &mut Vec::new()).unwrap_err();
    assert!(matches!(error, LogError::Corrupt(_)), "{error:?}");
}

#[test]
fn a_truncate_that_cuts_a_record_short_leaves_a_prefix_that_is_still_checked_whole() {
    let dir = Dir::new("prefix");
    let path = dir.0.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let a = bytes(1, 5000);
    let b = bytes(2, 9000);
    chunk.append(1, 0, &a).unwrap();
    chunk.append(1, 5000, &b).unwrap();
    chunk.commit().unwrap();
    chunk.truncate(2, 7000).unwrap(); // inside the second record
    let c = bytes(3, 3000);
    chunk.append(2, 7000, &c).unwrap();
    chunk.commit().unwrap();
    let mut expected = a.clone();
    expected.extend_from_slice(&b[..2000]);
    expected.extend_from_slice(&c);
    chunk.seal(2, true).unwrap();
    drop(chunk);
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(chunk.length(), expected.len() as u64);
    assert_eq!(chunk.read_vec(0, expected.len()).unwrap(), expected);
    assert_eq!(chunk.read_vec(4999, 2).unwrap(), expected[4999..5001]);
    assert_eq!(chunk.read_vec(6990, 20).unwrap(), expected[6990..7010]);
}

#[test]
fn an_open_tail_keeps_its_bytes_and_a_restart_reads_them_back() {
    let dir = Dir::new("tail");
    let path = dir.0.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let all = fill(&mut chunk, 3 * 1024 * 1024);
    assert!(chunk.is_resident());
    assert!(chunk.resident_bytes() >= all.len());
    assert_eq!(chunk.read_vec(10, 100_000).unwrap(), all[10..100_010]);
    drop(chunk);
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert!(chunk.is_resident(), "an unsealed chunk is a tail again after a restart");
    assert!(chunk.resident_bytes() >= all.len());
    assert_eq!(chunk.read_vec(0, all.len()).unwrap(), all);
}

#[test]
fn a_torn_final_record_is_cut_off_by_the_streaming_replay_and_everything_before_it_survives() {
    let dir = Dir::new("torn");
    let path = dir.0.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, MAX).unwrap();
    let all = fill(&mut chunk, 500_000);
    drop(chunk);
    // a header and half of a record's data that never finished
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    let torn = bytes(9, 100_000);
    let header = {
        let mut chunk = LogChunk::open(&dir.0.join("scratch.log"), 1, MAX).unwrap();
        chunk.append(1, 0, &torn).unwrap();
        chunk.commit().unwrap();
        let whole = fs::read(dir.0.join("scratch.log")).unwrap();
        let mut header = whole[..LOG_HEADER_LEN].to_vec();
        // make it continue the first chunk: its offset is the first chunk's length
        header[20..28].copy_from_slice(&(all.len() as u64).to_le_bytes());
        let crc = crc32c(&header[..36]);
        header[36..40].copy_from_slice(&crc.to_le_bytes());
        header
    };
    file.write_all(&header).unwrap();
    file.write_all(&torn[..40_000]).unwrap();
    file.sync_all().unwrap();
    let chunk = LogChunk::open(&path, 1, MAX).unwrap();
    assert_eq!(chunk.length(), all.len() as u64, "the unfinished record is not part of the chunk");
    assert_eq!(chunk.read_vec(0, all.len()).unwrap(), all);
}
