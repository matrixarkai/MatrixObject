// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "resident-tail-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self) -> PathBuf {
        self.0.join("chunk")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn read(chunk: &LogChunk, offset: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    chunk
        .read_into(offset, len, &mut RecordCache::new(0), &mut out)
        .unwrap();
    out
}
#[test]
fn publication_retries_and_reads_cross_resident_block_boundaries() {
    let dir = Fixture::new();
    let path = dir.file();
    let mut chunk = LogChunk::open(&path, 1, 16 << 20).unwrap();
    chunk.initialize_publication(1, [1; 16]).unwrap();
    let first = vec![7; 65535];
    let next: Vec<u8> = (0..65539).map(|i| (i % 251) as u8).collect();
    let end = (first.len() + next.len()) as u64;
    chunk
        .append_published(1, 0, &first, [2; 16], first.len() as u64)
        .unwrap();
    chunk
        .append_published(1, first.len() as u64, &next, [3; 16], end)
        .unwrap();
    chunk.commit().unwrap();
    assert_eq!(
        chunk
            .append_published(1, first.len() as u64, &next, [3; 16], end)
            .unwrap(),
        end
    );
    assert!(matches!(
        chunk.append_published(1, first.len() as u64, &next, [4; 16], end),
        Err(LogError::Overlap)
    ));
    let mut changed = next.clone();
    changed[32768] ^= 1;
    assert!(matches!(
        chunk.append_published(1, first.len() as u64, &changed, [3; 16], end),
        Err(LogError::Overlap)
    ));
    let expected = [first, next].concat();
    assert_eq!(read(&chunk, 65530, 65544), expected[65530..]);
    drop(chunk);
    let chunk = LogChunk::open(&path, 1, 16 << 20).unwrap();
    assert_eq!(read(&chunk, 0, expected.len()), expected);
}
#[test]
fn resident_truncation_preserves_tail_bytes_and_sealed_checksums() {
    for cut in [0usize, 65535, 65536, 65537] {
        let dir = Fixture::new();
        let path = dir.file();
        let mut chunk = LogChunk::open(&path, 1, 16 << 20).unwrap();
        let mut expected: Vec<u8> = (0..196615).map(|i| (i % 251) as u8).collect();
        chunk.append(1, 0, &expected).unwrap();
        chunk.commit().unwrap();
        chunk.truncate(2, cut as u64).unwrap();
        expected.truncate(cut);
        let next = vec![37; 65541];
        chunk.append(2, cut as u64, &next).unwrap();
        expected.extend(next);
        let sealed = chunk.seal(2, true).unwrap();
        assert_eq!(sealed.footer_crc, crc32c(&expected));
        assert_eq!(
            sealed.sha256.unwrap().as_slice(),
            ring::digest::digest(&ring::digest::SHA256, &expected).as_ref()
        );
        assert_eq!(chunk.resident_bytes(), 0);
        assert_eq!(read(&chunk, 0, expected.len()), expected);
        drop(chunk);
        let chunk = LogChunk::open(&path, 1, 16 << 20).unwrap();
        assert_eq!(chunk.sealed(), Some(&sealed));
        assert_eq!(read(&chunk, 0, expected.len()), expected);
    }
}
#[test]
fn small_append_to_a_recovered_large_tail_does_not_double_payload_capacity() {
    {
        let dir = Fixture::new();
        let mut chunk = LogChunk::open(&dir.file(), 1, 16 << 20).unwrap();
        chunk.append(1, 0, &[7; 4096]).unwrap();
        assert_eq!(
            chunk.resident_bytes(),
            4096,
            "one append reserved an entire block"
        );
        chunk.append(1, 4096, &[9; 4096]).unwrap();
        assert!(chunk.resident_bytes() <= 65536);
        assert_eq!(read(&chunk, 4088, 16), [vec![7; 8], vec![9; 8]].concat());
    }
    for publication in [false, true] {
        let dir = Fixture::new();
        let path = dir.file();
        let mut chunk = LogChunk::open(&path, 1, 16 << 20).unwrap();
        if publication {
            chunk.initialize_publication(1, [1; 16]).unwrap();
        }
        let prefix = 8 << 20;
        let data = vec![7; 65536];
        for offset in (0..prefix).step_by(data.len()) {
            if publication {
                chunk
                    .append_published(
                        1,
                        offset as u64,
                        &data,
                        [2; 16],
                        (offset + data.len()) as u64,
                    )
                    .unwrap();
            } else {
                chunk.append(1, offset as u64, &data).unwrap();
            }
        }
        chunk.commit().unwrap();
        drop(chunk);
        let mut chunk = LogChunk::open(&path, 1, 16 << 20).unwrap();
        let before = chunk.resident_bytes();
        let next = vec![19; 4096];
        if publication {
            chunk
                .append_published(
                    1,
                    prefix as u64,
                    &next,
                    [3; 16],
                    (prefix + next.len()) as u64,
                )
                .unwrap();
        } else {
            chunk.append(1, prefix as u64, &next).unwrap();
        }
        chunk.commit().unwrap();
        assert!(
            chunk.resident_bytes() <= before + 65536,
            "small append reallocated the accumulated payload"
        );
        assert_eq!(
            read(&chunk, prefix as u64 - 8, 4104),
            [vec![7; 8], next].concat()
        );
    }
}
