// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The heap a log chunk costs: opening a sealed chunk must not allocate its payload, and reading
//! it through the record cache stays inside the cache bound, however large the chunk is.
#![cfg(unix)]
use matrixobjectstore_rs::log_chunk::{LogChunk, RecordCache};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicIsize, Ordering},
};

struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
fn grew(by: isize) {
    if ACTIVE.load(Ordering::SeqCst) {
        let live = LIVE.fetch_add(by, Ordering::SeqCst) + by;
        PEAK.fetch_max(live, Ordering::SeqCst);
    }
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grew(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        grew(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        grew(size as isize - layout.size() as isize);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        grew(-(layout.size() as isize));
        unsafe { System.dealloc(ptr, layout) }
    }
}

static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
/// What `run` did, and the most heap it held at once above where it started.
fn measured<T>(run: impl FnOnce() -> T) -> (T, usize) {
    LIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    ACTIVE.store(true, Ordering::SeqCst);
    let value = run();
    ACTIVE.store(false, Ordering::SeqCst);
    (value, PEAK.load(Ordering::SeqCst).max(0) as usize)
}

const MIB: usize = 1024 * 1024;
const CHUNK: usize = 64 * MIB;

struct Dir(std::path::PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
/// A sealed chunk of 64 MiB in sixteen records of 4 MiB, and the bytes in it.
fn sealed_chunk(name: &str) -> (Dir, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("logchunkflat-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("c.log");
    let mut chunk = LogChunk::open(&path, 1, CHUNK as u64).unwrap();
    for record in 0..16usize {
        let data: Vec<u8> = (0..4 * MIB).map(|i| (i as u8).wrapping_mul(31).wrapping_add(record as u8)).collect();
        chunk.append(1, (record * 4 * MIB) as u64, &data).unwrap();
    }
    chunk.commit().unwrap();
    chunk.seal(1, true).unwrap();
    assert_eq!(chunk.length(), CHUNK as u64);
    (Dir(dir), path)
}

#[test]
fn opening_a_sealed_chunk_of_64_mib_does_not_allocate_its_payload() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (_dir, path) = sealed_chunk("open");
    let (chunk, peak) = measured(|| LogChunk::open(&path, 1, CHUNK as u64).unwrap());
    eprintln!("PEAK_HEAP_OPENING_A_64_MIB_SEALED_CHUNK={peak}");
    assert_eq!(chunk.length(), CHUNK as u64);
    assert!(chunk.sealed().is_some());
    assert!(peak < 2 * MIB, "opening a sealed 64 MiB chunk held {peak} bytes of heap at once");
    assert_eq!(chunk.resident_bytes(), 0); // NEWAPI
}

#[test]
fn reading_all_of_a_sealed_chunk_through_the_cache_stays_inside_the_cache_bound() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (_dir, path) = sealed_chunk("read");
    let chunk = LogChunk::open(&path, 1, CHUNK as u64).unwrap();
    let mut cache = RecordCache::new(8 * MIB);
    let mut checksum = 0u64;
    let ((), peak) = measured(|| {
        let mut out = Vec::with_capacity(MIB);
        for step in 0..64usize {
            out.clear();
            chunk.read_into((step * MIB) as u64, MIB, &mut cache, &mut out).unwrap();
            assert_eq!(out.len(), MIB);
            checksum = checksum.wrapping_add(out.iter().map(|byte| u64::from(*byte)).sum::<u64>());
        }
    });
    eprintln!("PEAK_HEAP_READING_64_MIB_THROUGH_AN_8_MIB_CACHE={peak} checksum={checksum}");
    assert!(cache.bytes() <= 8 * MIB);
    // the cache bound, one record being read (4 MiB), and the output buffer
    assert!(peak < 8 * MIB + 4 * MIB + 2 * MIB, "reading held {peak} bytes of heap at once");
    assert!(peak < CHUNK / 3, "memory must not grow with the chunk");
}
