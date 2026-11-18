// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(unix, any(feature = "replica-server", feature = "meta-server")))]
use matrixobjectstore_rs::durable_payload::{
    PayloadExtentStore, MAX_CAPACITY_BYTES, MAX_EXTENT_BYTES,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
fn record(size: usize) {
    if ACTIVE.load(Ordering::SeqCst) {
        BYTES.fetch_add(size, Ordering::Relaxed);
        CALLS.fetch_add(1, Ordering::Relaxed);
    }
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

static ALLOCATION_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());
struct Directory(std::path::PathBuf);
fn directory(label: &str, prefix: usize) -> Directory {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "payload-allocation-{label}-{}-{prefix}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&path).unwrap();
    Directory(path)
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn measured<T>(run: impl FnOnce() -> T) -> (T, usize, usize) {
    struct Active;
    impl Drop for Active {
        fn drop(&mut self) {
            ACTIVE.store(false, Ordering::SeqCst);
        }
    }
    BYTES.store(0, Ordering::Relaxed);
    CALLS.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::SeqCst);
    let active = Active;
    let result = run();
    drop(active);
    (
        result,
        CALLS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    )
}
#[test]
fn durable_payload_fixed_append_and_range_allocations_do_not_grow_with_file() {
    let _test = ALLOCATION_TESTS.lock().unwrap();
    let mut append_sizes = Vec::new();
    let mut read_sizes = Vec::new();
    for prefix in [4 * 1024 * 1024, 32 * 1024 * 1024, 128 * 1024 * 1024] {
        let directory = directory("raw", prefix);
        let store =
            PayloadExtentStore::open(directory.0.join("payload.data"), MAX_CAPACITY_BYTES).unwrap();
        let block = vec![0x31; MAX_EXTENT_BYTES];
        let mut last = None;
        for offset in (0..prefix).step_by(8 * MAX_EXTENT_BYTES) {
            let inputs = vec![block.as_slice(); ((prefix - offset) / MAX_EXTENT_BYTES).min(8)];
            last = store.append_batch(&inputs).unwrap().pop();
        }
        let last = last.unwrap();
        let suffix = [0x7b; 4096];
        store.append(&suffix).unwrap();
        store.read_range(&last, 4096, 4096).unwrap();
        let (written, append_calls, append_bytes) = measured(|| store.append(&suffix));
        let written = written.unwrap();
        assert_eq!(store.read(&written).unwrap(), suffix);
        let (read, read_calls, read_bytes) = measured(|| store.read_range(&last, 4096, 4096));
        assert_eq!(read.unwrap(), vec![0x31; 4096]);
        assert!(
            append_bytes < 16 * 1024,
            "append allocated {append_bytes} bytes after {prefix} bytes"
        );
        assert!(read_bytes <= 8192, "range allocated {read_bytes} bytes");
        append_sizes.push(append_bytes);
        read_sizes.push(read_bytes);
        println!("PAYLOAD_ALLOCATION prefix_bytes={prefix} append_calls={append_calls} append_bytes={append_bytes} read_calls={read_calls} read_bytes={read_bytes}");
    }
    assert!(append_sizes.iter().max().unwrap() - append_sizes.iter().min().unwrap() <= 1024);
    assert!(read_sizes.iter().max().unwrap() - read_sizes.iter().min().unwrap() <= 1024);
}

#[test]
fn stored_outcomes_append_and_metadata_allocations_do_not_grow_with_file() {
    use matrixobjectstore_rs::durable_payload::outcome_log::{DecidedMutation, DurableOutcomeLog};
    let _test = ALLOCATION_TESTS.lock().unwrap();
    let large = source_outcome(MAX_EXTENT_BYTES);
    let small = source_outcome(4096);
    let mut append_sizes = Vec::new();
    let mut metadata_sizes = Vec::new();
    for prefix in [4 * 1024 * 1024, 32 * 1024 * 1024, 128 * 1024 * 1024] {
        let directory = directory("records", prefix);
        let log =
            DurableOutcomeLog::open(directory.0.join("records.data"), MAX_CAPACITY_BYTES).unwrap();
        let mut index = 1000;
        while log.length() < prefix as u64 + 4096 {
            let count = ((prefix as u64 + 4096 - log.length()).div_ceil(MAX_EXTENT_BYTES as u64)
                as usize)
                .clamp(1, 7);
            let outcomes = (0..count)
                .map(|_| {
                    let mut value = large.clone();
                    value.page_index = index;
                    index += 1;
                    value
                })
                .collect::<Vec<_>>();
            let mutations = outcomes
                .iter()
                .map(DecidedMutation::Page)
                .collect::<Vec<_>>();
            log.append_decided_batch(&mutations).unwrap();
        }
        let mut warm = small.clone();
        warm.page_index = 1;
        log.append_page(&warm).unwrap();
        let mut value = small.clone();
        value.page_index = 2;
        let before = log.length();
        let (committed, append_calls, append_bytes) = measured(|| log.append_page(&value));
        let committed = committed.unwrap();
        let (metadata, metadata_calls, metadata_bytes) =
            measured(|| log.read_metadata(&committed.position));
        let metadata = metadata.unwrap();
        assert!(metadata.batch.decisions[0]
            .page
            .as_ref()
            .unwrap()
            .patches
            .iter()
            .all(|p| p.data.is_empty()));
        assert_eq!(
            log.read_outcomes(&committed.position).unwrap()[0]
                .to_payload()
                .unwrap(),
            value.to_payload().unwrap()
        );
        assert!(
            append_bytes < 128 * 1024,
            "record append allocated {append_bytes} bytes"
        );
        assert!(
            metadata_bytes < 64 * 1024,
            "metadata read allocated {metadata_bytes} bytes"
        );
        append_sizes.push(append_bytes);
        metadata_sizes.push(metadata_bytes);
        println!("STORED_OUTCOME_ALLOCATION target_prefix_bytes={prefix} file_prefix_bytes={before} append_calls={append_calls} append_bytes={append_bytes} metadata_calls={metadata_calls} metadata_bytes={metadata_bytes}");
    }
    assert!(append_sizes.iter().max().unwrap() - append_sizes.iter().min().unwrap() <= 4096);
    assert!(metadata_sizes.iter().max().unwrap() - metadata_sizes.iter().min().unwrap() <= 4096);
}

fn source_outcome(bytes: usize) -> matrixobjectstore_rs::outcome::PageOutcome {
    use matrixobjectstore_rs::{outcome::PageOutcome, ChunkId, PageId, PageState};
    let data = vec![0x31; bytes];
    let checksum = data.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    let state = PageState {
        logical_length: bytes as u64,
        write_sequence: 1,
        checksum_fnv1a64: checksum,
        data: std::sync::Arc::new(data),
        ec_parity_fragment: None,
        ec_fragment_digests: None,
        discarded_ranges: Vec::new(),
    };
    PageOutcome::between(
        &PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        },
        None,
        &state,
    )
    .unwrap()
}

#[test]
fn outcome_segments_append_and_cached_reads_stay_bounded_across_rollovers() {
    use matrixobjectstore_rs::durable_payload::{
        outcome_log::DecidedMutation,
        outcome_segments::{SegmentedOutcomeLog, MIN_SEGMENT_BYTES},
    };
    let _test = ALLOCATION_TESTS.lock().unwrap();
    let large = source_outcome(MAX_EXTENT_BYTES);
    let small = source_outcome(4096);
    let mut appends = Vec::new();
    let mut metadata = Vec::new();
    let mut reads = Vec::new();
    for prefix in [4 * 1024 * 1024, 32 * 1024 * 1024, 128 * 1024 * 1024] {
        let directory = directory("segments", prefix);
        let log =
            SegmentedOutcomeLog::open(directory.0.join("outcomes"), MIN_SEGMENT_BYTES).unwrap();
        let mut index = 1000;
        let mut first = None;
        while log.logical_length().unwrap() < prefix as u64 {
            let count = ((prefix as u64 - log.logical_length().unwrap())
                .div_ceil(MAX_EXTENT_BYTES as u64) as usize)
                .clamp(1, 7);
            let outcomes = (0..count)
                .map(|_| {
                    let mut value = large.clone();
                    value.page_index = index;
                    index += 1;
                    value
                })
                .collect::<Vec<_>>();
            let batch = outcomes
                .iter()
                .map(DecidedMutation::Page)
                .collect::<Vec<_>>();
            let record = log.append_decided_batch(&batch).unwrap();
            if first.is_none() {
                first = Some(record.position);
            }
        }
        let extent = log
            .read_metadata(first.as_ref().unwrap())
            .unwrap()
            .batch
            .payloads
            .remove(0);
        let mut warm = small.clone();
        warm.page_index = 1;
        log.append_page(&warm).unwrap();
        log.read_payload_range(&extent, 0, 4096).unwrap();
        let mut value = small.clone();
        value.page_index = 2;
        let (cursor, cursor_calls, cursor_bytes) = measured(|| log.metadata_cursor_after(None));
        drop(cursor.unwrap());
        assert_eq!(
            (cursor_calls, cursor_bytes),
            (0, 0),
            "cursor capture copied the segment index"
        );
        let before = log.segment_count().unwrap();
        let bytes = log.logical_length().unwrap();
        let (record, append_calls, append_bytes) = measured(|| log.append_page(&value));
        let record = record.unwrap();
        let (header, metadata_calls, metadata_bytes) =
            measured(|| log.read_metadata(&record.position));
        header.unwrap();
        let (read, read_calls, read_bytes) = measured(|| log.read_payload_range(&extent, 0, 4096));
        assert_eq!(read.unwrap(), vec![0x31; 4096]);
        assert_eq!(log.segment_count().unwrap(), before);
        assert!(append_bytes < 128 * 1024);
        assert!(metadata_bytes < 64 * 1024);
        assert!(read_bytes < 16 * 1024);
        appends.push(append_bytes);
        metadata.push(metadata_bytes);
        reads.push(read_bytes);
        println!("SEGMENTED_OUTCOME_ALLOCATION prefix_bytes={bytes} segments={before} append_calls={append_calls} append_bytes={append_bytes} metadata_calls={metadata_calls} metadata_bytes={metadata_bytes} read_calls={read_calls} read_bytes={read_bytes}");
    }
    for sizes in [appends, metadata, reads] {
        assert!(sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 4096);
    }
}
