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

struct Directory(std::path::PathBuf);
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
    let mut append_sizes = Vec::new();
    let mut read_sizes = Vec::new();
    for prefix in [4 * 1024 * 1024, 32 * 1024 * 1024, 128 * 1024 * 1024] {
        let directory = Directory(std::env::temp_dir().join(format!(
            "payload-allocation-{}-{prefix}",
            std::process::id()
        )));
        std::fs::create_dir(&directory.0).unwrap();
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
