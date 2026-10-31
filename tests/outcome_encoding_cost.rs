// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use matrixobjectstore_rs::{
    outcome::{BytePatch, PageOutcome, MAX_OUTCOME_BYTES, OUTCOME_MAGIC},
    ChunkId, MatrixReplicaService, PageId,
};
use prost::Message;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATED: Cell<Option<usize>> = const { Cell::new(None) };
}
struct CountingAllocator;
fn count(bytes: usize) {
    let _ = ALLOCATED.try_with(|counter| {
        if let Some(total) = counter.get() {
            counter.set(Some(total + bytes));
        }
    });
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measured<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    ALLOCATED.with(|counter| counter.set(Some(0)));
    let result = operation();
    let bytes = ALLOCATED.with(|counter| counter.replace(None).unwrap());
    (result, bytes)
}

#[test]
fn outcome_encoding_avoids_discarded_payloads_and_preserves_limits() {
    let page = PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: 0,
    };
    for size in [64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let mut service = MatrixReplicaService::new();
        service
            .write_chunk(page.clone(), 0, &vec![7; size])
            .unwrap();
        let after = service.read_chunk_state(&page).unwrap();
        let (outcome, construction_bytes) =
            measured(|| PageOutcome::between(&page, None, &after).unwrap());
        // Retain one full patch and allow checksum scratch space, but not a
        // second full payload serialized only for a size check.
        assert!(
            construction_bytes < size + size / 8 + 16384,
            "size={size} allocated={construction_bytes}"
        );
        let (payload, encoding_bytes) = measured(|| outcome.to_payload().unwrap());
        assert_eq!(encoding_bytes, payload.len());
        let mut expected = OUTCOME_MAGIC.to_vec();
        outcome.encode(&mut expected).unwrap();
        assert_eq!(payload, expected);
        assert_eq!(PageOutcome::from_payload(&payload).unwrap(), outcome);
        let mut recovered = MatrixReplicaService::new();
        outcome.apply(&mut recovered, 1).unwrap();
        assert_eq!(recovered.read_chunk_state(&page).unwrap().data, after.data);
        println!("OUTCOME size={size} construction_bytes={construction_bytes} encoding_bytes={encoding_bytes}");
    }
    let mut boundary = PageOutcome {
        version: 1,
        patches: vec![BytePatch {
            offset: 0,
            data: vec![7; MAX_OUTCOME_BYTES],
        }],
        ..Default::default()
    };
    let excess = boundary.encoded_len() + OUTCOME_MAGIC.len() - MAX_OUTCOME_BYTES;
    boundary.patches[0]
        .data
        .truncate(MAX_OUTCOME_BYTES - excess);
    assert_eq!(boundary.to_payload().unwrap().len(), MAX_OUTCOME_BYTES);
    boundary.patches[0].data.push(7);
    assert!(boundary.to_payload().is_err());
    let mut service = MatrixReplicaService::new();
    service
        .write_chunk(page.clone(), 0, &vec![7; MAX_OUTCOME_BYTES])
        .unwrap();
    let oversized = service.read_chunk_state(&page).unwrap();
    assert!(PageOutcome::between(&page, None, &oversized).is_err());
}
