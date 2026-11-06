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

#[test]
fn unchanged_outcome_reuses_page_bytes_and_changed_outcome_is_isolated() {
    use std::sync::Arc;
    let page = PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: 0,
    };
    for size in [64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let mut live = MatrixReplicaService::new();
        live.write_chunk(page.clone(), 0, &vec![7; size]).unwrap();
        let before = live.read_chunk_state(&page).unwrap();
        let mut expected = live.clone();
        expected.write_chunk(page.clone(), 0, &[7]).unwrap();
        let after = expected.read_chunk_state(&page).unwrap();
        let outcome = PageOutcome::between(&page, Some(&before), &after).unwrap();
        assert!(outcome.patches.is_empty());
        let (_, allocated) = measured(|| outcome.apply(&mut live, 1).unwrap());
        let applied = live.read_chunk_state(&page).unwrap();
        assert!(
            Arc::ptr_eq(&before.data, &applied.data),
            "unchanged bytes must stay shared"
        );
        assert_eq!(applied, after);
        // Checksum validation has size-proportional scratch space. Keep enough
        // headroom for that, but not enough for a second full page buffer.
        assert!(
            allocated < size + size / 2,
            "size={size} allocated={allocated}"
        );

        let mut changed = expected.clone();
        changed.write_chunk(page.clone(), 17, &[9]).unwrap();
        let changed_state = changed.read_chunk_state(&page).unwrap();
        let update = PageOutcome::between(&page, Some(&applied), &changed_state).unwrap();
        let saved = live.clone();
        let mut damaged = update.clone();
        damaged.after_checksum.push('x');
        assert!(damaged.apply(&mut live, 1).is_err());
        assert_eq!(live, saved);
        update.apply(&mut live, 1).unwrap();
        let applied_change = live.read_chunk_state(&page).unwrap();
        assert_eq!(applied_change, changed_state);
        assert!(!Arc::ptr_eq(&applied.data, &applied_change.data));
        assert_eq!(applied.data[17], 7);
        changed
            .write_chunk(page.clone(), size as u64 + 63, &[0])
            .unwrap();
        let extended_state = changed.read_chunk_state(&page).unwrap();
        let extension =
            PageOutcome::between(&page, Some(&applied_change), &extended_state).unwrap();
        assert!(extension.patches.is_empty());
        extension.apply(&mut live, 1).unwrap();
        let extended = live.read_chunk_state(&page).unwrap();
        assert_eq!(extended, extended_state);
        assert!(!Arc::ptr_eq(&applied_change.data, &extended.data));
        assert_eq!(applied_change.data.len(), size);
        println!("UNCHANGED_OUTCOME size={size} allocated={allocated}");
    }
}
