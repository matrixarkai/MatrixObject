// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use matrixobjectstore_rs::object_manifest::{
    ManifestCatalog, ManifestOutcome, ObjectExtent, ObjectManifest,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

thread_local! {
    static COUNT: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}
struct Allocator;
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn record(size: usize) {
    let _ = COUNT.try_with(|count| {
        if let Some((calls, bytes)) = count.get() {
            count.set(Some((calls + 1, bytes + size)));
        }
    });
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

fn measured<T>(body: impl FnOnce() -> T) -> (T, usize, usize) {
    COUNT.with(|count| count.set(Some((0, 0))));
    let value = body();
    let (calls, bytes) = COUNT.with(|count| count.replace(None).unwrap());
    (value, calls, bytes)
}

fn catalog(pages: u64, width: usize) -> ManifestCatalog {
    let mut catalog = ManifestCatalog::default();
    for object in 0..width {
        let manifest = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: format!("object-{object:02}"),
            generation: 1,
            length: pages,
            extents: (0..pages)
                .map(|page| ObjectExtent {
                    group: "data-group".into(),
                    chunk_id: 1,
                    page_index: page,
                    object_offset: page,
                    length: 1,
                    write_sequence: 1,
                    data_sha256: vec![7; 32],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        catalog
            .apply(&ManifestOutcome::decide(None, manifest).unwrap())
            .unwrap();
    }
    catalog
}

#[test]
#[ignore = "explicit manifest catalog clone allocation measurement"]
fn measure_manifest_catalog_clone_allocations() {
    for width in [1, 16] {
        for pages in [1, 4096] {
            let catalog = catalog(pages, width);
            let (copy, calls, bytes) = measured(|| catalog.clone());
            assert_eq!(copy, catalog);
            assert_eq!(copy.to_bytes().unwrap(), catalog.to_bytes().unwrap());
            println!("CATALOG_CLONE_ALLOC pages={pages} objects={width} allocation_calls={calls} allocated_bytes={bytes}");
        }
    }
}

#[test]
fn manifest_catalog_clones_do_not_copy_extent_lists() {
    for width in [1, 16] {
        let small = catalog(1, width);
        let large = catalog(4096, width);
        let (small_copy, small_calls, small_bytes) = measured(|| small.clone());
        let (large_copy, large_calls, large_bytes) = measured(|| large.clone());
        assert_eq!(small, small_copy);
        assert_eq!(large, large_copy);
        assert_eq!(
            (large_calls, large_bytes),
            (small_calls, small_bytes),
            "extent lists were copied"
        );
    }
}

#[test]
fn shared_catalog_clones_keep_current_and_prior_generations_independent() {
    let mut catalog = catalog(4095, 1);
    let prior = catalog.clone();
    let id = matrixobjectstore_rs::ObjectId::new("bucket", "object-00");
    let before = catalog.get(&id).unwrap().clone();
    let mut after = before.clone();
    after.generation += 1;
    after.length += 1;
    after.extents.push(ObjectExtent {
        page_index: 4095,
        object_offset: 4095,
        ..before.extents[0].clone()
    });
    catalog
        .apply(&ManifestOutcome::decide(Some(before.clone()), after.clone()).unwrap())
        .unwrap();
    assert_eq!(prior.get(&id), Some(&before));
    assert_eq!(catalog.get(&id), Some(&after));
    assert_eq!(
        ManifestCatalog::from_bytes(&prior.to_bytes().unwrap()).unwrap(),
        prior
    );
    assert_eq!(
        ManifestCatalog::from_bytes(&catalog.to_bytes().unwrap()).unwrap(),
        catalog
    );
}
