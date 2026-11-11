// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "meta-server")]
use matrixobjectstore_rs::{
    cluster::{meta_message::encode_meta_reply, ManifestRouteEntry, MetaReply, MetaRequestCode},
    object_manifest::{ObjectExtent, ObjectManifest},
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

fn reply(pages: u64, width: usize) -> MetaReply {
    let objects = (0..width)
        .map(|object| ManifestRouteEntry {
            publication_fence: 9,
            manifest: ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: format!("object-{object}"),
                generation: 1,
                length: pages,
                extents: (0..pages)
                    .map(|page| ObjectExtent {
                        group: "group-a".into(),
                        volume_id: 1,
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
            },
            routes: vec![],
        })
        .collect::<Vec<_>>();
    if width == 1 {
        let object = objects.into_iter().next().unwrap();
        MetaReply::ManifestRoutes {
            manifest: object.manifest,
            routes: object.routes,
            publication_fence: object.publication_fence,
        }
    } else {
        MetaReply::ManifestRoutesBatch { objects }
    }
}

#[test]
#[ignore = "explicit metadata reply allocation measurement"]
fn measure_manifest_route_encoding_allocations() {
    for pages in [1, 4096] {
        for width in [1, 4] {
            let reply = reply(pages, width);
            let (payload, payload_calls, payload_bytes) =
                measured(|| std::hint::black_box(&reply).encode_payload());
            let (frame, frame_calls, frame_bytes) =
                measured(|| encode_meta_reply(std::hint::black_box(&reply)));
            let code = if width == 1 {
                MetaRequestCode::ReadManifestRoutes
            } else {
                MetaRequestCode::ReadManifestRoutesBatch
            };
            let (decoded, decode_calls, decode_bytes) =
                measured(|| MetaReply::decode(code, &payload).unwrap());
            let length = payload.len();
            assert_eq!(&frame[6..], payload.as_slice());
            assert_eq!(decoded, reply);
            println!("ROUTE_ENCODING_ALLOC pages={pages} width={width} payload_calls={payload_calls} payload_bytes={payload_bytes} frame_calls={frame_calls} frame_bytes={frame_bytes} decode_calls={decode_calls} decode_bytes={decode_bytes} wire_bytes={length}");
        }
    }
}

#[test]
fn manifest_route_replies_keep_one_output_allocation() {
    for pages in [1, 4096] {
        for width in [1, 4] {
            let reply = reply(pages, width);
            let (payload, payload_calls, payload_bytes) = measured(|| reply.encode_payload());
            let (frame, frame_calls, frame_bytes) = measured(|| encode_meta_reply(&reply));
            assert!(payload_calls <= 1 && payload_bytes <= payload.len());
            assert!(frame_calls <= 1 && frame_bytes <= frame.len());
            assert_eq!(&frame[6..], payload.as_slice());
        }
    }
}
