// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "meta-server")]
use matrixobjectstore_rs::{
    cluster::{meta_message::encode_meta_reply, MetaClientOptions, MetaReply, MetaRequest,
        MetaRequestCode, RemoteMetaClient, PROTOCOL_STAMP},
    object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest},
};
use std::{
    alloc::{GlobalAlloc, Layout, System}, cell::Cell, io::{Read, Write},
    net::TcpListener, thread,
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

#[test]
#[ignore = "explicit publication allocation measurement"]
fn publication_allocation_cost_at_full_manifest_width() {
    for pages in [1, 4096] {
        for owned in [false, true] {
            let manifest = |generation, count| ObjectManifest {
                format_version: 2, bucket: "bucket".into(), key: "object".into(),
                generation, length: count, deleted: false,
                extents: (0..count).map(|index| ObjectExtent {
                    group: "group-a".into(), volume_id: 1, chunk_id: 1, page_index: index,
                    object_offset: index, length: 1, write_sequence: 1,
                    data_sha256: vec![1; 32], ..Default::default()
                }).collect(),
            };
            let outcome = ManifestOutcome::decide(Some(manifest(1, pages - 1)), manifest(2, pages)).unwrap();
            let socket = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = socket.local_addr().unwrap().to_string();
            let server = thread::spawn(move || {
                let (mut stream, _) = socket.accept().unwrap();
                let mut header = [0; 6];
                stream.read_exact(&mut header).unwrap();
                assert_eq!(header[0], PROTOCOL_STAMP);
                let mut bytes = vec![0; u32::from_be_bytes(header[2..].try_into().unwrap()) as usize];
                stream.read_exact(&mut bytes).unwrap();
                let request = MetaRequest::decode(MetaRequestCode::from_u8(header[1]).unwrap(), &bytes).unwrap();
                let reply = match request {
                    MetaRequest::PublishManifest { outcome, .. } if !owned =>
                        MetaReply::Manifest(outcome.after.unwrap()),
                    MetaRequest::StagedAllocation(command) if owned => {
                        assert_eq!(command.action, 4);
                        MetaReply::StagedAllocation(matrixobjectstore_rs::staged_allocation::StagedAllocationReply {
                            version: 1, manifest: command.outcome.unwrap().after, ..Default::default()
                        })
                    }
                    _ => panic!("unexpected publication"),
                };
                stream.write_all(&encode_meta_reply(&reply)).unwrap();
            });
            let mut client = RemoteMetaClient::connect(&address, &MetaClientOptions::default()).unwrap();
            COUNT.with(|count| count.set(Some((0, 0))));
            let result = if owned { client.publish_staged_manifest(1, outcome, 0) }
                else { client.publish_manifest(outcome, 0) };
            let (calls, bytes) = COUNT.with(|count| count.replace(None).unwrap());
            let value = result.unwrap();
            assert_eq!(value.extents.len(), pages as usize);
            assert_eq!(value.generation, 2);
            server.join().unwrap();
            println!("PUBLICATION_ALLOC pages={pages} owned={owned} calls={calls} bytes={bytes}");
        }
    }
}
