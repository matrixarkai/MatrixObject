// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(unix, feature = "replica-server"))]

use matrixobjectstore_rs::{cluster::*, ChunkId, PageId};
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

struct Server {
    stop: ReplicaNodeShutdown,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    directory: std::path::PathBuf,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn journal_append_rpc_allocations_do_not_copy_the_page_prefix() {
    for prefix in [16 * 1024, 4 * 1024 * 1024, 64 * 1024 * 1024] {
        let directory = std::env::temp_dir().join(format!(
            "append-allocation-{}-{}",
            std::process::id(),
            prefix
        ));
        // This run owns its newly created directory; never reuse an existing one.
        std::fs::create_dir(&directory).unwrap();
        let node = ReplicaNodeServer::bind_with_journal(
            "127.0.0.1:0",
            ReplicaNodeOptions {
                worker_threads: 2,
                local_recovery_sidecar: false,
                ..Default::default()
            },
            &directory.join("replica.journal"),
        )
        .unwrap();
        let endpoint = node.local_addr().unwrap().to_string();
        let _server = Server {
            stop: node.shutdown_handle(),
            worker: Some(std::thread::spawn(move || node.serve())),
            directory,
        };
        let mut client =
            RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
        let page = PageId {
            chunk_id: ChunkId::new(1, 2).unwrap(),
            page_index: 0,
        };
        let block = vec![0x31; prefix.min(1024 * 1024)];
        for offset in (0..prefix).step_by(block.len()) {
            client
                .write_chunk(page.clone(), offset as u64, &block)
                .unwrap();
        }
        // Establish the validation memo and geometric payload headroom outside
        // measurement, so the regression isolates retained-prefix copying.
        client
            .write_chunk(page.clone(), prefix as u64, &[0x35])
            .unwrap();
        let suffix = [0x7b; 4096];
        let target = page.clone();
        BYTES.store(0, Ordering::Relaxed);
        CALLS.store(0, Ordering::Relaxed);
        ACTIVE.store(true, Ordering::SeqCst);
        let result = client.write_chunk(target, prefix as u64 + 1, &suffix);
        ACTIVE.store(false, Ordering::SeqCst);
        result.unwrap();
        let bytes = BYTES.load(Ordering::Relaxed);
        let calls = CALLS.load(Ordering::Relaxed);
        println!("JOURNAL_APPEND_ALLOC prefix_bytes={prefix} suffix_bytes=4096 allocation_calls={calls} requested_bytes={bytes}");
        // Counts client and server threads in this process, including framing.
        // A payload copy of the 4-MiB prefix alone exceeds this budget eightfold.
        assert!(bytes < 512 * 1024, "prefix-sized RPC allocation: {bytes}");
        assert_eq!(
            client.head_chunk(&page).unwrap().logical_length,
            (prefix + 4097) as u64
        );
        assert_eq!(client.read_chunk(&page, 0, 16).unwrap().data, [0x31; 16]);
        assert_eq!(
            client
                .read_chunk(&page, prefix as u64 + 1, 4096)
                .unwrap()
                .data,
            suffix
        );
    }
}
