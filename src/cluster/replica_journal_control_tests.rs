// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::cluster::{
    RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer,
};
use crate::{ChunkId, PageId};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replica-journal-control-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Running {
    stop: crate::cluster::ReplicaNodeShutdown,
    worker: Option<thread::JoinHandle<std::io::Result<()>>>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap().unwrap();
        }
    }
}
fn run(server: ReplicaNodeServer) -> (Running, RemoteReplicaClient) {
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = thread::spawn(move || server.serve());
    let client = RemoteReplicaClient::connect(&endpoint, &ReplicaClientOptions::default()).unwrap();
    (
        Running {
            stop,
            worker: Some(worker),
        },
        client,
    )
}
#[test]
fn journal_control_tcp_checkpoint_reports_published_epoch_and_recovers_tail() {
    for segmented in [false, true] {
        let dir = Directory::new();
        let path = dir.0.join("journal");
        let options = ReplicaNodeOptions {
            local_recovery_sidecar: false,
            ..Default::default()
        };
        let server = if segmented {
            ReplicaNodeServer::bind_with_segmented_journal("127.0.0.1:0", options.clone(), &path)
        } else {
            ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options.clone(), &path)
        }
        .unwrap();
        let (running, mut client) = run(server);
        let page = PageId {
            chunk_id: ChunkId::new(1, 1).unwrap(),
            page_index: 0,
        };
        client.write_chunk(page.clone(), 0, b"base").unwrap();
        let before = client.journal_status().unwrap();
        assert!(before.bytes_since_checkpoint > 0);
        assert_eq!(before.layout, if segmented { 2 } else { 1 });
        let requested = client.checkpoint_journal().unwrap();
        if segmented {
            assert!(requested.started_epoch > before.published_epoch);
        }
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let status = client.journal_status().unwrap();
            if !status.checkpoint_in_flight && status.checkpoint_is_published(&requested).unwrap() {
                assert_eq!(status.bytes_since_checkpoint, 0);
                break;
            }
            assert!(Instant::now() < until, "checkpoint failed to publish");
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(client.read_chunk(&page, 0, 4).unwrap().data, b"base");
        client.write_chunk(page.clone(), 4, b"tail").unwrap();
        assert!(client.journal_status().unwrap().bytes_since_checkpoint > 0);
        drop(client);
        drop(running);
        let server = if segmented {
            ReplicaNodeServer::bind_with_segmented_journal("127.0.0.1:0", options, &path)
        } else {
            ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options, &path)
        }
        .unwrap();
        let (_running, mut client) = run(server);
        assert_eq!(client.read_chunk(&page, 0, 8).unwrap().data, b"basetail");
        let restored_status = client.journal_status().unwrap();
        if segmented {
            assert_eq!(restored_status.journal_id, requested.journal_id);
            assert!(restored_status.checkpoint_is_published(&requested).unwrap());
            assert!(restored_status.bytes_since_checkpoint > 0);
        } else {
            assert_ne!(restored_status.journal_id, requested.journal_id);
            assert!(restored_status.checkpoint_is_published(&requested).is_err());
        }
    }
}
#[test]
fn journal_control_tcp_negotiates_maintenance_without_mutating_status_reads() {
    for segmented in [false, true] {
        let dir = Directory::new();
        let path = dir.0.join("journal");
        let options = ReplicaNodeOptions {
            local_recovery_sidecar: false,
            ..Default::default()
        };
        let server = if segmented {
            ReplicaNodeServer::bind_with_segmented_journal("127.0.0.1:0", options, &path)
        } else {
            ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options, &path)
        }
        .unwrap();
        let (_running, mut client) = run(server);
        let page = PageId {
            chunk_id: ChunkId::new(1, 3).unwrap(),
            page_index: 0,
        };
        client.write_chunk(page.clone(), 0, b"durable").unwrap();
        let v1 = client.journal_status().unwrap();
        let v2 = client.journal_maintenance_status().unwrap();
        assert_eq!(v1.version, 1);
        assert_eq!(v2.version, 2);
        let mut downgrade = v2.clone();
        downgrade.version = 1;
        assert_eq!(v1, downgrade);
        for _ in 0..5 {
            assert_eq!(client.journal_maintenance_status().unwrap(), v2);
            assert_eq!(client.journal_status().unwrap(), v1);
        }
        let target = client.checkpoint_journal_with_maintenance().unwrap();
        assert_eq!(target.version, 2);
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let status = client.journal_maintenance_status().unwrap();
            if status.maintenance_is_complete(&target).unwrap() {
                break;
            }
            assert!(Instant::now() < until, "maintenance failed to finish");
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(client.journal_status().unwrap().version, 1);
        assert_eq!(client.read_chunk(&page, 0, 7).unwrap().data, b"durable");
    }
}
#[test]
fn journal_control_memory_refusal_does_not_fence_ordinary_requests() {
    let (_running, mut client) =
        run(ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap());
    assert!(client.journal_status().is_err());
    assert!(client.checkpoint_journal().is_err());
    assert_eq!(client.ping().unwrap(), 0);
}
#[test]
fn journal_control_tcp_write_statistics_preserve_old_status_and_reset_after_reopen() {
    for segmented in [false, true] {
        let dir = Directory::new();
        let path = dir.0.join("statistics-journal");
        let start = || {
            let options = ReplicaNodeOptions {
                local_recovery_sidecar: false,
                ..Default::default()
            };
            if segmented {
                ReplicaNodeServer::bind_with_segmented_journal("127.0.0.1:0", options, &path)
            } else {
                ReplicaNodeServer::bind_with_journal("127.0.0.1:0", options, &path)
            }
            .unwrap()
        };
        let (running, mut client) = run(start());
        let before = client.journal_write_statistics().unwrap();
        assert_eq!(before.version, 3);
        assert_eq!(before.write_statistics.is_some(), segmented);
        let page = PageId {
            chunk_id: ChunkId::new(1, 99).unwrap(),
            page_index: 0,
        };
        client
            .write_chunk(page.clone(), 0, &vec![0x33; 4096])
            .unwrap();
        let after = client.journal_write_statistics().unwrap();
        if segmented {
            let delta = after
                .write_statistics
                .as_ref()
                .unwrap()
                .checked_delta(before.write_statistics.as_ref().unwrap())
                .unwrap();
            assert_eq!(delta.foreground_batches, 1);
            assert_eq!(delta.foreground_decisions, 1);
            assert_eq!(delta.foreground_payload_bytes, 4096);
            assert_eq!(delta.foreground_record_bytes, 8192);
        }
        assert!(client.journal_status().unwrap().write_statistics.is_none());
        assert!(client
            .journal_maintenance_status()
            .unwrap()
            .write_statistics
            .is_none());
        let requested = client.checkpoint_journal_with_write_statistics().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let status = client.journal_write_statistics().unwrap();
            if status.maintenance_is_complete(&requested).unwrap() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        drop(client);
        drop(running);
        let (running, mut client) = run(start());
        let reopened = client.journal_write_statistics().unwrap();
        if segmented {
            assert_eq!(reopened.journal_id, after.journal_id);
            let reset = reopened.write_statistics.as_ref().unwrap();
            assert_eq!(reset.foreground_batches, 0);
            assert!(reset
                .checked_delta(after.write_statistics.as_ref().unwrap())
                .is_err());
        }
        assert_eq!(
            client.read_chunk(&page, 0, 4096).unwrap().data,
            vec![0x33; 4096]
        );
        drop(client);
        drop(running);
    }
}
