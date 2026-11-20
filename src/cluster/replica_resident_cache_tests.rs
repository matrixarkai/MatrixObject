// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::{ChunkId, PageId};
use std::{
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "replica-resident-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn journal(&self) -> PathBuf {
        self.0.join("outcomes")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn page(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(1, 9).unwrap(),
        page_index: index,
    }
}
fn options(budget: u64, entries: usize, capacity: u64, pages: usize) -> ReplicaNodeOptions {
    ReplicaNodeOptions {
        resident_payload_bytes: budget,
        resident_page_entries: entries,
        local_recovery_sidecar: false,
        capacity_bytes: capacity,
        max_pages: pages,
        ..Default::default()
    }
}
fn server(
    path: &Path,
    budget: u64,
    entries: usize,
    capacity: u64,
    pages: usize,
) -> ReplicaNodeServer {
    ReplicaNodeServer::bind_with_segmented_journal(
        "127.0.0.1:0",
        options(budget, entries, capacity, pages),
        path,
    )
    .unwrap()
}
fn write(
    node: &Arc<Mutex<NodeReplica>>,
    index: u64,
    offset: u64,
    data: &[u8],
) -> Result<ReplicaReply, ObjectError> {
    apply(
        node,
        ReplicaRequest::WriteChunk {
            page_id: page(index),
            offset,
            data: data.to_vec(),
        },
        MAX_PAYLOAD_BYTES,
    )
}
fn read(
    node: &Arc<Mutex<NodeReplica>>,
    index: u64,
    offset: u64,
    length: u64,
) -> crate::ReplicaReadResult {
    match apply(
        node,
        ReplicaRequest::ReadChunk {
            page_id: page(index),
            offset,
            length,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::Read(read) => read,
        other => panic!("unexpected reply {other:?}"),
    }
}
fn assert_bound(node: &Arc<Mutex<NodeReplica>>, bytes: u64, entries: usize) {
    let node = node.lock().unwrap();
    let capacities: u64 = node
        .service
        .pages_local()
        .values()
        .map(|p| p.data.capacity() as u64)
        .sum();
    assert!(
        capacities <= bytes,
        "resident capacities {capacities} exceed {bytes}"
    );
    assert!(node.service.page_count() <= entries);
}
fn checkpoint(node: &Arc<Mutex<NodeReplica>>) {
    let mut node = node.lock().unwrap();
    let NodeReplica {
        service, journal, ..
    } = &mut *node;
    let journal = journal.as_mut().unwrap();
    journal.checkpoint_requested(service).unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    while journal.status().unwrap().checkpoint_in_flight {
        journal.check_health().unwrap();
        assert!(Instant::now() < until, "checkpoint did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    journal.check_health().unwrap();
}
#[test]
fn resident_cache_bounds_allocations_and_enforces_global_capacity_and_inventory() {
    let dir = Directory::new();
    let server = server(&dir.journal(), 512, 2, 16 * 1024, 2);
    let node = &server.replica;
    write(node, 0, 0, &vec![0x51; 8192]).unwrap();
    write(node, 1, 0, &vec![0x72; 8192]).unwrap();
    assert_bound(node, 512, 2);
    assert_eq!(node.lock().unwrap().service.page_count(), 0);
    assert_eq!(node.lock().unwrap().used_bytes.get(), 16384);
    assert_eq!(
        apply(node, ReplicaRequest::Ping, MAX_PAYLOAD_BYTES).unwrap(),
        ReplicaReply::Pong { page_count: 2 }
    );
    match apply(
        node,
        ReplicaRequest::PageInventory {
            after: None,
            limit: 1,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::PageInventory(value) => {
            assert_eq!(value.entries, vec![(page(0), 1)]);
            assert!(value.has_more);
        }
        other => panic!("{other:?}"),
    }
    match apply(
        node,
        ReplicaRequest::PageInventory {
            after: Some(page(0)),
            limit: 1,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::PageInventory(value) => {
            assert_eq!(value.entries, vec![(page(1), 1)]);
            assert!(!value.has_more);
        }
        other => panic!("{other:?}"),
    }
    assert!(write(node, 2, 0, b"").is_err());
    assert!(write(node, 0, 8192, b"x").is_err());
    assert_bound(node, 512, 2);
    assert_eq!(read(node, 0, 0, 8192).data, vec![0x51; 8192]);
    assert_eq!(node.lock().unwrap().used_bytes.get(), 16384);
    assert!(!node.lock().unwrap().health.failed());
}
#[test]
fn resident_cache_cold_ranges_keep_their_version_without_holding_the_node_lock() {
    let dir = Directory::new();
    let server = server(&dir.journal(), 512, 2, 65536, 32);
    let node = &server.replica;
    write(node, 0, 0, &vec![0x51; 8192]).unwrap();
    let captured = {
        let mut guard = node.lock().unwrap();
        cold_reads::capture(
            &mut guard,
            &ReplicaRequest::ReadChunk {
                page_id: page(0),
                offset: 4094,
                length: 8,
            },
            MAX_PAYLOAD_BYTES,
        )
        .unwrap()
        .unwrap()
    };
    write(node, 0, 4096, b"new").unwrap();
    let guard = node.lock().unwrap();
    let old = match captured.finish().unwrap() {
        ReplicaReply::Read(read) => read,
        other => panic!("{other:?}"),
    };
    assert_eq!(old.data, vec![0x51; 8]);
    assert_eq!(old.write_sequence, 1);
    drop(guard);
    let current = read(node, 0, 4094, 8);
    assert_eq!(current.data, b"QQnewQQQ");
    assert_eq!(current.write_sequence, 2);
    match apply(
        node,
        ReplicaRequest::DigestChunk {
            page_id: page(0),
            offset: 4094,
            length: 8,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::Digest {
            write_sequence,
            length,
            sha256,
        } => {
            assert_eq!((write_sequence, length), (2, 8));
            assert_eq!(
                sha256.as_slice(),
                ring::digest::digest(&ring::digest::SHA256, b"QQnewQQQ").as_ref()
            );
        }
        other => panic!("{other:?}"),
    }
    assert_bound(node, 512, 2);
}
#[test]
fn resident_cache_empty_pages_obey_entry_limits_and_survive_metadata_only_restart() {
    let dir = Directory::new();
    let path = dir.journal();
    {
        let server = server(&path, 512, 2, 65536, 6);
        for index in 0..6 {
            write(&server.replica, index, 0, b"").unwrap();
            assert_bound(&server.replica, 512, 2);
        }
        assert!(write(&server.replica, 6, 0, b"").is_err());
        checkpoint(&server.replica);
    }
    let server = server(&path, 512, 2, 65536, 6);
    let node = server.replica.lock().unwrap();
    assert_eq!(node.service.page_count(), 0);
    assert_eq!(node.global_page_count(), 6);
    assert_eq!(node.used_bytes.get(), 0);
    for index in 0..6 {
        assert_eq!(node.page_head(&page(index)).unwrap().write_sequence, 1);
    }
}
#[test]
fn resident_cache_cold_reads_and_raw_transfer_preserve_discard_and_head_checksums() {
    let dir = Directory::new();
    let server = server(&dir.journal(), 512, 2, 65536, 32);
    let node = &server.replica;
    let mut expected = MatrixReplicaService::new();
    expected.set_keep_ec_sidecar(false);
    expected.write_chunk(page(0), 0, &vec![0x51; 8192]).unwrap();
    write(node, 0, 0, &vec![0x51; 8192]).unwrap();
    expected.discard_chunk(&page(0), 100, 20).unwrap();
    apply(
        node,
        ReplicaRequest::DiscardChunk {
            page_id: page(0),
            offset: 100,
            length: 20,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap();
    let head = expected.head_chunk(&page(0)).unwrap();
    assert_eq!(node.lock().unwrap().page_head(&page(0)).unwrap(), head);
    assert!(matches!(
        apply(
            node,
            ReplicaRequest::ReadChunk {
                page_id: page(0),
                offset: 99,
                length: 22
            },
            MAX_PAYLOAD_BYTES
        ),
        Err(ObjectError::Corruption(_))
    ));
    assert!(matches!(
        apply(
            node,
            ReplicaRequest::ReadChunk {
                page_id: page(0),
                offset: 8192,
                length: 1
            },
            MAX_PAYLOAD_BYTES
        ),
        Err(ObjectError::InvalidArgument(_))
    ));
    match apply(
        node,
        ReplicaRequest::ReadChunkStateRange {
            page_id: page(0),
            expected_state_checksum: head.state_checksum.clone(),
            offset: 99,
            length: 22,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::StateRange(range) => {
            assert_eq!(
                range.data,
                expected.read_chunk_state(&page(0)).unwrap().data[99..121]
            );
            assert_eq!(range.state_checksum, head.state_checksum);
        }
        other => panic!("{other:?}"),
    }
    assert!(apply(
        node,
        ReplicaRequest::ReadChunkStateRange {
            page_id: page(0),
            expected_state_checksum: "changed".into(),
            offset: 0,
            length: 1
        },
        MAX_PAYLOAD_BYTES
    )
    .is_err());
}
#[test]
fn resident_cache_repair_replaces_corrupt_cold_data_and_keeps_siblings_after_restart() {
    use crate::cluster::state_install::StateInstall;
    use std::os::unix::fs::FileExt;
    let dir = Directory::new();
    let path = dir.journal();
    let mut expected = MatrixReplicaService::new();
    expected.set_keep_ec_sidecar(false);
    expected.write_chunk(page(0), 0, &vec![0x51; 8192]).unwrap();
    expected.write_chunk(page(1), 0, &vec![0x72; 8192]).unwrap();
    {
        let server = server(&path, 512, 2, 65536, 32);
        let node = &server.replica;
        write(node, 0, 0, &vec![0x51; 8192]).unwrap();
        write(node, 1, 0, &vec![0x72; 8192]).unwrap();
        checkpoint(node);
        let prior = expected.head_chunk(&page(0)).unwrap();
        let extent = {
            let guard = node.lock().unwrap();
            let catalog = guard
                .resident
                .as_ref()
                .unwrap()
                .backing
                .catalog_snapshot()
                .unwrap();
            catalog.pages[&page(0)].spans[0].extent.clone()
        };
        let mut found = false;
        for entry in std::fs::read_dir(&dir.0).unwrap() {
            let file = entry.unwrap().path();
            if !file
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".segment-")
            {
                continue;
            }
            if std::fs::metadata(&file).unwrap().len() < 4096 {
                continue;
            }
            let mut header = [0; 4096];
            std::fs::File::open(&file)
                .unwrap()
                .read_exact_at(&mut header, 0)
                .unwrap();
            if !header
                .windows(extent.store_id.len())
                .any(|bytes| bytes == extent.store_id)
            {
                continue;
            }
            let file = std::fs::OpenOptions::new().write(true).open(file).unwrap();
            file.write_all_at(&[0x19], extent.offset + 3).unwrap();
            file.sync_data().unwrap();
            found = true;
            break;
        }
        assert!(found);
        assert!(matches!(
            apply(
                node,
                ReplicaRequest::ReadChunk {
                    page_id: page(0),
                    offset: 0,
                    length: 8
                },
                MAX_PAYLOAD_BYTES
            ),
            Err(ObjectError::Corruption(_))
        ));
        expected.write_chunk(page(0), 0, &vec![0x33; 8192]).unwrap();
        let incoming = expected.head_chunk(&page(0)).unwrap();
        let token = "cold-page-repair".to_string();
        apply(
            node,
            ReplicaRequest::StateInstall(StateInstall::begin(
                token.clone(),
                &page(0),
                Some(&prior.state_checksum),
                &incoming,
                8192,
            )),
            MAX_PAYLOAD_BYTES,
        )
        .unwrap();
        let mut append = StateInstall::command(2, token.clone());
        append.data = vec![0x33; 8192];
        apply(
            node,
            ReplicaRequest::StateInstall(append),
            MAX_PAYLOAD_BYTES,
        )
        .unwrap();
        apply(
            node,
            ReplicaRequest::StateInstall(StateInstall::command(3, token)),
            MAX_PAYLOAD_BYTES,
        )
        .unwrap();
        assert_eq!(read(node, 0, 0, 8192).data, vec![0x33; 8192]);
        assert_eq!(read(node, 1, 0, 8192).data, vec![0x72; 8192]);
        assert_bound(node, 512, 2);
        assert_eq!(node.lock().unwrap().used_bytes.get(), 16384);
    }
    let server = server(&path, 512, 2, 65536, 32);
    let node = &server.replica;
    assert_eq!(node.lock().unwrap().service.page_count(), 0);
    assert_eq!(node.lock().unwrap().used_bytes.get(), 16384);
    assert_eq!(read(node, 0, 0, 8192).data, vec![0x33; 8192]);
    assert_eq!(read(node, 1, 0, 8192).data, vec![0x72; 8192]);
    assert_eq!(
        node.lock().unwrap().page_head(&page(0)).unwrap(),
        expected.head_chunk(&page(0)).unwrap()
    );
}
#[test]
fn resident_cache_rejects_incompatible_storage_and_recovery_modes() {
    let dir = Directory::new();
    assert!(ReplicaNodeServer::bind("127.0.0.1:0", options(512, 2, 65536, 32)).is_err());
    assert!(ReplicaNodeServer::bind_with_journal(
        "127.0.0.1:0",
        options(512, 2, 65536, 32),
        &dir.journal()
    )
    .is_err());
    let mut sidecar = options(512, 2, 65536, 32);
    sidecar.local_recovery_sidecar = true;
    assert!(
        ReplicaNodeServer::bind_with_segmented_journal("127.0.0.1:0", sidecar, &dir.journal())
            .is_err()
    );
    assert!(ReplicaNodeServer::bind_with_segmented_journal(
        "127.0.0.1:0",
        options(512, 0, 65536, 32),
        &dir.journal()
    )
    .is_err());
}

#[test]
fn resident_cache_mixed_batches_keep_hot_reads_recent_and_leave_cold_pages_unloaded() {
    let dir = Directory::new();
    let server = server(&dir.journal(), 20 * 1024, 2, 65536, 16);
    let node = &server.replica;
    write(node, 0, 0, &vec![0x41; 8192]).unwrap();
    write(node, 1, 0, &vec![0x42; 8192]).unwrap();
    assert_eq!(read(node, 0, 0, 4).data, b"AAAA");
    write(node, 2, 0, &vec![0x43; 8192]).unwrap();
    {
        let guard = node.lock().unwrap();
        assert!(guard.service.page_write_sequence(&page(0)).is_some());
        assert!(guard.service.page_write_sequence(&page(1)).is_none());
        assert!(guard.service.page_write_sequence(&page(2)).is_some());
        assert_eq!(guard.global_page_count(), 3);
    }
    let reads: Vec<_> = (0..3).map(|index| (page(index), 1, 7)).collect();
    match apply(
        node,
        ReplicaRequest::ReadChunkBatch {
            reads: reads.clone(),
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::Reads(values) => {
            assert_eq!(values.len(), 3);
            for (index, value) in values.iter().enumerate() {
                assert_eq!(value.data, vec![0x41 + index as u8; 7]);
                assert_eq!(value.write_sequence, 1);
            }
        }
        other => panic!("{other:?}"),
    }
    match apply(
        node,
        ReplicaRequest::DigestChunkBatch {
            reads: reads.clone(),
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::Digests(values) => {
            assert_eq!(values.len(), 3);
            for (index, (sequence, length, digest)) in values.iter().enumerate() {
                assert_eq!((*sequence, *length), (1, 7));
                assert_eq!(
                    digest.as_slice(),
                    ring::digest::digest(&ring::digest::SHA256, &vec![0x41 + index as u8; 7])
                        .as_ref()
                );
            }
        }
        other => panic!("{other:?}"),
    }
    assert!(apply(node, ReplicaRequest::ReadChunkBatch { reads }, 128).is_err());
    assert!(node
        .lock()
        .unwrap()
        .service
        .page_write_sequence(&page(1))
        .is_none());
    assert_bound(node, 20 * 1024, 2);
}

#[test]
fn resident_cache_cold_conditional_appends_preserve_digests_and_refusal_bounds() {
    let dir = Directory::new();
    let server = server(&dir.journal(), 512, 2, 65536, 16);
    let node = &server.replica;
    write(node, 0, 0, &vec![0x51; 8192]).unwrap();
    match apply(
        node,
        ReplicaRequest::WriteChunkIfSequenceDigest {
            page_id: page(0),
            offset: 8192,
            data: b"tail".to_vec(),
            expected_sequence: 1,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap()
    {
        ReplicaReply::Digest {
            write_sequence,
            length,
            sha256,
        } => {
            assert_eq!((write_sequence, length), (2, 4));
            assert_eq!(
                sha256.as_slice(),
                ring::digest::digest(&ring::digest::SHA256, b"tail").as_ref()
            );
        }
        other => panic!("{other:?}"),
    }
    assert_bound(node, 512, 2);
    assert_eq!(read(node, 0, 8190, 6).data, b"QQtail");
    assert!(matches!(
        apply(
            node,
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page(0),
                offset: 8196,
                data: b"x".to_vec(),
                expected_sequence: 1
            },
            MAX_PAYLOAD_BYTES
        ),
        Err(ObjectError::PreconditionFailed(_))
    ));
    assert_bound(node, 512, 2);
    assert_eq!(node.lock().unwrap().used_bytes.get(), 8196);
    let head = node.lock().unwrap().page_head(&page(0)).unwrap();
    assert_eq!(head.write_sequence, 2);
    apply(
        node,
        ReplicaRequest::WriteChunkIfState {
            page_id: page(0),
            offset: 8196,
            data: b"x".to_vec(),
            expected_sequence: 2,
            expected_state_checksum: head.state_checksum,
        },
        MAX_PAYLOAD_BYTES,
    )
    .unwrap();
    assert_bound(node, 512, 2);
    assert_eq!(read(node, 0, 8190, 7).data, b"QQtailx");
    assert_eq!(node.lock().unwrap().used_bytes.get(), 8197);
}

fn queue_resident_write(
    node: &mut NodeReplica,
    index: u64,
    offset: u64,
    sequence: u64,
    data: &[u8],
) {
    assert_eq!(
        apply_locked(
            node,
            ReplicaRequest::WriteChunkIfSequence {
                page_id: page(index),
                offset,
                expected_sequence: sequence,
                data: data.to_vec(),
            },
            MAX_PAYLOAD_BYTES,
            false
        )
        .unwrap(),
        ReplicaReply::Applied
    );
}

#[test]
fn resident_cache_clean_eviction_preserves_deferred_write_groups() {
    for (length, budget) in [(4096usize, 8192u64), (0, 1)] {
        let dir = Directory::new();
        let running = server(&dir.journal(), budget, 2, 65536, 8);
        let payload = vec![0x51; length];
        for index in 0..2 {
            write(&running.replica, index, 0, &payload).unwrap();
        }
        assert_bound(&running.replica, budget, 2);
        {
            let mut node = running.replica.lock().unwrap();
            let synced = node.journal.as_ref().unwrap().sync_count();
            queue_resident_write(&mut node, 2, 0, 0, &payload);
            queue_resident_write(&mut node, 3, 0, 0, &payload);
            assert_eq!(node.journal.as_ref().unwrap().sync_count(), synced);
            assert_eq!(
                node.journal
                    .as_ref()
                    .unwrap()
                    .status()
                    .unwrap()
                    .pending_records,
                2
            );
            assert!(node.service.page_write_sequence(&page(0)).is_none());
            assert!(node.service.page_write_sequence(&page(1)).is_none());
            let backing = &node.resident.as_ref().unwrap().backing;
            assert!(backing.metadata(&page(0)).unwrap().is_some());
            assert!(backing.metadata(&page(2)).unwrap().is_none());
            finish_deferred_journal(&mut node).unwrap();
            assert_eq!(node.journal.as_ref().unwrap().sync_count(), synced + 1);
            assert_eq!(node.global_page_count(), 4);
            assert_eq!(node.used_bytes.get(), (length * 4) as u64);
        }
        assert_bound(&running.replica, budget, 2);
        for index in 0..4 {
            assert_eq!(
                read(&running.replica, index, 0, length as u64).data,
                payload
            );
        }
        drop(running);
        let recovered = server(&dir.journal(), budget, 2, 65536, 8);
        for index in 0..4 {
            let value = read(&recovered.replica, index, 0, length as u64);
            assert_eq!(value.data, payload);
            assert_eq!(value.write_sequence, 1);
        }
        assert_eq!(recovered.replica.lock().unwrap().global_page_count(), 4);
    }
}

#[test]
fn resident_cache_pending_victim_synchronizes_before_eviction() {
    let dir = Directory::new();
    let running = server(&dir.journal(), 8192, 2, 65536, 8);
    let payload = vec![0x51; 4096];
    for index in 0..2 {
        write(&running.replica, index, 0, &payload).unwrap();
    }
    {
        let mut node = running.replica.lock().unwrap();
        let synced = node.journal.as_ref().unwrap().sync_count();
        queue_resident_write(&mut node, 0, 4096, 1, b"x");
        assert_eq!(node.journal.as_ref().unwrap().sync_count(), synced);
        assert!(node.service.page_write_sequence(&page(1)).is_none());
        assert_eq!(node.service.page_write_sequence(&page(0)), Some(2));
        queue_resident_write(&mut node, 2, 0, 0, &payload);
        assert_eq!(node.journal.as_ref().unwrap().sync_count(), synced + 1);
        assert_eq!(
            node.journal
                .as_ref()
                .unwrap()
                .status()
                .unwrap()
                .pending_records,
            0
        );
        assert!(node.service.page_write_sequence(&page(0)).is_none());
        assert_eq!(
            node.resident
                .as_ref()
                .unwrap()
                .backing
                .metadata(&page(0))
                .unwrap()
                .unwrap()
                .write_sequence,
            2
        );
        assert!(!node.health.failed());
    }
    assert_bound(&running.replica, 8192, 2);
    drop(running);
    let recovered = server(&dir.journal(), 8192, 2, 65536, 8);
    let mut extended = payload.clone();
    extended.push(b'x');
    assert_eq!(read(&recovered.replica, 0, 0, 4097).data, extended);
    for index in 1..3 {
        assert_eq!(read(&recovered.replica, index, 0, 4096).data, payload);
    }
}

#[test]
fn resident_cache_cold_admission_keeps_unrelated_writes_grouped() {
    let dir = Directory::new();
    let running = server(&dir.journal(), 12288, 3, 65536, 8);
    let payload = vec![0x61; 4096];
    for index in 0..3 {
        write(&running.replica, index, 0, &payload).unwrap();
    }
    {
        let mut node = running.replica.lock().unwrap();
        let synced = node.journal.as_ref().unwrap().sync_count();
        queue_resident_write(&mut node, 3, 0, 0, &payload);
        assert!(node.service.page_write_sequence(&page(0)).is_none());
        queue_resident_write(&mut node, 0, 4096, 1, b"z");
        assert_eq!(node.journal.as_ref().unwrap().sync_count(), synced);
        assert_eq!(
            node.journal
                .as_ref()
                .unwrap()
                .status()
                .unwrap()
                .pending_records,
            2
        );
        assert!(node.service.page_write_sequence(&page(1)).is_none());
        assert!(node.service.page_write_sequence(&page(2)).is_none());
        assert_eq!(node.service.page_write_sequence(&page(0)), Some(2));
        finish_deferred_journal(&mut node).unwrap();
        assert_eq!(node.journal.as_ref().unwrap().sync_count(), synced + 1);
        assert_eq!(node.global_page_count(), 4);
    }
    assert_bound(&running.replica, 12288, 3);
    drop(running);
    let recovered = server(&dir.journal(), 12288, 3, 65536, 8);
    let mut extended = payload.clone();
    extended.push(b'z');
    assert_eq!(read(&recovered.replica, 0, 0, 4097).data, extended);
    for index in 1..4 {
        assert_eq!(read(&recovered.replica, index, 0, 4096).data, payload);
    }
}

#[test]
fn resident_cache_strict_trim_fences_unrecorded_versions() {
    let dir = Directory::new();
    let running = server(&dir.journal(), 4096, 1, 65536, 8);
    let payload = vec![0x71; 4096];
    write(&running.replica, 0, 0, &payload).unwrap();
    {
        let mut node = running.replica.lock().unwrap();
        // Inject a service mutation that has no matching journal outcome.
        node.service.write_chunk(page(0), 4096, b"x").unwrap();
        node.track_resident_mutation(&page(0), true).unwrap();
        assert!(node.trim_residency().is_err());
        assert!(node.health.failed());
        assert_eq!(node.service.page_write_sequence(&page(0)), Some(2));
        assert_eq!(
            node.resident
                .as_ref()
                .unwrap()
                .backing
                .metadata(&page(0))
                .unwrap()
                .unwrap()
                .write_sequence,
            1
        );
    }
    drop(running);
    let recovered = server(&dir.journal(), 4096, 1, 65536, 8);
    assert_eq!(read(&recovered.replica, 0, 0, 4096).data, payload);
    assert_eq!(read(&recovered.replica, 0, 0, 4096).write_sequence, 1);
}
