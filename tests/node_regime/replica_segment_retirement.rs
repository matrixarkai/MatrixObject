// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use matrixobjectstore_rs::cluster::RemoteReplicaSet;
use std::collections::BTreeSet;

fn hot(n: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(2_000_000_000, 101).unwrap(),
        page_index: n,
    }
}
fn segments(path: &Path) -> BTreeSet<PathBuf> {
    let prefix = format!("{}.segment-", path.file_name().unwrap().to_str().unwrap());
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_prefix(&prefix)
                .is_some_and(|suffix| {
                    suffix.len() == 16 && suffix.bytes().all(|b| b.is_ascii_hexdigit())
                })
        })
        .collect()
}
fn checkpoint(fixture: &Fixture) {
    for endpoint in &fixture.chunk_endpoints {
        let mut client =
            RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        while {
            let status = client.journal_maintenance_status().unwrap();
            status.checkpoint_in_flight
                || status.retirement_in_flight
                || status.retired_segments != 0
        } {
            assert!(
                Instant::now() < deadline,
                "automatic checkpoint did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let target = client.checkpoint_journal_with_maintenance().unwrap();
        loop {
            let status = client.journal_maintenance_status().unwrap();
            if status.maintenance_is_complete(&target).unwrap() {
                assert_eq!(status.pending_records, 0);
                assert_eq!(status.bytes_since_checkpoint, 0);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "checkpoint and retirement did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn wait_background(fixture: &Fixture, originals: &[BTreeSet<PathBuf>]) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let mut complete = originals.iter().flatten().all(|path| !path.exists());
        for endpoint in &fixture.chunk_endpoints {
            let mut client =
                RemoteReplicaClient::connect(endpoint, &ReplicaClientOptions::default()).unwrap();
            let status = client.journal_maintenance_status().unwrap();
            complete &= !status.checkpoint_in_flight
                && !status.retirement_in_flight
                && status.retired_segments == 0;
        }
        if complete {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "background compaction did not reclaim the original cold files"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn segmented_star_retirement_reclaims_files_preserves_cold_objects_and_recovers_all_processes() {
    star_retirement_workload(true);
}

#[test]
fn segmented_star_background_compaction_keeps_cold_objects_without_manual_barriers() {
    star_retirement_workload(false);
}

fn star_retirement_workload(manual: bool) {
    let mut fixture =
        Fixture::with_retention(true, true, Some(1024 * 1024), Some((24 * 1024 * 1024, 2)));
    let options = ObjectWriteOptions {
        page_bytes: 4096,
        target_groups: 1,
        ..Default::default()
    };
    let key = "retained-cold";
    let bytes = vec![0xcd; 4096];
    let mut client = fixture.client();
    let allocation = fixture.reserve(&mut client, key, None, 1, false);
    let manifest = client
        .write_staged_object(&allocation, None, &bytes, &options)
        .unwrap();
    verify_replicas(&fixture, &manifest, &bytes);
    let mut set =
        RemoteReplicaSet::connect(&fixture.chunk_endpoints, 3, 1, &options.replica).unwrap();
    assert_eq!(set.write_chunk(&hot(100), 0, b"probe").unwrap(), 3);
    let metadata_connections: Vec<_> = fixture
        .metadata
        .api_addresses
        .iter()
        .map(|address| TcpStream::connect(address).unwrap())
        .collect();
    let mut pids: Vec<_> = fixture.chunks.iter().map(|n| n.pid).collect();
    pids.extend(
        fixture
            .metadata
            .nodes
            .iter()
            .map(|n| n.as_ref().unwrap().pid),
    );
    assert_eq!(pids.iter().copied().collect::<BTreeSet<_>>().len(), 6);
    assert!(pids
        .iter()
        .all(|pid| *pid != std::process::id() && processes::is_live(*pid)));
    node_regime::sockets::assert_established_two_ways(&pids);
    let mut previous: Vec<_> = fixture.journals.iter().map(|p| segments(p)).collect();
    let original_segments = previous.clone();
    let mut reclaimed = [false; 3];
    let mut old_leader = None;
    for generation in 0..8 {
        let payload = vec![generation as u8 + 7; 512 * 1024];
        for n in 0..32 {
            assert_eq!(set.write_chunk(&hot(n), 0, &payload).unwrap(), 3);
        }
        if manual {
            checkpoint(&fixture);
        }
        for id in 0..3 {
            let current = segments(&fixture.journals[id]);
            reclaimed[id] |= previous[id].iter().any(|path| !current.contains(path));
            previous[id] = current;
            let mut replica =
                RemoteReplicaClient::connect(&fixture.chunk_endpoints[id], &options.replica)
                    .unwrap();
            let status = replica.journal_maintenance_status().unwrap();
            if manual {
                assert_eq!(status.retired_segments, 0);
                assert!(!status.retirement_in_flight);
            }
            println!(
                "RETENTION_SAMPLE generation={} replica={} logical_overwrite_bytes={} managed_segments={} managed_record_bytes={} reclaimed={}",
                generation + 1, id, (generation + 1) * 16 * 1024 * 1024,
                status.segments, status.journal_bytes, reclaimed[id]
            );
            for n in [0, 31] {
                assert_eq!(
                    replica
                        .read_chunk(&hot(n), 0, payload.len() as u64)
                        .unwrap()
                        .data,
                    payload
                );
            }
        }
        verify_replicas(&fixture, &manifest, &bytes);
        if generation == 3 {
            let leader = fixture.metadata.leader();
            fixture.metadata.kill(leader);
            fixture.metadata.leader();
            old_leader = Some(leader);
            drop(client);
            client = fixture.client();
        }
    }
    if manual {
        assert!(
            reclaimed.iter().all(|v| *v),
            "every data replica must physically retire history"
        );
    }
    if manual {
        for _ in 0..2 {
            checkpoint(&fixture);
            verify_replicas(&fixture, &manifest, &bytes);
        }
    } else {
        wait_background(&fixture, &original_segments);
    }
    for files in &original_segments {
        assert!(
            files.iter().all(|path| !path.exists()),
            "a cold object's original data or metadata still pins its first segment"
        );
    }
    for path in &fixture.journals {
        let manifest_path = path.with_file_name(format!(
            "{}.segments",
            path.file_name().unwrap().to_str().unwrap()
        ));
        assert_eq!(&std::fs::read(manifest_path).unwrap()[..8], b"MOSEGM02");
    }
    fixture.metadata.start(old_leader.unwrap());
    drop(client);
    drop(set);
    drop(metadata_connections);
    fixture.restart();
    let mut client = fixture.client();
    assert_eq!(
        client
            .read_object("append", key, bytes.len(), &options.replica)
            .unwrap(),
        (manifest.clone(), bytes.clone())
    );
    verify_replicas(&fixture, &manifest, &bytes);
    let expected = vec![14; 512 * 1024];
    for endpoint in &fixture.chunk_endpoints {
        let mut replica = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        for n in 0..32 {
            assert_eq!(
                replica
                    .read_chunk(&hot(n), 0, expected.len() as u64)
                    .unwrap()
                    .data,
                expected
            );
        }
        assert_eq!(replica.read_chunk(&hot(100), 0, 5).unwrap().data, b"probe");
    }
    let mut set =
        RemoteReplicaSet::connect(&fixture.chunk_endpoints, 3, 1, &options.replica).unwrap();
    assert_eq!(
        set.write_chunk(&hot(0), expected.len() as u64, b"after-retirement")
            .unwrap(),
        3
    );
    if manual {
        checkpoint(&fixture);
    } else {
        wait_background(&fixture, &original_segments);
    }
    for endpoint in &fixture.chunk_endpoints {
        let mut replica = RemoteReplicaClient::connect(endpoint, &options.replica).unwrap();
        assert_eq!(
            replica
                .read_chunk(&hot(0), expected.len() as u64, 16)
                .unwrap()
                .data,
            b"after-retirement"
        );
    }
}
