// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::chunk_catalog_command::prepare_at;
use crate::cluster::placement_journal::PlacementJournal;
use crate::cluster::{ChunkCatalogCommand, ChunkGroupCatalog, MembershipStatus};
use prost::Message;
use std::path::PathBuf;

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn directory(label: &str) -> Directory {
    let path = std::env::temp_dir().join(format!(
        "catalog-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    Directory(path)
}
fn catalog() -> ChunkGroupCatalog {
    let (group, health) = crate::cluster::chunk_groups::tests::dynamic_config();
    ChunkGroupCatalog::with_membership_health(vec![group], [("group-a".into(), health)].into())
        .unwrap()
}
fn report(id: u64) -> ChunkCatalogCommand {
    let group = crate::cluster::chunk_groups::tests::dynamic_config().0;
    ChunkCatalogCommand {
        version: 1,
        operation: 2,
        placement_id: "group-a".into(),
        expected_fence: vec![],
        bootstrap_id: 91,
        status: Some(MembershipStatus {
            version: 1,
            node_id: id,
            term: 2,
            applied_index: 11,
            voters: vec![1, 2, 3],
            outgoing_voters: vec![],
            learners: vec![4],
        }),
        member_id: group.member_ids[id as usize - 1].clone(),
        expected_endpoint: group.api_endpoints[id as usize - 1].clone(),
        used_bytes: id * 100,
        capacity_bytes: 4096,
        online: true,
    }
}
fn state(service: &MatrixMetaService) -> Vec<u8> {
    service.export_snapshot().to_bytes().unwrap()
}

#[test]
fn catalog_command_preparation_is_bounded_canonical_and_rolls_back() {
    let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    server.seed_chunk_groups(catalog()).unwrap();
    let mut guard = server.meta.lock().unwrap();
    let groups = guard.groups.clone();
    let initial = state(&guard.service);
    let health = groups.to_bytes().unwrap();
    let command = report(1);
    assert_eq!(
        MetaRequest::decode(
            crate::cluster::MetaRequestCode::ChunkCatalog,
            &command.encode_to_vec()
        )
        .unwrap(),
        MetaRequest::ChunkCatalog(command.clone())
    );
    let mut duplicate = command.encode_to_vec();
    duplicate.extend_from_slice(&[8, 1]);
    assert!(ChunkCatalogCommand::from_payload(&duplicate).is_err());
    for field in 0..8 {
        let mut bad = command.clone();
        match field {
            0 => bad.version = 2,
            1 => bad.bootstrap_id = 92,
            2 => bad.member_id = "voter-b".into(),
            3 => bad.capacity_bytes = 1,
            4 => bad.expected_endpoint = "127.0.0.1:9".into(),
            5 => bad.status.as_mut().unwrap().applied_index = 9,
            6 => bad.operation = 9,
            _ => bad.expected_fence = vec![1],
        }
        assert!(
            prepare_at(&mut guard.service, &groups, &bad, 65536, 100).is_err(),
            "field={field}"
        );
        assert_eq!(state(&guard.service), initial);
        assert_eq!(groups.to_bytes().unwrap(), health);
    }
    assert!(prepare_at(&mut guard.service, &groups, &command, 1, 100).is_err());
    assert_eq!(state(&guard.service), initial);
    let (outcome, reply) = prepare_at(&mut guard.service, &groups, &command, 65536, 100).unwrap();
    assert_eq!(state(&guard.service), initial);
    assert_eq!(groups.to_bytes().unwrap(), health);
    assert!(!matches!(reply,MetaReply::PlacementNode(ref node) if node.online));
    assert!(outcome.to_payload().unwrap().len() > reply.payload_len());
    assert!(prepare_at(
        &mut guard.service,
        &groups,
        &command,
        reply.payload_len(),
        100
    )
    .is_err());
    assert_eq!(state(&guard.service), initial);
    assert_eq!(groups.to_bytes().unwrap(), health);
    outcome.apply(&mut guard.service, &groups).unwrap();
    let first = state(&guard.service);
    assert!(outcome.apply(&mut guard.service, &groups).is_err());
    assert_eq!(state(&guard.service), first);
    let mut next = command.clone();
    next.status.as_mut().unwrap().applied_index = 12;
    next.used_bytes = 101;
    let (outcome, _) = prepare_at(&mut guard.service, &groups, &next, 65536, 90).unwrap();
    outcome.apply(&mut guard.service, &groups).unwrap();
    assert_eq!(guard.service.nodes["voter-a"].last_heartbeat_ms, 100);
    assert_eq!(guard.service.nodes["voter-a"].used_bytes, 101);
}

#[test]
fn catalog_journal_recovers_mixed_records_and_every_torn_tail() {
    let dir = directory("torn");
    let path = dir.0.join("outcomes");
    let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    server.seed_chunk_groups(catalog()).unwrap();
    let base = server.meta.lock().unwrap().service.clone();
    let groups = catalog();
    let mut service = base.clone();
    let (first, _) = prepare_at(&mut service, &groups, &report(1), 65536, 100).unwrap();
    first.apply(&mut service, &groups).unwrap();
    let expiry = crate::placement_outcome::PlacementOutcome {
        version: 1,
        before_index: service.placement_commit_index,
        after_index: service.placement_commit_index + 1,
        nodes: vec![],
        placements: vec![],
        manifest: None,
    };
    expiry.apply(&mut service).unwrap();
    let (second, _) = prepare_at(&mut service, &groups, &report(2), 65536, 110).unwrap();
    let prefix = [
        PlacementJournal::catalog_record(&first).unwrap(),
        PlacementJournal::record(&expiry).unwrap(),
    ]
    .concat();
    let suffix = PlacementJournal::catalog_record(&second).unwrap();
    let expected = state(&service);
    let expected_health = groups.to_bytes().unwrap();
    for cut in 0..suffix.len() {
        std::fs::write(&path, [prefix.as_slice(), &suffix[..cut]].concat()).unwrap();
        let restored_groups = catalog();
        let mut restored = base.clone();
        let mut journal =
            PlacementJournal::open_with_catalog(&path, &mut restored, &restored_groups, true)
                .unwrap();
        assert_eq!(state(&restored), expected);
        assert_eq!(restored_groups.to_bytes().unwrap(), expected_health);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), prefix.len() as u64);
        journal.append_catalog(&second).unwrap();
        drop(journal);
        let restored_groups = catalog();
        let mut restored = base.clone();
        drop(
            PlacementJournal::open_with_catalog(&path, &mut restored, &restored_groups, true)
                .unwrap(),
        );
        assert!(restored.nodes["group-a"].online);
        assert_eq!(restored.nodes["group-a"].used_bytes, 200);
    }
    for offset in 0..suffix.len() {
        let mut bad = suffix.clone();
        bad[offset] ^= 1;
        let bytes = [prefix.as_slice(), bad.as_slice()].concat();
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            PlacementJournal::open_with_catalog(&path, &mut base.clone(), &catalog(), true)
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}

#[test]
fn catalog_commands_sync_local_journal_before_reply_and_restore() {
    let dir = directory("local");
    let server =
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .unwrap();
    server.seed_chunk_groups(catalog()).unwrap();
    for id in [1, 2] {
        apply(&server.meta, MetaRequest::ChunkCatalog(report(id)), 65536).unwrap();
    }
    let before = std::fs::read(dir.0.join("metadata.outcomes")).unwrap();
    assert!(before.starts_with(b"MOCJ"));
    let expected = state(&server.meta.lock().unwrap().service);
    let health = server.meta.lock().unwrap().groups.to_bytes().unwrap();
    let mut bad = report(2);
    bad.bootstrap_id = 92;
    assert!(apply(&server.meta, MetaRequest::ChunkCatalog(bad), 65536).is_err());
    assert_eq!(
        std::fs::read(dir.0.join("metadata.outcomes")).unwrap(),
        before
    );
    drop(server);
    let restored =
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), &dir.0)
            .unwrap();
    let guard = restored.meta.lock().unwrap();
    assert_eq!(state(&guard.service), expected);
    assert_eq!(guard.groups.to_bytes().unwrap(), health);
    assert!(guard.service.nodes["group-a"].online);
    drop(guard);
    drop(restored);
    let volatile = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    volatile.seed_chunk_groups(catalog()).unwrap();
    assert!(apply(&volatile.meta, MetaRequest::ChunkCatalog(report(1)), 65536).is_err());
}

#[cfg(feature = "metadata-consensus")]
#[test]
fn catalog_commands_cross_metadata_quorum_and_survive_full_restart() {
    use crate::cluster::{MetaClientOptions, MetaRaftOptions, RemoteMetaClient};
    use std::collections::BTreeMap;
    use std::time::Instant;
    struct Running {
        address: String,
        stop: MetaNodeShutdown,
        thread: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    }
    impl Drop for Running {
        fn drop(&mut self) {
            self.stop.stop();
            if let Some(t) = self.thread.take() {
                t.join().unwrap().unwrap();
            }
        }
    }
    let dir = directory("quorum");
    let reservations: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let peers: BTreeMap<_, _> = reservations
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u64 + 1, s.local_addr().unwrap()))
        .collect();
    drop(reservations);
    let start = |id: u64| {
        let path = dir.0.join(id.to_string());
        std::fs::create_dir_all(&path).unwrap();
        let mut server = MetaNodeServer::bind(
            "127.0.0.1:0",
            MetaNodeOptions {
                worker_threads: 4,
                ..Default::default()
            },
        )
        .unwrap();
        server.seed_chunk_groups(catalog()).unwrap();
        server
            .enable_raft(MetaRaftOptions {
                node_id: id,
                group_id: 91,
                peers: peers.clone(),
                directory: path,
                request_timeout: Duration::from_secs(2),
                checkpoint_after_bytes: 4096,
                bootstrap_voters: None,
                peer_tls: None,
            })
            .unwrap();
        Running {
            address: server.local_addr().unwrap().to_string(),
            stop: server.shutdown_handle(),
            thread: Some(std::thread::spawn(move || server.serve())),
        }
    };
    let mut nodes: Vec<_> = (1..=3).map(|id| Some(start(id))).collect();
    let connect = |nodes: &[Option<Running>]| {
        let endpoints: Vec<_> = nodes.iter().flatten().map(|n| n.address.clone()).collect();
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            let options = MetaClientOptions {
                connect_timeout: Duration::from_millis(200),
                read_timeout: Duration::from_secs(4),
                write_timeout: Duration::from_secs(4),
                ..Default::default()
            };
            if let Ok(mut client) = RemoteMetaClient::connect_catalog(&endpoints, &options) {
                if client.list_placement_nodes().is_ok() {
                    break client;
                }
            }
            assert!(
                Instant::now() < until,
                "metadata quorum did not become ready"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    let mut client = connect(&nodes);
    assert_eq!(
        client.read_chunk_group("group-a").unwrap(),
        catalog().get("group-a").unwrap().clone()
    );
    assert!(!client.update_chunk_catalog(report(1)).unwrap().online);
    assert!(client.update_chunk_catalog(report(2)).unwrap().online);
    // A learner cannot supply the second voter required by a changed configuration.
    let mut observation = report(1);
    observation.operation = 1;
    observation.expected_fence = catalog()
        .membership_health("group-a")
        .unwrap()
        .unwrap()
        .fence()
        .to_bytes();
    observation.member_id.clear();
    observation.expected_endpoint.clear();
    observation.used_bytes = 0;
    observation.capacity_bytes = 0;
    observation.online = false;
    let status = observation.status.as_mut().unwrap();
    status.applied_index = 12;
    status.voters = vec![1, 2, 4];
    status.outgoing_voters = vec![1, 2, 3];
    status.learners.clear();
    assert!(
        !client
            .update_chunk_catalog(observation.clone())
            .unwrap()
            .online
    );
    assert!(
        client.update_chunk_catalog(observation.clone()).is_err(),
        "stale membership fence must be rejected"
    );
    assert!(
        client.update_chunk_catalog(report(1)).is_err(),
        "old member status must be rejected"
    );
    for id in [1, 2] {
        let mut command = report(id);
        command.status = observation.status.clone();
        command.status.as_mut().unwrap().node_id = id;
        let reply = client.update_chunk_catalog(command).unwrap();
        assert_eq!(reply.online, id == 2);
    }
    drop(client);
    for node in &mut nodes {
        drop(node.take());
    }
    for (i, node) in nodes.iter_mut().enumerate() {
        *node = Some(start(i as u64 + 1));
    }
    let mut client = connect(&nodes);
    let roster = client.list_placement_nodes().unwrap();
    let group = roster.iter().find(|n| n.node_id == "group-a").unwrap();
    assert!(group.online);
    assert_eq!(group.used_bytes, 200);
    drop(client);
    drop(nodes[1].take());
    drop(nodes[2].take());
    let options = MetaClientOptions {
        read_timeout: Duration::from_secs(4),
        ..Default::default()
    };
    let mut minority =
        RemoteMetaClient::connect(&nodes[0].as_ref().unwrap().address, &options).unwrap();
    let mut command = report(1);
    command.status = observation.status.clone();
    assert!(
        minority.update_chunk_catalog(command).is_err(),
        "one voter must not acknowledge catalog mutations"
    );
}
