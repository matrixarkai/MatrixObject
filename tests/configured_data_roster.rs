// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "meta-server")]
use matrixobjectstore_rs::cluster::{
    DataNodeSeed, MetaClientOptions, MetaNodeOptions, MetaNodeServer, RemoteMetaClient,
};

#[test]
fn configured_roster_is_canonical_and_rejects_invalid_identities() {
    let first =
        DataNodeSeed::parse_roster("b@[::1]:18002@rack-2,a@127.0.0.1:18001@rack-1").unwrap();
    let second =
        DataNodeSeed::parse_roster("a@127.0.0.1:18001@rack-1,b@[::1]:18002@rack-2").unwrap();
    assert_eq!(first, second);
    for input in [
        "",
        "a@127.0.0.1:0@rack",
        "a@0.0.0.0:1@rack",
        "a@224.0.0.1:1@rack",
        "a@127.0.0.1:1@rack,a@127.0.0.1:2@rack",
        "a@127.0.0.1:1@rack,b@127.0.0.1:1@other",
        "a@127.0.0.1:1@rack,b@[::ffff:127.0.0.1]:1@other",
        "a@127.0.0.1:1@",
        "a@127.0.0.1:1@rack@extra",
    ] {
        assert!(
            DataNodeSeed::parse_roster(input).is_err(),
            "accepted {input}"
        );
    }
    assert!(DataNodeSeed::parse_roster(&"x".repeat(1024 * 1024 + 1)).is_err());
    let too_many = (0..4097)
        .map(|i| format!("n{i}@127.0.0.1:{}@rack", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    assert!(DataNodeSeed::parse_roster(&too_many).is_err());
}

#[test]
fn refused_roster_is_atomic_and_valid_nodes_start_offline() {
    let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    let valid =
        DataNodeSeed::parse_roster("a@127.0.0.1:18001@rack-1,b@127.0.0.1:18002@rack-2").unwrap();
    let mut invalid = valid.clone();
    invalid[1].node_id = invalid[0].node_id.clone();
    assert!(server.seed_data_roster(&invalid, 1024).is_err());
    assert!(server.seed_data_roster(&valid, 0).is_err());
    assert_eq!(server.seed_data_roster(&valid, 1024).unwrap(), 2);
    // A second bootstrap cannot replace the roster or install a prefix.
    let collision =
        DataNodeSeed::parse_roster("0-new@127.0.0.1:18003@rack-3,a@127.0.0.1:18001@rack-1")
            .unwrap();
    assert!(server.seed_data_roster(&collision, 1024).is_err());
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    let roster = client.list_placement_nodes().unwrap();
    assert_eq!(roster.len(), 2);
    assert!(roster.iter().all(|n| !n.online && n.last_heartbeat_ms == 0));
    assert_eq!(roster[0].endpoint, valid[0].endpoint.to_string());
    assert!(client.plan_object_placement("b", "k", 1, 1).is_err());
    client
        .heartbeat_placement_node("a", 0, 1024, false, true, 1)
        .unwrap();
    assert_eq!(
        client
            .plan_object_placement("b", "k", 1, 1)
            .unwrap()
            .node_ids,
        vec!["a"]
    );
    drop(client);
    stop.stop();
    worker.join().unwrap();
}
