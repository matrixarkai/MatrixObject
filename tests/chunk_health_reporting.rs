// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::cluster::meta_message::MetaRequest;
use matrixobjectstore_rs::cluster::{
    DataNodeSeed, MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaRequestCode,
    RemoteMetaClient, ReplicaHeartbeatOptions, ReplicaNodeOptions, ReplicaNodeServer,
};
use std::time::{Duration, Instant};

#[test]
fn conditional_chunk_heartbeat_is_binary_and_refuses_every_truncated_payload() {
    let request = MetaRequest::HeartbeatChunkNode {
        node_id: "chunk-1".into(),
        expected_endpoint: "127.0.0.1:17400".into(),
        used_bytes: 123,
        capacity_bytes: 1024,
        online: true,
        now_ms: 456,
    };
    let payload = request.encode_payload();
    assert_eq!(request.code() as u8, 71);
    assert_eq!(MetaRequestCode::from_u8(71), Some(request.code()));
    assert_eq!(
        MetaRequest::decode(request.code(), &payload).unwrap(),
        request
    );
    for length in 0..payload.len() {
        assert!(MetaRequest::decode(request.code(), &payload[..length]).is_err());
    }
    let mut extra = payload;
    extra.push(0);
    assert!(MetaRequest::decode(request.code(), &extra).is_err());
}

#[test]
fn wrong_endpoint_heartbeat_cannot_admit_a_node_or_clear_draining() {
    let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    server
        .seed_data_roster(
            &DataNodeSeed::parse_roster("chunk-1@127.0.0.1:17400@rack").unwrap(),
            1024,
        )
        .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    let before = client.list_placement_nodes().unwrap();
    assert!(client
        .heartbeat_chunk_node("chunk-1", "127.0.0.1:17401", 0, 1024, true, 1)
        .is_err());
    assert_eq!(client.list_placement_nodes().unwrap(), before);
    client
        .heartbeat_placement_node("chunk-1", 0, 1024, true, true, 2)
        .unwrap();
    let report = client
        .heartbeat_chunk_node("chunk-1", "127.0.0.1:17400", 10, 1024, true, 3)
        .unwrap();
    assert!(report.draining);
    assert_eq!(report.used_bytes, 10);
    drop(client);
    stop.stop();
    worker.join().unwrap();
}

#[test]
fn health_worker_shutdown_is_bounded_and_invalid_configuration_is_refused() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut server = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    let options = ReplicaHeartbeatOptions {
        placement_id: None,
        tls: None,
        expected_metadata_identity: None,
        node_id: "chunk-1".into(),
        advertised_endpoint: server.local_addr().unwrap(),
        metadata_endpoints: vec![silent.local_addr().unwrap()],
        interval: Duration::from_secs(60),
    };
    let mut invalid = options.clone();
    invalid
        .metadata_endpoints
        .push(invalid.metadata_endpoints[0]);
    assert!(server.enable_heartbeats(invalid).is_err());
    let mut invalid = options.clone();
    invalid.interval = Duration::from_millis(99);
    assert!(server.enable_heartbeats(invalid).is_err());
    let mut invalid = options.clone();
    invalid.advertised_endpoint.set_port(0);
    assert!(server.enable_heartbeats(invalid).is_err());
    let mut invalid = options.clone();
    invalid.expected_metadata_identity = Some(Default::default());
    assert!(server.enable_heartbeats(invalid).is_err());
    let mut invalid = options.clone();
    invalid.placement_id = Some(String::new());
    assert!(server.enable_heartbeats(invalid).is_err());
    let mut standalone = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    let mut mismatch = options.clone();
    mismatch.placement_id = Some("group-a".into());
    standalone.enable_heartbeats(mismatch).unwrap();
    assert!(standalone.serve().unwrap_err().to_string().contains("placement ID"));
    server.enable_heartbeats(options).unwrap();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    silent.set_nonblocking(true).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    let connection = loop {
        if let Ok((connection, _)) = silent.accept() {
            break connection;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(10));
    };
    let started = Instant::now();
    stop.stop();
    worker.join().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "shutdown waited for the 60-second interval"
    );
    drop(connection);

    // Also stop a worker already sleeping after a successful report.
    let meta = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    let mut chunk = ReplicaNodeServer::bind("127.0.0.1:0", ReplicaNodeOptions::default()).unwrap();
    let address = chunk.local_addr().unwrap();
    let metadata = meta.local_addr().unwrap();
    meta.seed_data_roster(
        &DataNodeSeed::parse_roster(&format!("sleeping@{address}@rack")).unwrap(),
        1024,
    )
    .unwrap();
    chunk
        .enable_heartbeats(ReplicaHeartbeatOptions {
            placement_id: None,
            tls: None,
            expected_metadata_identity: None,
            node_id: "sleeping".into(),
            advertised_endpoint: address,
            metadata_endpoints: vec![metadata],
            interval: Duration::from_secs(60),
        })
        .unwrap();
    let meta_stop = meta.shutdown_handle();
    let chunk_stop = chunk.shutdown_handle();
    let meta_worker = std::thread::spawn(move || meta.serve().unwrap());
    let chunk_worker = std::thread::spawn(move || chunk.serve().unwrap());
    let mut client =
        RemoteMetaClient::connect(&metadata.to_string(), &MetaClientOptions::default()).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while !client.list_placement_nodes().unwrap()[0].online {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(50));
    let started = Instant::now();
    chunk_stop.stop();
    chunk_worker.join().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "sleeping health worker ignored shutdown"
    );
    drop(client);
    meta_stop.stop();
    meta_worker.join().unwrap();
}

#[test]
fn partial_chunk_heartbeat_configuration_is_refused_before_serving() {
    use std::process::{Command, Stdio};
    for environment in [
        vec![("MATRIXOBJECT_CHUNK_NODE_ID", "chunk-1")],
        vec![("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", "127.0.0.1:18000")],
        vec![
            ("MATRIXOBJECT_CHUNK_NODE_ID", "chunk-1"),
            ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", "127.0.0.1:18000"),
            ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "0"),
        ],
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_chunkserver"));
        for key in [
            "MATRIXOBJECT_CHUNK_NODE_ID",
            "MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS",
            "MATRIXOBJECT_CHUNK_ADVERTISE_ADDR",
            "MATRIXOBJECT_CHUNK_HEARTBEAT_MS",
            "MATRIXOBJECT_REPLICA_JOURNAL",
        ] {
            command.env_remove(key);
        }
        let mut child = command
            .env("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")
            .envs(environment)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(2));
                break;
            }
            if Instant::now() >= until {
                let _ = child.kill();
                let _ = child.wait();
                panic!("partial configuration started serving");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn reported_usage_and_committed_allocations_both_limit_placement() {
    let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    server
        .seed_data_roster(
            &DataNodeSeed::parse_roster("capacity@127.0.0.1:17400@rack").unwrap(),
            1024,
        )
        .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    client
        .heartbeat_chunk_node("capacity", "127.0.0.1:17400", 0, 1024, true, 1)
        .unwrap();
    let plan = client
        .plan_object_placement("bucket", "stored", 900, 1)
        .unwrap();
    client
        .commit_object_placement(
            matrixobjectstore_rs::cluster::CommittedPlacement {
                bucket: plan.bucket,
                key: plan.key,
                length: plan.length,
                node_ids: plan.node_ids,
                version: 1,
                update_time_ms: 2,
            },
            &plan.plan_checksum,
        )
        .unwrap();
    client
        .heartbeat_chunk_node("capacity", "127.0.0.1:17400", 900, 1024, true, 3)
        .unwrap();
    assert!(
        client
            .plan_object_placement("bucket", "fits", 124, 1)
            .is_ok(),
        "reported bytes and committed bytes overlap"
    );
    client
        .heartbeat_chunk_node("capacity", "127.0.0.1:17400", 1000, 1024, true, 4)
        .unwrap();
    assert!(
        client
            .plan_object_placement("bucket", "too-large", 25, 1)
            .is_err(),
        "physical usage must constrain placement"
    );
    assert!(client
        .plan_object_placement("bucket", "fits", 24, 1)
        .is_ok());
    client
        .heartbeat_chunk_node("capacity", "127.0.0.1:17400", 1, 1024, true, 5)
        .unwrap();
    assert!(
        client
            .plan_object_placement("bucket", "too-large", 125, 1)
            .is_err(),
        "lower physical usage cannot erase committed allocations"
    );
    drop(client);
    stop.stop();
    worker.join().unwrap();
}
