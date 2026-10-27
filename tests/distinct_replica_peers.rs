// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![cfg(all(target_os = "linux", feature = "replica-server"))]

#[allow(dead_code)]
mod node_regime;

use matrixobjectstore_rs::cluster::{RemoteReplicaSet, ReplicaClientOptions};
use matrixobjectstore_rs::{ChunkId, ObjectError, PageId};
use node_regime::processes::start_replica;

#[test]
fn one_endpoint_cannot_supply_multiple_votes() {
    let token = node_regime::run_token("duplicate-peer");
    let node = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let result = RemoteReplicaSet::connect(
        &[node.endpoint.clone(), node.endpoint.clone()],
        2,
        2,
        &ReplicaClientOptions::default(),
    );
    assert!(
        matches!(result, Err(ObjectError::InvalidArgument(_))),
        "duplicate membership was accepted: {result:?}"
    );
}

#[test]
fn two_spellings_of_one_peer_cannot_supply_multiple_votes() {
    let token = node_regime::run_token("aliased-peer");
    let node = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let address: std::net::SocketAddr = node.endpoint.parse().unwrap();
    let alias = format!("{}:0{}", address.ip(), address.port());
    assert_ne!(alias, node.endpoint);
    assert_eq!(alias.parse::<std::net::SocketAddr>().unwrap(), address);
    let result = RemoteReplicaSet::connect(
        &[node.endpoint.clone(), alias],
        2,
        2,
        &ReplicaClientOptions::default(),
    );
    assert!(
        matches!(result, Err(ObjectError::InvalidArgument(_))),
        "aliased membership was accepted: {result:?}"
    );
}

#[test]
fn distinct_processes_can_still_form_a_quorum() {
    let token = node_regime::run_token("distinct-peers");
    let a = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    let b = start_replica(env!("CARGO_BIN_EXE_chunkserver"), &token);
    assert_ne!(a.pid, b.pid);
    assert_ne!(a.pid, std::process::id());
    assert_ne!(b.pid, std::process::id());
    let mut set = RemoteReplicaSet::connect(
        &[a.endpoint.clone(), b.endpoint.clone()],
        2,
        2,
        &ReplicaClientOptions::default(),
    )
    .unwrap();
    let page = PageId {
        chunk_id: ChunkId::new(17, 3).unwrap(),
        page_index: 0,
    };
    assert_eq!(
        set.write_chunk(&page, 0, b"two independent peers").unwrap(),
        2
    );
    assert_eq!(
        set.read_chunk(&page, 0, 21).unwrap().data,
        b"two independent peers"
    );
}
