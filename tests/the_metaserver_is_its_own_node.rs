// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Whether this store's control plane is really a separate operating-system process, and what
//! the store does when it is gone.
//!
//! # THE QUESTION THIS FILE SETTLES
//!
//! `MatrixMetaService` -- the roster, placement, the leader lease, the metadata records -- has
//! always been a field of `MatrixObjectStore`. `metaserver_catalog_endpoints` has existed
//! alongside it for a long time, parsed, validated, hashed into the store snapshot's checksum
//! and written to the journal, and **nothing ever opened a socket to it**. This is the test
//! that it does now.
//!
//! # AND THE QUESTION BEHIND IT
//!
//! A control plane in its own process can be lost. What a store does then is a product
//! decision and not an accident, so it is asserted rather than discovered:
//!
//! * **new placement stops**, because choosing nodes needs the roster, which is on the node
//!   that is gone -- and the refusal names the control plane rather than the data;
//! * **reads of pages the replicas already hold keep working**, because they consult the
//!   control plane on no path.
//!
//! Both halves are asserted at the same moment, on the same cluster, which is what makes it a
//! statement about this arrangement rather than two separate observations.
//!
//! # THE REGIME IS ASSERTED BEFORE ANYTHING ELSE
//!
//! A test that planned a placement, killed something and read bytes back would pass
//! identically against one process holding everything. So the first thing
//! [`the_metaserver_node_stops_placement_and_not_reads`] does is establish that it is in the
//! regime it argues about: four live PIDs that are not this test's, and an ESTABLISHED socket
//! per node attributed to that node's PID, read from `ss -tnp` and from `/proc` and required
//! to agree.
//!
//! # AND THE ROSTER IS REQUIRED TO BE NON-UNIFORM
//!
//! A fixture that is uniform in the dimension under test cannot test that dimension. The
//! placement policy prefers one node per failure domain, so a roster of three identical nodes
//! would satisfy it whatever it did. The seeded nodes therefore carry one failure domain and
//! one allocator shard index each, and this test asserts that they are distinct **before** it
//! asserts anything about the plan the metaserver returns.

#![cfg(all(feature = "meta-server", feature = "replica-server"))]

use matrixobjectstore_rs as store;

use store::cluster::{
    MetaClientOptions, MetaDirectory, MetaRequestCode, RemoteMetaClient, RequestCode,
    META_CODE_BASE,
};
use store::{ChunkId, ObjectError, PageId};

use std::time::Duration;

#[cfg(target_os = "linux")]
mod node_regime;

// ---------------------------------------------------------------------------------------
// The corpus and the fixture
// ---------------------------------------------------------------------------------------

/// How many replica processes hold the data plane.
const REPLICA_NODES: usize = 3;

/// How many data nodes the metaserver's roster is seeded with.
const ROSTER_NODES: usize = 3;

/// How many pages are written and then demanded back exactly after the control plane dies.
const CORPUS_PAGES: u64 = 24;

const PAGE_BYTES: usize = 4096;

fn corpus_page_id(index: u64) -> PageId {
    PageId {
        chunk_id: ChunkId::new(index % 2 + 5, index * 2 + 19).expect("a corpus chunk id fits"),
        page_index: index * 3 + 4,
    }
}

/// The bytes this key holds, distinct per key and not a constant fill.
///
/// A constant payload makes "zero wrong" unfalsifiable: every page would read back correctly
/// even if the store served the wrong one.
fn corpus_payload(index: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PAGE_BYTES);
    for at in 0..PAGE_BYTES {
        payload.push(((index as usize)
            .wrapping_mul(61)
            .wrapping_add(at.wrapping_mul(17))
            % 241) as u8);
    }
    payload
}

/// A page the corpus never writes, for the negative control.
fn never_written_page_id() -> PageId {
    PageId {
        chunk_id: ChunkId::new(41, 555_555).expect("the control chunk id fits"),
        page_index: 212_121,
    }
}

const PLACED_BUCKET: &str = "metaserver-bucket";
const PLACED_KEY: &str = "placed/object";
/// An object no placement is ever committed for, for the control-plane negative control.
const UNPLACED_KEY: &str = "never/placed";

// ---------------------------------------------------------------------------------------
// Portable: the two services cannot be confused for each other
// ---------------------------------------------------------------------------------------

/// A metaserver verb and a replica verb can never be the same byte.
///
/// # WHY THIS IS WORTH A TEST RATHER THAN A CONSTANT
///
/// The frame header carries a stamp and a code and nothing saying which service the frame is
/// for. Two services numbering their verbs from 1 would agree about every byte of a
/// misdirected frame: a replica's `WriteChunk` is code 1, and at a metaserver's port it would
/// decode as whatever that service called 1 -- and be applied, a page address read as a
/// heartbeat.
///
/// The ranges are disjoint by construction, and this is the guard that they stay so as both
/// grow. Both clauses are checked for non-vacuity: a run where either list came back empty
/// would make the overlap check pass by having nothing to compare.
#[test]
fn a_metaserver_verb_and_a_replica_verb_are_never_the_same_byte() {
    let replica_codes: Vec<u8> = [
        RequestCode::WriteChunk,
        RequestCode::ReadChunk,
        RequestCode::HeadChunk,
        RequestCode::DiscardChunk,
        RequestCode::Ping,
    ]
    .iter()
    .map(|code| *code as u8)
    .collect();
    let meta_codes: Vec<u8> = MetaRequestCode::all()
        .iter()
        .map(|code| *code as u8)
        .collect();

    // NEITHER LIST IS EMPTY, or the disjointness below is a comparison with nothing.
    assert_eq!(replica_codes.len(), 5, "the replica service has five verbs");
    assert!(
        meta_codes.len() >= 7,
        "the metaserver service should have at least seven verbs, it has {}",
        meta_codes.len()
    );

    for replica in &replica_codes {
        assert!(
            *replica < META_CODE_BASE,
            "replica code {replica} is at or past the metaserver base {META_CODE_BASE}"
        );
        assert!(
            !meta_codes.contains(replica),
            "replica code {replica} is also a metaserver code"
        );
        // A REPLICA'S FRAME IS AN UNKNOWN VERB HERE. This is the property that makes a
        // client pointed at the wrong port find out instead of having its write applied.
        assert!(
            MetaRequestCode::from_u8(*replica).is_none(),
            "the metaserver claims to know replica code {replica}"
        );
    }
    for meta in &meta_codes {
        assert!(
            *meta >= META_CODE_BASE,
            "metaserver code {meta} is below its own base {META_CODE_BASE}"
        );
        assert!(
            RequestCode::from_u8(*meta).is_none(),
            "the replica claims to know metaserver code {meta}"
        );
    }
    // AND THE CODES ARE DISTINCT FROM EACH OTHER, so a renumbering that collided two verbs
    // inside one service fails here rather than silently dispatching one as the other.
    let distinct: std::collections::BTreeSet<u8> = meta_codes.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        meta_codes.len(),
        "two metaserver verbs share a code: {meta_codes:?}"
    );
}

/// An empty endpoint list is the in-process control plane, and that is the default.
///
/// There is no new setting for this. `metaserver_catalog_endpoints` has always existed and
/// has never been dialled; an empty one is what this store has always been.
#[test]
fn an_empty_endpoint_list_is_the_control_plane_this_store_has_always_had() {
    let options = MetaClientOptions::default();
    let mut local = MetaDirectory::open("", &options).expect("an empty list opens in-process");
    assert!(
        !local.is_remote(),
        "an empty endpoint list must be the in-process control plane"
    );
    let status = local.status();
    assert!(!status.remote);
    assert!(status.reachable, "an in-process control plane is reachable");
    assert_eq!(status.node_count, 0, "a fresh control plane holds no nodes");
    assert_eq!(status.placement_count, 0);
    assert!(status.detail.is_empty());

    // Whitespace is not an endpoint either -- the store's own parser is what decides, and it
    // treats a blank list as absent rather than as one nameless node.
    assert!(
        !MetaDirectory::open("   ", &options)
            .expect("a blank list opens in-process")
            .is_remote(),
        "a blank endpoint list must not be read as a remote endpoint"
    );
}

// ---------------------------------------------------------------------------------------
// The test that decides it
// ---------------------------------------------------------------------------------------

/// A metaserver in its own process; kill it; placement stops and reads do not.
///
/// The order of the assertions is the argument, so it is written out in the order it is made:
///
/// 1. **the regime** -- four distinct live PIDs, none of them this test's, and an ESTABLISHED
///    socket per node attributed to that node's PID, read two independent ways and required
///    to agree;
/// 2. **the roster is not uniform** -- three nodes, three failure domains, three allocator
///    shard indices, asserted before anything about a plan;
/// 3. **placement crosses the socket** -- a plan from the metaserver process, its nodes drawn
///    from distinct failure domains, committed under its own checksum, and read back;
/// 4. **the fence crosses too** -- a commit offering the wrong checksum is refused;
/// 5. **the data plane is loaded** -- 24 pages acknowledged by all three replicas;
/// 6. **the metaserver is killed by PID**, after `/proc/<pid>/environ` confirms it is ours;
/// 7. **placement stops** -- a plan is refused, naming the control plane; a placement READ is
///    refused too, and NOT as absent, because an unreachable metaserver cannot tell a caller
///    that something does not exist;
/// 8. **reads do not stop** -- 24/24 pages back from the replicas, exact, with a negative
///    control, at the same moment.
#[cfg(target_os = "linux")]
#[test]
fn the_metaserver_node_stops_placement_and_not_reads() {
    use node_regime::{processes, sockets};
    use store::cluster::{majority, ReplicaClientOptions, ReplicaFanout, ReplicaPlacement};

    let meta_binary = env!("CARGO_BIN_EXE_metaserver");
    let replica_binary = env!("CARGO_BIN_EXE_replica_server");
    let token = node_regime::run_token("metaserver-node");

    let mut meta_node = processes::start_metaserver(meta_binary, &token, ROSTER_NODES);
    let replicas: Vec<processes::NodeProcess> = (0..REPLICA_NODES)
        .map(|_| processes::start_replica(replica_binary, &token))
        .collect();

    // ---------------------------------------------------------------- 1. THE REGIME
    let meta_pid = meta_node.pid;
    let replica_pids: Vec<u32> = replicas.iter().map(|node| node.pid).collect();
    let mut all_pids = vec![meta_pid];
    all_pids.extend(replica_pids.iter().copied());
    let distinct: std::collections::BTreeSet<u32> = all_pids.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        REPLICA_NODES + 1,
        "a metaserver and {REPLICA_NODES} replicas must be {} distinct processes, got {all_pids:?}",
        REPLICA_NODES + 1
    );
    assert!(
        !distinct.contains(&std::process::id()),
        "a node PID is this test's own PID {}, so a node is not a separate process",
        std::process::id()
    );
    for pid in &all_pids {
        assert!(
            processes::is_live(*pid),
            "/proc/{pid} does not exist, so that node is not running"
        );
    }
    // THE STARTUP LINE IS A CONTRACT, so the rest of it is read rather than assumed.
    assert!(
        meta_node.startup_line.contains("metaserver listening on"),
        "the metaserver's first line should say what it is, and it says {:?}",
        meta_node.startup_line
    );
    assert_eq!(
        processes::field(&meta_node.startup_line, "roster_nodes=").as_deref(),
        Some(ROSTER_NODES.to_string().as_str()),
        "the metaserver should report the roster it seeded before serving, line {:?}",
        meta_node.startup_line
    );

    let options = MetaClientOptions {
        connect_timeout: Duration::from_secs(5),
        read_timeout: Duration::from_secs(10),
        write_timeout: Duration::from_secs(10),
        ..MetaClientOptions::default()
    };
    // OPENED THE WAY A STORE WOULD: through the endpoint list, which is the setting that has
    // existed all along and never been dialled.
    let mut control = MetaDirectory::open(&meta_node.endpoint, &options)
        .expect("a named metaserver endpoint connects");
    assert!(
        control.is_remote(),
        "a named endpoint must be the remote control plane"
    );

    let status = control.status();
    assert!(status.remote);
    assert!(
        status.reachable,
        "the metaserver process should answer, and it said {:?}",
        status.detail
    );
    assert_eq!(
        status.node_count, ROSTER_NODES as u64,
        "the metaserver should hold the {ROSTER_NODES} nodes it seeded"
    );
    assert_eq!(status.placement_count, 0, "it has placed nothing yet");

    // The replica side, at a majority quorum of three.
    let endpoints: Vec<String> = replicas.iter().map(|node| node.endpoint.clone()).collect();
    let replica_quorum = majority(REPLICA_NODES);
    let mut fanout = ReplicaPlacement::remote_with_majority(endpoints.clone())
        .open(&ReplicaClientOptions {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            ..ReplicaClientOptions::default()
        })
        .expect("three replica processes connect");
    let replica_set = match &mut fanout {
        ReplicaFanout::Remote(set) => set,
        ReplicaFanout::InProcess(_) => {
            panic!("the remote placement opened as the in-process one, so nothing below \
                    crosses a socket")
        }
    };
    assert_eq!(replica_set.read_quorum(), replica_quorum);

    // The sockets, from `ss -tnp` with PID attribution and then again from `/proc` with no
    // external tool. Both readings, and they are required to agree -- for the metaserver as
    // well as the replicas, because the metaserver is the node this test is about.
    sockets::assert_established_two_ways(&all_pids);

    let connections = replica_set
        .connections()
        .expect("replica connection addresses are readable");
    assert_eq!(connections.len(), REPLICA_NODES);
    for (index, (local, peer)) in connections.iter().enumerate() {
        assert_eq!(peer, &endpoints[index], "replica connection {index} peer");
        assert_ne!(
            local, peer,
            "replica connection {index} has the same address on both ends"
        );
    }

    // ------------------------------------------------- 2. THE ROSTER IS NOT UNIFORM
    // A fixture uniform in the dimension under test cannot test that dimension. The policy
    // prefers one node per failure domain, so this is checked before the plan is.
    let roster = control
        .list_placement_nodes()
        .expect("the roster crosses the socket");
    assert_eq!(roster.len(), ROSTER_NODES);
    let domains: std::collections::BTreeSet<&str> = roster
        .iter()
        .map(|node| node.failure_domain.as_str())
        .collect();
    assert_eq!(
        domains.len(),
        ROSTER_NODES,
        "the seeded nodes must sit in {ROSTER_NODES} distinct failure domains, and they \
         report {domains:?} -- a uniform roster cannot test a policy that spreads across them"
    );
    let shards: std::collections::BTreeSet<u64> = roster
        .iter()
        .map(|node| node.allocator_shard_index)
        .collect();
    assert_eq!(
        shards.len(),
        ROSTER_NODES,
        "the seeded nodes must carry distinct allocator shard indices, got {shards:?}"
    );
    let ids: std::collections::BTreeSet<&str> =
        roster.iter().map(|node| node.node_id.as_str()).collect();
    assert_eq!(ids.len(), ROSTER_NODES, "node ids must be distinct");
    for node in &roster {
        assert!(node.online, "a seeded node should be online: {node:?}");
        assert!(node.data_eligible, "a seeded node should take data");
        assert_eq!(
            node.committed_bytes, 0,
            "nothing is placed yet, so no node is charged for bytes"
        );
    }

    // --------------------------------------- 3. PLACEMENT CROSSES THE SOCKET
    let placed_length = 4096u64;
    let plan = control
        .plan_object_placement(PLACED_BUCKET, PLACED_KEY, placed_length, 2)
        .expect("the metaserver process plans a placement");
    assert_eq!(plan.bucket, PLACED_BUCKET);
    assert_eq!(plan.key, PLACED_KEY);
    assert_eq!(plan.length, placed_length);
    assert_eq!(plan.node_ids.len(), 2, "a width of two chooses two nodes");
    assert!(
        !plan.plan_checksum.is_empty(),
        "a plan must carry the checksum a commit has to prove"
    );
    // THE POLICY SPREAD, which is only observable because the roster is not uniform.
    let chosen_domains: std::collections::BTreeSet<&str> = roster
        .iter()
        .filter(|node| plan.node_ids.contains(&node.node_id))
        .map(|node| node.failure_domain.as_str())
        .collect();
    assert_eq!(
        chosen_domains.len(),
        2,
        "the two chosen nodes should sit in two failure domains, and they sit in \
         {chosen_domains:?}"
    );
    assert_eq!(
        plan.skipped_offline, 0,
        "no seeded node is offline, so none should be skipped for it"
    );

    let committed = control
        .commit_object_placement(
            store::cluster::CommittedPlacement {
                bucket: PLACED_BUCKET.to_string(),
                key: PLACED_KEY.to_string(),
                version: 1,
                length: placed_length,
                node_ids: plan.node_ids.clone(),
                update_time_ms: 1,
            },
            &plan.plan_checksum,
        )
        .expect("the plan commits under its own checksum");
    assert_eq!(committed.node_ids, plan.node_ids);

    let read_back = control
        .object_placement(PLACED_BUCKET, PLACED_KEY)
        .expect("the committed placement reads back over the socket");
    assert_eq!(read_back.node_ids, plan.node_ids);
    assert_eq!(read_back.length, placed_length);

    // THE CONTROL-PLANE NEGATIVE CONTROL, while the metaserver is up: an object nothing has
    // placed must read as absent. Without this, the hit above is consistent with a read path
    // that answers for every key it is given.
    match control.object_placement(PLACED_BUCKET, UNPLACED_KEY) {
        Err(ObjectError::NotFound(_)) => {}
        other => panic!(
            "an object with no placement must read absent while the metaserver is up, and it \
             gave {other:?}"
        ),
    }

    // The commit charged the chosen nodes, which is state only the metaserver holds.
    let charged: u64 = control
        .list_placement_nodes()
        .expect("the roster still reads")
        .iter()
        .map(|node| node.committed_bytes)
        .sum();
    assert_eq!(
        charged,
        placed_length * 2,
        "committing a two-node placement of {placed_length} bytes should charge both nodes"
    );

    // --------------------------------------------- 4. THE FENCE CROSSES TOO
    let wrong = control.commit_object_placement(
        store::cluster::CommittedPlacement {
            bucket: PLACED_BUCKET.to_string(),
            key: "another/object".to_string(),
            version: 1,
            length: placed_length,
            node_ids: plan.node_ids.clone(),
            update_time_ms: 2,
        },
        "fnv1a64:0000000000000000",
    );
    assert!(
        wrong.is_err(),
        "a commit offering a checksum the metaserver did not produce must be refused, and it \
         returned {wrong:?}"
    );

    // ------------------------------------------- 5. THE DATA PLANE IS LOADED
    for index in 0..CORPUS_PAGES {
        let acknowledged = replica_set
            .write_chunk(&corpus_page_id(index), 0, &corpus_payload(index))
            .unwrap_or_else(|err| panic!("corpus page {index} was refused: {err}"));
        assert_eq!(
            acknowledged, REPLICA_NODES,
            "corpus page {index} was acknowledged by {acknowledged}, not all {REPLICA_NODES}"
        );
    }

    // -------------------------------------- 6. KILL THE METASERVER, BY PID
    processes::confirm_and_kill(
        &mut meta_node,
        node_regime::METASERVER_TOKEN_VAR,
        &token,
        Duration::from_secs(10),
    );
    for pid in &replica_pids {
        assert!(
            processes::is_live(*pid),
            "replica {pid} is gone too; the kill was not confined to the metaserver"
        );
    }
    assert!(
        !sockets::established_pids_from_ss().contains(&meta_pid),
        "ss -tnp still attributes an ESTABLISHED socket to the killed metaserver {meta_pid}"
    );

    // ----------------------------------------------- 7. PLACEMENT STOPS
    let status = control.status();
    assert!(
        !status.reachable,
        "the control plane is dead and the status says it is reachable; a health answer read \
         from a remembered value is a comparison with itself and can never fire"
    );
    assert!(
        !status.detail.is_empty(),
        "an unreachable control plane should say why"
    );
    assert_eq!(
        status.node_count, 0,
        "an unreachable control plane must not report counts carried over from a previous \
         call; it reported {status:?}"
    );

    let refused = control.plan_object_placement(PLACED_BUCKET, "after/the/kill", 4096, 2);
    match &refused {
        Err(ObjectError::PreconditionFailed(message)) => {
            assert!(
                message.contains("control plane is unreachable"),
                "the refusal should name the control plane, and it says {message:?}"
            );
            assert!(
                message.contains("Reads of objects this store already knows are unaffected"),
                "the refusal should say what is still working, so an operator reading only \
                 the text is not told the store is damaged; it says {message:?}"
            );
        }
        other => panic!(
            "planning a placement without a control plane must be refused by name, and it \
             gave {other:?}"
        ),
    }

    // THE SAME READ, THE OPPOSITE VERDICT. Section 3 required an unplaced object to read
    // ABSENT while the metaserver was up. With the metaserver gone, "absent" is a sentence
    // this store is not entitled to say: it cannot ask. Reporting absence here would be a
    // confident wrong answer, and a caller cannot retry one of those.
    match control.object_placement(PLACED_BUCKET, UNPLACED_KEY) {
        Err(ObjectError::NotFound(_)) => panic!(
            "with the metaserver dead, a placement read reported the object ABSENT. That \
             answer happens to be true and was not established -- the roster and the \
             placements are on the process that is gone."
        ),
        Err(_) => {}
        Ok(placement) => panic!(
            "a placement read answered {placement:?} with no control plane to ask"
        ),
    }
    // And a placement that DOES exist is equally unreadable, for the same reason.
    assert!(
        control
            .object_placement(PLACED_BUCKET, PLACED_KEY)
            .is_err(),
        "a committed placement cannot be read either; the metaserver held it"
    );

    // ------------------------------------------- 8. READS DO NOT STOP
    // Same moment, same cluster. This is the other half of the product decision.
    let mut recovered = 0usize;
    let mut missing: Vec<u64> = Vec::new();
    let mut wrong_bytes: Vec<u64> = Vec::new();
    for index in 0..CORPUS_PAGES {
        match replica_set.read_chunk(&corpus_page_id(index), 0, PAGE_BYTES as u64) {
            Ok(read) => {
                if read.data == corpus_payload(index) {
                    recovered += 1;
                } else {
                    wrong_bytes.push(index);
                }
            }
            Err(_) => missing.push(index),
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {CORPUS_PAGES} page(s) became unreadable when the CONTROL PLANE died, which is \
         the thing this arrangement exists to avoid: {missing:?}",
        missing.len()
    );
    assert!(
        wrong_bytes.is_empty(),
        "{} page(s) read back with the wrong bytes: {wrong_bytes:?}",
        wrong_bytes.len()
    );
    assert_eq!(recovered as u64, CORPUS_PAGES);

    // THE DATA NEGATIVE CONTROL, at the same moment: a page never written reads absent. The
    // replicas still hold a read quorum, so they ARE entitled to say so.
    match replica_set.read_chunk(&never_written_page_id(), 0, PAGE_BYTES as u64) {
        Err(ObjectError::NotFound(_)) => {}
        other => panic!(
            "a page that was never written must read as absent from a replica quorum that is \
             still whole, and it gave {other:?}"
        ),
    }

    // A WRITE STILL LANDS, TOO. The data plane is not merely readable without a control
    // plane; it is still writable, because a page write does not consult the roster.
    let after = replica_set.write_chunk(&corpus_page_id(CORPUS_PAGES + 3), 0, b"after the kill");
    assert_eq!(
        after.ok(),
        Some(REPLICA_NODES),
        "a page write should still reach all {REPLICA_NODES} replicas with no control plane"
    );

    // The survivors are reaped by `NodeProcess::drop`, which also runs when one of the
    // assertions above fires.
    drop(replicas);
    drop(meta_node);
}

/// A replica's frame sent to a metaserver port is refused, not applied.
///
/// The disjoint code ranges are what make this possible; this is the end-to-end form of it,
/// over a real socket, because an operator pointing a client at the wrong port is the case
/// the numbering exists for.
#[cfg(target_os = "linux")]
#[test]
fn a_replica_frame_sent_to_the_metaserver_is_refused_as_an_unknown_verb() {
    use node_regime::processes;
    use std::io::{Read, Write};

    let meta_binary = env!("CARGO_BIN_EXE_metaserver");
    let token = node_regime::run_token("wrong-port");
    let node = processes::start_metaserver(meta_binary, &token, 0);

    let mut stream = std::net::TcpStream::connect(&node.endpoint)
        .expect("a raw connection to the metaserver");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read timeout");
    // A REPLICA'S `Ping`: the right stamp, a code the replica service owns, an empty payload.
    // Everything except the service is valid, so a refusal can only be about the verb.
    let replica_ping = RequestCode::Ping as u8;
    stream
        .write_all(&[store::cluster::PROTOCOL_STAMP, replica_ping, 0, 0, 0, 0])
        .expect("the frame was sent");
    stream.flush().expect("the frame was flushed");

    let mut header = [0u8; 6];
    stream
        .read_exact(&mut header)
        .expect("the metaserver answered rather than dropping the connection");
    assert_eq!(
        header[0],
        store::cluster::PROTOCOL_STAMP,
        "the refusal must carry this build's stamp"
    );
    assert_eq!(
        header[1], 5,
        "a verb from another service is an unknown request (status 5), got status {}",
        header[1]
    );
    let payload_len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).expect("the message");
    let message = String::from_utf8_lossy(&payload).to_string();
    assert!(
        message.contains(&replica_ping.to_string()) && message.contains("metaserver"),
        "the refusal should name the code and the service that does not implement it, got \
         {message:?}"
    );

    drop(node);
}

/// A metaserver client cannot be built from an endpoint nothing is listening on.
///
/// The failure arrives at `open`, before any caller has been told it has a control plane.
#[test]
fn an_endpoint_with_nothing_behind_it_fails_to_open() {
    // Port 1 on loopback: a port no test of this repository binds, and one a process would
    // need privilege to bind even deliberately.
    let opened = MetaDirectory::open("127.0.0.1:1", &MetaClientOptions::default());
    assert!(
        opened.is_err(),
        "opening a control plane with nothing behind it must fail at open rather than at the \
         first call"
    );
}

/// A client built directly carries the endpoint it was asked for, and probes it live.
#[cfg(target_os = "linux")]
#[test]
fn a_metaserver_client_probes_on_every_call_rather_than_remembering() {
    use node_regime::processes;

    let meta_binary = env!("CARGO_BIN_EXE_metaserver");
    let token = node_regime::run_token("probe");
    let mut node = processes::start_metaserver(meta_binary, &token, 2);

    let mut client = RemoteMetaClient::connect(&node.endpoint, &MetaClientOptions::default())
        .expect("the metaserver connects");
    assert_eq!(client.endpoint(), node.endpoint);
    assert!(
        !client.probe().is_unreachable(),
        "a live metaserver should answer a probe"
    );
    // Two probes in a row, so that a cached answer and a live one are distinguishable below.
    assert!(!client.probe().is_unreachable());

    processes::confirm_and_kill(
        &mut node,
        node_regime::METASERVER_TOKEN_VAR,
        &token,
        Duration::from_secs(10),
    );

    assert!(
        client.probe().is_unreachable(),
        "after the process is gone the probe must say unreachable; a probe that answered \
         from what it last saw would still be reporting a control plane that is not there"
    );
}
