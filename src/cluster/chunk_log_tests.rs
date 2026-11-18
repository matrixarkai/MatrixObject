// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! The log chunk client against real replica nodes: three nodes with journals, a writer and a
//! reader talking to them over TCP.
use super::*;
use crate::cluster::{RemoteReplicaClient, ReplicaClientOptions, ReplicaNodeOptions, ReplicaNodeServer, StatusCode};
use crate::ObjectError;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static NEXT: AtomicU64 = AtomicU64::new(0);
const V: u64 = 3;
const C: u64 = 11;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("chunk-log-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Node {
    stop: crate::cluster::ReplicaNodeShutdown,
    worker: Option<thread::JoinHandle<std::io::Result<()>>>,
    addr: String,
    dir: PathBuf,
}
impl Node {
    fn start(dir: &Path, addr: &str) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let server = ReplicaNodeServer::bind_with_journal(addr, ReplicaNodeOptions::default(), &dir.join("journal")).unwrap();
        let addr = server.local_addr().unwrap().to_string();
        let stop = server.shutdown_handle();
        let worker = thread::spawn(move || server.serve());
        Self { stop, worker: Some(worker), addr, dir: dir.to_path_buf() }
    }
    fn halt(&mut self) {
        self.stop.stop();
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap().unwrap();
        }
    }
    fn restart(&mut self) {
        self.halt();
        let addr = self.addr.clone();
        *self = Self::start(&self.dir.clone(), &addr);
    }
    fn tail(&self) -> LogOpReply {
        RemoteReplicaClient::connect(&self.addr, &ReplicaClientOptions::default()).unwrap().log_tail(V, C, 1).unwrap()
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.halt();
    }
}

fn cluster() -> (Directory, Vec<Node>) {
    let dir = Directory::new();
    let nodes = (0..3).map(|i| Node::start(&dir.0.join(format!("n{i}")), "127.0.0.1:0")).collect();
    (dir, nodes)
}
fn endpoints(nodes: &[Node]) -> Vec<String> {
    nodes.iter().map(|n| n.addr.clone()).collect()
}
fn options() -> ReplicaClientOptions {
    ReplicaClientOptions { read_timeout: Duration::from_secs(10), connect_timeout: Duration::from_secs(2), ..Default::default() }
}
fn writer(endpoints: &[String], epoch: u64) -> ChunkLogWriter {
    ChunkLogWriter::open(endpoints, V, C, 1, epoch, options()).unwrap()
}
fn reader(endpoints: &[String]) -> ChunkLogReader {
    ChunkLogReader::new(endpoints, V, C, 1, options())
}
fn bytes(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add((i * 7) as u8)).collect()
}
/// Settle the writer until every node holds `total` bytes (a node that was behind catches up).
fn until_all_hold(w: &mut ChunkLogWriter, nodes: &[Node], total: u64) {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let _ = w.settle();
        if nodes.iter().all(|n| n.tail().durable_length == total) {
            return;
        }
        assert!(Instant::now() < until, "the nodes did not all reach {total}");
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn pipelined_appends_are_acknowledged_by_two_nodes_and_read_back() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let mut expected = Vec::new();
    for i in 0..20u8 {
        let piece = bytes(i, 300 * 1024);
        expected.extend_from_slice(&piece);
        let acked = w.append(&piece).unwrap();
        assert!(acked >= expected.len() as u64, "an append returns only once it is acknowledged");
    }
    let total = expected.len() as u64;
    assert_eq!(w.durable_length(), total);
    until_all_hold(&mut w, &nodes, total);
    let mut r = reader(&eps);
    assert_eq!(r.length().unwrap(), total);
    assert_eq!(r.read(0, expected.len()).unwrap(), expected);
    assert_eq!(r.read(1000, 5000).unwrap(), expected[1000..6000]);
}

#[test]
fn a_large_append_is_split_into_records_at_the_right_offsets() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(9, 3 * 1024 * 1024 + 12345);
    assert_eq!(w.append(&data).unwrap(), data.len() as u64);
    until_all_hold(&mut w, &nodes, data.len() as u64);
    assert_eq!(reader(&eps).read(0, data.len()).unwrap(), data);
    for node in &nodes {
        assert_eq!(node.tail().length, data.len() as u64);
    }
}

#[test]
fn one_node_down_two_of_three_still_acknowledge_and_it_catches_up_after_a_restart() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    nodes[2].halt();
    let mut w = writer(&eps, 1);
    let mut expected = Vec::new();
    for i in 0..5u8 {
        let piece = bytes(i, 100 * 1024);
        expected.extend_from_slice(&piece);
        w.append(&piece).unwrap();
    }
    let total = expected.len() as u64;
    assert_eq!(reader(&eps).read(0, expected.len()).unwrap(), expected);
    // the third node comes back at the same address, empty of everything it missed
    let (dir, addr) = (nodes[2].dir.clone(), nodes[2].addr.clone());
    nodes[2] = Node::start(&dir, &addr);
    assert_eq!(nodes[2].tail().length, 0);
    let more = bytes(77, 50 * 1024);
    expected.extend_from_slice(&more);
    w.append(&more).unwrap();
    until_all_hold(&mut w, &nodes, expected.len() as u64);
    assert_eq!(nodes[2].tail().length, total + more.len() as u64);
    assert_eq!(reader(&eps).read(0, expected.len()).unwrap(), expected);
}

#[test]
fn with_two_nodes_down_nothing_is_acknowledged() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    w.append(&bytes(1, 1000)).unwrap();
    nodes[1].halt();
    nodes[2].halt();
    let error = w.append(&bytes(2, 1000)).unwrap_err();
    assert!(matches!(error, ChunkLogError::NoQuorum { wanted: 2000, durable: 1000 }), "{error:?}");
}

fn slow_proxy(target: String, delay: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(client) = connection else { break };
            let Ok(upstream) = TcpStream::connect(&target) else { continue };
            let (mut client_in, mut client_out) = (client.try_clone().unwrap(), client);
            let (mut up_in, mut up_out) = (upstream.try_clone().unwrap(), upstream);
            thread::spawn(move || {
                let mut buffer = [0u8; 65536];
                loop {
                    match client_in.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            thread::sleep(delay);
                            if up_out.write_all(&buffer[..count]).is_err() {
                                break;
                            }
                        }
                    }
                }
                let _ = up_out.shutdown(Shutdown::Both);
            });
            thread::spawn(move || {
                let _ = std::io::copy(&mut up_in, &mut client_out);
                let _ = client_out.shutdown(Shutdown::Both);
            });
        }
    });
    addr
}

#[test]
fn a_slow_node_does_not_hold_back_the_acknowledgement_and_still_converges() {
    let (_dir, nodes) = cluster();
    let mut eps = endpoints(&nodes);
    // a delay so long that waiting for this node would take 1.5 s per append: even on a
    // loaded disk, four acknowledged appends that took less than 3 s did not wait for it
    eps[2] = slow_proxy(nodes[2].addr.clone(), Duration::from_millis(1500));
    let mut w = writer(&eps, 1);
    let started = Instant::now();
    let mut expected = Vec::new();
    for i in 0..4u8 {
        let piece = bytes(i, 64 * 1024);
        expected.extend_from_slice(&piece);
        w.append(&piece).unwrap();
    }
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(3), "four acknowledged appends took {elapsed:?} behind a 1.5 s node");
    until_all_hold(&mut w, &nodes, expected.len() as u64);
    assert_eq!(reader(&endpoints(&nodes)).read(0, expected.len()).unwrap(), expected);
}

#[test]
fn a_stale_epoch_is_a_typed_error_and_ends_the_writer() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut old = writer(&eps, 5);
    old.append(&bytes(1, 4096)).unwrap();
    let mut new = writer(&eps, 6);
    assert_eq!(new.append(&bytes(2, 4096)).unwrap(), 8192);
    assert_eq!(old.append(&bytes(3, 4096)).unwrap_err(), ChunkLogError::StaleEpoch { current: 6 });
    // the error is sticky: the old writer does not try again
    assert_eq!(old.append(&bytes(3, 4096)).unwrap_err(), ChunkLogError::StaleEpoch { current: 6 });
    let mut expected = bytes(1, 4096);
    expected.extend_from_slice(&bytes(2, 4096));
    assert_eq!(reader(&eps).read(0, 8192).unwrap(), expected);
}

#[test]
fn a_restart_of_each_node_loses_nothing_that_was_acknowledged() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(4, 700 * 1024);
    w.append(&data).unwrap();
    until_all_hold(&mut w, &nodes, data.len() as u64);
    for index in 0..3 {
        nodes[index].restart();
        assert_eq!(nodes[index].tail().durable_length, data.len() as u64, "node {index} lost bytes in a restart");
        assert_eq!(reader(&eps).read(0, data.len()).unwrap(), data);
    }
}

fn unknown_verb_node() -> (String, thread::JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let (mut peer, _) = listener.accept().unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut count = 0;
        loop {
            let mut header = [0u8; 6];
            if peer.read_exact(&mut header).is_err() {
                break;
            }
            let mut payload = vec![0; u32::from_be_bytes(header[2..].try_into().unwrap()) as usize];
            if peer.read_exact(&mut payload).is_err() {
                break;
            }
            count += 1;
            peer.write_all(&crate::cluster::message::encode_failure(StatusCode::UnknownRequest, "no log chunks here")).unwrap();
        }
        count
    });
    (addr, worker)
}

#[test]
fn a_node_that_does_not_know_the_verb_is_a_typed_answer_and_costs_one_exchange() {
    let (addr, worker) = unknown_verb_node();
    let mut client = RemoteReplicaClient::connect(&addr, &ReplicaClientOptions::default()).unwrap();
    assert_eq!(client.log_tail(V, C, 1).unwrap_err(), LogCallError::NotSpoken);
    let ops: Vec<LogOpRequest> = (0..3).map(|i| LogOpRequest::append(V, C, 1, 1, i * 10, bytes(1, 10))).collect();
    let results = client.log_append_pipelined(&ops);
    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|r| *r.as_ref().unwrap_err() == LogCallError::NotSpoken));
    drop(client);
    // one tail, then the first append alone: the other two appends were never sent
    assert_eq!(worker.join().unwrap(), 2);

    let nodes: Vec<_> = (0..3).map(|_| unknown_verb_node()).collect();
    let eps: Vec<String> = nodes.iter().map(|(addr, _)| addr.clone()).collect();
    assert_eq!(ChunkLogWriter::open(&eps, V, C, 1, 1, options()).err(), Some(ChunkLogError::NotSpoken));
    let nodes: Vec<_> = (0..3).map(|_| unknown_verb_node()).collect();
    let eps: Vec<String> = nodes.iter().map(|(addr, _)| addr.clone()).collect();
    assert_eq!(reader(&eps).read(0, 10).unwrap_err(), ChunkLogError::NotSpoken);
}

#[test]
fn a_reader_never_goes_above_the_quorum_durable_length() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(5, 100 * 1024);
    w.append(&data).unwrap();
    until_all_hold(&mut w, &nodes, data.len() as u64);
    // bytes only one node holds: never acknowledged
    let mut direct = RemoteReplicaClient::connect(&nodes[0].addr, &ReplicaClientOptions::default()).unwrap();
    direct.log_call(LogOpRequest::append(V, C, 1, 1, data.len() as u64, bytes(6, 50 * 1024))).unwrap();
    assert_eq!(nodes[0].tail().durable_length, 150 * 1024);
    let mut r = reader(&eps);
    assert_eq!(r.length().unwrap(), data.len() as u64);
    assert_eq!(r.read(0, 150 * 1024).unwrap(), data);
    assert!(r.read(data.len() as u64, 10).unwrap().is_empty());
}

#[test]
fn opening_a_chunk_cuts_off_bytes_beyond_the_quorum() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(5, 100 * 1024);
    w.append(&data).unwrap();
    until_all_hold(&mut w, &nodes, data.len() as u64);
    let mut direct = RemoteReplicaClient::connect(&nodes[0].addr, &ReplicaClientOptions::default()).unwrap();
    direct.log_call(LogOpRequest::append(V, C, 1, 1, data.len() as u64, bytes(6, 50 * 1024))).unwrap();
    let mut next = writer(&eps, 2);
    assert_eq!(next.length(), data.len() as u64);
    assert_eq!(nodes[0].tail().length, data.len() as u64, "the unacknowledged bytes were cut off");
    let more = bytes(8, 10 * 1024);
    assert_eq!(next.append(&more).unwrap(), (data.len() + more.len()) as u64);
}

#[test]
fn opening_a_chunk_fences_every_node_including_one_that_holds_exactly_the_quorum() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut old = writer(&eps, 1);
    let data = bytes(1, 10_000);
    old.append(&data).unwrap();
    until_all_hold(&mut old, &nodes, data.len() as u64);
    // every node holds exactly the quorum length, so opening has no byte to cut anywhere: the
    // truncate it sends is what carries the new epoch to each node
    let _new = writer(&eps, 2);
    for node in &nodes {
        let mut direct = RemoteReplicaClient::connect(&node.addr, &ReplicaClientOptions::default()).unwrap();
        let late = direct.log_call(LogOpRequest::append(V, C, 1, 1, data.len() as u64, bytes(2, 100)));
        assert_eq!(late.unwrap_err(), LogCallError::StaleEpoch { current: 2 }, "a late append from the replaced writer was accepted");
        assert_eq!(node.tail().length, data.len() as u64);
    }
    assert_eq!(old.append(&bytes(3, 10)).unwrap_err(), ChunkLogError::StaleEpoch { current: 2 });
}

#[test]
fn a_refused_request_in_a_pipeline_leaves_the_rest_answered() {
    let (_dir, nodes) = cluster();
    let mut client = RemoteReplicaClient::connect(&nodes[0].addr, &ReplicaClientOptions::default()).unwrap();
    client.log_call(LogOpRequest::append(V, C, 1, 5, 0, bytes(1, 100))).unwrap();
    let ops = vec![
        LogOpRequest::append(V, C, 1, 5, 100, bytes(2, 100)),
        LogOpRequest::append(V, C, 1, 3, 200, bytes(3, 100)),
        LogOpRequest::append(V, C, 1, 5, 200, bytes(4, 100)),
    ];
    let results = client.log_append_pipelined(&ops);
    assert_eq!(results[0].as_ref().unwrap().length, 200);
    assert_eq!(*results[1].as_ref().unwrap_err(), LogCallError::StaleEpoch { current: 5 });
    assert_eq!(results[2].as_ref().unwrap().length, 300);
    // the connection is still good
    assert_eq!(client.log_tail(V, C, 1).unwrap().length, 300);
}

#[test]
fn the_node_messages_the_client_classifies_are_the_ones_the_node_sends() {
    use crate::cluster::log_chunk_store::to_object_error;
    use crate::log_chunk::LogError;
    // what a client receives is the node's error rendered as text, then read back as a
    // precondition failure by the status code: the same text the real wire carries
    let on_the_wire = |error: ObjectError| ObjectError::PreconditionFailed(error.to_string());
    let stale = to_object_error(LogError::Fenced { current: 7 });
    assert_eq!(LogCallError::from_object(on_the_wire(stale), false), LogCallError::StaleEpoch { current: 7 });
    assert_eq!(LogCallError::from_object(on_the_wire(to_object_error(LogError::Sealed)), false), LogCallError::Sealed);
    let overlap = to_object_error(LogError::Overlap);
    assert!(matches!(LogCallError::from_object(on_the_wire(overlap), false), LogCallError::Refused(_)));
    let unknown = StatusCode::UnknownRequest.into_error("x".into());
    assert_eq!(LogCallError::from_object(unknown, false), LogCallError::NotSpoken);
    // whatever the message says, a failed connection is a transport failure
    let stale_but_transport = to_object_error(LogError::Fenced { current: 7 });
    assert!(matches!(LogCallError::from_object(stale_but_transport, true), LogCallError::Transport(_)));
}
