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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
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
        Self::start_with_options(dir, addr, ReplicaNodeOptions::default())
    }
    fn start_with_options(dir: &Path, addr: &str, options: ReplicaNodeOptions) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let server = ReplicaNodeServer::bind_with_journal(addr, options, &dir.join("journal")).unwrap();
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
fn pipelined_appends_are_acknowledged_by_three_nodes_and_read_back() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let mut expected = Vec::new();
    for i in 0..20u8 {
        let piece = bytes(i, 300 * 1024);
        expected.extend_from_slice(&piece);
        let acked = w.append(&piece).unwrap();
        assert!(acked >= expected.len() as u64, "an append returns only once it is acknowledged");
        assert!(nodes.iter().all(|node| node.tail().durable_length >= acked), "success requires all three durable replicas");
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
fn one_node_down_prevents_success_and_pending_bytes_catch_up_after_restart() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let mut expected = bytes(1, 1000);
    w.append(&expected).unwrap();
    nodes[2].halt();
    let more = bytes(2, 100 * 1024);
    expected.extend_from_slice(&more);
    assert_eq!(w.append(&more), Err(ChunkLogError::NoQuorum { wanted: expected.len() as u64, durable: 1000 }));
    assert_eq!(w.durable_length(), 1000);
    assert_eq!(w.append(&[]), Err(ChunkLogError::NoQuorum { wanted: expected.len() as u64, durable: 1000 }));
    assert_eq!(nodes[0].tail().durable_length, expected.len() as u64);
    assert_eq!(nodes[1].tail().durable_length, expected.len() as u64);
    nodes[2].restart();
    assert_eq!(w.settle().unwrap(), expected.len() as u64);
    for node in &nodes { assert_eq!(node.tail().durable_length, expected.len() as u64); }
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


#[test]
fn native_star_all_replicas_append_waits_for_the_third_durable_replica() {
    let (_dir, nodes) = cluster();
    let mut eps = endpoints(&nodes);
    let gate = Arc::new(AtomicBool::new(true));
    eps[2] = gated_proxy(nodes[2].addr.clone(), Arc::clone(&gate));
    let mut w = writer(&eps, 1);
    let data = bytes(7, 64 * 1024);
    gate.store(false, Ordering::SeqCst);
    thread::scope(|scope| {
        let _release = ReleaseGate(Arc::clone(&gate));
        let (sent, answer) = std::sync::mpsc::channel();
        let data = &data;
        let worker = scope.spawn(move || { let result = w.append(data); sent.send(result).unwrap(); w });
        let deadline = Instant::now() + Duration::from_secs(10);
        while nodes[..2].iter().any(|node| node.tail().durable_length < data.len() as u64) {
            assert!(Instant::now() < deadline, "the first two replicas did not persist the append");
            thread::sleep(Duration::from_millis(5));
        }
        assert!(matches!(answer.recv_timeout(Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)), "two replicas must not complete a write");
        assert_eq!(nodes[2].tail().length, 0);
        gate.store(true, Ordering::SeqCst);
        assert_eq!(answer.recv_timeout(Duration::from_secs(10)).unwrap().unwrap(), data.len() as u64);
        let w = worker.join().unwrap();
        assert_eq!(w.durable_length(), data.len() as u64);
    });
    for node in &nodes { assert_eq!(node.tail().durable_length, data.len() as u64); }
    assert_eq!(reader(&endpoints(&nodes)).read(0, data.len()).unwrap(), data);
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

/// A proxy in front of one node that remembers every connection it carries, so a test can cut
/// them all at once: what a node does to a connection that sat idle for its read timeout.
fn severable_proxy(target: String) -> (String, Arc<std::sync::Mutex<Vec<TcpStream>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let carried: Arc<std::sync::Mutex<Vec<TcpStream>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = Arc::clone(&carried);
    thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(client) = connection else { break };
            let Ok(upstream) = TcpStream::connect(&target) else { continue };
            kept.lock().unwrap().push(client.try_clone().unwrap());
            let (mut client_in, mut client_out) = (client.try_clone().unwrap(), client);
            let (mut up_in, mut up_out) = (upstream.try_clone().unwrap(), upstream);
            thread::spawn(move || {
                let _ = std::io::copy(&mut client_in, &mut up_out);
                let _ = up_out.shutdown(Shutdown::Both);
            });
            thread::spawn(move || {
                let _ = std::io::copy(&mut up_in, &mut client_out);
                let _ = client_out.shutdown(Shutdown::Both);
            });
        }
    });
    (addr, carried)
}

// A reader keeps its connections between calls. A node closes a connection that stays idle for its
// read timeout, so a reader that is used again after a long pause finds every connection dead; that
// must cost a reconnect, not the answer. Before the reader retried a call that failed on a kept
// connection, every node counted as silent and the call reported no quorum with all three up.
#[test]
fn a_reader_whose_connections_were_closed_while_idle_reconnects_and_still_answers() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(9, 100 * 1024);
    w.append(&data).unwrap();
    until_all_hold(&mut w, &nodes, data.len() as u64);

    let proxies: Vec<(String, Arc<std::sync::Mutex<Vec<TcpStream>>>)> = eps.iter().map(|e| severable_proxy(e.clone())).collect();
    let via: Vec<String> = proxies.iter().map(|(addr, _)| addr.clone()).collect();
    let mut r = reader(&via);
    assert_eq!(r.length().unwrap(), data.len() as u64, "the first call opens a connection to every node");

    for (_, carried) in &proxies {
        for stream in carried.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
    assert_eq!(r.length().unwrap(), data.len() as u64, "every kept connection is dead; the nodes are all up");
    assert_eq!(r.read(0, data.len()).unwrap(), data);
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

/// A proxy in front of one node that forwards nothing toward the node while `open` is false:
/// bytes sent to it are held (and arrive in order) until the gate opens. Replies flow freely.
fn gated_proxy(target: String, open: Arc<AtomicBool>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(client) = connection else { break };
            let Ok(upstream) = TcpStream::connect(&target) else { continue };
            let (mut client_in, mut client_out) = (client.try_clone().unwrap(), client);
            let (mut up_in, mut up_out) = (upstream.try_clone().unwrap(), upstream);
            let open = Arc::clone(&open);
            thread::spawn(move || {
                let mut buffer = [0u8; 65536];
                loop {
                    match client_in.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(count) => {
                            while !open.load(Ordering::SeqCst) {
                                thread::sleep(Duration::from_millis(1));
                            }
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

// The seal is final, so it must reach only a node that holds every byte. The first test is the
// deterministic one: a node that came back empty after the chunk was written has never held
// the bytes, so sealing it would leave a replica sealed at length 0 while the chunk is longer.
#[test]
fn native_star_all_replicas_preserves_legacy_two_copy_data_until_repair() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut expected = bytes(1, 100 * 1024);
    // Reproduce data persisted by an older two-copy writer without using the new SDK.
    for endpoint in &eps[..2] {
        let mut client = RemoteReplicaClient::connect(endpoint, &options()).unwrap();
        client.log_call(LogOpRequest::append(V, C, 1, 1, 0, expected.clone())).unwrap();
    }
    let mut w = writer(&eps, 2);
    assert_eq!(w.length(), expected.len() as u64, "recovery must retain the two-copy prefix");
    assert_eq!(reader(&eps).read(0, expected.len()).unwrap(), expected);
    let more = bytes(2, 20 * 1024);
    expected.extend_from_slice(&more);
    assert!(matches!(w.append(&more), Err(ChunkLogError::NoQuorum { .. })));
    assert!(matches!(w.seal_footer(true), Err(ChunkLogError::NoQuorum { .. })));
    for node in &nodes { assert!(!node.tail().sealed, "an incomplete write must not seal any replica"); }
    assert_eq!(nodes[2].tail().length, 0);
    // Peer repair supplies the old prefix and pending bytes at the active epoch.
    let mut repair = RemoteReplicaClient::connect(&eps[2], &options()).unwrap();
    repair.log_call(LogOpRequest::append(V, C, 1, 2, 0, expected.clone())).unwrap();
    assert_eq!(w.settle().unwrap(), expected.len() as u64);
    let footer = w.seal_footer(true).unwrap();
    assert_eq!(footer.length, expected.len() as u64);
    for node in &nodes { assert!(node.tail().sealed); assert_eq!(node.tail().length, footer.length); }
    assert_eq!(reader(&eps).read(0, expected.len()).unwrap(), expected);
}

// A node whose appends are still in flight when the seal starts must be waited for, then sealed
// at the full length; it must not be sealed at the shorter length it happens to hold, nor skipped.
#[test]
fn a_seal_waits_for_a_node_that_lags_and_no_node_seals_short() {
    let (_dir, nodes) = cluster();
    let mut eps = endpoints(&nodes);
    let gate = Arc::new(AtomicBool::new(true));
    eps[2] = gated_proxy(nodes[2].addr.clone(), Arc::clone(&gate));
    let mut w = writer(&eps, 1);
    let data = bytes(7, 300_000);
    w.append(&data).unwrap();
    gate.store(false, Ordering::SeqCst);
    thread::scope(|scope| {
        let _release = ReleaseGate(Arc::clone(&gate));
        let (sent, answer) = std::sync::mpsc::channel();
        let worker = scope.spawn(move || sent.send(w.seal_footer(true)).unwrap());
        let deadline = Instant::now() + Duration::from_secs(10);
        while nodes[..2].iter().any(|node| !node.tail().sealed) {
            assert!(Instant::now() < deadline, "the first two replicas did not seal");
            thread::sleep(Duration::from_millis(5));
        }
        assert!(!nodes[2].tail().sealed);
        assert_eq!(nodes[2].tail().length, data.len() as u64);
        assert!(matches!(answer.recv_timeout(Duration::from_millis(100)), Err(std::sync::mpsc::RecvTimeoutError::Timeout)));
        gate.store(true, Ordering::SeqCst);
        assert_eq!(answer.recv_timeout(Duration::from_secs(10)).unwrap().unwrap().length, data.len() as u64);
        worker.join().unwrap();
    });
    for node in &nodes { assert!(node.tail().sealed); assert_eq!(node.tail().length, data.len() as u64); }
}

// A node that was down while a newer writer took the chunk comes back at the older epoch, holding
// bytes the newer writer never saw (they were never acknowledged). A reader must take nothing
// from it, even when it is the first node the reader tries.
#[test]
fn a_reader_takes_nothing_from_a_node_left_at_an_older_writer_epoch() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut first = writer(&eps, 1);
    let base = bytes(1, 100 * 1024);
    first.append(&base).unwrap();
    until_all_hold(&mut first, &nodes, base.len() as u64);
    // bytes only the third node receives, which the first writer never got acknowledged
    let mut direct = RemoteReplicaClient::connect(&nodes[2].addr, &ReplicaClientOptions::default()).unwrap();
    direct.log_call(LogOpRequest::append(V, C, 1, 1, base.len() as u64, bytes(9, 50 * 1024))).unwrap();
    drop(direct);
    drop(first);
    // the third node is down while a second writer takes the chunk and writes its own bytes
    nodes[2].halt();
    let second = writer(&eps, 2);
    let mine = bytes(2, 20 * 1024);
    // Reproduce the older two-copy writer policy at the new epoch.
    for endpoint in &eps[..2] {
        let mut client = RemoteReplicaClient::connect(endpoint, &options()).unwrap();
        client.log_call(LogOpRequest::append(V, C, 1, 2, base.len() as u64, mine.clone())).unwrap();
    }
    drop(second);
    // it comes back still at the older epoch, holding bytes the second writer never saw
    let (dir, addr) = (nodes[2].dir.clone(), nodes[2].addr.clone());
    nodes[2] = Node::start(&dir, &addr);
    assert_eq!(nodes[2].tail().length, 150 * 1024);
    let mut expected = base.clone();
    expected.extend_from_slice(&mine);
    // the stale node is the first one the reader tries
    let order = vec![eps[2].clone(), eps[0].clone(), eps[1].clone()];
    let mut r = reader(&order);
    assert_eq!(r.length().unwrap(), expected.len() as u64);
    assert_eq!(r.read(0, expected.len()).unwrap(), expected, "bytes from the stale node reached the reader");
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


struct ReleaseGate(Arc<AtomicBool>);
impl Drop for ReleaseGate {
    fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
}

#[test]
fn native_star_all_replicas_seal_requires_three_and_retries_after_restart() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(9, 4096);
    w.append(&data).unwrap();
    nodes[2].halt();
    assert!(matches!(w.seal_footer(true), Err(ChunkLogError::NoQuorum { .. })));
    assert!(nodes[0].tail().sealed && nodes[1].tail().sealed);
    nodes[2].restart();
    assert!(!nodes[2].tail().sealed);
    assert_eq!(w.seal_footer(true).unwrap().length, data.len() as u64);
    for node in &nodes { assert!(node.tail().sealed); }
}

#[test]
fn native_star_all_replicas_reconnect_refuses_a_newer_epoch() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    w.append(&bytes(1, 1000)).unwrap();
    nodes[2].halt();
    assert!(matches!(w.append(&bytes(2, 1000)), Err(ChunkLogError::NoQuorum { .. })));
    nodes[2].restart();
    let mut other = RemoteReplicaClient::connect(&eps[2], &options()).unwrap();
    other.log_truncate(V, C, 1, 2, 1000).unwrap();
    assert_eq!(w.settle().unwrap_err(), ChunkLogError::StaleEpoch { current: 2 });
    assert_eq!(w.append(&bytes(3, 1000)).unwrap_err(), ChunkLogError::StaleEpoch { current: 2 });
    assert_eq!(nodes[2].tail().length, 1000);
    assert_eq!(nodes[0].tail().length, 2000);
}

#[test]
fn native_star_all_replicas_deferred_completion_survives_restart() {
    let dir = Directory::new();
    let mut nodes: Vec<_> = (0..3).map(|index| {
        Node::start_with_options(&dir.0.join(format!("deferred-{index}")), "127.0.0.1:0",
            ReplicaNodeOptions { journal_deferred_completion: true, ..Default::default() })
    }).collect();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(13, 2 * super::chunk_log::LOG_RECORD_BYTES + 17);
    assert_eq!(w.append(&data).unwrap(), data.len() as u64);
    for node in &mut nodes {
        assert_eq!(node.tail().durable_length, data.len() as u64);
        node.restart();
        assert_eq!(node.tail().durable_length, data.len() as u64);
    }
    assert_eq!(reader(&eps).read(0, data.len()).unwrap(), data);
}


/// A bounded frame proxy that drops one selected request or its reply.
pub(super) struct FaultProxy {
    pub(super) addr: String,
    armed: Arc<AtomicBool>,
    pub(super) operations: Arc<std::sync::Mutex<Vec<u32>>>,
    stop: Arc<AtomicBool>,
    sockets: Arc<std::sync::Mutex<Vec<TcpStream>>>,
    worker: Option<thread::JoinHandle<()>>,
}
impl FaultProxy {
    pub(super) fn new(target: String, operation: u32, drop_reply: bool) -> Self {
        Self::with_probe_gate(target, operation, drop_reply, None)
    }
    fn with_probe_gate(
        target: String,
        operation: u32,
        drop_reply: bool,
        probe_gate: Option<Arc<(std::sync::Mutex<usize>, std::sync::Condvar)>>,
    ) -> Self {
        Self::with_operation_gate(target,operation,drop_reply,probe_gate,super::log_op::OP_TAIL)
    }
    pub(super) fn with_operation_gate(
        target:String,operation:u32,drop_reply:bool,
        probe_gate:Option<Arc<(std::sync::Mutex<usize>,std::sync::Condvar)>>,
        gate_operation:u32,
    )->Self {
        use prost::Message as _;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let armed = Arc::new(AtomicBool::new(true));
        let operations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&operations);
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(std::sync::Mutex::new(Vec::<TcpStream>::new()));
        let (fault, stopping, tracked) = (Arc::clone(&armed), Arc::clone(&stop), Arc::clone(&sockets));
        let worker = thread::spawn(move || thread::scope(|scope| {
            while !stopping.load(Ordering::SeqCst) {
                let (mut client, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(_) => break,
                };
                let Ok(mut upstream) = TcpStream::connect(&target) else { continue };
                client.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                upstream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
                {
                    let mut tracked = tracked.lock().unwrap();
                    if stopping.load(Ordering::SeqCst) { break; }
                    tracked.push(client.try_clone().unwrap());
                    tracked.push(upstream.try_clone().unwrap());
                }
                let fault = Arc::clone(&fault);
                let recorded = Arc::clone(&recorded);
                let probe_gate = probe_gate.clone();
                scope.spawn(move || {
                    let mut transfer = || -> std::io::Result<()> {
                        loop {
                            let mut header = [0u8; 6];
                            client.read_exact(&mut header)?;
                            let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                            if size > 8 * 1024 * 1024 { return Err(std::io::ErrorKind::InvalidData.into()); }
                            let mut body = vec![0; size];
                            client.read_exact(&mut body)?;
                            let seen = (header[1] == crate::cluster::wire::RequestCode::LogOp as u8)
                                .then(|| LogOpRequest::decode(body.as_slice()).ok().map(|request| request.op)).flatten();
                            if let Some(op) = seen { recorded.lock().unwrap().push(op); }
                            if seen == Some(gate_operation) {
                                if let Some(gate) = &probe_gate {
                                    let mut count = gate.0.lock().unwrap();
                                    *count += 1;
                                    gate.1.notify_all();
                                    let (count, _) = gate.1.wait_timeout_while(
                                        count, Duration::from_secs(3), |count| *count < 3,
                                    ).unwrap();
                                    if *count < 3 { return Err(std::io::ErrorKind::TimedOut.into()); }
                                }
                            }
                            let selected = seen == Some(operation) && fault.swap(false, Ordering::SeqCst);
                            if selected && !drop_reply { return Ok(()); }
                            upstream.write_all(&header)?;
                            upstream.write_all(&body)?;
                            upstream.read_exact(&mut header)?;
                            let size = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                            if size > 8 * 1024 * 1024 { return Err(std::io::ErrorKind::InvalidData.into()); }
                            body.resize(size, 0);
                            upstream.read_exact(&mut body)?;
                            if selected { return Ok(()); }
                            client.write_all(&header)?;
                            client.write_all(&body)?;
                        }
                    };
                    let _ = transfer();
                    let _ = client.shutdown(Shutdown::Both);
                    let _ = upstream.shutdown(Shutdown::Both);
                });
            }
        }));
        Self { addr, armed, operations, stop, sockets, worker: Some(worker) }
    }
}
impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for socket in self.sockets.lock().unwrap().iter() { let _ = socket.shutdown(Shutdown::Both); }
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
    }
}

#[test]
fn native_star_all_replicas_retries_an_initial_fence_before_counting_an_older_tail() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let base = bytes(1, 128);
    writer(&eps, 1).append(&base).unwrap();
    let mut old = RemoteReplicaClient::connect(&eps[2], &options()).unwrap();
    old.log_call(LogOpRequest::append(V, C, 1, 1, base.len() as u64, bytes(99, 64))).unwrap();
    drop(old);
    let proxy = FaultProxy::new(eps[2].clone(), super::log_op::OP_TRUNCATE, false);
    let mut via = eps.clone();
    via[2] = proxy.addr.clone();
    let mut w = writer(&via, 2);
    assert!(!proxy.armed.load(Ordering::SeqCst));
    assert_eq!(w.length(), base.len() as u64);
    assert_eq!(nodes[2].tail().epoch, 1);
    assert_eq!(nodes[2].tail().length, 192);
    let more = bytes(7, 64);
    assert_eq!(w.append(&more).unwrap(), 192);
    let mut expected = base;
    expected.extend_from_slice(&more);
    for endpoint in &eps {
        let mut client = RemoteReplicaClient::connect(endpoint, &options()).unwrap();
        let tail = client.log_tail(V, C, 1).unwrap();
        assert_eq!(tail.epoch, 2);
        assert_eq!(tail.durable_length, 192);
        assert_eq!(client.log_call(LogOpRequest::read(V, C, 1, 0, 192)).unwrap().data, expected);
    }
}

#[test]
fn native_star_all_replicas_lost_seal_reply_is_recovered_without_rewriting_bytes() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let proxy = FaultProxy::new(eps[2].clone(), super::log_op::OP_SEAL, true);
    let mut via = eps.clone();
    via[2] = proxy.addr.clone();
    let mut w = writer(&via, 1);
    let data = bytes(11, 4096);
    w.append(&data).unwrap();
    assert!(matches!(w.seal_footer(true), Err(ChunkLogError::NoQuorum { .. })));
    assert!(!proxy.armed.load(Ordering::SeqCst));
    for node in &nodes { assert!(node.tail().sealed); }
    let files: Vec<_> = nodes.iter().map(|node| node.dir.join("logchunks").join(format!("{V:016x}-{C:016x}.log"))).collect();
    let before: Vec<_> = files.iter().map(|path| std::fs::read(path).unwrap()).collect();
    let footer = w.seal_footer(true).unwrap();
    assert_eq!(footer.length, data.len() as u64);
    for (path, bytes) in files.iter().zip(before) { assert_eq!(std::fs::read(path).unwrap(), bytes, "seal retry must not write another record"); }
    assert_eq!(reader(&eps).read(0, data.len()).unwrap(), data);
}


#[test]
fn native_star_all_replicas_retains_a_large_pending_append_until_restart() {
    let (_dir, mut nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let prefix = bytes(1, 4096);
    w.append(&prefix).unwrap();
    nodes[2].halt();
    let data = bytes(17, 33 * 1024 * 1024);
    let end = (prefix.len() + data.len()) as u64;
    assert!(matches!(w.append(&data), Err(ChunkLogError::NoQuorum { wanted, .. }) if wanted == end));
    nodes[2].restart();
    assert_eq!(w.settle().unwrap(), end, "retained bytes must survive the earlier lag threshold");
    for node in &nodes { assert_eq!(node.tail().durable_length, end); }
    let mut r = reader(&eps);
    assert_eq!(r.read(prefix.len() as u64, data.len()).unwrap(), data);
}


#[test]
fn native_log_sealed_read_uses_one_rpc_and_checks_the_expected_footer() {
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let mut w = writer(&eps, 1);
    let data = bytes(43, 4096);
    w.append(&data).unwrap();
    let footer = w.seal_footer(true).unwrap();
    let proxies: Vec<_> = eps.iter().map(|endpoint| FaultProxy::new(endpoint.clone(), 0, false)).collect();
    let via: Vec<_> = proxies.iter().map(|proxy| proxy.addr.clone()).collect();
    let mut reader = reader(&via);
    assert_eq!(reader.read_sealed(7, 19, &footer).unwrap(), data[7..26]);
    let operations: Vec<_> = proxies.iter().flat_map(|proxy| proxy.operations.lock().unwrap().clone()).collect();
    assert_eq!(operations, vec![super::log_op::OP_READ], "healthy sealed reads must not send tail probes");
    let mut invalid = footer.clone();
    invalid.crc ^= 1;
    assert!(reader.read_sealed(7, 19, &invalid).is_err(), "a mismatched metadata footer cannot serve bytes");
    invalid = footer.clone();
    invalid.sha256[0] ^= 1;
    assert!(reader.read_sealed(7, 19, &invalid).is_err(), "a mismatched digest cannot serve bytes");
    assert_eq!(reader.read_sealed(u64::MAX, 1, &footer).unwrap_err(), ChunkLogError::Refused(ObjectError::InvalidArgument("range overflow".into())));
}

#[test]
fn native_log_append_pipeline_refuses_oversized_payloads_before_sending_anything() {
    use prost::Message as _;
    let (_dir, nodes) = cluster();
    let eps = endpoints(&nodes);
    let proxy = FaultProxy::new(eps[0].clone(), 0, false);
    let operation = LogOpRequest::append(V, C, 1, 1, 0, bytes(7, 4096));
    let size = operation.encoded_len();
    let mut limits = options();
    limits.max_payload_bytes = size - 1;
    let mut client = RemoteReplicaClient::connect(&proxy.addr, &limits).unwrap();
    assert!(matches!(client.log_append_pipelined(std::slice::from_ref(&operation))[0], Err(LogCallError::Refused(ObjectError::InvalidArgument(_)))));
    assert!(proxy.operations.lock().unwrap().is_empty());
    assert_eq!(nodes[0].tail().length, 0);
    limits.max_payload_bytes = size;
    let mut exact = RemoteReplicaClient::connect(&proxy.addr, &limits).unwrap();
    assert_eq!(exact.log_append_pipelined(&[operation])[0].as_ref().unwrap().durable_length, 4096);
    assert_eq!(*proxy.operations.lock().unwrap(), vec![super::log_op::OP_APPEND]);
}

#[test]
fn identified_rpc_records_survive_three_replica_restarts_without_byte_based_ownership() {
    let (_dir, mut nodes) = cluster();
    for n in &nodes {
        let mut client = RemoteReplicaClient::connect(&n.addr, &options()).unwrap();
        let op = LogOpRequest::append_identified(V,C,1,7,0,b"hello".to_vec(),[13;16]);
        assert_eq!(client.log_call(op).unwrap().durable_length,5);
    }
    for n in &mut nodes { n.restart(); }
    for n in &nodes {
        let mut client = RemoteReplicaClient::connect(&n.addr, &options()).unwrap();
        let identity = client.log_identity(V,C,1,0).unwrap().record_identity().unwrap();
        assert_eq!((identity.offset,identity.length,identity.attempt_tag),(0,5,Some([13;16])));
        assert!(client.log_call(LogOpRequest::append_identified(V,C,1,7,0,b"hello".to_vec(),[14;16])).is_err());
        assert_eq!(client.log_read(V,C,1,0,5).unwrap().data,b"hello");
        assert!(!client.log_identity(V,C,1,1).unwrap().record_present);
    }
}

#[path = "identified_log_writer_tests.rs"]
mod identified_writer_tests;


#[path = "published_tail_reader_tests.rs"]
mod published_reader_tests;

#[path = "initialized_member_binding_tests.rs"]
mod initialized_member_binding_tests;

#[path = "storage_incarnation_tests.rs"]
mod storage_incarnation_tests;

#[path="physical_pinned_reader_tests.rs"]
mod physical_pinned_reader_tests;
