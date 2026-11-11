// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "meta-server")]
use matrixobjectstore_rs::{
    cluster::{
        MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaNodeShutdown, RemoteMetaClient,
    },
    staged_allocation::PreparedStagedAllocation,
};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

struct Running {
    stop: MetaNodeShutdown,
    task: Option<thread::JoinHandle<std::io::Result<()>>>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.stop();
        self.task.take().unwrap().join().unwrap().unwrap();
    }
}
struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn root() -> Root {
    let path = std::env::temp_dir().join(format!(
        "staged-request-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&path).unwrap();
    Root(path)
}
fn options() -> MetaClientOptions {
    MetaClientOptions {
        read_timeout: Duration::from_secs(3),
        write_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}
fn start(root: &Path) -> (String, Running) {
    let server =
        MetaNodeServer::bind_with_snapshot("127.0.0.1:0", MetaNodeOptions::default(), root)
            .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let stop = server.shutdown_handle();
    (
        endpoint,
        Running {
            stop,
            task: Some(thread::spawn(move || server.serve())),
        },
    )
}
fn frame(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut header = [0; 6];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
    assert!(length < 8192);
    let mut bytes = header.to_vec();
    bytes.resize(6 + length, 0);
    stream.read_exact(&mut bytes[6..]).unwrap();
    bytes
}

#[test]
fn lost_allocation_reply_recovers_exact_identity_and_never_resurrects_after_restart() {
    let root = root();
    let (endpoint, server) = start(&root.0);
    let mut client = RemoteMetaClient::connect(&endpoint, &options()).unwrap();
    client.enable_publication_fence().unwrap();
    let prepared = client
        .prepare_staged_allocation("bucket", "object", None, 2, false, 0)
        .unwrap();
    let prepared = PreparedStagedAllocation::from_bytes(&prepared.to_bytes()).unwrap();
    let competing = client
        .prepare_staged_allocation("bucket", "object", None, 2, false, 0)
        .unwrap();
    assert_eq!(prepared.allocation_id(), competing.allocation_id());
    assert_ne!(prepared.to_bytes(), competing.to_bytes());
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = socket.local_addr().unwrap().to_string();
    let upstream = endpoint.clone();
    let worker = thread::spawn(move || {
        let (mut downstream, _) = socket.accept().unwrap();
        let request = frame(&mut downstream);
        let mut upstream = TcpStream::connect(upstream).unwrap();
        upstream.write_all(&request).unwrap();
        let reply = frame(&mut upstream);
        assert_eq!(
            reply[1], 0,
            "the upstream must acknowledge before the reply is lost"
        );
        // Drop a successful reply after the durable allocation, before the client sees it.
        drop(downstream);
        socket.set_nonblocking(true).unwrap();
        socket
    });
    let mut uncertain = RemoteMetaClient::connect(&proxy, &options()).unwrap();
    assert!(matches!(uncertain.try_allocate_prepared_staged_object(&prepared),
        Err(matrixobjectstore_rs::staged_allocation::StagedAllocationFailure::Unconfirmed(_))));
    let socket = worker.join().unwrap();
    assert!(
        matches!(socket.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    let recovered = client.allocate_prepared_staged_object(&prepared).unwrap();
    assert_eq!(recovered.allocation_id, prepared.allocation_id());
    assert!(matches!(client.try_allocate_prepared_staged_object(&competing),
        Err(matrixobjectstore_rs::staged_allocation::StagedAllocationFailure::PositionConsumed)));
    assert_eq!(
        client.list_staged_allocations(0, 32).unwrap(),
        (vec![recovered.allocation_id], false)
    );
    drop(client);
    drop(server);
    let (endpoint, server) = start(&root.0);
    let mut client = RemoteMetaClient::connect(&endpoint, &options()).unwrap();
    assert_eq!(
        client.allocate_prepared_staged_object(&prepared).unwrap(),
        recovered
    );
    client
        .abort_staged_allocation(recovered.allocation_id)
        .unwrap();
    assert!(client.allocate_prepared_staged_object(&prepared).is_err());
    drop(client);
    drop(server);
    let (endpoint, _server) = start(&root.0);
    let mut client = RemoteMetaClient::connect(&endpoint, &options()).unwrap();
    assert!(client.allocate_prepared_staged_object(&prepared).is_err());
    let next = client
        .prepare_staged_allocation("bucket", "object", None, 2, false, 0)
        .unwrap();
    assert!(next.allocation_id() > recovered.allocation_id);
    client.allocate_prepared_staged_object(&next).unwrap();
    assert!(client.allocate_prepared_staged_object(&prepared).is_err());
}
