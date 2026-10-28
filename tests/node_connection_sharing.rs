// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::cluster::*;
use std::{io::Write, net::TcpStream, time::Duration};
struct Cleanup(Option<Box<dyn FnOnce()>>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}
macro_rules! sharing_test {
    ($name:ident, $server:ty, $options:ident, $client:ty, $client_options:ident, $probe:ident) => {
        #[test]
        fn $name() {
            let server = <$server>::bind(
                "127.0.0.1:0",
                $options {
                    worker_threads: 1,
                    read_timeout: Duration::from_secs(5),
                    ..Default::default()
                },
            )
            .unwrap();
            let endpoint = server.local_addr().unwrap().to_string();
            let shutdown = server.shutdown_handle();
            let worker = std::thread::spawn(move || server.serve().unwrap());
            let _cleanup = Cleanup(Some(Box::new(move || {
                shutdown.stop();
                let _ = worker.join();
            })));
            let mut held = Vec::new();
            for i in 0..16 {
                let mut socket = TcpStream::connect(&endpoint).unwrap();
                if i % 2 == 1 {
                    socket.write_all(&[0]).unwrap();
                }
                held.push(socket);
            }
            let options = $client_options {
                read_timeout: Duration::from_secs(1),
                ..Default::default()
            };
            let mut clients = Vec::new();
            for _ in 0..8 {
                let mut client = <$client>::connect(&endpoint, &options).unwrap();
                client.$probe().unwrap();
                clients.push(client);
            }
            // Existing persistent sockets remain reusable while newer clients are admitted.
            for client in &mut clients {
                client.$probe().unwrap();
            }
        }
    };
}
sharing_test!(
    metadata_idle_and_partial_sockets_do_not_block_other_clients,
    MetaNodeServer,
    MetaNodeOptions,
    RemoteMetaClient,
    MetaClientOptions,
    list_placement_nodes
);
sharing_test!(
    chunk_idle_and_partial_sockets_do_not_block_other_clients,
    ReplicaNodeServer,
    ReplicaNodeOptions,
    RemoteReplicaClient,
    ReplicaClientOptions,
    ping
);

#[test]
fn metadata_fragmented_and_pipelined_frames_keep_their_boundaries() {
    use std::io::Read;
    let server = MetaNodeServer::bind("127.0.0.1:0", MetaNodeOptions::default()).unwrap();
    let endpoint = server.local_addr().unwrap();
    let shutdown = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let _cleanup = Cleanup(Some(Box::new(move || {
        shutdown.stop();
        let _ = worker.join();
    })));
    let mut stream = TcpStream::connect(endpoint).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let ping = [PROTOCOL_STAMP, MetaRequestCode::Ping as u8, 0, 0, 0, 0];
    for byte in ping {
        stream.write_all(&[byte]).unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    let read_reply = |stream: &mut TcpStream| {
        let mut header = [0; 6];
        stream.read_exact(&mut header).unwrap();
        assert_eq!(header[0], PROTOCOL_STAMP);
        assert_eq!(header[1], StatusCode::Ok as u8);
        let n = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
        assert!(n > 0 && n <= MAX_PAYLOAD_BYTES);
        let mut bytes = vec![0; n];
        stream.read_exact(&mut bytes).unwrap();
    };
    read_reply(&mut stream);
    stream.write_all(&[ping, ping].concat()).unwrap();
    read_reply(&mut stream);
    read_reply(&mut stream);
}

#[test]
fn metadata_partial_frame_has_one_deadline() {
    use std::io::Read;
    let server = MetaNodeServer::bind(
        "127.0.0.1:0",
        MetaNodeOptions {
            read_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .unwrap();
    let endpoint = server.local_addr().unwrap();
    let shutdown = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let _cleanup = Cleanup(Some(Box::new(move || {
        shutdown.stop();
        let _ = worker.join();
    })));
    let mut stream = TcpStream::connect(endpoint).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    // Enough bytes to keep a per-read timer alive, never a complete frame.
    for byte in [PROTOCOL_STAMP, MetaRequestCode::Ping as u8, 0, 0] {
        let _ = stream.write_all(&[byte]);
        std::thread::sleep(Duration::from_millis(45));
    }
    let mut byte = [0];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        other => panic!("partial frame did not expire: {other:?}"),
    }
}

#[test]
fn chunk_catalog_reconnects_before_writing_after_idle_expiry() {
    use matrixobjectstore_rs::{ChunkId, PageId};
    let server = ReplicaNodeServer::bind(
        "127.0.0.1:0",
        ReplicaNodeOptions {
            read_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .unwrap();
    let endpoint = server.local_addr().unwrap().to_string();
    let shutdown = server.shutdown_handle();
    let worker = std::thread::spawn(move || server.serve().unwrap());
    let _cleanup = Cleanup(Some(Box::new(move || {
        shutdown.stop();
        let _ = worker.join();
    })));
    let mut client = RemoteReplicaClient::connect_catalog(
        &[endpoint],
        &ReplicaClientOptions {
            read_timeout: Duration::from_millis(80),
            ..Default::default()
        },
    )
    .unwrap();
    let page = PageId {
        chunk_id: ChunkId::new(1, 1).unwrap(),
        page_index: 0,
    };
    client.write_chunk(page.clone(), 0, b"first").unwrap();
    let old_socket = client.local_addr().unwrap();
    std::thread::sleep(Duration::from_millis(200));
    client.write_chunk(page.clone(), 5, b"second").unwrap();
    assert_ne!(client.local_addr().unwrap(), old_socket);
    assert_eq!(client.head_chunk(&page).unwrap().write_sequence, 2);
    assert_eq!(
        client.read_chunk(&page, 0, 11).unwrap().data,
        b"firstsecond"
    );
}
