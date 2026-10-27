// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#![cfg(all(feature = "meta-server", feature = "replica-server"))]

use matrixobjectstore_rs::cluster::*;
use std::sync::mpsc;
use std::time::Duration;

macro_rules! shutdown_tests {
    ($idle:ident, $connected:ident, $before:ident, $server:ty, $options:ty, $client:ty, $client_options:ty, $probe:ident) => {
        #[test]
        fn $idle() {
            let server = <$server>::bind("127.0.0.1:0", <$options>::default()).unwrap();
            let shutdown = server.shutdown_handle();
            let (tx, rx) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                tx.send(server.serve()).unwrap();
            });
            std::thread::sleep(Duration::from_millis(100));
            shutdown.stop();
            shutdown.stop();
            rx.recv_timeout(Duration::from_secs(2))
                .expect("idle workers did not stop")
                .unwrap();
            worker.join().unwrap();
        }

        #[test]
        fn $connected() {
            let mut options = <$options>::default();
            options.worker_threads = 2;
            let server = <$server>::bind("127.0.0.1:0", options).unwrap();
            let endpoint = server.local_addr().unwrap().to_string();
            let shutdown = server.shutdown_handle();
            let (tx, rx) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                tx.send(server.serve()).unwrap();
            });
            let mut clients = Vec::new();
            for _ in 0..2 {
                let mut client =
                    <$client>::connect(&endpoint, &<$client_options>::default()).unwrap();
                client.$probe().unwrap();
                clients.push(client);
            }
            shutdown.stop();
            rx.recv_timeout(Duration::from_secs(2))
                .expect("connected workers did not stop")
                .unwrap();
            worker.join().unwrap();
            // Keep both confirmed connections open until every server worker has exited.
            drop(clients);
        }

        #[test]
        fn $before() {
            let server = <$server>::bind("127.0.0.1:0", <$options>::default()).unwrap();
            server.shutdown_handle().stop();
            server.serve().unwrap();
        }
    };
}

shutdown_tests!(
    replica_idle_shutdown,
    replica_connected_shutdown,
    replica_stopped_before_serve,
    ReplicaNodeServer,
    ReplicaNodeOptions,
    RemoteReplicaClient,
    ReplicaClientOptions,
    ping
);
shutdown_tests!(
    meta_idle_shutdown,
    meta_connected_shutdown,
    meta_stopped_before_serve,
    MetaNodeServer,
    MetaNodeOptions,
    RemoteMetaClient,
    MetaClientOptions,
    list_placement_nodes
);
