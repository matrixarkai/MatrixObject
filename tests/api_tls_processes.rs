// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(
    target_os = "linux",
    feature = "peer-tls",
    feature = "replica-server",
    feature = "meta-server"
))]
mod node_regime;
use matrixobjectstore_rs::cluster::*;
use std::time::{Duration, Instant};

#[test]
fn encrypted_cli_health_and_durable_restart_refuse_plaintext_downgrade() {
    let files = node_regime::tls::TlsFiles::new();
    let ca = files.path("ca.pem").to_str().unwrap().to_owned();
    let certificate = files.path("peer.pem").to_str().unwrap().to_owned();
    let key = files.path("peer.key").to_str().unwrap().to_owned();
    let tls = ApiTlsConfig::load(&ApiTlsOptions {
        ca_certificate: ca.clone().into(),
        certificate: certificate.clone().into(),
        private_key: key.clone().into(),
    })
    .unwrap();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap().to_string();
    let roster = format!("chunk-1@{address}@test");
    let metadata_directory = files.root.join("metadata");
    std::fs::create_dir(&metadata_directory).unwrap();
    let metadata_directory = metadata_directory.to_str().unwrap();
    let metadata = node_regime::processes::start(
        env!("CARGO_BIN_EXE_metaserver"),
        &[
            ("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0"),
            ("MATRIXOBJECT_METASERVER_DATA_ROSTER", &roster),
            ("MATRIXOBJECT_METASERVER_STATE_DIR", metadata_directory),
            ("MATRIXOBJECT_METASERVER_SEED_CAPACITY_BYTES", "1048576"),
            ("MATRIXOBJECT_API_TLS_CA_CERT", &ca),
            ("MATRIXOBJECT_API_TLS_CERT", &certificate),
            ("MATRIXOBJECT_API_TLS_KEY", &key),
        ],
    );
    let journal = files.root.join("chunk.wal");
    let journal = journal.to_str().unwrap();
    let start_chunk = || {
        node_regime::processes::start(
            env!("CARGO_BIN_EXE_chunkserver"),
            &[
                ("MATRIXOBJECT_REPLICA_ADDR", &address),
                ("MATRIXOBJECT_REPLICA_JOURNAL", journal),
                ("MATRIXOBJECT_REPLICA_CAPACITY_BYTES", "1048576"),
                ("MATRIXOBJECT_CHUNK_NODE_ID", "chunk-1"),
                ("MATRIXOBJECT_CHUNK_METADATA_ENDPOINTS", &metadata.endpoint),
                ("MATRIXOBJECT_CHUNK_HEARTBEAT_MS", "100"),
                ("MATRIXOBJECT_API_TLS_CA_CERT", &ca),
                ("MATRIXOBJECT_API_TLS_CERT", &certificate),
                ("MATRIXOBJECT_API_TLS_KEY", &key),
                ("MATRIXOBJECT_CLIENT_TLS_CA_CERT", &ca),
                ("MATRIXOBJECT_CLIENT_TLS_CERT", &certificate),
                ("MATRIXOBJECT_CLIENT_TLS_KEY", &key),
            ],
        )
    };
    drop(reserved);
    let chunk = start_chunk();
    let mut meta = RemoteMetaClient::connect(
        &metadata.endpoint,
        &MetaClientOptions {
            tls: Some(tls.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let options = ReplicaClientOptions {
        tls: Some(tls.clone()),
        ..Default::default()
    };
    let mut client = RemoteReplicaClient::connect(&chunk.endpoint, &options).unwrap();
    let page = matrixobjectstore_rs::PageId {
        chunk_id: matrixobjectstore_rs::ChunkId::new(1, 1).unwrap(),
        page_index: 0,
    };
    let bytes = vec![37; 256 * 1024];
    client.write_chunk(page.clone(), 0, &bytes).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        let nodes = meta.list_placement_nodes().unwrap();
        if nodes.iter().any(|node| {
            node.node_id == "chunk-1" && node.online && node.used_bytes == bytes.len() as u64
        }) {
            break;
        }
        assert!(
            Instant::now() < until,
            "encrypted heartbeat did not update metadata: {nodes:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(client);
    drop(chunk);
    let chunk = start_chunk();
    let mut client = RemoteReplicaClient::connect(&chunk.endpoint, &options).unwrap();
    assert_eq!(
        client
            .read_chunk(&page, 0, bytes.len() as u64)
            .unwrap()
            .data,
        bytes
    );
    drop((client, chunk, meta, metadata));
    for (binary, name, path) in [
        (
            env!("CARGO_BIN_EXE_chunkserver"),
            "MATRIXOBJECT_REPLICA_JOURNAL",
            journal,
        ),
        (
            env!("CARGO_BIN_EXE_metaserver"),
            "MATRIXOBJECT_METASERVER_STATE_DIR",
            metadata_directory,
        ),
    ] {
        let output = std::process::Command::new(binary)
            .env(name, path)
            .env("MATRIXOBJECT_REPLICA_ADDR", "127.0.0.1:0")
            .env("MATRIXOBJECT_METASERVER_ADDR", "127.0.0.1:0")
            .env_remove("MATRIXOBJECT_API_TLS_CA_CERT")
            .env_remove("MATRIXOBJECT_API_TLS_CERT")
            .env_remove("MATRIXOBJECT_API_TLS_KEY")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("stored API transport requires valid TLS configuration"),
            "{stderr}"
        );
    }
}
