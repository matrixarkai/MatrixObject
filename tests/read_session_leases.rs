// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::{
    cluster::{
        MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaNodeShutdown, RemoteMetaClient,
        ReplicaClientOptions,
    },
    manifest_retention::ManifestRetentionRequest,
    object_manifest::{ManifestOutcome, ObjectManifest},
};
use prost::Message;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

struct Running {
    stop: MetaNodeShutdown,
    task: Option<thread::JoinHandle<std::io::Result<()>>>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop.stop();
        let _ = self.task.take().unwrap().join();
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
        "read-session-{}-{}",
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
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
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
fn client(endpoint: &str) -> RemoteMetaClient {
    RemoteMetaClient::connect(endpoint, &options()).unwrap()
}
fn publish(client: &mut RemoteMetaClient) -> ObjectManifest {
    client
        .publish_manifest(
            ManifestOutcome::decide(
                None,
                ObjectManifest {
                    format_version: 2,
                    bucket: "bucket".into(),
                    key: "object".into(),
                    generation: 1,
                    ..Default::default()
                },
            )
            .unwrap(),
            0,
        )
        .unwrap()
}
fn frame(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
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
fn automatic_read_session_holds_selected_generation_across_deletion_and_releases_after_read() {
    let root = root();
    let (endpoint, _server) = start(&root.0);
    let mut writer = client(&endpoint);
    let original = publish(&mut writer);
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = socket.local_addr().unwrap().to_string();
    let (committed, observed) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let worker = thread::spawn(move || {
        let (mut reader, _) = socket.accept().unwrap();
        let mut upstream = TcpStream::connect(endpoint).unwrap();
        let request = frame(&mut reader);
        assert_eq!(
            ManifestRetentionRequest::decode(&request[6..])
                .unwrap()
                .action,
            5
        );
        upstream.write_all(&request).unwrap();
        let reply = frame(&mut upstream);
        assert_eq!(reply[1], 0);
        committed.send(()).unwrap();
        resumed.recv_timeout(Duration::from_secs(10)).unwrap();
        reader.write_all(&reply).unwrap();
        let release = frame(&mut reader);
        assert_eq!(
            ManifestRetentionRequest::decode(&release[6..])
                .unwrap()
                .action,
            2
        );
        upstream.write_all(&release).unwrap();
        reader.write_all(&frame(&mut upstream)).unwrap();
    });
    let read = thread::spawn(move || {
        client(&proxy).read_object_in_session(
            "bucket",
            "object",
            0,
            &ReplicaClientOptions::default(),
            60_000,
        )
    });
    observed.recv_timeout(Duration::from_secs(10)).unwrap();
    let handles = writer.list_retained_manifests(0, 32).unwrap().0;
    assert_eq!(handles.len(), 1);
    writer.delete_object("bucket", "object", 0).unwrap();
    assert_eq!(
        writer.read_retained_manifest(handles[0]).unwrap().manifest,
        Some(original.clone())
    );
    resume.send(()).unwrap();
    assert_eq!(read.join().unwrap().unwrap(), (original, Vec::new()));
    worker.join().unwrap();
    assert!(writer.list_retained_manifests(0, 32).unwrap().0.is_empty());
}

#[test]
fn read_session_deadline_refuses_success_when_cleanup_exhausts_the_operation_window() {
    let root = root();
    let (endpoint, _server) = start(&root.0);
    publish(&mut client(&endpoint));
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let (mut reader, _) = socket.accept().unwrap();
        let mut upstream = TcpStream::connect(endpoint).unwrap();
        let open = frame(&mut reader);
        upstream.write_all(&open).unwrap();
        reader.write_all(&frame(&mut upstream)).unwrap();
        let release = frame(&mut reader);
        upstream.write_all(&release).unwrap();
        let reply = frame(&mut upstream);
        thread::sleep(Duration::from_millis(1100));
        let _ = reader.write_all(&reply);
    });
    assert!(client(&proxy)
        .read_object_in_session(
            "bucket",
            "object",
            0,
            &ReplicaClientOptions::default(),
            1000
        )
        .is_err());
    worker.join().unwrap();
}

#[test]
fn read_session_cleanup_preserves_renewals_and_permanent_handles_across_journal_restart() {
    let root = root();
    let (endpoint, server) = start(&root.0);
    let mut reader = client(&endpoint);
    let original = publish(&mut reader);
    let permanent = reader.retain_manifest("bucket", "object", 1).unwrap();
    let expiring = reader
        .open_read_session("bucket", "object", None, 1000)
        .unwrap();
    let mut renewed = reader
        .open_read_session("bucket", "object", Some(1), 60_000)
        .unwrap();
    reader.delete_object("bucket", "object", 0).unwrap();
    reader.renew_read_session(&mut renewed, 120_000).unwrap();
    assert_eq!(
        reader
            .read_retained_object(renewed.retention_id, 0, &ReplicaClientOptions::default())
            .unwrap(),
        (original, Vec::new())
    );
    assert!(reader.expire_read_session(&renewed).is_err());
    drop(reader);
    drop(server);
    let (endpoint, server) = start(&root.0);
    let mut reader = client(&endpoint);
    assert_eq!(
        reader.read_retained_manifest(renewed.retention_id).unwrap(),
        renewed
    );
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let ids = reader.list_retained_manifests(0, 32).unwrap().0;
        if !ids.contains(&expiring.retention_id) {
            break;
        }
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(20));
    }
    assert!(reader
        .read_retained_manifest(expiring.retention_id)
        .is_err());
    assert_eq!(
        reader
            .read_retained_manifest(permanent.retention_id)
            .unwrap(),
        permanent
    );
    let before = std::fs::read(root.0.join("metadata.outcomes")).unwrap();
    thread::sleep(Duration::from_millis(1200));
    assert_eq!(
        std::fs::read(root.0.join("metadata.outcomes")).unwrap(),
        before
    );
    reader.release_manifest(renewed.retention_id).unwrap();
    drop(reader);
    drop(server);
    let (endpoint, _server) = start(&root.0);
    assert!(client(&endpoint)
        .read_retained_manifest(renewed.retention_id)
        .is_err());
}
