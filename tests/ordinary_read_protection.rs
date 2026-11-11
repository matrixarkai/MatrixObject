// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(all(feature = "meta-server", feature = "replica-server"))]
use matrixobjectstore_rs::{
    cluster::{
        MetaClientOptions, MetaNodeOptions, MetaNodeServer, MetaNodeShutdown, RemoteMetaClient,
        ReplicaClientOptions,
    },
    manifest_retention::{ManifestRetentionReply, ManifestRetentionRequest},
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

// Each operation uses the normal public read API, rather than the explicit helper.
#[test]
fn ordinary_reads_hold_selected_generation_across_delete_and_release() {
    for mode in 0..3 {
        let root = root();
        let (endpoint, _server) = start(&root.0);
        let mut writer = client(&endpoint);
        let original = publish(&mut writer);
        writer.enable_publication_fence().unwrap();
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy = socket.local_addr().unwrap().to_string();
        let (committed, observed) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        let worker = thread::spawn(move || {
            let (mut reader, _) = socket.accept().unwrap();
            let mut upstream = TcpStream::connect(endpoint).unwrap();
            // The single native lookup carries the catalog policy. Duplicate
            // batch entries must still use exactly one shared grant.
            let lookup = frame(&mut reader);
            upstream.write_all(&lookup).unwrap();
            reader.write_all(&frame(&mut upstream)).unwrap();
            let open = frame(&mut reader);
            let request = ManifestRetentionRequest::decode(&open[6..]).unwrap();
            assert_eq!(request.action, 8);
            assert_eq!(request.version, 3);
            assert_eq!(request.manifest_sha256.len(), 32);
            assert_eq!(request.generation, 1);
            assert_eq!(request.ttl_ms, 60_000);
            upstream.write_all(&open).unwrap();
            let reply = frame(&mut upstream);
            assert_eq!(reply[1], 0);
            assert!(reply.len() <= 6 + 63);
            let grant = ManifestRetentionReply::decode(&reply[6..]).unwrap();
            assert_eq!(grant.version, 4);
            assert!(grant.retained.is_none());
            assert_eq!(grant.manifest_sha256, request.manifest_sha256);
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
            let mut client = client(&proxy);
            let options = ReplicaClientOptions::default();
            match mode {
                0 => client
                    .read_object("bucket", "object", 0, &options)
                    .map(|value| vec![value]),
                1 => client
                    .read_object_range("bucket", "object", 0, 0, 0, &options)
                    .map(|value| vec![value]),
                _ => client.read_objects(
                    &[("bucket", "object", 0), ("bucket", "object", 0)],
                    &options,
                ),
            }
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
        let values = read.join().unwrap().unwrap();
        assert_eq!(values.len(), if mode == 2 { 2 } else { 1 });
        assert!(values
            .into_iter()
            .all(|value| value == (original.clone(), Vec::new())));
        worker.join().unwrap();
        assert!(writer.list_retained_manifests(0, 32).unwrap().0.is_empty());
    }
}

#[test]
fn range_failure_releases_automatic_protection() {
    let root = root();
    let (endpoint, _server) = start(&root.0);
    let mut writer = client(&endpoint);
    publish(&mut writer);
    writer.enable_publication_fence().unwrap();
    let mut reader = client(&endpoint);
    assert!(reader
        .read_object_range(
            "bucket",
            "object",
            1,
            0,
            0,
            &ReplicaClientOptions::default()
        )
        .is_err());
    assert!(writer.list_retained_manifests(0, 32).unwrap().0.is_empty());
}

#[test]
fn batch_conflict_releases_prior_grants_and_returns_no_result() {
    let root = root();
    let (endpoint, _server) = start(&root.0);
    let mut writer = client(&endpoint);
    publish(&mut writer);
    writer
        .publish_manifest(
            ManifestOutcome::decide(
                None,
                ObjectManifest {
                    format_version: 2,
                    bucket: "bucket".into(),
                    key: "other".into(),
                    generation: 1,
                    ..Default::default()
                },
            )
            .unwrap(),
            0,
        )
        .unwrap();
    writer.enable_publication_fence().unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = socket.local_addr().unwrap().to_string();
    let (committed, observed) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let worker = thread::spawn(move || {
        let (mut reader, _) = socket.accept().unwrap();
        let mut upstream = TcpStream::connect(endpoint).unwrap();
        for index in 0..4 {
            let request = frame(&mut reader);
            if index == 2 {
                let command = ManifestRetentionRequest::decode(&request[6..]).unwrap();
                assert_eq!(command.action, 8);
                assert_eq!(command.key, "other");
            }
            if index == 3 {
                assert_eq!(
                    ManifestRetentionRequest::decode(&request[6..])
                        .unwrap()
                        .action,
                    2
                );
            }
            upstream.write_all(&request).unwrap();
            let reply = frame(&mut upstream);
            if index == 1 {
                committed.send(()).unwrap();
                resumed.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            reader.write_all(&reply).unwrap();
        }
    });
    let read = thread::spawn(move || {
        client(&proxy).read_objects(
            &[("bucket", "object", 0), ("bucket", "other", 0)],
            &ReplicaClientOptions::default(),
        )
    });
    observed.recv_timeout(Duration::from_secs(10)).unwrap();
    writer.delete_object("bucket", "other", 0).unwrap();
    resume.send(()).unwrap();
    assert!(read.join().unwrap().is_err());
    worker.join().unwrap();
    assert!(writer.list_retained_manifests(0, 32).unwrap().0.is_empty());
}

#[test]
fn cleanup_cannot_turn_an_expired_operation_into_success() {
    let root = root();
    let (endpoint, _server) = start(&root.0);
    let mut writer = client(&endpoint);
    publish(&mut writer);
    writer.enable_publication_fence().unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let (mut reader, _) = socket.accept().unwrap();
        let mut upstream = TcpStream::connect(endpoint).unwrap();
        for index in 0..3 {
            upstream.write_all(&frame(&mut reader)).unwrap();
            let reply = frame(&mut upstream);
            if index == 2 {
                thread::sleep(Duration::from_millis(600));
            }
            let _ = reader.write_all(&reply);
        }
    });
    assert!(client(&proxy)
        .read_object(
            "bucket",
            "object",
            0,
            &ReplicaClientOptions {
                deadline: Some(Instant::now() + Duration::from_millis(400)),
                ..Default::default()
            }
        )
        .is_err());
    worker.join().unwrap();
    assert!(writer.list_retained_manifests(0, 32).unwrap().0.is_empty());
}
#[test]
fn lost_acquisition_reply_does_not_replay_a_grant() {
    let root = root();
    let (endpoint, _server) = start(&root.0);
    let mut writer = client(&endpoint);
    publish(&mut writer);
    writer.enable_publication_fence().unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = socket.local_addr().unwrap().to_string();
    let worker = thread::spawn(move || {
        let (mut reader, _) = socket.accept().unwrap();
        let mut upstream = TcpStream::connect(endpoint).unwrap();
        upstream.write_all(&frame(&mut reader)).unwrap();
        reader.write_all(&frame(&mut upstream)).unwrap();
        let request = frame(&mut reader);
        assert_eq!(
            ManifestRetentionRequest::decode(&request[6..])
                .unwrap()
                .action,
            8
        );
        upstream.write_all(&request).unwrap();
        assert_eq!(frame(&mut upstream)[1], 0);
    });
    assert!(client(&proxy)
        .read_object("bucket", "object", 0, &ReplicaClientOptions::default())
        .is_err());
    worker.join().unwrap();
    let handles = writer.list_retained_manifests(0, 32).unwrap().0;
    assert_eq!(handles.len(), 1);
    assert!(
        writer
            .read_retained_manifest(handles[0])
            .unwrap()
            .expires_at_ms
            != 0
    );
    writer.release_manifest(handles[0]).unwrap();
}
