// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::meta_server::native_object_path_tests::Cluster;
use crate::cluster::MetaClientOptions;
use std::io::{Read, Write};
use std::net::TcpListener;

fn staged(cluster: &Cluster, name: &str, nonce: u8) -> (NativeObjectClient, NativeStageHandle) {
    let mut client = cluster.client();
    let mut stage = client
        .begin("bucket", name, "writer", [nonce; 32], 4, None, 20_000)
        .unwrap();
    let owned = client.allocate(&mut stage, "native-group", 4).unwrap();
    client
        .write_chunk(&stage, &owned, b"init", [nonce; 16])
        .unwrap();
    (client, stage)
}
fn writer(cluster: &Cluster, name: &str, nonce: u8) -> (NativeObjectClient, NativeTailWriter) {
    let (mut client, stage) = staged(cluster, name, nonce);
    let published = client.publish(&stage, true).unwrap();
    let writer = client.writer(&published).unwrap();
    (client, writer)
}

// Native mutations travel through the real group client and committed metadata.
// Only its returned frame is delayed, changed or dropped at the last connection.
fn reply_proxy_for(
    mut upstream: NativeObjectClient,
    select: fn(&Operation) -> bool,
    transform: impl FnOnce(NativeObjectReply) -> Option<NativeObjectReply> + Send + 'static,
) -> (
    NativeObjectClient,
    std::thread::JoinHandle<(Instant, NativeObjectReply)>,
) {
    // The proxy replaces only the transport of this real client, not its origin.
    let origin = std::sync::Arc::clone(&upstream.origin);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let worker = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(5);
        let mut peer = loop {
            match listener.accept() {
                Ok((peer, _)) => break peer,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < until,
                        "renewal proxy did not accept a client"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("renewal proxy accept: {error}"),
            }
        };
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        peer.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        loop {
            let mut header = [0; super::super::wire::HEADER_BYTES];
            peer.read_exact(&mut header).unwrap();
            assert_eq!(header[0], super::super::wire::PROTOCOL_STAMP);
            let code = super::super::meta_wire::MetaRequestCode::from_u8(header[1]).unwrap();
            let length = u32::from_be_bytes(header[2..6].try_into().unwrap()) as usize;
            assert!(length <= 2 * 1024 * 1024);
            let mut body = vec![0; length];
            peer.read_exact(&mut body).unwrap();
            let request = MetaRequest::decode(code, &body).unwrap();
            let selected = matches!(&request, MetaRequest::NativeObject(native) if native.operation.as_ref().is_some_and(select));
            let received = Instant::now();
            let reply = match upstream.meta.try_call(request) {
                MetaCallOutcome::Answered(reply) => reply,
                other => panic!("upstream operation did not answer: {other:?}"),
            };
            if selected {
                let MetaReply::NativeObject(original) = reply else {
                    panic!("native mutation returned another reply");
                };
                if let Some(changed) = transform(original.clone()) {
                    let frame = super::super::meta_message::encode_meta_reply(
                        &MetaReply::NativeObject(changed),
                    );
                    peer.write_all(&frame).unwrap();
                }
                return (received, original);
            }
            peer.write_all(&super::super::meta_message::encode_meta_reply(&reply))
                .unwrap();
        }
    });
    let meta = RemoteMetaClient::connect(
        &address,
        &MetaClientOptions {
            read_timeout: Duration::from_secs(15),
            ..Default::default()
        },
    )
    .unwrap();
    let mut client = NativeObjectClient::new(meta, ReplicaClientOptions::default());
    client.origin = origin;
    (client, worker)
}

fn reply_proxy(
    upstream: NativeObjectClient,
    transform: impl FnOnce(NativeObjectReply) -> Option<NativeObjectReply> + Send + 'static,
) -> (
    NativeObjectClient,
    std::thread::JoinHandle<(Instant, NativeObjectReply)>,
) {
    reply_proxy_for(
        upstream,
        |operation| matches!(operation, Operation::RenewWriter(_)),
        transform,
    )
}

#[test]
fn renewal_deadline_starts_before_the_reply_arrives() {
    let cluster = Cluster::new();
    let (client, mut writer) = writer(&cluster, "delayed-renewal", 31);
    let (mut proxy, worker) = reply_proxy(client, |reply| {
        std::thread::sleep(Duration::from_millis(100));
        Some(reply)
    });
    let result = proxy.renew_writer(&mut writer, 60_000);
    let (received, original) = worker.join().unwrap();
    result.unwrap();
    assert_eq!(writer.lease, original.lease.unwrap());
    // Causality, not a timing percentile: the request was sent before the proxy read it.
    assert!(writer.deadline <= received + Duration::from_secs(60));
}

#[test]
fn lost_renewal_reply_keeps_prior_authority_and_does_not_retry() {
    let cluster = Cluster::new();
    let (mut client, mut writer) = writer(&cluster, "lost-renewal", 32);
    let before_position = client.metadata_position().unwrap();
    let before_lease = writer.lease.clone();
    let before_deadline = writer.deadline;
    let before_pending = writer.pending_identity();
    let (mut proxy, worker) = reply_proxy(client, |_| None);
    let result = proxy.renew_writer(&mut writer, 60_000);
    let (_, original) = worker.join().unwrap();
    assert!(matches!(result, Err(NativeObjectError::Unknown(_))));
    assert!(original.lease.unwrap().expires_at_ms > before_lease.expires_at_ms);
    assert_eq!(writer.lease, before_lease);
    assert_eq!(writer.deadline, before_deadline);
    assert_eq!(writer.pending_identity(), before_pending);
    assert_eq!(
        cluster.client().metadata_position().unwrap(),
        before_position + 1
    );
}

#[test]
fn malformed_renewal_grants_never_extend_local_authority() {
    let cluster = Cluster::new();
    for field in 0..5 {
        let (client, mut writer) =
            writer(&cluster, &format!("invalid-renewal-{field}"), 40 + field);
        let lease = writer.lease.clone();
        let deadline = writer.deadline;
        let prior_expiry = lease.expires_at_ms;
        let (mut proxy, worker) = reply_proxy(client, move |mut reply| {
            match field {
                0 => reply.lease = None,
                1 => reply.lease.as_mut().unwrap().holder = "other-writer".into(),
                2 => reply.lease.as_mut().unwrap().epoch += 1,
                3 => reply.lease.as_mut().unwrap().expires_at_ms = prior_expiry,
                _ => reply.now_ms = 0,
            }
            Some(reply)
        });
        let result = proxy.renew_writer(&mut writer, 60_000);
        let (_, original) = worker.join().unwrap();
        assert!(
            matches!(result, Err(NativeObjectError::Unknown(_))),
            "accepted reply field {field}"
        );
        assert!(original.lease.unwrap().expires_at_ms > prior_expiry);
        assert_eq!(writer.lease, lease);
        assert_eq!(writer.deadline, deadline);
        assert!(writer.pending_identity().is_none());
    }
}

#[test]
fn malformed_publication_grants_do_not_create_a_writer_handle() {
    let cluster = Cluster::new();
    for field in 0..6 {
        let key = format!("publication-grant-{field}");
        let (client, stage) = staged(&cluster, &key, 60 + field);
        let (mut proxy, worker) = reply_proxy_for(
            client,
            |operation| matches!(operation, Operation::Publish(_)),
            move |mut reply| {
                match field {
                    0 => reply.lease = None,
                    1 => reply.lease.as_mut().unwrap().holder = "other-writer".into(),
                    2 => reply.lease.as_mut().unwrap().epoch += 1,
                    3 => {
                        reply.lease.as_mut().unwrap().expires_at_ms =
                            reply.now_ms + MAX_LEASE_TTL_MS + 1
                    }
                    4 => {
                        let manifest = reply.manifest.as_mut().unwrap();
                        manifest.length = 3;
                        manifest.extents.last_mut().unwrap().length = 3;
                    }
                    _ => {
                        reply
                            .manifest
                            .as_mut()
                            .unwrap()
                            .extents
                            .last_mut()
                            .unwrap()
                            .flags = CHUNK_FLAG_SEALED;
                        reply.lease = None;
                    }
                }
                Some(reply)
            },
        );
        let result = proxy.publish(&stage, true);
        let (_, original) = worker.join().unwrap();
        assert!(
            matches!(result, Err(NativeObjectError::Unknown(_))),
            "accepted publication reply field {field}"
        );
        assert_eq!(original.manifest.as_ref().unwrap().length, 4);
        assert_eq!(original.lease.as_ref().unwrap().holder, "writer");
        let mut reader = cluster.client();
        assert_eq!(
            reader.metadata_position().unwrap(),
            original.commit_index,
            "publication was retried"
        );
        let pinned = reader.pin("bucket", &key, 30_000).unwrap();
        assert_eq!(reader.read_range(&pinned, 0, 4).unwrap(), b"init");
        reader.close_pin(&pinned).unwrap();
    }
}

#[test]
fn only_the_opening_client_renews_its_data_writer() {
    let cluster = Cluster::new();
    let (mut original, mut writer) = writer(&cluster, "scoped-renewal", 49);
    let mut other = cluster.client();
    let before = original.metadata_position().unwrap();
    let lease = writer.lease.clone();
    let deadline = writer.deadline;
    assert!(matches!(
        other.renew_writer(&mut writer, 60_000),
        Err(NativeObjectError::Refused(_))
    ));
    assert_eq!(original.metadata_position().unwrap(), before);
    assert_eq!(writer.lease, lease);
    assert_eq!(writer.deadline, deadline);
    original.renew_writer(&mut writer, 60_000).unwrap();
    assert_eq!(original.metadata_position().unwrap(), before + 1);
}
