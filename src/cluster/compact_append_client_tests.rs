// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[test]
fn compact_append_keeps_original_outcome_and_never_replays_uncertain_acknowledgements() {
    for mode in 0..4 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let mut stream = accept(&listener);
            let MetaRequest::StagedAllocation(command) = receive(&mut stream) else {
                panic!()
            };
            assert_eq!(
                (command.version, command.action, command.allocation_id),
                (4, 11, 7)
            );
            assert!(command.outcome.is_none());
            let delta = command.manifest_append.unwrap();
            if mode != 0 {
                let mut digest = delta.after_sha256;
                if mode == 1 {
                    digest[0] ^= 1;
                }
                stream
                    .write_all(&crate::cluster::meta_message::encode_meta_reply(
                        &MetaReply::StagedAllocation(
                            crate::staged_allocation::StagedAllocationReply {
                                version: 3,
                                committed_allocation_id: if mode == 3 { 8 } else { 7 },
                                manifest_sha256: digest,
                                ..Default::default()
                            },
                        ),
                    ))
                    .unwrap();
                assert_eq!(stream.read(&mut [0]).unwrap(), 0);
            }
            drop(stream);
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
            );
        });
        let before = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 1,
            ..Default::default()
        };
        let after = ObjectManifest {
            generation: 2,
            length: 1,
            extents: vec![ObjectExtent {
                group: "group".into(),
                chunk_id: 7,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![9; 32],
                ..Default::default()
            }],
            ..before.clone()
        };
        let outcome = ManifestOutcome::decide(Some(before), after.clone()).unwrap();
        let mut client =
            RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let (result, unadmitted, retained) =
            client.publish_compact_append_preserving_outcome(outcome.clone(), 7, 0);
        assert!(!unadmitted);
        assert_eq!(retained, outcome);
        if mode == 2 {
            assert!(result.is_ok());
            assert_eq!(retained.after, Some(after));
        } else {
            assert!(result.is_err());
        }
        drop(client);
        worker.join().unwrap();
    }
}

fn compact_publication_owned_fixture(prefix: usize) -> ManifestOutcome {
    let extent = |index: usize| ObjectExtent {
        group: "group".into(),
        chunk_id: index as u64 + 1,
        object_offset: index as u64,
        length: 1,
        write_sequence: 1,
        data_sha256: vec![9; 32],
        ..Default::default()
    };
    let before = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "key".into(),
        generation: 1,
        length: prefix as u64,
        extents: (0..prefix).map(extent).collect(),
        ..Default::default()
    };
    let mut after = before.clone();
    after.generation = 2;
    after.length += 1;
    after.extents.push(extent(prefix));
    ManifestOutcome::decide(Some(before), after).unwrap()
}

#[test]
fn compact_publication_returns_the_owned_candidate_without_copying_its_extent_buffer() {
    for prefix in [1, 128, 4095] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let mut stream = accept(&listener);
            let MetaRequest::StagedAllocation(command) = receive(&mut stream) else {
                panic!("missing compact append")
            };
            assert_eq!(
                (command.version, command.action, command.allocation_id),
                (4, 11, 7)
            );
            assert!(command.outcome.is_none());
            let delta = command.manifest_append.unwrap();
            assert_eq!(delta.suffix.len(), 1);
            let reply =
                MetaReply::StagedAllocation(crate::staged_allocation::StagedAllocationReply {
                    version: 3,
                    committed_allocation_id: 7,
                    manifest_sha256: delta.after_sha256,
                    ..Default::default()
                });
            stream
                .write_all(&crate::cluster::meta_message::encode_meta_reply(&reply))
                .unwrap();
            assert_eq!(stream.read(&mut [0]).unwrap(), 0);
            assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
        });
        let outcome = compact_publication_owned_fixture(prefix);
        let expected = outcome.after.as_ref().unwrap().clone();
        let original = outcome.after.as_ref().unwrap().extents.as_ptr() as usize;
        let mut client =
            RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let published = client.publish_staged_append_compact(7, outcome, 0).unwrap();
        let returned = published.extents.as_ptr() as usize;
        drop(client);
        worker.join().unwrap();
        assert_eq!(published, expected);
        assert_eq!(
            returned, original,
            "compact publication must move the owned candidate extent buffer"
        );
    }
}

#[test]
fn compact_publication_reservation_keeps_the_outcome_on_every_acknowledgement_path() {
    for mode in 0..5 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let mut stream = accept(&listener);
            let MetaRequest::StagedAllocation(command) = receive(&mut stream) else {
                panic!("missing reserving append")
            };
            assert_eq!(
                (command.version, command.action, command.allocation_id),
                (5, 12, 7)
            );
            assert_eq!((command.page_count, command.ttl_ms), (1, 10000));
            assert!(command.outcome.is_none());
            let delta = command.manifest_append.unwrap();
            assert_eq!(delta.suffix.len(), 1);
            if mode != 0 {
                let mut digest = delta.after_sha256;
                if mode == 1 {
                    digest[0] ^= 1;
                }
                let reply =
                    MetaReply::StagedAllocation(crate::staged_allocation::StagedAllocationReply {
                        version: if mode == 4 { 3 } else { 5 },
                        committed_allocation_id: if mode == 3 { 8 } else { 7 },
                        manifest_sha256: digest,
                        ..Default::default()
                    });
                stream
                    .write_all(&crate::cluster::meta_message::encode_meta_reply(&reply))
                    .unwrap();
                assert_eq!(stream.read(&mut [0]).unwrap(), 0);
            }
            drop(stream);
            assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
        });
        let outcome = compact_publication_owned_fixture(128);
        let expected = outcome.clone();
        let original = outcome.after.as_ref().unwrap().extents.as_ptr() as usize;
        let mut client =
            RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let (result, unadmitted, retained) =
            client.publish_compact_append_reserving(outcome, 7, 0, 1, 10000);
        assert!(!unadmitted);
        assert_eq!(retained, expected);
        assert_eq!(
            retained.after.as_ref().unwrap().extents.as_ptr() as usize,
            original
        );
        if mode == 2 {
            assert!(result.unwrap().is_none());
        } else {
            assert!(result.is_err());
        }
        drop(client);
        worker.join().unwrap();
    }
}

#[test]
fn manifest_digest_binding_rejects_changed_prefix_with_a_warm_memo() {
    let genuine = compact_publication_owned_fixture(3);
    let before = genuine.before.as_ref().unwrap().clone();
    let server_before = crate::read_session_outcome::manifest_sha256(&before).unwrap();
    let server_after =
        crate::read_session_outcome::manifest_sha256(genuine.after.as_ref().unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let worker = std::thread::spawn(move || {
        let mut stream = accept(&listener);
        let MetaRequest::StagedAllocation(command) = receive(&mut stream) else {
            panic!("missing compact publication")
        };
        let append = command.manifest_append.unwrap();
        let frame = if append.before_sha256 == server_before {
            assert_eq!(append.after_sha256, server_after);
            crate::cluster::meta_message::encode_meta_reply(&MetaReply::StagedAllocation(
                crate::staged_allocation::StagedAllocationReply {
                    version: 3,
                    committed_allocation_id: 7,
                    manifest_sha256: server_after,
                    ..Default::default()
                },
            ))
        } else {
            crate::cluster::message::encode_failure(
                StatusCode::PreconditionFailed,
                "prior manifest digest differs",
            )
        };
        stream.write_all(&frame).unwrap();
        assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    });
    let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
    client.manifest_digest(&before).unwrap();
    let mut changed_before = before;
    changed_before.extents[1].data_sha256[0] ^= 1;
    let mut changed_after = genuine.after.unwrap();
    changed_after.extents[1].data_sha256[0] ^= 1;
    let changed = ManifestOutcome::decide(Some(changed_before), changed_after).unwrap();
    let result = client.publish_staged_append_compact(7, changed, 0);
    drop(client);
    worker.join().unwrap();
    assert!(
        result.is_err(),
        "a warm digest memo must not publish a changed interior prefix"
    );
}
