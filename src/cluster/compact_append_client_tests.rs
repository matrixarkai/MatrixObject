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
            assert_eq!(result.unwrap(), after);
        } else {
            assert!(result.is_err());
        }
        drop(client);
        worker.join().unwrap();
    }
}
