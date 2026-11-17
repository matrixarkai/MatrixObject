// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// A publication the metaserver refuses because the manifest changed under it was not admitted.

#[test]
fn a_refusal_that_names_a_changed_manifest_proves_the_publication_was_not_admitted() {
    for (message, proves) in [
        ("object manifest changed before publication", true),
        ("compact append prior manifest changed", true),
        ("append plan prior manifest changed", true),
        ("object changed before staging", true),
        ("metadata leadership changed; write outcome may be committed", false),
        ("the object manifest is corrupt", false),
        ("staged allocation is no longer active", false),
    ] {
        let wire = crate::cluster::meta_client::refusal_proves_not_admitted_for_tests(&ObjectError::PreconditionFailed(
            message.into(),
        ));
        assert_eq!(wire, proves, "{message}");
    }
    assert!(!crate::cluster::meta_client::refusal_proves_not_admitted_for_tests(&ObjectError::Corruption(
        "object manifest changed before publication".into()
    )));
    for mode in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let mut stream = accept(&listener);
            let _ = receive(&mut stream);
            match mode {
                // a typed refusal: the manifest changed
                0 | 1 => {
                    let message = if mode == 0 { "object manifest changed before publication" } else { "something else went wrong" };
                    let error = ObjectError::PreconditionFailed(message.into());
                    stream
                        .write_all(&crate::cluster::message::encode_bounded_failure(
                            StatusCode::for_error(&error),
                            &error.to_string(),
                            1 << 20,
                        ))
                        .unwrap();
                }
                // the connection drops without an answer
                _ => {}
            }
            drop(stream);
        });
        let before = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "key".into(),
            generation: 1,
            ..Default::default()
        };
        let after = ObjectManifest { generation: 2, ..before.clone() };
        let outcome = ManifestOutcome::decide(Some(before), after).unwrap();
        let mut client = RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let (result, unadmitted, _) = client.publish_preserving_outcome(outcome, None, 0);
        assert!(result.is_err());
        // only the refusal that says the manifest changed proves nothing was admitted
        assert_eq!(unadmitted, mode == 0, "mode {mode}");
        drop(client);
        worker.join().unwrap();
    }
}
