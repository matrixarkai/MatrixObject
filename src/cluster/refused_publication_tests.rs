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
    // The two refusals that say the catalog has no room are decided by the leader before any proposal:
    // they prove nothing was admitted, whatever prefix the wire put in front of them, and a message that
    // only mentions the words proves nothing.
    for (error, proves) in [
        (crate::object_manifest::catalog_full(8), true),
        (crate::object_manifest::catalog_image_too_large(), true),
        (ObjectError::InvalidArgument("invalid argument: catalog full: 8 objects".into()), true),
        (ObjectError::InvalidArgument("invalid argument: invalid argument: catalog image too large: x".into()), true),
        (ObjectError::InvalidArgument("bad key near catalog full: ".into()), false),
        (ObjectError::Corruption("catalog full: 8 objects".into()), false),
        (ObjectError::PreconditionFailed("catalog full: 8 objects".into()), false),
    ] {
        assert_eq!(
            crate::cluster::meta_client::refusal_proves_not_admitted_for_tests(&error),
            proves,
            "{error:?}"
        );
    }
    for mode in 0..5 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let mut stream = accept(&listener);
            let _ = receive(&mut stream);
            match mode {
                // a typed refusal: the manifest changed
                0 | 1 | 3 | 4 => {
                    // 3 and 4 are the catalog-full refusals, sent as the metaserver sends them
                    let error = match mode {
                        0 => ObjectError::PreconditionFailed("object manifest changed before publication".into()),
                        1 => ObjectError::PreconditionFailed("something else went wrong".into()),
                        3 => crate::object_manifest::catalog_full(8),
                        _ => crate::object_manifest::catalog_image_too_large(),
                    };
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
        // only a refusal that says the manifest changed, or that the catalog has no room, proves nothing
        // was admitted; the connection ends after ONE request, so a read-back or a retry would show here
        assert_eq!(unadmitted, matches!(mode, 0 | 3 | 4), "mode {mode}");
        if matches!(mode, 3 | 4) {
            let error = result.as_ref().unwrap_err();
            assert!(crate::object_manifest::catalog_full_refusal(error), "{error:?}");
            assert!(matches!(error, ObjectError::InvalidArgument(_)), "never corruption: {error:?}");
        }
        drop(client);
        worker.join().unwrap();
    }
}
