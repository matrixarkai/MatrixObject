// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

#[test]
fn compact_selected_grants_bound_wire_size_without_changing_durable_decisions() {
    let mut measured = Vec::new();
    for count in [1, 4096] {
        let service = fixture(count);
        let manifest = service
            .object_manifests
            .get(&crate::ObjectId::new("bucket", "object"))
            .unwrap();
        let command = ManifestRetentionRequest {
            version: 3,
            action: 8,
            generation: manifest.generation,
            manifest_sha256: crate::read_session_outcome::manifest_sha256(manifest).unwrap(),
            ..open()
        };
        let (legacy, legacy_reply) = prepare_at(&service, &open(), usize::MAX, 1000).unwrap();
        let (compact, reply) = prepare_at(&service, &command, 63, 1000).unwrap();
        assert_eq!(legacy.to_payload().unwrap(), compact.to_payload().unwrap());
        assert!(prepare_at(&service, &open(), 63, 1000).is_err());
        let MetaReply::ManifestRetention(reply) = reply else {
            panic!()
        };
        use prost::Message;
        let encoded = reply.encode_to_vec();
        assert_eq!(encoded.len(), 44);
        assert_eq!(
            ManifestRetentionReply::from_payload(&encoded).unwrap(),
            reply
        );
        assert_eq!(reply.manifest_sha256, command.manifest_sha256);
        assert!(reply.retained.is_none());
        measured.push((
            count,
            legacy_reply.payload_len(),
            encoded.len(),
            command.encoded_len(),
        ));
        let mut restored = MatrixMetaService::new();
        let snapshot = service.export_snapshot();
        restored.install_placement_roster(&snapshot).unwrap();
        restored.install_snapshot(snapshot).unwrap();
        compact.apply(&mut restored).unwrap();
        assert_eq!(
            restored
                .object_manifests
                .retained_manifest(reply.retention_id)
                .unwrap()
                .manifest
                .as_ref(),
            Some(manifest)
        );
    }
    assert_eq!(measured[0].2, measured[1].2);
    assert_eq!(measured[0].3, measured[1].3);
    assert!(measured[1].1 > 200_000);
    println!("SELECTED_GRANT_WIRE extent_count,full_reply,compact_reply,request: {measured:?}");
}

#[test]
fn compact_selected_grants_refuse_changed_digest_generation_and_capacity_before_mutation() {
    let mut service = fixture(1);
    let manifest = service
        .object_manifests
        .get(&crate::ObjectId::new("bucket", "object"))
        .unwrap();
    let command = ManifestRetentionRequest {
        version: 3,
        action: 8,
        generation: manifest.generation,
        manifest_sha256: crate::read_session_outcome::manifest_sha256(manifest).unwrap(),
        ..open()
    };
    let before = service.export_snapshot().to_bytes().unwrap();
    let mut wrong = command.clone();
    wrong.manifest_sha256[0] ^= 1;
    assert!(prepare_at(&service, &wrong, 8192, 1000).is_err());
    wrong = command.clone();
    wrong.generation += 1;
    assert!(prepare_at(&service, &wrong, 8192, 1000).is_err());
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
    assert!(prepare_at(&service, &command, 43, 1000).is_err());
    for _ in 0..crate::manifest_retention::MAX_RETAINED_MANIFESTS {
        let (decision, _) = prepare_at(&service, &command, 63, 1000).unwrap();
        decision.apply(&mut service).unwrap();
    }
    let full = service.export_snapshot().to_bytes().unwrap();
    assert!(prepare_at(&service, &command, 63, 1000).is_err());
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), full);
}
