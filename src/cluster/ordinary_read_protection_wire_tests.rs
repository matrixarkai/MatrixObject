// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn payload(fence: u64) -> ManifestRoutesPayload {
    ManifestRoutesPayload {
        version: if fence == 0 { 1 } else { 2 },
        publication_fence: fence,
        manifest: Some(crate::object_manifest::ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "object".into(),
            generation: 1,
            ..Default::default()
        }),
        routes: vec![],
    }
}

#[test]
fn policy_requires_explicit_wire_version_and_preserves_legacy_bytes() {
    for (version, fence, valid) in [(1, 0, true), (2, 9, true), (1, 9, false), (2, 0, false)] {
        let mut value = payload(fence);
        value.version = version;
        assert_eq!(validate_manifest_route_payload(value, 4).is_ok(), valid);
    }
    let old = payload(0);
    let encoded = old.encode_to_vec();
    // Field four is omitted at zero: the legacy envelope remains byte-identical.
    let manifest = old.manifest.unwrap().encode_to_vec();
    let mut expected = vec![8, 1, 18, manifest.len() as u8];
    expected.extend(manifest);
    assert_eq!(encoded, expected);
    let bytes = payload(9).encode_to_vec();
    let decoded = MetaReply::decode(MetaRequestCode::ReadManifestRoutes, &bytes).unwrap();
    assert!(matches!(
        decoded,
        MetaReply::ManifestRoutes {
            publication_fence: 9,
            ..
        }
    ));
}

#[test]
fn batch_refuses_mixed_policies_and_noncanonical_payloads() {
    let code = MetaRequestCode::ReadManifestRoutesBatch;
    let mut batch = ManifestRoutesBatchPayload {
        version: 1,
        objects: vec![payload(9), payload(0)],
    };
    assert!(MetaReply::decode(code, &batch.encode_to_vec()).is_err());
    batch.objects[1] = payload(9);
    let mut bytes = batch.encode_to_vec();
    assert!(MetaReply::decode(code, &bytes).is_ok());
    bytes.extend([0xa0, 0x06, 1]);
    assert!(MetaReply::decode(code, &bytes).is_err());
}
