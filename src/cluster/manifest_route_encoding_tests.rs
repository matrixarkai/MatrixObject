// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::cluster::meta_message::{
    encode_meta_reply, ManifestRoutesBatchPayload, ManifestRoutesPayload, MetaReply,
};
use crate::cluster::{ChunkGroupConfig, ChunkGroupIdentity, MetaRequestCode, PROTOCOL_STAMP};
use crate::object_manifest::ObjectExtent;
use prost::Message;

fn manifest(count: u64, object: usize) -> ObjectManifest {
    let key_length = match count {
        127 => 127,
        128 => 128,
        4096 => 1024,
        _ => 1,
    };
    ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: format!("{}{object}", "k".repeat(key_length - 1)),
        generation: if count == 4096 { u64::MAX } else { 128 },
        length: count,
        extents: (0..count)
            .map(|page| ObjectExtent {
                group: "group-a".into(),
                volume_id: 1,
                chunk_id: 1,
                page_index: page,
                object_offset: page,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![7; 32],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn route(identity_version: u32) -> ChunkGroupRoute {
    ChunkGroupRoute {
        version: 1,
        group: Some(ChunkGroupConfig {
            placement_id: "group-a".into(),
            failure_domain: "rack-a".into(),
            identity: Some(ChunkGroupIdentity {
                version: identity_version,
                group_id: 1,
                node_id: 1,
                capacity_bytes: 1 << 28,
                max_pages: 65536,
                voters: vec![1],
                peer_endpoints: if identity_version == 1 {
                    vec!["127.0.0.1:19000".into()]
                } else {
                    vec![]
                },
                bootstrap_voters: vec![],
                bootstrap_id: 0,
            }),
            api_endpoints: vec!["127.0.0.1:20000".into()],
            member_ids: vec!["member-1".into()],
        }),
        fence: vec![],
    }
}

fn check(reply: MetaReply, code: MetaRequestCode, expected: Vec<u8>) {
    assert_eq!(reply.payload_len(), expected.len());
    assert_eq!(reply.encode_payload(), expected);
    let mut frame = vec![PROTOCOL_STAMP, 0];
    frame.extend_from_slice(&(expected.len() as u32).to_be_bytes());
    frame.extend_from_slice(&expected);
    assert_eq!(encode_meta_reply(&reply), frame);
    assert_eq!(MetaReply::decode(code, &expected).unwrap(), reply);
}

#[test]
fn borrowed_envelopes_match_derived_protobuf_at_length_boundaries() {
    for count in [0, 1, 127, 128, 4096] {
        for fence in [0, 128, u64::MAX] {
            for route_kind in [0, 1, 3] {
                let routes = if route_kind != 0 && count != 0 {
                    vec![route(route_kind)]
                } else {
                    vec![]
                };
                let value = manifest(count, 0);
                let expected = ManifestRoutesPayload {
                    version: version(fence),
                    manifest: Some(value.clone()),
                    routes: routes.clone(),
                    publication_fence: fence,
                }
                .encode_to_vec();
                check(
                    MetaReply::ManifestRoutes {
                        manifest: value,
                        routes: routes.clone(),
                        publication_fence: fence,
                    },
                    MetaRequestCode::ReadManifestRoutesWide,
                    expected,
                );
                let objects = (0..4)
                    .map(|object| ManifestRouteEntry {
                        manifest: manifest(count, object),
                        routes: routes.clone(),
                        publication_fence: fence,
                    })
                    .collect::<Vec<_>>();
                let expected = ManifestRoutesBatchPayload {
                    version: 1,
                    objects: objects
                        .iter()
                        .map(|object| ManifestRoutesPayload {
                            version: version(fence),
                            manifest: Some(object.manifest.clone()),
                            routes: object.routes.clone(),
                            publication_fence: fence,
                        })
                        .collect(),
                }
                .encode_to_vec();
                check(
                    MetaReply::ManifestRoutesBatch { objects },
                    MetaRequestCode::ReadManifestRoutesBatch,
                    expected,
                );
            }
        }
    }
}

#[test]
fn canonical_decode_still_refuses_duplicate_unknown_and_explicit_default_fields() {
    let reply = MetaReply::ManifestRoutes {
        manifest: manifest(1, 0),
        routes: vec![],
        publication_fence: 0,
    };
    for (code, payload) in [
        (MetaRequestCode::ReadManifestRoutes, reply.encode_payload()),
        (
            MetaRequestCode::ReadManifestRoutesBatch,
            MetaReply::ManifestRoutesBatch {
                objects: vec![ManifestRouteEntry {
                    manifest: manifest(1, 0),
                    routes: vec![],
                    publication_fence: 0,
                }],
            }
            .encode_payload(),
        ),
    ] {
        for extra in [&[8, 1][..], &[0xa0, 0x06, 1][..]] {
            let mut wrong = payload.clone();
            wrong.extend_from_slice(extra);
            assert!(MetaReply::decode(code, &wrong).is_err());
        }
        let mut overlong = payload.clone();
        overlong.splice(1..2, [0x81, 0]);
        assert!(MetaReply::decode(code, &overlong).is_err());
        assert!(MetaReply::decode(code, &payload[..payload.len() - 1]).is_err());
    }
    let mut default_fence = reply.encode_payload();
    default_fence.extend([32, 0]);
    assert!(MetaReply::decode(MetaRequestCode::ReadManifestRoutes, &default_fence).is_err());
}
