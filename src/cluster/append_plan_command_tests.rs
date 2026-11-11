// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[test]
fn compact_append_plan_checks_identity_and_keeps_prefix_off_wire() {
    use super::super::append_plan::AppendPlanRequest;
    use crate::cluster::MetaRequestCode;
    use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest};
    use prost::Message;
    for count in [1, 32, 33] {
        let mut lengths = Vec::new();
        for extents in [1, 4095] {
            let (mut service, groups) = fixture(count, 0);
            let manifest = ObjectManifest {
                format_version: 2,
                bucket: "bucket".into(),
                key: "planning".into(),
                generation: 1,
                length: extents,
                extents: (0..extents)
                    .map(|index| ObjectExtent {
                        group: "group-0".into(),
                        chunk_id: index + 1000,
                        object_offset: index,
                        length: 1,
                        write_sequence: 1,
                        data_sha256: vec![1; 32],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            PlacementOutcome::publish_manifest(
                &service,
                ManifestOutcome::decide(None, manifest.clone()).unwrap(),
                0,
            )
            .unwrap()
            .apply(&mut service)
            .unwrap();
            let command = AppendPlanRequest {
                version: 1,
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                generation: 1,
                manifest_sha256: crate::read_session_outcome::manifest_sha256(&manifest).unwrap(),
                length: 1,
                replica_count: count as u64,
            };
            assert!(read_only(&MetaRequest::PlanAppendRoutes(command.clone())));
            let saved = service.export_snapshot().to_bytes().unwrap();
            let dispatch = |service: &mut MatrixMetaService, command, limit| {
                super::super::meta_server::apply_with_groups(
                    service,
                    MetaRequest::PlanAppendRoutes(command),
                    limit,
                    &groups,
                )
            };
            assert!(dispatch(&mut service, command.clone(), 1).is_err());
            for field in 0..6 {
                let mut wrong = command.clone();
                match field {
                    0 => wrong.generation += 1,
                    1 => wrong.manifest_sha256[0] ^= 1,
                    2 => wrong.manifest_sha256.pop().map(|_| ()).unwrap(),
                    3 => wrong.version = 2,
                    4 => wrong.replica_count = 0,
                    _ => wrong.replica_count = 4097,
                }
                assert!(
                    dispatch(&mut service, wrong, usize::MAX).is_err(),
                    "field={field}"
                );
                assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
            }
            let reply = dispatch(&mut service, command.clone(), usize::MAX).unwrap();
            assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
            let encoded = reply.encode_payload();
            let decoded = MetaReply::decode(MetaRequestCode::PlanAppendRoutes, &encoded).unwrap();
            assert_eq!(decoded, reply);
            let MetaReply::AppendPlan(value) = decoded else {
                panic!()
            };
            let plan = value.checked_plan().unwrap();
            assert_eq!(plan.node_ids.len(), count);
            assert_eq!(value.routes.len(), if count <= 32 { count } else { 0 });
            let mut wrong = value.clone();
            wrong.generation = 0;
            assert!(
                MetaReply::decode(MetaRequestCode::PlanAppendRoutes, &wrong.encode_to_vec())
                    .is_err()
            );
            if !value.routes.is_empty() {
                wrong = value.clone();
                wrong.routes[0].group.as_mut().unwrap().placement_id = "other".into();
                assert!(wrong.checked_plan().is_err());
            } else {
                wrong = value.clone();
                wrong
                    .routes
                    .push(groups.routing_view(&plan.node_ids[0]).unwrap());
                assert!(wrong.checked_plan().is_err());
            }
            let mut duplicate = command.encode_to_vec();
            duplicate.extend_from_slice(&[8, 1]);
            assert!(AppendPlanRequest::from_payload(&duplicate).is_err());
            let mut unknown = encoded.clone();
            unknown.extend_from_slice(&[0xf8, 0x07, 0]);
            assert!(MetaReply::decode(MetaRequestCode::PlanAppendRoutes, &unknown).is_err());
            lengths.push((command.encoded_len(), encoded.len()));
        }
        assert_eq!(
            lengths[0], lengths[1],
            "prefix changed plan frame size at width {count}"
        );
    }
}

#[test]
fn compact_append_plan_client_fetches_wide_routes_and_rejects_wrong_identity() {
    use crate::cluster::{MetaClientOptions, MetaRequestCode, RemoteMetaClient};
    use crate::object_manifest::{ManifestOutcome, ObjectManifest};
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };
    for (count, wrong_identity) in [(1, 0), (32, 0), (33, 0), (1, 1), (1, 2)] {
        let (mut service, groups) = fixture(count, 0);
        let manifest = ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: "wide-plan".into(),
            generation: 1,
            ..Default::default()
        };
        PlacementOutcome::publish_manifest(
            &service,
            ManifestOutcome::decide(None, manifest.clone()).unwrap(),
            0,
        )
        .unwrap()
        .apply(&mut service)
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let worker = std::thread::spawn(move || {
            let saved = service.export_snapshot().to_bytes().unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut calls = Vec::new();
            loop {
                let mut header = [0; 6];
                match stream.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => panic!("plan client did not finish: {error}"),
                }
                let length = u32::from_be_bytes(header[2..].try_into().unwrap()) as usize;
                assert!(length < 8192);
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).unwrap();
                let code = MetaRequestCode::from_u8(header[1]).unwrap();
                calls.push(code);
                let request = MetaRequest::decode(code, &bytes).unwrap();
                assert!(read_only(&request));
                let mut reply = super::super::meta_server::apply_with_groups(
                    &mut service,
                    request,
                    usize::MAX,
                    &groups,
                )
                .unwrap();
                if let MetaReply::AppendPlan(value) = &mut reply {
                    if wrong_identity == 1 {
                        value.generation += 1;
                    }
                    if wrong_identity == 2 {
                        value.manifest_sha256[0] ^= 1;
                    }
                }
                stream
                    .write_all(&super::super::meta_message::encode_meta_reply(&reply))
                    .unwrap();
            }
            assert_eq!(service.export_snapshot().to_bytes().unwrap(), saved);
            let expected = if count <= 32 {
                vec![MetaRequestCode::PlanAppendRoutes]
            } else {
                vec![
                    MetaRequestCode::PlanAppendRoutes,
                    MetaRequestCode::ReadChunkRoutesWide,
                    MetaRequestCode::ReadChunkRoute,
                ]
            };
            assert_eq!(
                calls, expected,
                "unexpected full-manifest read or route batch"
            );
        });
        let mut client =
            RemoteMetaClient::connect(&endpoint, &MetaClientOptions::default()).unwrap();
        let result = client.plan_append_routes_compact(&manifest, 1, count as u64);
        if wrong_identity != 0 {
            assert!(result.is_err());
        } else {
            let (plan, routes) = result.unwrap();
            assert_eq!(routes.len(), count);
            assert_eq!(
                plan.node_ids,
                routes
                    .iter()
                    .map(|route| route.group.as_ref().unwrap().placement_id.clone())
                    .collect::<Vec<_>>()
            );
        }
        drop(client);
        worker.join().unwrap();
    }
}
