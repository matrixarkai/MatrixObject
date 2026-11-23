// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::native_object_outcome::{native_operation::Operation, *};
use crate::native_object_state::NativeReadPin;
use crate::native_object_state_tests::{chunk, hash, publication, start};
use crate::placement_outcome::PlacementOutcome;
use crate::MatrixMetaService;

pub(crate) fn service() -> MatrixMetaService {
    let mut s = MatrixMetaService::new();
    s.cluster_features.raised = crate::cluster_features::FEATURE_NATIVE_OBJECT_STATE
        | crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS;
    s.placement_commit_index = 1;
    s.next_commit_index = 2;
    s
}
fn stage(revision: u64) -> NativeStageRef {
    NativeStageRef {
        stage_id: 2,
        expected_revision: revision,
        holder: "writer".into(),
    }
}
pub(crate) fn commands() -> Vec<(Operation, u64)> {
    let s = start(2, None, 4);
    let owned = chunk(&s, 3, 0, 4, 0);
    let manifest = publication(&s, &[owned.clone()]);
    vec![
        (
            Operation::Begin(NativeBegin {
                start: Some(s),
                ..Default::default()
            }),
            1000,
        ),
        (
            Operation::Extend(NativeExtend {
                stage: Some(stage(2)),
                chunk: Some(owned),
            }),
            1001,
        ),
        (
            Operation::Publish(NativePublish {
                stage: Some(stage(3)),
                manifest: Some(manifest),
                expected_lease: None,
            }),
            1002,
        ),
    ]
}
fn bytes(s: &MatrixMetaService) -> Vec<u8> {
    s.export_snapshot().to_bytes().unwrap()
}
fn commit(s: &mut MatrixMetaService, op: Operation, now: u64) -> Vec<u8> {
    let before = bytes(s);
    let decided = NativeObjectChange::decide(s, op, now).unwrap();
    assert_eq!(bytes(s), before, "leader decision mutated live state");
    let payload = decided.to_payload().unwrap();
    PlacementOutcome::from_payload(&payload)
        .unwrap()
        .apply(s)
        .unwrap();
    payload
}
fn published() -> MatrixMetaService {
    let mut s = service();
    for (op, now) in commands() {
        commit(&mut s, op, now);
    }
    s
}
#[test]
fn binary_commands_replay_identically_across_three_metadata_replicas() {
    let mut leader = service();
    let mut followers = [service(), service(), service()];
    let mut journal = Vec::new();
    for (op, now) in commands() {
        journal.push(commit(&mut leader, op, now));
    }
    let manifest = publication(&start(2, None, 4), &[chunk(&start(2, None, 4), 3, 0, 4, 0)]);
    journal.push(commit(
        &mut leader,
        Operation::OpenPin(NativeReadPin {
            pin_id: 5,
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            generation: manifest.generation,
            manifest_digest: hash(&manifest),
            expires_at_ms: 5000,
        }),
        1003,
    ));
    let before = leader
        .writer_leases
        .get(&manifest.bucket, &manifest.key)
        .unwrap();
    let mut after = before.clone();
    after.expires_at_ms = 12000;
    journal.push(commit(
        &mut leader,
        Operation::RenewWriter(NativeWriterRenewal {
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            before: Some(before),
            after: Some(after.clone()),
        }),
        1004,
    ));
    journal.push(commit(
        &mut leader,
        Operation::RenewPin(NativePinRenewal {
            pin_id: 5,
            expected_expiry: 5000,
            expires_at_ms: 6000,
        }),
        1005,
    ));
    journal.push(commit(
        &mut leader,
        Operation::ClosePin(NativePinRelease {
            pin_id: 5,
            expected_expiry: 6000,
        }),
        1006,
    ));
    journal.push(commit(
        &mut leader,
        Operation::ReleaseWriter(NativeWriterRelease {
            bucket: manifest.bucket.clone(),
            key: manifest.key.clone(),
            before: Some(after),
            holder: "writer".into(),
            expired_only: false,
        }),
        1007,
    ));
    for follower in &mut followers {
        for payload in &journal {
            PlacementOutcome::from_payload(payload)
                .unwrap()
                .apply(follower)
                .unwrap();
        }
        assert_eq!(bytes(follower), bytes(&leader));
    }
    assert_eq!(leader.placement_commit_index, 9);
    assert!(leader
        .writer_leases
        .get(&manifest.bucket, &manifest.key)
        .is_none());
}
#[test]
fn rejected_stage_commands_leave_full_metadata_snapshot_unchanged() {
    let mut s = service();
    let (begin, now) = commands().remove(0);
    commit(&mut s, begin, now);
    let original = bytes(&s);
    for field in 0..5 {
        let mut command = NativeExtend {
            stage: Some(stage(2)),
            chunk: Some(chunk(&start(2, None, 4), 3, 0, 4, 0)),
        };
        match field {
            0 => command.stage.as_mut().unwrap().holder = "other".into(),
            1 => command.stage.as_mut().unwrap().expected_revision = 1,
            2 => command.chunk.as_mut().unwrap().allocation_id = 4,
            3 => {
                command
                    .chunk
                    .as_mut()
                    .unwrap()
                    .initialization
                    .as_mut()
                    .unwrap()
                    .writer_epoch = 1
            }
            _ => {
                command
                    .chunk
                    .as_mut()
                    .unwrap()
                    .extent
                    .as_mut()
                    .unwrap()
                    .object_offset = 1
            }
        }
        let change = NativeObjectChange {
            version: 1,
            now_ms: 1001,
            operation: Some(Operation::Extend(command)),
        };
        assert!(change.apply(&mut s, 2, 3).is_err());
        assert_eq!(bytes(&s), original);
    }
    let mut extension = commands().remove(1);
    extension.1 = 10000;
    assert!(NativeObjectChange::decide(&s, extension.0, extension.1).is_err());
    assert_eq!(bytes(&s), original);
}
#[test]
fn outcome_version_and_envelope_fields_are_exclusive() {
    let s = service();
    let (op, now) = commands().remove(0);
    let good = NativeObjectChange::decide(&s, op, now).unwrap();
    for field in 0..6 {
        let mut bad = good.clone();
        match field {
            0 => bad.version = 17,
            1 => bad.native = None,
            2 => bad.native.as_mut().unwrap().version = 2,
            3 => bad.native.as_mut().unwrap().now_ms = 0,
            4 => bad.after_index += 1,
            _ => bad.forget = true,
        }
        assert!(bad.to_payload().is_err(), "accepted field {field}");
    }
    let mut payload = good.to_payload().unwrap();
    payload[5] ^= 1;
    assert!(PlacementOutcome::from_payload(&payload).is_err());
}
#[test]
fn staged_and_published_native_objects_reject_legacy_capture() {
    let mut s = service();
    let id = start(2, None, 4).header.unwrap().object_id();
    for (op, now) in commands() {
        commit(&mut s, op, now);
        assert!(PlacementOutcome::capture(&s, vec![], Some(id.clone())).is_err());
    }
}
#[test]
fn leader_admission_and_follower_replay_have_separate_limits() {
    let s = service();
    let (op, now) = commands().remove(0);
    let outcome = NativeObjectChange::decide(&s, op, now).unwrap();
    let change = outcome.native.unwrap();
    assert!(change.check(&s, 1, 2, Some(0)).is_err());
    assert!(change.check(&s, 1, 2, None).is_ok());
    let mut disabled = service();
    disabled.cluster_features.raised = 0;
    assert!(change.apply(&mut disabled, 1, 2).is_err());
    assert!(disabled.native_objects.is_default());
}
#[test]
fn expired_stages_keep_allocations_for_durable_retirement() {
    for expired in [false, true] {
        let mut s = service();
        for (op, now) in commands().into_iter().take(2) {
            commit(&mut s, op, now);
        }
        let (op, now) = if expired {
            (
                Operation::ExpireStage(NativeStageExpiry {
                    stage_id: 2,
                    expected_revision: 3,
                    expected_expiry: 10000,
                }),
                10000,
            )
        } else {
            (Operation::Abort(stage(3)), 1002)
        };
        commit(&mut s, op, now);
        assert_eq!(s.native_objects.stage_count(), 0);
        assert_eq!(s.native_objects.retired_stage_count(), 1);
        assert_eq!(
            s.native_objects
                .retired_chunks()
                .next()
                .unwrap()
                .1
                .allocation_id,
            3
        );
        let restored = crate::MetaSnapshot::from_bytes(&bytes(&s)).unwrap();
        assert_eq!(restored.native_objects, s.native_objects);
    }
}
#[test]
fn stale_writer_renewal_and_release_are_atomic() {
    let mut s = published();
    let manifest = commands().remove(2).0;
    let Operation::Publish(p) = manifest else {
        unreachable!()
    };
    let m = p.manifest.unwrap();
    let lease = s.writer_leases.get(&m.bucket, &m.key).unwrap();
    let saved = bytes(&s);
    for field in 0..4 {
        let mut before = lease.clone();
        let mut after = lease.clone();
        after.expires_at_ms = 11000;
        match field {
            0 => before.holder = "other".into(),
            1 => before.epoch = 1,
            2 => after.epoch = 3,
            _ => after.expires_at_ms = 9000,
        }
        assert!(NativeObjectChange::decide(
            &s,
            Operation::RenewWriter(NativeWriterRenewal {
                bucket: m.bucket.clone(),
                key: m.key.clone(),
                before: Some(before),
                after: Some(after)
            }),
            1003
        )
        .is_err());
        assert_eq!(bytes(&s), saved);
    }
    assert!(NativeObjectChange::decide(
        &s,
        Operation::ReleaseWriter(NativeWriterRelease {
            bucket: m.bucket.clone(),
            key: m.key.clone(),
            before: Some(lease.clone()),
            holder: String::new(),
            expired_only: true
        }),
        9999
    )
    .is_err());
    commit(
        &mut s,
        Operation::ReleaseWriter(NativeWriterRelease {
            bucket: m.bucket,
            key: m.key,
            before: Some(lease),
            holder: String::new(),
            expired_only: true,
        }),
        10000,
    );
}

#[test]
fn canonical_binary_rejects_duplicate_and_unknown_fields_even_with_valid_checksums() {
    use prost::Message;
    fn envelope(body: &[u8]) -> Vec<u8> {
        let mut bytes = b"MOMP".to_vec();
        bytes.extend_from_slice(body);
        let checksum = crate::fnv1a64(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes
    }
    #[derive(Clone, PartialEq, Message)]
    struct RawEnvelope {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(uint64, tag = "2")]
        before: u64,
        #[prost(uint64, tag = "3")]
        after: u64,
        #[prost(bytes = "vec", tag = "22")]
        native: Vec<u8>,
    }
    let s = service();
    let (op, now) = commands().remove(0);
    let good = NativeObjectChange::decide(&s, op, now).unwrap();
    let mut duplicate = good.encode_to_vec();
    duplicate.extend_from_slice(&[8, 18]);
    assert!(PlacementOutcome::from_payload(&envelope(&duplicate)).is_err());
    let mut unknown = good.encode_to_vec();
    unknown.extend_from_slice(&[0xf8, 1, 1]);
    assert!(PlacementOutcome::from_payload(&envelope(&unknown)).is_err());
    let mut inner = good.native.unwrap().encode_to_vec();
    inner.extend_from_slice(&[0xf8, 1, 1]);
    let raw = RawEnvelope {
        version: 18,
        before: 1,
        after: 2,
        native: inner,
    };
    assert!(PlacementOutcome::from_payload(&envelope(&raw.encode_to_vec())).is_err());
    assert!(PlacementOutcome::from_payload(&vec![0; 8 * 1024 * 1024 + 1]).is_err());
}
#[test]
fn pin_expiry_requires_exact_deadline_and_committed_clock() {
    let mut s = published();
    let Operation::Publish(p) = commands().remove(2).0 else {
        unreachable!()
    };
    let m = p.manifest.unwrap();
    commit(
        &mut s,
        Operation::OpenPin(NativeReadPin {
            pin_id: 5,
            bucket: m.bucket,
            key: m.key,
            generation: m.generation,
            manifest_digest: hash(&publication(
                &start(2, None, 4),
                &[chunk(&start(2, None, 4), 3, 0, 4, 0)],
            )),
            expires_at_ms: 5000,
        }),
        1003,
    );
    let saved = bytes(&s);
    for (expected, now) in [(4999, 5000), (5000, 4999)] {
        assert!(NativeObjectChange::decide(
            &s,
            Operation::ExpirePin(NativePinRelease {
                pin_id: 5,
                expected_expiry: expected
            }),
            now
        )
        .is_err());
        assert_eq!(bytes(&s), saved);
    }
    commit(
        &mut s,
        Operation::ExpirePin(NativePinRelease {
            pin_id: 5,
            expected_expiry: 5000,
        }),
        5000,
    );
    assert_ne!(bytes(&s), saved);
}
#[test]
fn fully_sealed_publication_creates_no_ready_writer_lease() {
    let mut s = service();
    for (op, now) in commands().into_iter().take(2) {
        commit(&mut s, op, now);
    }
    let Operation::Publish(mut p) = commands().remove(2).0 else {
        unreachable!()
    };
    let m = p.manifest.as_mut().unwrap();
    m.extents.last_mut().unwrap().flags = crate::chunk_manifest::CHUNK_FLAG_SEALED;
    let id = m.object_id();
    commit(&mut s, Operation::Publish(p), 1002);
    assert!(s.writer_leases.get(&id.bucket, &id.key).is_none());
    assert_eq!(
        s.chunk_catalog.get(&id).unwrap().extents[0].flags,
        crate::chunk_manifest::CHUNK_FLAG_SEALED
    );
}
#[test]
fn native_begin_cannot_take_a_legacy_manifest_namespace() {
    let mut s = service();
    let id = start(2, None, 4).header.unwrap().object_id();
    let legacy = crate::object_manifest::ObjectManifest {
        format_version: 1,
        bucket: id.bucket.to_string(),
        key: id.key.to_string(),
        generation: 1,
        length: 0,
        extents: vec![],
        deleted: false,
    };
    s.object_manifests
        .apply(&crate::object_manifest::ManifestOutcome::decide(None, legacy).unwrap())
        .unwrap();
    let saved = bytes(&s);
    let (op, now) = commands().remove(0);
    assert!(NativeObjectChange::decide(&s, op, now).is_err());
    assert_eq!(bytes(&s), saved);
}
