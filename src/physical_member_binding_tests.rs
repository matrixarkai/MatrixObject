// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::initialized_log_manifest::*;
use crate::native_object_outcome::{native_operation::Operation, *};
use crate::native_object_state_tests::{chunk, hash, publication, start};
use crate::{MatrixMetaService, MetaSnapshot};
pub(crate) fn physical_start(
    id: u64,
    before: Option<&crate::chunk_manifest::ChunkManifest>,
    length: u64,
) -> crate::native_object_state::NativeStageStart {
    let mut s = start(id, before, length);
    s.header.as_mut().unwrap().format_version = CHUNK_MANIFEST_PHYSICAL_FORMAT;
    s
}
pub(crate) fn commit(s: &mut MatrixMetaService, op: Operation, now: u64) {
    let decided = NativeObjectChange::decide(s, op, now).unwrap();
    assert_eq!(decided.version, 19);
    crate::placement_outcome::PlacementOutcome::from_payload(&decided.to_payload().unwrap())
        .unwrap()
        .apply(s)
        .unwrap();
}
pub(crate) fn stage_ref(id: u64, revision: u64) -> NativeStageRef {
    NativeStageRef {
        stage_id: id,
        expected_revision: revision,
        holder: "writer".into(),
    }
}
pub(crate) fn published() -> MatrixMetaService {
    let mut s = crate::native_object_outcome_tests::service();
    for (op, now) in crate::native_object_outcome_tests::commands() {
        commit(&mut s, op, now);
    }
    s
}
#[test]
fn physical_manifest_and_selected_binding_preserve_member_order_and_coordinates() {
    let start = physical_start(2, None, 4);
    let owned = chunk(&start, 3, 0, 4, 0);
    let m = publication(&start, &[owned]);
    let decoded = crate::chunk_manifest::ChunkManifest::from_bytes(&m.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded, m);
    let binding = m
        .initialized_log
        .as_ref()
        .unwrap()
        .physical_binding(&m, 0)
        .unwrap();
    assert_eq!(
        (
            binding.volume_id,
            binding.chunk_id,
            binding.generation,
            binding.writer_epoch
        ),
        (0, 3, 1, 2)
    );
    assert_eq!(binding.storage_incarnations, [[40; 16], [41; 16], [42; 16]]);
    assert!(m
        .initialized_log
        .as_ref()
        .unwrap()
        .physical_binding(&m, 1)
        .is_err());
}
#[test]
fn versioned_manifests_refuse_missing_repeated_and_silently_dropped_physical_ids() {
    let start = physical_start(2, None, 4);
    let good = publication(&start, &[chunk(&start, 3, 0, 4, 0)]);
    for field in 0..5 {
        let mut m = good.clone();
        let replicas = &mut m.initialized_log.as_mut().unwrap().chunks[0].replicas;
        match field {
            0 => replicas[0].storage_incarnation.clear(),
            1 => replicas[0].storage_incarnation = vec![0; 16],
            2 => replicas[1].storage_incarnation = replicas[0].storage_incarnation.clone(),
            3 => replicas[0].storage_incarnation.push(1),
            _ => m.format_version = CHUNK_MANIFEST_INITIALIZED_FORMAT,
        };
        assert!(m.to_bytes().is_err(), "accepted field{field}");
    }
}
fn set_floor(mut bytes: Vec<u8>, floor: &[u8]) -> Vec<u8> {
    let at = bytes.windows(11).position(|s| s == b"MOSRSMETA26").unwrap();
    bytes[at..at + 11].copy_from_slice(floor);
    let mut placeholder = String::new();
    crate::append_etag_of_digest(&mut placeholder, 0);
    bytes.truncate(bytes.len() - 8 - placeholder.len());
    let mut checksum = String::new();
    crate::append_etag_of_digest(&mut checksum, crate::fnv1a64(&bytes));
    crate::append_string(&mut bytes, &checksum);
    bytes
}
#[test]
fn snapshot26_binds_physical_state_and_resigned_snapshot25_cannot_hide_it() {
    let s = published();
    let bytes = s.export_snapshot().to_bytes().unwrap();
    assert_eq!(
        MetaSnapshot::from_bytes(&bytes).unwrap(),
        s.export_snapshot()
    );
    assert_eq!(
        MetaSnapshot::read_from(&mut std::io::Cursor::new(&bytes), bytes.len() as u64).unwrap(),
        s.export_snapshot()
    );
    let downgraded = set_floor(bytes, b"MOSRSMETA25");
    assert!(MetaSnapshot::from_bytes(&downgraded)
        .unwrap_err()
        .to_string()
        .contains("snapshot26"));
}
#[test]
fn historical_snapshot25_and_outcome18_remain_readable() {
    let mut s = crate::native_object_outcome_tests::service();
    let start = start(2, None, 4);
    let change = NativeObjectChange {
        version: 1,
        now_ms: 1000,
        operation: Some(Operation::Begin(NativeBegin {
            start: Some(start),
            ..Default::default()
        })),
    };
    let outcome = crate::placement_outcome::PlacementOutcome {
        version: 18,
        before_index: 1,
        after_index: 2,
        native: Some(change),
        ..Default::default()
    };
    let bytes = outcome.to_payload().unwrap();
    crate::placement_outcome::PlacementOutcome::from_payload(&bytes)
        .unwrap()
        .apply(&mut s)
        .unwrap();
    let snapshot = s.export_snapshot();
    let old = set_floor(snapshot.to_bytes().unwrap(), b"MOSRSMETA25");
    assert_eq!(MetaSnapshot::from_bytes(&old).unwrap(), snapshot);
}
#[test]
fn retained_pin_keeps_physical_members_across_a_new_object_incarnation() {
    let mut s = published();
    let id = start(2, None, 4).header.unwrap().object_id();
    let old = (**s.chunk_catalog.get(&id).unwrap()).clone();
    commit(
        &mut s,
        Operation::OpenPin(crate::native_object_state::NativeReadPin {
            pin_id: 5,
            bucket: id.bucket.to_string(),
            key: id.key.to_string(),
            generation: old.generation,
            manifest_digest: hash(&old),
            expires_at_ms: 5000,
        }),
        1003,
    );
    let captured = s.native_objects.physical_read_binding(5, 0, 1003).unwrap();
    let lease = s.writer_leases.get(&id.bucket, &id.key);
    let replacement = physical_start(6, Some(&old), 4);
    let owned = chunk(&replacement, 7, 0, 4, 0);
    let after = publication(&replacement, &[owned.clone()]);
    commit(
        &mut s,
        Operation::Begin(NativeBegin {
            start: Some(replacement),
            expected_lease: lease.clone(),
            bypass_governance: false,
        }),
        1000,
    );
    commit(
        &mut s,
        Operation::Extend(NativeExtend {
            stage: Some(stage_ref(6, 6)),
            chunk: Some(owned),
        }),
        1004,
    );
    commit(
        &mut s,
        Operation::Publish(NativePublish {
            stage: Some(stage_ref(6, 7)),
            manifest: Some(after),
            expected_lease: lease,
        }),
        1005,
    );
    assert_eq!(
        s.native_objects.physical_read_binding(5, 0, 1006).unwrap(),
        captured
    );
    let mut restored = MatrixMetaService::new();
    restored
        .install_snapshot(
            MetaSnapshot::from_bytes(&s.export_snapshot().to_bytes().unwrap()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        restored
            .native_objects
            .physical_read_binding(5, 0, 1006)
            .unwrap(),
        captured
    );
    assert!(restored
        .native_objects
        .physical_read_binding(5, 0, 5000)
        .is_err());
}
#[test]
fn legacy_native_commands_cannot_operate_on_physical_member_state() {
    let mut s = published();
    let id = start(2, None, 4).header.unwrap().object_id();
    let m = (**s.chunk_catalog.get(&id).unwrap()).clone();
    let change = NativeObjectChange {
        version: 1,
        now_ms: 1003,
        operation: Some(Operation::OpenPin(
            crate::native_object_state::NativeReadPin {
                pin_id: 5,
                bucket: m.bucket,
                key: m.key,
                generation: m.generation,
                manifest_digest: hash(s.chunk_catalog.get(&id).unwrap()),
                expires_at_ms: 5000,
            },
        )),
    };
    let saved = s.export_snapshot().to_bytes().unwrap();
    assert!(change.apply(&mut s, 4, 5).is_err());
    assert_eq!(s.export_snapshot().to_bytes().unwrap(), saved);
    assert!(NativeObjectChange::decide(
        &s,
        Operation::Begin(NativeBegin {
            start: Some(start(5, Some(s.chunk_catalog.get(&id).unwrap()), 4)),
            expected_lease: s.writer_leases.get(&id.bucket, &id.key),
            bypass_governance: false
        }),
        1000
    )
    .is_err());
}
