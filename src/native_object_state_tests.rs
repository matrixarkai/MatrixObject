// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::chunk_catalog::{ChunkCatalog, ChunkOutcome};
use crate::chunk_manifest::*;
use crate::initialized_log_manifest::*;
use crate::native_object_state::*;
pub(crate) fn hash(m: &ChunkManifest) -> Vec<u8> {
    crate::chunk_integrity::sha256(&m.to_bytes().unwrap()).to_vec()
}
fn old_manifest() -> ChunkManifest {
    let mut m = crate::initialized_log_manifest_tests::fixture();
    m.extents[0].volume_id = 0;
    m.initialized_log.as_mut().unwrap().chunks[0].writer_epoch = 5;
    m
}
fn catalog() -> ChunkCatalog {
    let mut c = ChunkCatalog::default();
    c.apply(&ChunkOutcome::decide(None, old_manifest()).unwrap())
        .unwrap();
    c
}
pub(crate) fn start(id: u64, before: Option<&ChunkManifest>, target: u64) -> NativeStageStart {
    let mut header = old_manifest();
    header.generation = before.map_or(1, |m| m.generation + 1);
    header.length = 0;
    header.extents.clear();
    header.initialized_log.as_mut().unwrap().chunks.clear();
    let mut s = NativeStageStart {
        stage_id: id,
        kind: STAGE_REPLACE,
        header: Some(header),
        expected_generation: before.map_or(0, |m| m.generation),
        expected_manifest_digest: before.map_or_else(Vec::new, hash),
        holder: "writer".into(),
        writer_epoch: id,
        created_ms: 1000,
        expires_at_ms: 10000,
        request_nonce: vec![9; 32],
        target_length: target,
        base_length: 0,
    };
    let birth = s.replacement_incarnation().unwrap();
    s.header
        .as_mut()
        .unwrap()
        .initialized_log
        .as_mut()
        .unwrap()
        .object_incarnation = birth.to_vec();
    s.validate().unwrap();
    s
}
pub(crate) fn chunk(
    s: &NativeStageStart,
    allocation: u64,
    offset: u64,
    length: u64,
    extent_index: u32,
) -> NativeOwnedChunk {
    let (volume_id, chunk_id) = crate::object_manifest::chunk_identity_of_index(allocation);
    NativeOwnedChunk {
        allocation_id: allocation,
        extent: Some(ChunkExtent {
            group: "data-a".into(),
            volume_id,
            chunk_id,
            kind: CHUNK_KIND_LOG,
            object_offset: offset,
            length,
            ..Default::default()
        }),
        initialization: Some(InitializedChunk {
            extent_index,
            chunk_generation: 1,
            writer_epoch: s.writer_epoch,
            replicas: (0..3)
                .map(|i| InitializedReplica {
                    member_id: format!("member-{i}"),
                    storage_incarnation: if s.header.as_ref().unwrap().format_version
                        == CHUNK_MANIFEST_PHYSICAL_FORMAT
                    {
                        vec![40 + i; 16]
                    } else {
                        Vec::new()
                    },
                    initialization: vec![(allocation as u8).wrapping_mul(3).wrapping_add(i); 16],
                })
                .collect(),
        }),
    }
}
pub(crate) fn publication(s: &NativeStageStart, owned: &[NativeOwnedChunk]) -> ChunkManifest {
    let mut m = s.header.clone().unwrap();
    m.length = s.target_length;
    for (i, c) in owned.iter().enumerate() {
        let mut extent = c.extent.clone().unwrap();
        extent.flags = if i + 1 == owned.len() {
            CHUNK_FLAG_TAIL
        } else {
            CHUNK_FLAG_SEALED
        };
        m.extents.push(extent);
        m.initialized_log
            .as_mut()
            .unwrap()
            .chunks
            .push(c.initialization.clone().unwrap());
    }
    m.validate().unwrap();
    m
}
fn assert_same(state: &NativeObjectState, before: &[u8]) {
    assert_eq!(state.to_bytes().unwrap(), before);
}
#[test]
fn staged_chunks_remain_unpublished_and_recover_before_atomic_publication() {
    let mut c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    assert!(state.begin(s.clone(), &c, 1000).unwrap());
    assert!(!state.begin(s.clone(), &c, 1000).unwrap());
    let owned = chunk(&s, 11, 0, 4, 0);
    state.extend(10, 10, "writer", 1001, owned.clone()).unwrap();
    assert!(c.is_empty());
    assert_eq!(state.owned_chunks(10).count(), 1);
    let saved = state.to_bytes().unwrap();
    let mut restored = NativeObjectState::from_bytes(&saved).unwrap();
    assert_eq!(state, restored);
    let after = publication(&s, &[owned]);
    restored
        .publish(10, 11, "writer", 1002, 12, after.clone(), &mut c)
        .unwrap();
    assert_eq!(**c.get(&after.object_id()).unwrap(), after);
    assert_eq!(restored.stage_count(), 0);
    assert!(restored
        .publish(10, 11, "writer", 1002, 13, after, &mut c)
        .is_err());
}
#[test]
fn object_stage_ownership_and_expected_version_reject_conflicts_without_mutation() {
    let c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    state.begin(s, &c, 1000).unwrap();
    let before = state.to_bytes().unwrap();
    assert!(state.begin(start(12, None, 4), &c, 1000).is_err());
    assert_same(&state, &before);
    let c = catalog();
    let mut empty = NativeObjectState::default();
    assert!(empty.begin(start(10, None, 4), &c, 1000).is_err());
    let mut wrong = start(20, Some(&old_manifest()), 4);
    wrong.expected_manifest_digest[0] ^= 1;
    assert!(empty.begin(wrong, &c, 1000).is_err());
    assert!(empty.is_default());
}
#[test]
fn allocation_offsets_epoch_and_initializations_are_checked_before_state_changes() {
    let c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    state.begin(s.clone(), &c, 1000).unwrap();
    let before = state.to_bytes().unwrap();
    for field in 0..6 {
        let mut owned = chunk(&s, 11, 0, 4, 0);
        match field {
            0 => owned.extent.as_mut().unwrap().chunk_id += 1,
            1 => owned.extent.as_mut().unwrap().object_offset = 1,
            2 => owned.extent.as_mut().unwrap().length = 5,
            3 => owned.initialization.as_mut().unwrap().writer_epoch = 9,
            4 => {
                let init = owned.initialization.as_mut().unwrap();
                init.replicas[1] = init.replicas[0].clone();
            }
            _ => owned.extent.as_mut().unwrap().flags = CHUNK_FLAG_TAIL,
        }
        assert!(state.extend(10, 10, "writer", 1001, owned).is_err());
        assert_same(&state, &before);
    }
}
#[test]
fn expiration_releases_the_mutation_slot_but_preserves_durable_cleanup_ownership() {
    let c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    state.begin(s.clone(), &c, 1000).unwrap();
    state
        .extend(10, 10, "writer", 1001, chunk(&s, 11, 0, 4, 0))
        .unwrap();
    let before = state.to_bytes().unwrap();
    assert!(state.abort(10, 11, "writer", 10000, 12).is_err());
    assert!(state.expire_stage(10, 11, 10000, 9999, 12).is_err());
    assert_same(&state, &before);
    state.expire_stage(10, 11, 10000, 10000, 12).unwrap();
    assert_eq!(state.stage_count(), 0);
    assert_eq!(state.retired_stage_count(), 1);
    let recovered = NativeObjectState::from_bytes(&state.to_bytes().unwrap()).unwrap();
    assert_eq!(
        recovered.retired_chunks().next().unwrap().1.allocation_id,
        11
    );
    state.begin(start(13, None, 4), &c, 10000 - 1).unwrap();
    assert_eq!(state.stage_count(), 1);
    assert_eq!(state.retired_stage_count(), 1);
}
#[test]
fn abort_cleanup_and_recovery_never_reuse_consumed_metadata_positions() {
    let c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    state.begin(s.clone(), &c, 1000).unwrap();
    state
        .extend(10, 10, "writer", 1001, chunk(&s, 11, 0, 4, 0))
        .unwrap();
    state.abort(10, 11, "writer", 1002, 12).unwrap();
    let before = state.to_bytes().unwrap();
    assert!(state.forget_retired_stage(10, 11, 13).is_err());
    assert_same(&state, &before);
    state.forget_retired_stage(10, 12, 13).unwrap();
    let mut recovered = NativeObjectState::from_bytes(&state.to_bytes().unwrap()).unwrap();
    assert!(!recovered.is_default());
    assert_eq!(recovered.stage_count(), 0);
    assert_eq!(recovered.retired_stage_count(), 0);
    assert!(recovered.begin(s, &c, 1000).is_err());
}
#[test]
fn publication_checks_owner_deadline_identity_and_current_catalog_atomically() {
    let mut c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    state.begin(s.clone(), &c, 1000).unwrap();
    let owned = chunk(&s, 11, 0, 4, 0);
    state.extend(10, 10, "writer", 1001, owned.clone()).unwrap();
    let after = publication(&s, &[owned]);
    let before = state.to_bytes().unwrap();
    assert!(state
        .publish(10, 11, "other", 1002, 12, after.clone(), &mut c)
        .is_err());
    assert!(state
        .publish(10, 10, "writer", 1002, 12, after.clone(), &mut c)
        .is_err());
    assert!(state
        .publish(10, 11, "writer", 10000, 12, after.clone(), &mut c)
        .is_err());
    let mut wrong = after.clone();
    wrong.initialized_log.as_mut().unwrap().chunks[0].replicas[0].initialization = vec![77; 16];
    assert!(state
        .publish(10, 11, "writer", 1002, 12, wrong, &mut c)
        .is_err());
    assert!(c.is_empty());
    assert_same(&state, &before);
    c.apply(&ChunkOutcome::decide(None, old_manifest()).unwrap())
        .unwrap();
    let catalog_before = c.to_bytes().unwrap();
    assert!(state
        .publish(10, 11, "writer", 1002, 12, after, &mut c)
        .is_err());
    assert_eq!(c.to_bytes().unwrap(), catalog_before);
    assert_same(&state, &before);
}
#[test]
fn read_pins_share_retention_and_survive_overwrite_with_expected_deadline_removal() {
    let mut c = catalog();
    let old = old_manifest();
    let mut state = NativeObjectState::default();
    for id in [20, 21] {
        state
            .open_pin(
                NativeReadPin {
                    pin_id: id,
                    bucket: old.bucket.clone(),
                    key: old.key.clone(),
                    generation: old.generation,
                    manifest_digest: hash(&old),
                    expires_at_ms: 10000,
                },
                &c,
                1000,
            )
            .unwrap();
    }
    assert_eq!(state.retained_count(), 1);
    assert!(std::sync::Arc::ptr_eq(
        &state.pinned_manifest(20, 2000).unwrap(),
        &state.pinned_manifest(21, 2000).unwrap()
    ));
    let captured = state.clone();
    let s = start(30, Some(&old), 4);
    state.begin(s.clone(), &c, 1000).unwrap();
    let owned = chunk(&s, 31, 0, 4, 0);
    state.extend(30, 30, "writer", 1001, owned.clone()).unwrap();
    let after = publication(&s, &[owned]);
    state
        .publish(30, 31, "writer", 1002, 32, after, &mut c)
        .unwrap();
    assert_eq!(*state.pinned_manifest(20, 2000).unwrap(), old);
    assert_eq!(captured.pin_count(), 2);
    let mut recovered = NativeObjectState::from_bytes(&state.to_bytes().unwrap()).unwrap();
    recovered.renew_pin(20, 10000, 11000, 2000, 33).unwrap();
    assert!(recovered.close_pin(20, 10000, 34).is_err());
    recovered.close_pin(20, 11000, 34).unwrap();
    assert_eq!(recovered.retained_count(), 1);
    recovered.close_pin(21, 10000, 35).unwrap();
    assert_eq!(recovered.retained_count(), 0);
    assert!(recovered.pinned_manifest(20, 2000).is_err());
    assert_eq!(captured.pin_count(), 2);
}
#[test]
fn read_pin_admission_expiry_and_damaged_images_fail_closed() {
    let c = catalog();
    let old = old_manifest();
    let mut state = NativeObjectState::default();
    let pin = NativeReadPin {
        pin_id: 20,
        bucket: old.bucket.clone(),
        key: old.key.clone(),
        generation: 1,
        manifest_digest: hash(&old),
        expires_at_ms: 10000,
    };
    let mut wrong = pin.clone();
    wrong.manifest_digest[0] ^= 1;
    assert!(state.open_pin(wrong, &c, 1000).is_err());
    state.open_pin(pin, &c, 1000).unwrap();
    assert!(state.pinned_manifest(20, 10000).is_err());
    let before = state.to_bytes().unwrap();
    assert!(state.expire_pin(20, 10000, 9999, 21).is_err());
    assert_same(&state, &before);
    state.expire_pin(20, 10000, 10000, 21).unwrap();
    assert_eq!(state.retained_count(), 0);
    let mut bytes = before;
    let i = bytes.len() - 1;
    bytes[i] ^= 1;
    assert!(NativeObjectState::from_bytes(&bytes).is_err());
}
#[test]
fn multi_chunk_stages_recover_and_reject_cross_chunk_nonce_reuse() {
    let mut c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 8);
    state.begin(s.clone(), &c, 1000).unwrap();
    let first = chunk(&s, 11, 0, 4, 0);
    state.extend(10, 10, "writer", 1001, first.clone()).unwrap();
    let mut wrong = chunk(&s, 12, 4, 4, 1);
    wrong.initialization.as_mut().unwrap().replicas =
        first.initialization.as_ref().unwrap().replicas.clone();
    assert!(state.extend(10, 11, "writer", 1001, wrong).is_err());
    let last = chunk(&s, 12, 4, 4, 1);
    state.extend(10, 11, "writer", 1001, last.clone()).unwrap();
    let mut recovered = NativeObjectState::from_bytes(&state.to_bytes().unwrap()).unwrap();
    recovered
        .publish(
            10,
            12,
            "writer",
            1002,
            13,
            publication(&s, &[first, last]),
            &mut c,
        )
        .unwrap();
    assert_eq!(c.iter().next().unwrap().1.length, 8);
}
#[test]
fn rollover_preserves_old_bytes_and_seals_only_the_tail_under_the_admitted_epoch() {
    let mut c = catalog();
    let old = old_manifest();
    let mut state = NativeObjectState::default();
    let mut s = start(20, Some(&old), 10);
    s.kind = STAGE_ADD_TAIL;
    s.base_length = 6;
    s.header
        .as_mut()
        .unwrap()
        .initialized_log
        .as_mut()
        .unwrap()
        .object_incarnation = old
        .initialized_log
        .as_ref()
        .unwrap()
        .object_incarnation
        .clone();
    state.begin(s.clone(), &c, 1000).unwrap();
    let owned = chunk(&s, 21, 6, 4, 1);
    state.extend(20, 20, "writer", 1001, owned.clone()).unwrap();
    let mut after = old.clone();
    after.generation = 2;
    after.length = 10;
    after.extents[0].length = 6;
    after.extents[0].flags = CHUNK_FLAG_SEALED;
    after.initialized_log.as_mut().unwrap().chunks[0].writer_epoch = 20;
    let mut extent = owned.extent.clone().unwrap();
    extent.flags = CHUNK_FLAG_TAIL;
    after.extents.push(extent);
    after
        .initialized_log
        .as_mut()
        .unwrap()
        .chunks
        .push(owned.initialization.clone().unwrap());
    after.validate().unwrap();
    let before = state.to_bytes().unwrap();
    let mut wrong = after.clone();
    wrong.extents[0].chunk_id += 1;
    assert!(state
        .publish(20, 21, "writer", 1002, 22, wrong, &mut c)
        .is_err());
    assert_same(&state, &before);
    state
        .publish(20, 21, "writer", 1002, 22, after.clone(), &mut c)
        .unwrap();
    assert_eq!(**c.get(&old.object_id()).unwrap(), after);
}

fn native_service() -> crate::MatrixMetaService {
    let mut service = crate::MatrixMetaService::new();
    service.cluster_features.raised = crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS
        | crate::cluster_features::FEATURE_NATIVE_OBJECT_STATE;
    service
}
#[test]
fn metadata_snapshot_26_preserves_staged_ownership_and_immutable_captures() {
    let mut service = native_service();
    let s = start(10, None, 4);
    let owned = chunk(&s, 11, 0, 4, 0);
    service
        .native_objects
        .begin(s.clone(), &service.chunk_catalog, 1000)
        .unwrap();
    service
        .native_objects
        .extend(10, 10, "writer", 1001, owned.clone())
        .unwrap();
    service.placement_commit_index = 11;
    let captured = service.export_snapshot();
    let bytes = captured.to_bytes().unwrap();
    assert!(bytes.windows(11).any(|b| b == b"MOSRSMETA26"));
    let decoded = crate::MetaSnapshot::from_bytes(&bytes).unwrap();
    assert_eq!(decoded, captured);
    let streamed =
        crate::MetaSnapshot::read_from(&mut std::io::Cursor::new(&bytes), bytes.len() as u64)
            .unwrap();
    assert_eq!(streamed, decoded);
    let mut restored = crate::MatrixMetaService::new();
    restored.install_snapshot(decoded).unwrap();
    assert_eq!(
        restored.native_objects.owned_chunks(10).next().unwrap(),
        &owned
    );
    restored
        .native_objects
        .publish(
            10,
            11,
            "writer",
            1002,
            12,
            publication(&s, &[owned]),
            &mut restored.chunk_catalog,
        )
        .unwrap();
    restored.placement_commit_index = 12;
    restored.export_snapshot().to_bytes().unwrap();
    assert_eq!(captured.to_bytes().unwrap(), bytes);
    assert!(captured.chunk_catalog.is_empty());
}
#[test]
fn snapshot_floor_requires_activation_dependencies_and_committed_native_positions() {
    let mut service = crate::MatrixMetaService::new();
    let s = start(10, None, 4);
    service
        .native_objects
        .begin(s, &service.chunk_catalog, 1000)
        .unwrap();
    service.placement_commit_index = 10;
    assert!(service.export_snapshot().to_bytes().is_err());
    service.cluster_features.raised = crate::cluster_features::FEATURE_NATIVE_OBJECT_STATE;
    assert!(service.export_snapshot().to_bytes().is_err());
    service.cluster_features.raised |= crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS;
    service.placement_commit_index = 9;
    assert!(service.export_snapshot().to_bytes().is_err());
    service.placement_commit_index = 10;
    service.export_snapshot().to_bytes().unwrap();
    // A readable snapshot alone is not committed-outcome support. Advertisement waits
    // until the actual native outcome application path is installed.
    assert_eq!(
        crate::cluster_features::SUPPORTED_FEATURES
            & crate::cluster_features::FEATURE_NATIVE_OBJECT_STATE,
        0
    );
}
#[test]
fn native_snapshot_checksum_binds_contents_even_when_revisions_match() {
    let mut first = native_service();
    let s = start(10, None, 4);
    first
        .native_objects
        .begin(s, &first.chunk_catalog, 1000)
        .unwrap();
    first.placement_commit_index = 10;
    let mut snapshot = first.export_snapshot();
    let mut alternate = start(10, None, 4);
    alternate.request_nonce = vec![8; 32];
    let birth = alternate.replacement_incarnation().unwrap();
    alternate
        .header
        .as_mut()
        .unwrap()
        .initialized_log
        .as_mut()
        .unwrap()
        .object_incarnation = birth.to_vec();
    let mut other = NativeObjectState::default();
    other.begin(alternate, &first.chunk_catalog, 1000).unwrap();
    assert_eq!(other.revision(), first.native_objects.revision());
    snapshot.native_objects = other;
    assert!(snapshot.to_bytes().is_err());
    snapshot.checksum = snapshot.checksum_material_hash();
    snapshot.to_bytes().unwrap();
}
#[test]
fn native_snapshot_rejects_downgraded_and_future_floor_and_corrupted_state_blob() {
    let mut service = native_service();
    service
        .native_objects
        .begin(start(10, None, 4), &service.chunk_catalog, 1000)
        .unwrap();
    service.placement_commit_index = 10;
    let bytes = service.export_snapshot().to_bytes().unwrap();
    let magic = bytes.windows(11).position(|b| b == b"MOSRSMETA26").unwrap();
    for last in [b'4', b'5', b'7'] {
        let mut bad = bytes.clone();
        bad[magic + 10] = last;
        assert!(crate::MetaSnapshot::from_bytes(&bad).is_err());
    }
    let at = bytes.windows(4).position(|b| b == b"MONS").unwrap();
    let mut bad = bytes;
    bad[at + 12] ^= 1;
    assert!(crate::MetaSnapshot::from_bytes(&bad).is_err());
}
#[test]
fn native_snapshot_cache_changes_for_pin_mutations_without_catalog_changes() {
    let mut service = native_service();
    service.next_commit_index = 2;
    let s = start(10, None, 4);
    let owned = chunk(&s, 11, 0, 4, 0);
    service
        .native_objects
        .begin(s.clone(), &service.chunk_catalog, 1000)
        .unwrap();
    service
        .native_objects
        .extend(10, 10, "writer", 1001, owned.clone())
        .unwrap();
    let after = publication(&s, &[owned]);
    service
        .native_objects
        .publish(
            10,
            11,
            "writer",
            1002,
            12,
            after.clone(),
            &mut service.chunk_catalog,
        )
        .unwrap();
    service.placement_commit_index = 20;
    let first = service.export_snapshot_chunk(0, 1 << 20).unwrap();
    let revision = service.chunk_catalog.revision();
    service
        .native_objects
        .open_pin(
            NativeReadPin {
                pin_id: 13,
                bucket: after.bucket.clone(),
                key: after.key.clone(),
                generation: 1,
                manifest_digest: hash(&after),
                expires_at_ms: 10000,
            },
            &service.chunk_catalog,
            1000,
        )
        .unwrap();
    let next = service.export_snapshot_chunk(0, 1 << 20).unwrap();
    assert_eq!(service.chunk_catalog.revision(), revision);
    assert_ne!(first.full_checksum, next.full_checksum);
    let decoded = crate::MetaSnapshot::from_bytes(&next.payload).unwrap();
    assert_eq!(decoded.native_objects.pin_count(), 1);
}
#[test]
fn snapshot_validation_rejects_shared_native_storage_and_live_retirement_overlap() {
    let mut c = ChunkCatalog::default();
    let mut state = NativeObjectState::default();
    let s = start(10, None, 4);
    let owned = chunk(&s, 11, 0, 4, 0);
    state.begin(s.clone(), &c, 1000).unwrap();
    state.extend(10, 10, "writer", 1001, owned.clone()).unwrap();
    state.abort(10, 11, "writer", 1002, 12).unwrap();
    let first = publication(&s, &[owned]);
    c.apply(&ChunkOutcome::decide(None, first.clone()).unwrap())
        .unwrap();
    assert!(state.validate_snapshot(&c, 12).is_err());
    let mut other = first;
    other.key = "other".into();
    c.apply(&ChunkOutcome::decide(None, other).unwrap())
        .unwrap();
    assert!(NativeObjectState::default()
        .validate_snapshot(&c, 12)
        .is_err());
}
#[test]
fn many_read_pins_encode_one_large_retained_manifest_per_generation() {
    let mut m = old_manifest();
    m.extents.clear();
    m.initialized_log.as_mut().unwrap().chunks.clear();
    for i in 0..1024u32 {
        m.extents.push(ChunkExtent {
            group: "data-a".into(),
            volume_id: 0,
            chunk_id: u64::from(i) + 1,
            kind: CHUNK_KIND_LOG,
            object_offset: u64::from(i) * 4,
            length: 4,
            flags: if i == 1023 {
                CHUNK_FLAG_TAIL
            } else {
                CHUNK_FLAG_SEALED
            },
            ..Default::default()
        });
        m.initialized_log
            .as_mut()
            .unwrap()
            .chunks
            .push(InitializedChunk {
                extent_index: i,
                chunk_generation: 1,
                writer_epoch: 1,
                replicas: (0..3u32)
                    .map(|slot| {
                        let mut key = i.to_le_bytes().to_vec();
                        key.extend_from_slice(&slot.to_le_bytes());
                        InitializedReplica {
                            member_id: format!("member-{slot}"),
                            storage_incarnation: Vec::new(),
                            initialization: crate::chunk_integrity::sha256(&key)[..16].to_vec(),
                        }
                    })
                    .collect(),
            });
    }
    m.length = 4096;
    m.validate().unwrap();
    let digest = hash(&m);
    let mut c = ChunkCatalog::default();
    c.apply(&ChunkOutcome::decide(None, m.clone()).unwrap())
        .unwrap();
    let mut state = NativeObjectState::default();
    let mut first_len = 0;
    for pin_id in 5000..5128 {
        state
            .open_pin(
                NativeReadPin {
                    pin_id,
                    bucket: m.bucket.clone(),
                    key: m.key.clone(),
                    generation: 1,
                    manifest_digest: digest.clone(),
                    expires_at_ms: 10000,
                },
                &c,
                1000,
            )
            .unwrap();
        if pin_id == 5000 {
            first_len = state.to_bytes().unwrap().len();
        }
    }
    let bytes = state.to_bytes().unwrap();
    assert!(bytes.len() - first_len < 128 * 128);
    let restored = NativeObjectState::from_bytes(&bytes).unwrap();
    assert_eq!(restored.retained_count(), 1);
    assert_eq!(restored.pin_count(), 128);
    restored.validate_snapshot(&c, 5128).unwrap();
    assert!(std::sync::Arc::ptr_eq(
        &restored.pinned_manifest(5000, 2000).unwrap(),
        &restored.pinned_manifest(5127, 2000).unwrap()
    ));
}

#[test]
fn native_state_images_reject_future_versions_and_unknown_semantics() {
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    struct Candidate {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(uint64, tag = "99")]
        unknown_semantics: u64,
    }
    let encode = |version, unknown_semantics| {
        crate::object_manifest::frame(
            b"MONS",
            &Candidate {
                version,
                unknown_semantics,
            },
            1024,
        )
        .unwrap()
    };
    assert_eq!(
        NativeObjectState::from_bytes(&encode(1, 0)).unwrap(),
        NativeObjectState::default()
    );
    assert!(NativeObjectState::from_bytes(&encode(2, 0)).is_err());
    assert!(NativeObjectState::from_bytes(&encode(1, 1)).is_err());
}
