// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use crate::chunk_catalog::{ChunkCatalog, ChunkOutcome};
use crate::chunk_manifest::*;
use crate::initialized_log_manifest::*;
use crate::MatrixMetaService;

pub(crate) fn fixture() -> ChunkManifest {
    ChunkManifest {
        format_version: CHUNK_MANIFEST_INITIALIZED_FORMAT,
        bucket: "bucket".into(),
        key: "object".into(),
        generation: 1,
        length: 4,
        kind: CHUNK_KIND_LOG,
        extents: vec![ChunkExtent {
            group: "data-a".into(),
            volume_id: 1,
            chunk_id: 9,
            kind: CHUNK_KIND_LOG,
            length: 4,
            flags: CHUNK_FLAG_TAIL,
            ..Default::default()
        }],
        initialized_log: Some(InitializedLogManifest {
            object_incarnation: vec![1; 16],
            chunks: vec![InitializedChunk {
                extent_index: 0,
                chunk_generation: 7,
                writer_epoch: 99,
                replicas: (0..3)
                    .map(|i| InitializedReplica {
                        member_id: format!("member-{i}"),
                        initialization: vec![i + 2; 16],
                    })
                    .collect(),
            }],
        }),
        ..Default::default()
    }
}
#[test]
fn framed_manifest_and_catalog_recovery_preserve_each_storage_identity() {
    let m = fixture();
    let framed = m.to_bytes().unwrap();
    assert_eq!(ChunkManifest::from_bytes(&framed).unwrap(), m);
    let mut catalog = ChunkCatalog::default();
    catalog
        .apply(&ChunkOutcome::decide(None, m.clone()).unwrap())
        .unwrap();
    let recovered = ChunkCatalog::from_bytes(&catalog.to_bytes().unwrap()).unwrap();
    assert_eq!(recovered, catalog);
    assert_eq!(**recovered.get(&m.object_id()).unwrap(), m);
}
#[test]
fn initialized_fields_and_manifest_version_cannot_be_downgraded_independently() {
    let mut m = fixture();
    m.format_version = CHUNK_MANIFEST_FORMAT;
    assert!(m.to_bytes().is_err());
    m = fixture();
    m.initialized_log = None;
    assert!(m.to_bytes().is_err());
    m.format_version = CHUNK_MANIFEST_FORMAT;
    assert_eq!(
        ChunkManifest::from_bytes(&m.to_bytes().unwrap()).unwrap(),
        m
    );
}
#[test]
fn zero_short_or_long_object_and_storage_identities_are_refused() {
    for bad in [vec![], vec![0; 16], vec![1; 15], vec![1; 17]] {
        let mut m = fixture();
        m.initialized_log.as_mut().unwrap().object_incarnation = bad.clone();
        assert!(m.validate().is_err());
        let mut m = fixture();
        m.initialized_log.as_mut().unwrap().chunks[0].replicas[0].initialization = bad;
        assert!(m.validate().is_err());
    }
}
#[test]
fn exactly_three_distinct_configured_members_and_initializations_are_required() {
    let original = fixture();
    for count in [0, 1, 2, 4] {
        let mut m = original.clone();
        let replicas = &mut m.initialized_log.as_mut().unwrap().chunks[0].replicas;
        replicas.resize(count, replicas[0].clone());
        assert!(m.validate().is_err());
    }
    let mut m = original.clone();
    let replicas = &mut m.initialized_log.as_mut().unwrap().chunks[0].replicas;
    replicas[1].member_id = replicas[0].member_id.clone();
    assert!(m.validate().is_err());
    let mut m = original;
    let replicas = &mut m.initialized_log.as_mut().unwrap().chunks[0].replicas;
    replicas[1].initialization = replicas[0].initialization.clone();
    assert!(m.validate().is_err());
}
#[test]
fn bindings_must_cover_extents_in_order_with_nonzero_generation_and_epoch() {
    for field in 0..4 {
        let mut m = fixture();
        let initialized = m.initialized_log.as_mut().unwrap();
        match field {
            0 => initialized.chunks.clear(),
            1 => initialized.chunks[0].extent_index = 1,
            2 => initialized.chunks[0].chunk_generation = 0,
            _ => initialized.chunks[0].writer_epoch = 0,
        }
        assert!(m.validate().is_err());
    }
}
#[test]
fn storage_initialization_cannot_be_reused_by_another_chunk() {
    let mut m = fixture();
    m.extents[0].flags = CHUNK_FLAG_SEALED;
    let mut next = m.extents[0].clone();
    next.chunk_id += 1;
    next.object_offset = 4;
    next.flags = CHUNK_FLAG_TAIL;
    m.extents.push(next);
    m.length = 8;
    let initialized = m.initialized_log.as_mut().unwrap();
    let mut binding = initialized.chunks[0].clone();
    binding.extent_index = 1;
    initialized.chunks.push(binding);
    assert!(m.validate().is_err());
    for (i, replica) in m.initialized_log.as_mut().unwrap().chunks[1]
        .replicas
        .iter_mut()
        .enumerate()
    {
        replica.initialization = vec![i as u8 + 5; 16];
    }
    m.validate().unwrap();
}
#[test]
fn empty_objects_and_tombstones_preserve_incarnation_without_chunk_bindings() {
    let mut m = fixture();
    m.extents.clear();
    m.length = 0;
    m.initialized_log.as_mut().unwrap().chunks.clear();
    m.validate().unwrap();
    let incarnation = m
        .initialized_log
        .as_ref()
        .unwrap()
        .object_incarnation
        .clone();
    m.deleted = true;
    m.kind = 0;
    m.validate().unwrap();
    let decoded = ChunkManifest::from_bytes(&m.to_bytes().unwrap()).unwrap();
    assert_eq!(
        decoded.initialized_log.unwrap().object_incarnation,
        incarnation
    );
}
#[test]
fn paged_objects_and_invalid_member_names_cannot_carry_native_bindings() {
    let mut m = fixture();
    m.kind = CHUNK_KIND_PAGED;
    assert!(m.validate().is_err());
    for name in ["", "host/alias", "member name"] {
        let mut m = fixture();
        m.initialized_log.as_mut().unwrap().chunks[0].replicas[0].member_id = name.into();
        assert!(m.validate().is_err());
    }
}
#[test]
fn initialized_manifest_corruption_is_detected_by_its_frame() {
    let m = fixture();
    let mut bytes = m.to_bytes().unwrap();
    let end = bytes.len() - 1;
    bytes[end] ^= 1;
    assert!(ChunkManifest::from_bytes(&bytes).is_err());
}
#[test]
fn metadata_snapshot_cannot_emit_native_records_before_its_reader_floor_is_wired() {
    let mut service = MatrixMetaService::new();
    let m = fixture();
    service
        .chunk_catalog
        .apply(&ChunkOutcome::decide(None, m).unwrap())
        .unwrap();
    assert!(service.export_snapshot().to_bytes().is_err());
}
