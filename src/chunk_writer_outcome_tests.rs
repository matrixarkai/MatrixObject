// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::chunk_catalog::ChunkOutcome;
use crate::chunk_manifest::{
    ChunkExtent, ChunkManifest, CHUNK_FLAG_SEALED, CHUNK_FLAG_TAIL, CHUNK_KIND_LOG,
    CHUNK_MANIFEST_FORMAT,
};
use crate::chunk_writer_outcome::ChunkWriterChange;
use crate::cluster_features::FEATURE_UNIFIED_LOG_WRITERS;
use crate::log_chunk_outcome::{LogChunkChange, WriterLease, ACTION_GRANT, ACTION_SEAL_SWITCH};
use crate::placement_outcome::PlacementOutcome;
pub(crate) fn service() -> MatrixMetaService {
    let mut s = MatrixMetaService::new();
    s.cluster_features.raised = FEATURE_UNIFIED_LOG_WRITERS;
    s.placement_commit_index = 10;
    s
}
fn lease(holder: &str, epoch: u64, expires: u64) -> WriterLease {
    WriterLease {
        holder: holder.into(),
        epoch,
        expires_at_ms: expires,
    }
}
fn tail(index: u64, offset: u64) -> ChunkExtent {
    let (volume_id, chunk_id) = crate::object_manifest::chunk_identity_of_index(index);
    ChunkExtent {
        group: "group-a".into(),
        volume_id,
        chunk_id,
        kind: CHUNK_KIND_LOG,
        object_offset: offset,
        flags: CHUNK_FLAG_TAIL,
        ..Default::default()
    }
}
pub(crate) fn grant() -> PlacementOutcome {
    let manifest = ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: "log".into(),
        generation: 1,
        kind: CHUNK_KIND_LOG,
        extents: vec![tail(11, 0)],
        ..Default::default()
    };
    PlacementOutcome {
        version: 17,
        before_index: 10,
        after_index: 11,
        chunk_writer: Some(ChunkWriterChange {
            lease: Some(LogChunkChange {
                action: ACTION_GRANT,
                bucket: "bucket".into(),
                key: "log".into(),
                lease_before: None,
                lease_after: Some(lease("writer-a", 11, 11000)),
                now_ms: 1000,
            }),
            unchanged: None,
            publication: Some(ChunkOutcome::decide(None, manifest).unwrap()),
        }),
        ..Default::default()
    }
}
fn current(s: &MatrixMetaService) -> ChunkManifest {
    (**s.chunk_catalog
        .get(&ObjectId::new("bucket", "log"))
        .unwrap())
    .clone()
}
fn state(s: &MatrixMetaService) -> (Vec<u8>, crate::log_chunk_outcome::WriterLeaseTable, u64) {
    (
        s.chunk_catalog.to_bytes().unwrap(),
        s.writer_leases.clone(),
        s.placement_commit_index,
    )
}
fn takeover(s: &MatrixMetaService) -> PlacementOutcome {
    PlacementOutcome {
        version: 17,
        before_index: 11,
        after_index: 12,
        chunk_writer: Some(ChunkWriterChange {
            lease: Some(LogChunkChange {
                action: ACTION_GRANT,
                bucket: "bucket".into(),
                key: "log".into(),
                lease_before: s.writer_leases.get("bucket", "log"),
                lease_after: Some(lease("writer-b", 12, 30000)),
                now_ms: 20000,
            }),
            unchanged: Some(current(s)),
            publication: None,
        }),
        ..Default::default()
    }
}
fn switched(s: &MatrixMetaService) -> PlacementOutcome {
    let before = current(s);
    let mut after = before.clone();
    after.generation += 1;
    after.length = 9;
    after.extents[0].flags = CHUNK_FLAG_SEALED;
    after.extents[0].length = 9;
    after.extents[0].footer_crc = 7;
    after.extents.push(tail(12, 9));
    PlacementOutcome {
        version: 17,
        before_index: 11,
        after_index: 12,
        chunk_writer: Some(ChunkWriterChange {
            lease: Some(LogChunkChange {
                action: ACTION_SEAL_SWITCH,
                bucket: "bucket".into(),
                key: "log".into(),
                lease_before: s.writer_leases.get("bucket", "log"),
                lease_after: Some(lease("writer-a", 11, 15000)),
                now_ms: 2000,
            }),
            unchanged: None,
            publication: Some(ChunkOutcome::decide(Some(before), after).unwrap()),
        }),
        ..Default::default()
    }
}
#[test]
fn unified_grant_round_trips_as_one_binary_outcome() {
    let outcome = grant();
    let bytes = outcome.to_payload().unwrap();
    assert_eq!(PlacementOutcome::from_payload(&bytes).unwrap(), outcome);
    println!(
        "UNIFIED_OUTCOME_HEX {}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
}
#[test]
fn grant_changes_catalog_lease_and_index_together() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    assert_eq!(s.placement_commit_index, 11);
    assert_eq!(current(&s).extents, vec![tail(11, 0)]);
    assert_eq!(
        s.writer_leases.get("bucket", "log"),
        Some(lease("writer-a", 11, 11000))
    );
    assert!(s.object_manifests.is_empty());
}
#[test]
fn stale_lease_does_not_publish_a_catalog_half() {
    let mut s = service();
    s.writer_leases
        .set("bucket", "log", &lease("other", 9, 10000));
    let before = state(&s);
    assert!(grant().apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn an_already_published_manifest_is_not_a_partial_transaction_replay() {
    let mut s = service();
    let outcome = grant();
    s.chunk_catalog
        .apply(
            outcome
                .chunk_writer
                .as_ref()
                .unwrap()
                .publication
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    let before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn a_replayed_transaction_cannot_advance_or_change_either_half() {
    let mut s = service();
    let outcome = grant();
    outcome.apply(&mut s).unwrap();
    let before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn expired_lease_takeover_preserves_the_catalog_entry_and_revision() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    let before = s
        .chunk_catalog
        .entry(&ObjectId::new("bucket", "log"))
        .unwrap();
    let revision = s.chunk_catalog.revision();
    takeover(&s).apply(&mut s).unwrap();
    assert_eq!(s.placement_commit_index, 12);
    assert_eq!(
        s.writer_leases.get("bucket", "log"),
        Some(lease("writer-b", 12, 30000))
    );
    assert_eq!(s.chunk_catalog.revision(), revision);
    assert!(std::sync::Arc::ptr_eq(
        &before,
        s.chunk_catalog.get(&before.object_id()).unwrap()
    ));
}
#[test]
fn a_lease_only_grant_binds_the_complete_prior_manifest() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    let outcome = takeover(&s);
    let before = current(&s);
    let mut after = before.clone();
    after.generation += 1;
    after.content_type = "changed".into();
    s.chunk_catalog
        .apply(&ChunkOutcome::decide(Some(before), after).unwrap())
        .unwrap();
    let state_before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), state_before);
}
#[test]
fn a_live_rival_lease_is_refused_without_catalog_mutation() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    let mut outcome = takeover(&s);
    let change = outcome
        .chunk_writer
        .as_mut()
        .unwrap()
        .lease
        .as_mut()
        .unwrap();
    change.now_ms = 2000;
    change.lease_after.as_mut().unwrap().expires_at_ms = 10000;
    let before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn switch_seals_the_old_extent_and_reserves_the_next_identity_atomically() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    let outcome = switched(&s);
    let bytes = outcome.to_payload().unwrap();
    PlacementOutcome::from_payload(&bytes)
        .unwrap()
        .apply(&mut s)
        .unwrap();
    let manifest = current(&s);
    assert_eq!(manifest.length, 9);
    assert_eq!(manifest.extents[0].flags, CHUNK_FLAG_SEALED);
    assert_eq!(manifest.extents[1], tail(12, 9));
    assert_eq!(
        s.writer_leases.get("bucket", "log"),
        Some(lease("writer-a", 11, 15000))
    );
}
#[test]
fn wrong_sealed_identity_is_refused_before_either_table_changes() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    let mut outcome = switched(&s);
    outcome
        .chunk_writer
        .as_mut()
        .unwrap()
        .publication
        .as_mut()
        .unwrap()
        .after
        .as_mut()
        .unwrap()
        .extents[0]
        .chunk_id = 99;
    let before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn the_new_tail_is_bound_to_the_outcome_index() {
    let mut s = service();
    let mut outcome = grant();
    outcome
        .chunk_writer
        .as_mut()
        .unwrap()
        .publication
        .as_mut()
        .unwrap()
        .after
        .as_mut()
        .unwrap()
        .extents[0]
        .chunk_id = 77;
    let before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn switch_cannot_change_object_attributes() {
    let mut s = service();
    grant().apply(&mut s).unwrap();
    let mut outcome = switched(&s);
    outcome
        .chunk_writer
        .as_mut()
        .unwrap()
        .publication
        .as_mut()
        .unwrap()
        .after
        .as_mut()
        .unwrap()
        .content_type = "other".into();
    let before = state(&s);
    assert!(outcome.apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn leader_admission_is_not_reapplied_by_committed_outcome_consumers() {
    let mut s = service();
    let outcome = grant();
    let change = outcome.chunk_writer.as_ref().unwrap();
    assert!(change.check(&s, 10, 11, Some(0)).is_err());
    assert!(s.chunk_catalog.is_empty());
    outcome.apply(&mut s).unwrap();
    assert_eq!(s.chunk_catalog.len(), 1);
}
#[test]
fn replicated_activation_and_index_are_part_of_the_prior_state_binding() {
    let mut s = service();
    s.cluster_features.raised = 0;
    let before = state(&s);
    assert!(grant().apply(&mut s).is_err());
    assert_eq!(state(&s), before);
    s.cluster_features.raised = FEATURE_UNIFIED_LOG_WRITERS;
    s.placement_commit_index = 9;
    let before = state(&s);
    assert!(grant().apply(&mut s).is_err());
    assert_eq!(state(&s), before);
}
#[test]
fn unified_writer_outcomes_reject_unrelated_fields_and_wrong_versions() {
    let mut mixed = grant();
    mixed.forget = true;
    assert!(mixed.to_payload().is_err());
    mixed.forget = false;
    mixed.version = 15;
    assert!(mixed.to_payload().is_err());
    mixed.version = 17;
    mixed.chunk_writer = None;
    assert!(mixed.to_payload().is_err());
}
#[test]
fn activation_uses_a_new_snapshot_floor_even_with_an_empty_catalog() {
    let s = service();
    let snapshot = s.export_snapshot();
    let bytes = snapshot.to_bytes().unwrap();
    assert!(bytes
        .windows(b"MOSRSMETA24".len())
        .any(|b| b == b"MOSRSMETA24"));
    let decoded = MetaSnapshot::from_bytes(&bytes).unwrap();
    assert!(decoded.chunk_catalog.is_empty());
    assert_eq!(decoded.cluster_features, snapshot.cluster_features);
    let streamed =
        MetaSnapshot::read_from(&mut std::io::Cursor::new(&bytes), bytes.len() as u64).unwrap();
    assert_eq!(streamed.cluster_features, decoded.cluster_features);
    println!(
        "UNIFIED_SNAPSHOT_HEX {}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
}
#[test]
fn snapshots_retain_both_halves_and_invalidate_an_earlier_export() {
    let mut s = service();
    let before = s.export_snapshot().checksum;
    grant().apply(&mut s).unwrap();
    let snapshot = s.export_snapshot();
    assert_ne!(snapshot.checksum, before);
    let decoded = MetaSnapshot::from_bytes(&snapshot.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded.chunk_catalog, s.chunk_catalog);
    assert_eq!(decoded.writer_leases, s.writer_leases);
}

#[test]
fn reader_declaration_does_not_activate_the_new_semantics_or_snapshot_floor() {
    assert_ne!(
        crate::cluster_features::SUPPORTED_FEATURES & FEATURE_UNIFIED_LOG_WRITERS,
        0
    );
    let mut service = MatrixMetaService::new();
    service
        .cluster_features
        .declared
        .insert(1, crate::cluster_features::SUPPORTED_FEATURES);
    assert!(!service.cluster_features.has(FEATURE_UNIFIED_LOG_WRITERS));
    let snapshot = service.export_snapshot();
    let bytes = snapshot.to_bytes().unwrap();
    assert!(!bytes.windows(10).any(|value| value == b"MOSRSMETA24"));
    assert_eq!(
        MetaSnapshot::from_bytes(&bytes).unwrap().cluster_features,
        snapshot.cluster_features
    );
    println!(
        "UNIFIED_DECLARATION_SNAPSHOT_HEX {}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
}
