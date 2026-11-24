// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::initialized_log_manifest_tests::fixture;
#[test]
fn legacy_publication_prepare_and_apply_refuse_initialized_records_without_mutation() {
    let mut service = MatrixMetaService::new();
    let before = service.export_snapshot().to_bytes().unwrap();
    let outcome = ChunkOutcome::decide(None, fixture()).unwrap();
    assert!(prepare(&service, &outcome).is_err());
    assert!(apply(&mut service, &outcome).is_err());
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
}
#[test]
fn old_writer_outcomes_cannot_create_initialized_objects() {
    use crate::log_chunk_outcome::{LogChunkChange, WriterLease, ACTION_GRANT};
    let change = crate::chunk_writer_outcome::ChunkWriterChange {
        lease: Some(LogChunkChange {
            action: ACTION_GRANT,
            bucket: "bucket".into(),
            key: "object".into(),
            lease_before: None,
            lease_after: Some(WriterLease {
                holder: "writer".into(),
                epoch: 1,
                expires_at_ms: 2000,
            }),
            now_ms: 1000,
        }),
        publication: Some(ChunkOutcome::decide(None, fixture()).unwrap()),
        unchanged: None,
    };
    let error = change.validate(0, 1).unwrap_err().to_string();
    assert!(error.contains("native writer outcomes"), "{error}");
}

#[test]
fn legacy_chunk_commands_cannot_enter_a_staged_native_namespace() {
    let mut service = crate::native_object_outcome_tests::service();
    let (op, now) = crate::native_object_outcome_tests::commands().remove(0);
    crate::native_object_outcome::NativeObjectChange::decide(&service, op, now)
        .unwrap()
        .apply(&mut service)
        .unwrap();
    let mut manifest = fixture();
    manifest.format_version = crate::chunk_manifest::CHUNK_MANIFEST_FORMAT;
    manifest.initialized_log = None;
    let outcome = ChunkOutcome::decide(None, manifest).unwrap();
    let before = service.export_snapshot().to_bytes().unwrap();
    assert!(prepare(&service, &outcome).is_err());
    assert!(apply(&mut service, &outcome).is_err());
    assert_eq!(service.export_snapshot().to_bytes().unwrap(), before);
}
