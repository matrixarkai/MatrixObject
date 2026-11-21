// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
fn plan(length: usize, chunk: u64) -> ObjectWriteData {
    ObjectWriteData {
        manifest: ObjectManifest {
            format_version: 2,
            bucket: "bucket".into(),
            key: format!("object-{chunk}"),
            generation: 2,
            length: length as u64,
            extents: vec![ObjectExtent {
                group: "group".into(),
                chunk_id: chunk,
                length: length as u64,
                write_sequence: 1,
                ..Default::default()
            }],
            ..Default::default()
        },
        groups: Default::default(),
        connections: Vec::new(),
    }
}
#[test]
fn joining_staged_data_keeps_each_page_bound_to_its_payload() {
    let first = vec![0x51; 4154];
    let second = vec![0x2b; 125];
    let (combined, bytes) =
        join_staged_data(plan(first.len(), 1), &first, plan(second.len(), 2), &second).unwrap();
    assert_eq!(combined.manifest.length, 4279);
    assert_eq!(combined.manifest.extents.len(), 2);
    for (extent, expected) in combined.manifest.extents.iter().zip([&first, &second]) {
        let start = extent.object_offset as usize;
        assert_eq!(&bytes[start..start + extent.length as usize], expected);
    }
    assert_eq!(combined.manifest.extents[0].chunk_id, 1);
    assert_eq!(combined.manifest.extents[1].chunk_id, 2);
}
#[test]
fn joining_refuses_a_payload_that_does_not_match_its_plan() {
    assert!(join_staged_data(plan(3, 1), b"ab", plan(1, 2), b"x").is_err());
    assert!(join_staged_data(plan(2, 1), b"ab", plan(2, 2), b"x").is_err());
}
#[test]
fn joining_has_a_fixed_payload_memory_bound() {
    let first = vec![1; MAX_ORDERED_APPEND_BYTES];
    assert!(join_staged_data(plan(first.len(), 1), &first, plan(1, 2), b"x").is_err());
}
