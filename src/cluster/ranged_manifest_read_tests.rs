// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
// The ranged manifest read through the real handler: what is looked at, sent and decoded does
// not grow with the object.
fn ranged_fixture(
    extents: u64,
) -> (MatrixMetaService, ChunkGroupCatalog, crate::object_manifest::ObjectManifest) {
    use crate::object_manifest::{ManifestOutcome, ObjectExtent, ObjectManifest};
    let (mut service, groups) = fixture(1, 0);
    let manifest = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "ranged".into(),
        generation: 1,
        length: extents * 10,
        extents: (0..extents)
            .map(|index| ObjectExtent {
                group: "group-0".into(),
                chunk_id: index + 1000,
                object_offset: index * 10,
                length: 10,
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
    (service, groups, manifest)
}

fn ranged_read(
    service: &mut MatrixMetaService,
    groups: &ChunkGroupCatalog,
    offset: u64,
    length: u64,
) -> Result<super::super::meta_message::ManifestRangeEntry, crate::ObjectError> {
    let request = MetaRequest::ReadManifestRange {
        bucket: "bucket".into(),
        key: "ranged".into(),
        offset,
        length,
    };
    assert!(read_only(&request));
    match super::super::meta_server::apply_with_groups(service, request, usize::MAX, groups)? {
        MetaReply::ManifestRange(entry) => Ok(entry),
        other => panic!("unexpected reply {other:?}"),
    }
}

/// A range reply holds the extents under the range, the digest of the WHOLE manifest and the
/// routes of their groups. The extents looked at, encoded and decoded are the same few at 100
/// extents and at 4000, where a whole-manifest reply grows forty-fold.
#[test]
fn a_ranged_read_names_only_the_extents_under_its_range_at_any_size() {
    use crate::cluster::MetaRequestCode;
    let mut sizes = Vec::new();
    for count in [100u64, 4000] {
        let (mut service, groups, manifest) = ranged_fixture(count);
        let middle = count / 2;
        // Ten bytes from the middle of extent `middle` into extent `middle + 1`.
        let (offset, length) = (middle * 10 + 5, 10);
        crate::extent_list::reset_selection_steps();
        let entry = ranged_read(&mut service, &groups, offset, length).unwrap();
        let looked_at = crate::extent_list::selection_steps();
        assert!(looked_at <= 20, "{count} extents: {looked_at} looked at");
        assert_eq!(entry.manifest.extents, manifest.extents[middle as usize..middle as usize + 2]);
        assert_eq!(entry.manifest.length, manifest.length);
        assert_eq!(entry.manifest.generation, 1);
        assert_eq!(
            entry.manifest_sha256,
            crate::read_session_outcome::manifest_sha256(&manifest).unwrap(),
            "the digest is the whole manifest's"
        );
        assert_eq!(entry.routes.len(), 1);
        // What goes over the wire decodes to the same few extents.
        let reply = MetaReply::ManifestRange(entry);
        let payload = reply.encode_payload();
        assert_eq!(payload.len(), reply.payload_len());
        let MetaReply::ManifestRange(decoded) =
            MetaReply::decode(MetaRequestCode::ReadManifestRange, &payload).unwrap()
        else {
            panic!()
        };
        assert_eq!(decoded.manifest.extents.len(), 2);
        decoded.manifest.validate_range_slice(offset, length).unwrap();
        let whole = super::super::meta_server::apply_with_groups(
            &mut service,
            MetaRequest::ReadManifestRoutesWide { bucket: "bucket".into(), key: "ranged".into() },
            usize::MAX,
            &groups,
        )
        .unwrap()
        .payload_len();
        sizes.push((payload.len(), whole));
    }
    let ((small, _), (large, whole)) = (sizes[0], sizes[1]);
    assert!(large <= small + 16, "the range reply grew with the object: {small} -> {large}");
    assert!(whole > 100 * large, "a whole manifest of 4000 extents is {whole}, the range reply {large}");
}

/// A range the reader refuses, and the ones that select nothing, are answered with the header
/// and no extents, so the reader names the failure from the object's own length.
#[test]
fn a_range_that_selects_nothing_is_answered_with_the_header_alone() {
    let (mut service, groups, manifest) = ranged_fixture(300);
    for (offset, length) in [(0, 0), (manifest.length - 5, 10), (manifest.length, 1), (u64::MAX, 2)] {
        let entry = ranged_read(&mut service, &groups, offset, length).unwrap();
        assert!(entry.manifest.extents.is_empty(), "{offset}+{length}");
        assert_eq!(entry.manifest.length, manifest.length);
        assert!(entry.routes.is_empty());
        entry.manifest.validate_range_slice(offset, length).unwrap();
    }
    // The last byte, and the whole object, are selected.
    let last = ranged_read(&mut service, &groups, manifest.length - 1, 1).unwrap();
    assert_eq!(last.manifest.extents, manifest.extents[299..]);
    let all = ranged_read(&mut service, &groups, 0, manifest.length).unwrap();
    assert_eq!(all.manifest.extents, manifest.extents);
    // An object that is not there is not found, as a manifest read says.
    let missing = MetaRequest::ReadManifestRange {
        bucket: "bucket".into(),
        key: "absent".into(),
        offset: 0,
        length: 1,
    };
    assert!(matches!(
        super::super::meta_server::apply_with_groups(&mut service, missing, usize::MAX, &groups),
        Err(crate::ObjectError::NotFound(_))
    ));
}

/// A deleted object selects nothing, whatever the range.
#[test]
fn a_deleted_object_is_answered_with_a_deleted_header() {
    use crate::object_manifest::{ManifestOutcome, ObjectManifest};
    let (mut service, groups, manifest) = ranged_fixture(10);
    let tombstone = ObjectManifest {
        format_version: 2,
        bucket: manifest.bucket.clone(),
        key: manifest.key.clone(),
        generation: 2,
        deleted: true,
        ..Default::default()
    };
    PlacementOutcome::publish_manifest(
        &service,
        ManifestOutcome::decide(Some(manifest), tombstone).unwrap(),
        0,
    )
    .unwrap()
    .apply(&mut service)
    .unwrap();
    let entry = ranged_read(&mut service, &groups, 0, 1).unwrap();
    assert!(entry.manifest.deleted && entry.manifest.extents.is_empty());
    assert_eq!(entry.manifest.generation, 2);
    entry.manifest.validate_range_slice(0, 1).unwrap();
}
