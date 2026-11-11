// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[test]
fn shared_catalog_entries_isolate_generations_and_keep_canonical_images() {
    let before = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "shared".into(),
        generation: 127,
        length: 4095,
        extents: (0..4095)
            .map(|page| ObjectExtent {
                group: "data-group".into(),
                chunk_id: 1,
                page_index: page,
                object_offset: page,
                length: 1,
                write_sequence: 1,
                data_sha256: vec![3; 32],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    // A valid initial generation starts at one; advance directly through a restored image.
    let untouched = ObjectManifest {
        format_version: 2,
        bucket: "bucket".into(),
        key: "untouched".into(),
        generation: 1,
        ..Default::default()
    };
    let image = CatalogImage {
        format_version: 1,
        objects: vec![before.clone(), untouched.clone()],
        retained: vec![],
        staged: vec![],
        publication_fence: 0,
    };
    let original_bytes = frame(b"MOMC", &image, MAX_SNAPSHOT).unwrap();
    let mut catalog = ManifestCatalog::from_bytes(&original_bytes).unwrap();
    assert_eq!(catalog.encoded_size(), original_bytes.len());
    let saved = catalog.clone();
    let id = before.object_id();
    #[cfg(feature = "metadata-consensus")]
    let old_entry = {
        let old = catalog.entry_arc(&id).unwrap();
        assert!(Arc::ptr_eq(&old, &saved.entry_arc(&id).unwrap()));
        old
    };
    let mut after = before.clone();
    after.generation = 128;
    after.length += 1;
    after.extents.push(ObjectExtent {
        page_index: 4095,
        object_offset: 4095,
        ..before.extents[0].clone()
    });
    let outcome = ManifestOutcome::decide(Some(before.clone()), after.clone()).unwrap();
    assert!(catalog.apply(&outcome).unwrap());
    assert_eq!(saved.get(&id), Some(&before));
    assert_eq!(saved.to_bytes().unwrap(), original_bytes);
    let expected = frame(
        b"MOMC",
        &CatalogImage {
            objects: vec![after.clone(), untouched.clone()],
            ..image
        },
        MAX_SNAPSHOT,
    )
    .unwrap();
    assert_eq!(catalog.to_bytes().unwrap(), expected);
    assert_eq!(catalog.encoded_size(), expected.len());
    let revision = catalog.revision;
    assert!(!catalog.apply(&outcome).unwrap());
    assert_eq!(catalog.revision, revision);
    let mut deleted = after.clone();
    deleted.generation += 1;
    deleted.deleted = true;
    deleted.length = 0;
    deleted.extents.clear();
    assert!(catalog
        .apply(&ManifestOutcome::decide(Some(after), deleted.clone()).unwrap())
        .unwrap());
    assert!(catalog.get(&id).unwrap().deleted);
    assert_eq!(catalog.encoded_size(), catalog.to_bytes().unwrap().len());
    assert_eq!(saved.get(&id), Some(&before));
    let recreated = ObjectManifest {
        generation: 130,
        deleted: false,
        ..deleted.clone()
    };
    assert!(catalog
        .apply(&ManifestOutcome::decide(Some(deleted), recreated.clone()).unwrap())
        .unwrap());
    assert_eq!(catalog.get(&id), Some(&recreated));
    assert_eq!(saved.get(&id), Some(&before));
    let recreated_bytes = catalog.to_bytes().unwrap();
    assert!(catalog.apply(&outcome).is_err());
    assert_eq!(catalog.to_bytes().unwrap(), recreated_bytes);
    assert_eq!(catalog.encoded_size(), recreated_bytes.len());
    #[cfg(feature = "metadata-consensus")]
    {
        catalog.restore_entry_arc(id.clone(), Some(old_entry.clone()));
        assert!(Arc::ptr_eq(&old_entry, &catalog.entry_arc(&id).unwrap()));
        assert_eq!(catalog.to_bytes().unwrap(), original_bytes);
        assert_eq!(catalog.revision, saved.revision);
        assert_eq!(catalog.encoded_size(), original_bytes.len());
        catalog.restore_entry_arc(id, None);
        assert_eq!(catalog.revision, 1);
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.get(&untouched.object_id()), Some(&untouched));
        assert_eq!(catalog.encoded_size(), catalog.to_bytes().unwrap().len());
    }
}
