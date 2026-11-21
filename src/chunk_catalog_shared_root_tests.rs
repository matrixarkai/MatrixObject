// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

#[test]
fn captured_catalogs_share_the_root_and_keep_the_ordered_image() {
    let mut catalog = ChunkCatalog::new();
    let mut expected = Vec::new();
    for count in [1, 512, 8192] {
        while expected.len() < count {
            let manifest = small(&format!("object-{:08}", expected.len()), 1, 1);
            publish(&mut catalog, None, manifest.clone()).unwrap();
            expected.push(manifest);
        }
        let captured = catalog.clone();
        assert!(captured.objects.ptr_eq(&catalog.objects));
        assert_eq!(captured.revision(), catalog.revision());
        assert_eq!(captured.body_bytes(), catalog.body_bytes());
        assert_eq!(
            captured.to_bytes().unwrap(),
            framed_image(expected.clone(), IMAGE_FORMAT)
        );
        for (id, manifest) in captured.iter() {
            assert!(Arc::ptr_eq(manifest, catalog.get(id).unwrap()));
        }
    }
}

#[test]
fn a_captured_catalog_survives_updates_deletion_and_draft_rollback() {
    let mut catalog = ChunkCatalog::new();
    let first = small("first", 1, 1);
    let second = small("second", 1, 2);
    publish(&mut catalog, None, first.clone()).unwrap();
    publish(&mut catalog, None, second.clone()).unwrap();
    let captured = catalog.clone();
    let image = captured.to_bytes().unwrap();
    let revision = catalog.revision();
    let bytes = catalog.body_bytes();
    let saved = catalog.entry(&first.object_id());
    let updated = small("first", 2, 3);
    publish(&mut catalog, Some(&first), updated).unwrap();
    assert!(!captured.objects.ptr_eq(&catalog.objects));
    catalog.restore_entry(&first.object_id(), saved, revision);
    assert_eq!(catalog.revision(), revision);
    assert_eq!(catalog.body_bytes(), bytes);
    assert_eq!(catalog.to_bytes().unwrap(), image);
    publish(&mut catalog, Some(&second), tombstone("second", 2)).unwrap();
    let third = small("third", 1, 4);
    publish(&mut catalog, None, third.clone()).unwrap();
    catalog.restore_entry(&third.object_id(), None, catalog.revision() - 1);
    assert!(catalog.get(&third.object_id()).is_none());
    assert_eq!(captured.to_bytes().unwrap(), image);
    assert_eq!(captured.get(&second.object_id()).unwrap().generation, 1);
    assert!(!captured.get(&second.object_id()).unwrap().deleted);
    drop(catalog);
    let encoded = std::thread::spawn(move || captured.to_bytes().unwrap())
        .join()
        .unwrap();
    assert_eq!(encoded, image);
    let restored = ChunkCatalog::from_bytes(&encoded).unwrap();
    assert_eq!(restored.to_bytes().unwrap(), image);
}
