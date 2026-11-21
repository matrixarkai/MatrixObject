// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn value(index: usize) -> ObjectManifest {
    ObjectManifest {
        format_version: 2,
        bucket: "b".into(),
        key: format!("object_{index:08}"),
        generation: 1,
        ..Default::default()
    }
}
fn id(index: usize) -> ObjectId {
    value(index).object_id()
}
fn seed(count: usize) -> ManifestCatalog {
    let mut catalog = ManifestCatalog::default();
    for index in 0..count {
        assert!(catalog
            .apply(&ManifestOutcome::decide(None, value(index)).unwrap())
            .unwrap());
    }
    catalog
}
fn advance(catalog: &mut ManifestCatalog, index: usize) {
    let before = catalog.get_manifest(&id(index)).unwrap();
    let mut after = before.clone();
    after.generation += 1;
    assert!(catalog
        .apply(&ManifestOutcome::decide(Some(before), after).unwrap())
        .unwrap());
}

#[test]
fn a_captured_catalog_survives_replacement_deletion_and_creation() {
    let mut live = seed(512);
    let encoded = live.to_bytes().unwrap();
    let captured = live.clone();
    advance(&mut live, 256);
    let before = live.get_manifest(&id(12)).unwrap();
    let mut deleted = before.clone();
    deleted.generation += 1;
    deleted.deleted = true;
    live.apply(&ManifestOutcome::decide(Some(before), deleted).unwrap())
        .unwrap();
    live.forget_entry(&id(0));
    live.apply(&ManifestOutcome::decide(None, value(512)).unwrap())
        .unwrap();
    assert_eq!(live.len(), 512);
    assert_eq!(captured.get_manifest(&id(256)).unwrap().generation, 1);
    assert!(!captured.get_manifest(&id(12)).unwrap().deleted);
    assert!(captured.get_manifest(&id(0)).is_some());
    assert!(captured.get_manifest(&id(512)).is_none());
    assert_eq!(captured.to_bytes().unwrap(), encoded);
    assert_eq!(ManifestCatalog::from_bytes(&encoded).unwrap(), captured);
    assert_eq!(
        ManifestCatalog::from_bytes(&live.to_bytes().unwrap()).unwrap(),
        live
    );
}

#[test]
fn catalog_encoding_and_equality_do_not_depend_on_insertion_order() {
    let mut ascending = ManifestCatalog::default();
    let mut permuted = ManifestCatalog::default();
    for index in 0..1024 {
        for (catalog, index) in [(&mut ascending, index), (&mut permuted, index * 137 % 1024)] {
            let mut item = value(index);
            item.bucket = format!("b{}", index % 3);
            catalog
                .apply(&ManifestOutcome::decide(None, item).unwrap())
                .unwrap();
        }
    }
    assert_eq!(ascending, permuted);
    assert_eq!(ascending.to_bytes().unwrap(), permuted.to_bytes().unwrap());
    assert_eq!(
        ManifestCatalog::from_bytes(&ascending.to_bytes().unwrap()).unwrap(),
        permuted
    );
}

#[test]
fn a_snapshot_listing_keeps_its_order_and_revision_during_live_writes() {
    let mut live = seed(128);
    let captured = live.clone();
    let mut after = String::new();
    let mut revision = Vec::new();
    let mut keys = Vec::new();
    let mut pages = 0;
    loop {
        let mut request = ManifestListRequest {
            version: 2,
            bucket: "b".into(),
            after: after.clone(),
            limit: 7,
            expected_revision: revision.clone(),
            ..Default::default()
        };
        let page = captured.list_page(&request).unwrap();
        if revision.is_empty() {
            revision = page.revision.clone();
        }
        assert_eq!(page.revision, revision);
        assert!(!page.entries.is_empty());
        after = page.entries.last().unwrap().key.clone();
        keys.extend(page.entries.into_iter().map(|entry| entry.key));
        live.apply(&ManifestOutcome::decide(None, value(128 + pages)).unwrap())
            .unwrap();
        pages += 1;
        request.expected_revision = revision.clone();
        assert!(matches!(
            live.list_page(&request),
            Err(ObjectError::PreconditionFailed(_))
        ));
        if !page.has_more {
            break;
        }
        assert!(pages <= 19);
    }
    assert_eq!(
        keys,
        (0..128).map(|index| value(index).key).collect::<Vec<_>>()
    );
    assert_eq!(captured.len(), 128);
}

#[test]
fn a_captured_catalog_can_be_read_while_the_live_catalog_changes() {
    let mut live = seed(2048);
    let captured = live.clone();
    let expected = captured.to_bytes().unwrap();
    let start = Arc::new(std::sync::Barrier::new(2));
    let reader_start = Arc::clone(&start);
    let reader = std::thread::spawn(move || {
        reader_start.wait();
        for _ in 0..32 {
            assert_eq!(captured.get_manifest(&id(1024)).unwrap().generation, 1);
            assert_eq!(captured.to_bytes().unwrap(), expected);
        }
    });
    start.wait();
    for _ in 0..32 {
        advance(&mut live, 1024);
    }
    reader.join().unwrap();
    assert_eq!(live.get_manifest(&id(1024)).unwrap().generation, 33);
}

#[test]
#[cfg(feature = "metadata-consensus")]
fn speculative_catalog_undo_preserves_an_independent_capture() {
    let mut live = seed(64);
    let captured = live.clone();
    let expected = live.to_bytes().unwrap();
    let object = id(25);
    let before = live.entry_arc(&object).unwrap();
    advance(&mut live, 25);
    live.restore_entry_arc(object, Some(before));
    let created = value(100);
    let object = created.object_id();
    live.apply(&ManifestOutcome::decide(None, created).unwrap())
        .unwrap();
    live.restore_entry_arc(object, None);
    assert_eq!(live.to_bytes().unwrap(), expected);
    assert_eq!(captured.to_bytes().unwrap(), expected);
    assert_eq!(live, captured);
}
