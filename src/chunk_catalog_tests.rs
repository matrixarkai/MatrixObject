// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use crate::chunk_manifest::{ChunkExtent, CHUNK_FLAG_SEALED, CHUNK_KIND_PAGED, CHUNK_MANIFEST_FORMAT};

fn small(key: &str, generation: u64, byte: u8) -> ChunkManifest {
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: key.into(),
        generation,
        length: 4096,
        extents: vec![ChunkExtent {
            group: "group-a".into(),
            volume_id: 1,
            chunk_id: 9,
            kind: CHUNK_KIND_PAGED,
            object_offset: 0,
            chunk_offset: 0,
            length: 4096,
            flags: CHUNK_FLAG_SEALED,
            digest: vec![byte; 32],
            ..Default::default()
        }],
        deleted: false,
        kind: CHUNK_KIND_PAGED,
        page_bytes: 1 << 20,
    }
}

fn tombstone(key: &str, generation: u64) -> ChunkManifest {
    ChunkManifest {
        format_version: CHUNK_MANIFEST_FORMAT,
        bucket: "bucket".into(),
        key: key.into(),
        generation,
        deleted: true,
        ..Default::default()
    }
}

fn publish(catalog: &mut ChunkCatalog, before: Option<&ChunkManifest>, after: ChunkManifest) -> Result<bool, ObjectError> {
    let outcome = ChunkOutcome::decide(before.cloned(), after)?;
    catalog.apply(&outcome)
}

fn refused<T: std::fmt::Debug>(result: Result<T, ObjectError>) -> String {
    format!("{:?}", result.expect_err("the publication must be refused"))
}

fn framed_image(manifests: Vec<ChunkManifest>, version: u32) -> Vec<u8> {
    frame(b"MOCC", &ChunkCatalogImage { format_version: version, manifests }, MAX_IMAGE).unwrap()
}

#[test]
fn a_first_publication_takes_generation_one_and_the_next_takes_the_next() {
    let mut catalog = ChunkCatalog::new();
    let first = small("a", 1, 1);
    assert!(publish(&mut catalog, None, first.clone()).unwrap());
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog.revision(), 1);
    assert_eq!(**catalog.get(&first.object_id()).unwrap(), first);
    let second = small("a", 2, 2);
    assert!(publish(&mut catalog, Some(&first), second.clone()).unwrap());
    assert_eq!(catalog.revision(), 2);
    assert_eq!(**catalog.get(&first.object_id()).unwrap(), second);
    // a generation that is not the next one is not an outcome at all
    assert!(refused(ChunkOutcome::decide(Some(second.clone()), small("a", 4, 4))).contains("next generation"));
    assert!(refused(ChunkOutcome::decide(None, small("a", 2, 4))).contains("next generation"));
    // the same generation again is not the next one either
    assert!(refused(ChunkOutcome::decide(Some(second), small("a", 2, 3))).contains("next generation"));
}

#[test]
fn a_publication_decided_against_another_manifest_is_refused_and_changes_nothing() {
    let mut catalog = ChunkCatalog::new();
    let first = small("a", 1, 1);
    publish(&mut catalog, None, first.clone()).unwrap();
    let second = small("a", 2, 2);
    publish(&mut catalog, Some(&first), second.clone()).unwrap();
    let before_image = catalog.to_bytes().unwrap();
    let revision = catalog.revision();
    // decided against the generation before the stored one
    let stale = ChunkOutcome::decide(Some(first.clone()), small("a", 2, 9)).unwrap();
    assert!(refused(catalog.apply(&stale)).contains("prior state mismatch"));
    // decided against an object the catalog never held, for a key it does hold
    let against_none = ChunkOutcome::decide(None, small("a", 1, 9)).unwrap();
    assert!(refused(catalog.apply(&against_none)).contains("prior state mismatch"));
    // the same generation with other bytes is another manifest
    let other = ChunkOutcome::decide(Some(small("a", 1, 7)), small("a", 2, 9)).unwrap();
    assert!(refused(catalog.apply(&other)).contains("prior state mismatch"));
    assert_eq!(catalog.to_bytes().unwrap(), before_image);
    assert_eq!(catalog.revision(), revision);
}

#[test]
fn two_writers_of_one_generation_cannot_both_win() {
    let mut catalog = ChunkCatalog::new();
    let first = small("a", 1, 1);
    publish(&mut catalog, None, first.clone()).unwrap();
    let writer_a = ChunkOutcome::decide(Some(first.clone()), small("a", 2, 20)).unwrap();
    let writer_b = ChunkOutcome::decide(Some(first), small("a", 2, 30)).unwrap();
    assert!(catalog.apply(&writer_a).unwrap());
    assert!(refused(catalog.apply(&writer_b)).contains("prior state mismatch"));
    assert_eq!(catalog.get(&small("a", 1, 1).object_id()).unwrap().extents[0].digest, vec![20; 32]);
}

#[test]
fn a_replay_of_an_applied_outcome_changes_nothing() {
    let mut catalog = ChunkCatalog::new();
    let outcome = ChunkOutcome::decide(None, small("a", 1, 1)).unwrap();
    assert!(catalog.apply(&outcome).unwrap());
    let image = catalog.to_bytes().unwrap();
    let (revision, bytes) = (catalog.revision(), catalog.body_bytes());
    assert!(!catalog.check(&outcome, None).unwrap(), "the catalog already holds the new manifest");
    assert!(!catalog.apply(&outcome).unwrap());
    assert_eq!((catalog.revision(), catalog.body_bytes()), (revision, bytes));
    assert_eq!(catalog.to_bytes().unwrap(), image);
}

#[test]
fn a_delete_leaves_a_tombstone_and_a_create_takes_the_next_generation() {
    let mut catalog = ChunkCatalog::new();
    let live = small("a", 1, 1);
    publish(&mut catalog, None, live.clone()).unwrap();
    let gone = tombstone("a", 2);
    publish(&mut catalog, Some(&live), gone.clone()).unwrap();
    assert_eq!(catalog.len(), 1, "the tombstone stays in the catalog");
    assert!(catalog.get(&live.object_id()).unwrap().deleted);
    // a writer that read the live manifest cannot publish over the tombstone
    let stale = ChunkOutcome::decide(Some(live), small("a", 2, 5)).unwrap();
    assert!(refused(catalog.apply(&stale)).contains("prior state mismatch"));
    // a create against nothing is refused too: the key is held by its tombstone
    let blind = ChunkOutcome::decide(None, small("a", 1, 5)).unwrap();
    assert!(refused(catalog.apply(&blind)).contains("prior state mismatch"));
    // the create that saw the tombstone takes the generation after it
    let again = small("a", 3, 6);
    assert!(publish(&mut catalog, Some(&gone), again.clone()).unwrap());
    assert_eq!(**catalog.get(&again.object_id()).unwrap(), again);
}

#[test]
fn an_outcome_names_one_object_and_two_valid_manifests() {
    // another object
    assert!(refused(ChunkOutcome::decide(Some(small("a", 1, 1)), small("b", 2, 1))).contains("changes the object"));
    // a manifest that does not validate, on either side
    let mut broken = small("a", 2, 1);
    broken.length += 1;
    assert!(ChunkOutcome::decide(Some(small("a", 1, 1)), broken).is_err());
    let mut broken_before = small("a", 1, 1);
    broken_before.length += 1;
    assert!(ChunkOutcome::decide(Some(broken_before), small("a", 2, 1)).is_err());
    // an outcome without a new manifest, or of another version, is not an outcome
    let none = ChunkOutcome { format_version: 1, before: None, after: None };
    assert!(refused(none.validate()).contains("no new manifest"));
    let future = ChunkOutcome { format_version: 2, before: None, after: Some(small("a", 1, 1)) };
    assert!(refused(future.validate()).contains("unsupported"));
}

#[test]
fn the_admission_limit_applies_to_new_objects_when_a_leader_decides_and_never_when_a_voter_applies() {
    let mut catalog = ChunkCatalog::new();
    publish(&mut catalog, None, small("a", 1, 1)).unwrap();
    publish(&mut catalog, None, small("b", 1, 1)).unwrap();
    let new_object = ChunkOutcome::decide(None, small("c", 1, 1)).unwrap();
    assert!(refused(catalog.check(&new_object, Some(2))).contains("catalog full"));
    assert!(catalog.check(&new_object, Some(3)).unwrap());
    // an update of an object the catalog holds is not a new object
    let update = ChunkOutcome::decide(Some(small("a", 1, 1)), small("a", 2, 2)).unwrap();
    assert!(catalog.check(&update, Some(2)).unwrap());
    // a voter applying a committed outcome has no limit: it must not refuse what was committed
    assert!(catalog.apply(&new_object).unwrap());
    assert_eq!(catalog.len(), 3);
}

#[test]
fn the_image_round_trips_and_rebuilds_the_counts_and_the_order() {
    let mut catalog = ChunkCatalog::new();
    publish(&mut catalog, None, small("b", 1, 2)).unwrap();
    publish(&mut catalog, None, small("a", 1, 1)).unwrap();
    let live = small("c", 1, 3);
    publish(&mut catalog, None, live.clone()).unwrap();
    publish(&mut catalog, Some(&live), tombstone("c", 2)).unwrap();
    let image = catalog.to_bytes().unwrap();
    assert_eq!(&image[..4], b"MOCC");
    let restored = ChunkCatalog::from_bytes(&image).unwrap();
    assert_eq!(restored.len(), 3);
    assert_eq!(restored.body_bytes(), catalog.body_bytes());
    assert_eq!(restored.revision(), 3, "a restored catalog counts its objects");
    for (id, manifest) in catalog.iter() {
        assert_eq!(restored.get(id).unwrap(), manifest);
    }
    assert_eq!(restored.to_bytes().unwrap(), image);
    // what a catalog applied after the image was taken gives the same state as one that applied all
    let suffix = ChunkOutcome::decide(Some(small("a", 1, 1)), small("a", 2, 5)).unwrap();
    let mut full = catalog.clone();
    full.apply(&suffix).unwrap();
    let mut recovered = ChunkCatalog::from_bytes(&image).unwrap();
    recovered.apply(&suffix).unwrap();
    assert_eq!(recovered.to_bytes().unwrap(), full.to_bytes().unwrap());
}

#[test]
fn an_image_that_is_out_of_order_duplicated_of_another_version_or_damaged_is_refused() {
    let (a, b) = (small("a", 1, 1), small("b", 1, 2));
    ChunkCatalog::from_bytes(&framed_image(vec![a.clone(), b.clone()], 1)).unwrap();
    assert!(refused(ChunkCatalog::from_bytes(&framed_image(vec![b.clone(), a.clone()], 1))).contains("not in order"));
    assert!(refused(ChunkCatalog::from_bytes(&framed_image(vec![a.clone(), a.clone()], 1))).contains("not in order"));
    assert!(refused(ChunkCatalog::from_bytes(&framed_image(vec![a.clone(), b.clone()], 2))).contains("unsupported"));
    // a manifest in the image that does not validate
    let mut broken = b.clone();
    broken.length += 1;
    assert!(ChunkCatalog::from_bytes(&framed_image(vec![a.clone(), broken], 1)).is_err());
    // a flipped byte, a wrong magic, a truncated image
    let good = framed_image(vec![a, b], 1);
    let mut flipped = good.clone();
    flipped[10] ^= 1;
    assert!(ChunkCatalog::from_bytes(&flipped).is_err());
    let mut other_magic = good.clone();
    other_magic[..4].copy_from_slice(b"MOMC");
    assert!(ChunkCatalog::from_bytes(&other_magic).is_err());
    assert!(ChunkCatalog::from_bytes(&good[..good.len() - 1]).is_err());
    assert!(ChunkCatalog::from_bytes(&[]).is_err());
}

#[test]
fn the_image_bound_refuses_a_publication_and_an_image_that_would_pass_it() {
    // room for one small manifest (about 80 bytes) but not for two
    let limit = IMAGE_HEADER_ALLOWANCE + 120;
    let mut catalog = ChunkCatalog::with_image_limit(limit);
    publish(&mut catalog, None, small("a", 1, 1)).unwrap();
    let second = ChunkOutcome::decide(None, small("b", 1, 1)).unwrap();
    assert!(refused(catalog.check(&second, None)).contains("catalog image too large"));
    assert!(refused(catalog.apply(&second)).contains("catalog image too large"));
    assert_eq!(catalog.len(), 1);
    // the same two objects fit a catalog with the default bound, and their image does not load
    // into a catalog with the small one
    let mut roomy = ChunkCatalog::new();
    publish(&mut roomy, None, small("a", 1, 1)).unwrap();
    publish(&mut roomy, None, small("b", 1, 1)).unwrap();
    let image = roomy.to_bytes().unwrap();
    assert!(refused(ChunkCatalog::from_bytes_with_image_limit(&image, limit)).contains("catalog image too large"));
}

#[test]
fn an_entry_put_back_restores_the_object_the_byte_count_and_the_revision() {
    let mut catalog = ChunkCatalog::new();
    let first = small("a", 1, 1);
    publish(&mut catalog, None, first.clone()).unwrap();
    let image = catalog.to_bytes().unwrap();
    let id = first.object_id();
    // an update: capture, apply, put back
    let (entry, revision) = (catalog.entry(&id), catalog.revision());
    publish(&mut catalog, Some(&first), small("a", 2, 2)).unwrap();
    assert_ne!(catalog.to_bytes().unwrap(), image);
    catalog.restore_entry(&id, entry, revision);
    assert_eq!(catalog.to_bytes().unwrap(), image);
    assert_eq!((catalog.revision(), catalog.len()), (revision, 1));
    // a create: capture the absence, apply, put it back
    let new_id = small("b", 1, 1).object_id();
    let (entry, revision, bytes) = (catalog.entry(&new_id), catalog.revision(), catalog.body_bytes());
    publish(&mut catalog, None, small("b", 1, 1)).unwrap();
    assert_eq!(catalog.len(), 2);
    catalog.restore_entry(&new_id, entry, revision);
    assert_eq!((catalog.len(), catalog.revision(), catalog.body_bytes()), (1, revision, bytes));
    assert_eq!(catalog.to_bytes().unwrap(), image);
}

#[test]
fn the_revision_moves_by_one_for_every_change_and_for_nothing_else() {
    let mut catalog = ChunkCatalog::new();
    let first = small("a", 1, 1);
    let create = ChunkOutcome::decide(None, first.clone()).unwrap();
    catalog.apply(&create).unwrap();
    assert_eq!(catalog.revision(), 1);
    catalog.apply(&create).unwrap(); // replay
    let _ = catalog.apply(&ChunkOutcome::decide(None, small("a", 1, 9)).unwrap()); // refused
    assert_eq!(catalog.revision(), 1);
    publish(&mut catalog, Some(&first), small("a", 2, 2)).unwrap();
    assert_eq!(catalog.revision(), 2);
}

#[test]
fn an_outcome_is_framed_with_its_own_magic_and_checked_when_it_is_read() {
    let outcome = ChunkOutcome::decide(Some(small("a", 1, 1)), small("a", 2, 2)).unwrap();
    let bytes = outcome.to_bytes().unwrap();
    assert_eq!(&bytes[..4], b"MOCO");
    assert_eq!(ChunkOutcome::from_bytes(&bytes).unwrap(), outcome);
    let mut flipped = bytes.clone();
    flipped[12] ^= 1;
    assert!(ChunkOutcome::from_bytes(&flipped).is_err());
    // the frame of a manifest is not the frame of an outcome
    let manifest_frame = small("a", 1, 1).to_bytes().unwrap();
    assert!(ChunkOutcome::from_bytes(&manifest_frame).is_err());
    // an outcome that does not validate is not framed
    let none = ChunkOutcome { format_version: 1, before: None, after: None };
    assert!(none.to_bytes().is_err());
}
