// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Format 3 manifests (log-chunk extents): readers accept them, nothing publishes them yet.
use crate::object_manifest::*;
use prost::Message;

fn page(offset: u64, id: u64) -> ObjectExtent {
    ObjectExtent {
        group: "g".into(),
        volume_id: 1,
        chunk_id: id,
        page_index: 0,
        object_offset: offset,
        page_offset: 0,
        length: 4,
        write_sequence: 1,
        data_checksum: 7,
        data_sha256: vec![9; 32],
        ..Default::default()
    }
}
fn chunk(offset: u64, id: u64, length: u64, flags: u32) -> ObjectExtent {
    ObjectExtent {
        group: "g".into(),
        volume_id: 2,
        chunk_id: id,
        object_offset: offset,
        length,
        write_sequence: 1,
        kind: EXTENT_KIND_LOG_CHUNK,
        flags,
        footer_crc: if flags & EXTENT_FLAG_SEALED != 0 { 0xdead_beef } else { 0 },
        footer_sha256: if flags & EXTENT_FLAG_FOOTER_SHA256 != 0 { vec![5; 32] } else { Vec::new() },
        ..Default::default()
    }
}
fn manifest(version: u32, extents: Vec<ObjectExtent>) -> ObjectManifest {
    let length = extents.iter().map(|e| e.length).sum();
    ObjectManifest {
        format_version: version,
        bucket: "b".into(),
        key: "k".into(),
        generation: 1,
        length,
        extents,
        deleted: false,
    }
}
/// pages, then a sealed chunk, then the tail chunk
fn chunked() -> ObjectManifest {
    manifest(
        3,
        vec![
            page(0, 1),
            chunk(4, 10, 64 * 1024 * 1024, EXTENT_FLAG_SEALED),
            chunk(4 + 64 * 1024 * 1024, 11, 100, EXTENT_FLAG_TAIL),
        ],
    )
}

#[test]
fn a_chunked_manifest_validates_and_round_trips() {
    let m = chunked();
    m.validate().unwrap();
    let bytes = m.to_bytes().unwrap();
    assert_eq!(ObjectManifest::from_bytes(&bytes).unwrap(), m);
    manifest(3, vec![page(0, 1), page(4, 2)]).validate().unwrap();
}

#[test]
fn a_page_extent_encodes_without_the_new_fields() {
    // kind, flags and footer_crc are tags 11, 12 and 13: none may appear for a page extent,
    // or every pinned digest of an existing manifest would change.
    let bytes = page(0, 1).encode_to_vec();
    let decoded = ObjectExtent::decode(bytes.as_slice()).unwrap();
    assert_eq!((decoded.kind, decoded.flags, decoded.footer_crc), (0, 0, 0));
    // re-encoding with the fields spelled out as their defaults gives the same bytes
    let mut explicit = page(0, 1);
    explicit.kind = EXTENT_KIND_PAGE;
    explicit.flags = 0;
    explicit.footer_crc = 0;
    assert_eq!(explicit.encode_to_vec(), bytes);
    // and a chunk extent is longer by exactly its set fields
    let tail = chunk(0, 1, 5, EXTENT_FLAG_TAIL);
    let mut bare = tail.clone();
    bare.kind = 0;
    bare.flags = 0;
    assert_eq!(tail.encode_to_vec().len(), bare.encode_to_vec().len() + 2 + 2);
}

#[test]
fn an_earlier_format_refuses_log_chunk_extents_and_flags() {
    for version in [1, 2] {
        let m = manifest(version, vec![chunk(0, 1, 5, EXTENT_FLAG_TAIL)]);
        assert!(m.validate().is_err());
        let mut p = page(0, 1);
        if version == 1 {
            p.data_sha256.clear();
        }
        p.flags = EXTENT_FLAG_SEALED;
        assert!(manifest(version, vec![p.clone()]).validate().is_err(), "flags on a page extent");
        p.flags = 0;
        p.footer_crc = 1;
        assert!(manifest(version, vec![p]).validate().is_err(), "footer on a page extent");
    }
    assert!(manifest(4, vec![page(0, 1)]).validate().is_err());
    let mut m = manifest(3, vec![page(0, 1)]);
    m.extents[0].kind = 2;
    assert!(m.validate().is_err(), "unknown kind");
}

#[test]
fn log_chunk_extent_shape_rules() {
    let bad: Vec<(&str, Box<dyn Fn(&mut ObjectExtent)>)> = vec![
        ("page_index", Box::new(|e| e.page_index = 1)),
        ("page_offset", Box::new(|e| e.page_offset = 1)),
        ("checksum", Box::new(|e| e.data_checksum = 1)),
        ("sha", Box::new(|e| e.data_sha256 = vec![0; 32])),
        ("unknown flag bit", Box::new(|e| e.flags |= 4)),
        ("sealed and tail", Box::new(|e| e.flags = EXTENT_FLAG_SEALED | EXTENT_FLAG_TAIL)),
        (
            "footer without seal",
            Box::new(|e| {
                e.flags = EXTENT_FLAG_TAIL;
                e.footer_crc = 1
            }),
        ),
        ("too long", Box::new(|e| e.length = 512 * 1024 * 1024 + 1)),
        // only a chunk still being written may be empty (a grant that holds no byte yet)
        (
            "empty and sealed",
            Box::new(|e| {
                e.length = 0;
                e.flags = EXTENT_FLAG_SEALED
            }),
        ),
        (
            "empty and neither sealed nor tail",
            Box::new(|e| {
                e.length = 0;
                e.flags = 0
            }),
        ),
    ];
    for (name, change) in bad {
        let mut e = chunk(0, 1, 100, EXTENT_FLAG_TAIL);
        change(&mut e);
        let m = ObjectManifest { length: e.length, ..manifest(3, vec![e]) };
        assert!(m.validate().is_err(), "{name} must be refused");
    }
    manifest(3, vec![chunk(0, 1, 512 * 1024 * 1024, EXTENT_FLAG_SEALED)]).validate().unwrap();
    manifest(3, vec![chunk(0, 1, 0, EXTENT_FLAG_TAIL)]).validate().unwrap();
}

#[test]
fn log_chunk_ordering_rules() {
    assert!(manifest(3, vec![chunk(0, 1, 5, EXTENT_FLAG_TAIL), chunk(5, 2, 5, EXTENT_FLAG_TAIL)])
        .validate()
        .is_err());
    assert!(manifest(3, vec![chunk(0, 1, 5, 0), chunk(5, 2, 5, EXTENT_FLAG_SEALED)]).validate().is_err());
    assert!(manifest(3, vec![chunk(0, 1, 5, EXTENT_FLAG_SEALED), page(5, 3)]).validate().is_err());
    assert!(manifest(3, vec![chunk(0, 1, 5, EXTENT_FLAG_SEALED), chunk(5, 1, 5, EXTENT_FLAG_TAIL)])
        .validate()
        .is_err());
    for flags in [0, EXTENT_FLAG_SEALED, EXTENT_FLAG_TAIL] {
        manifest(3, vec![chunk(0, 1, 5, flags)]).validate().unwrap();
    }
}

#[test]
fn extension_refuses_to_grow_past_an_unsealed_tail_or_add_pages_after_chunks() {
    let before = manifest(3, vec![page(0, 1), chunk(4, 10, 100, EXTENT_FLAG_TAIL)]);
    let mut after = before.clone();
    after.generation = 2;
    after.extents.push(chunk(104, 11, 5, EXTENT_FLAG_TAIL));
    after.length += 5;
    assert!(after.validate().is_err(), "full validation refuses an unsealed middle chunk");
    assert!(after.validate_extension(&before).is_err());
    let sealed = manifest(3, vec![page(0, 1), chunk(4, 10, 100, EXTENT_FLAG_SEALED)]);
    let mut grown = sealed.clone();
    grown.generation = 2;
    grown.extents.push(chunk(104, 11, 5, EXTENT_FLAG_TAIL));
    grown.length += 5;
    grown.validate_extension(&sealed).unwrap();
    grown.validate().unwrap();
    let mut with_page = sealed.clone();
    with_page.generation = 2;
    with_page.extents.push(page(104, 3));
    with_page.length += 4;
    assert!(with_page.validate_extension(&sealed).is_err());
    assert!(with_page.validate().is_err());
}

#[test]
fn this_build_reads_a_chunked_manifest_but_does_not_publish_one() {
    let m = chunked();
    assert!(ManifestOutcome::decide(None, m.clone()).is_err());
    let before = manifest(3, vec![page(0, 1)]);
    let mut after = before.clone();
    after.generation = 2;
    after.extents.push(page(4, 2));
    after.length += 4;
    assert!(ManifestOutcome::decide_append(before, after).is_err());
    // replay path: an outcome decided elsewhere applies to the catalog and survives a snapshot
    let outcome = ManifestOutcome { format_version: 1, before: None, after: Some(m.clone()) };
    let mut catalog = ManifestCatalog::default();
    assert!(catalog.apply(&outcome).unwrap());
    assert_eq!(catalog.get_manifest(&m.object_id()), Some(m.clone()));
    let restored = ManifestCatalog::from_bytes(&catalog.to_bytes().unwrap()).unwrap();
    assert_eq!(restored.get_manifest(&m.object_id()), Some(m.clone()));
    let framed = outcome.to_bytes().unwrap();
    assert_eq!(ManifestOutcome::from_bytes(&framed).unwrap(), outcome);
}

#[test]
fn the_footer_sha256_is_recorded_with_a_presence_flag() {
    let sealed_with = EXTENT_FLAG_SEALED | EXTENT_FLAG_FOOTER_SHA256;
    manifest(3, vec![chunk(0, 1, 100, sealed_with)]).validate().unwrap();
    manifest(3, vec![chunk(0, 1, 100, EXTENT_FLAG_SEALED)]).validate().unwrap();
    // flag without a 32-byte digest, a digest without the flag, a digest of the wrong size
    let mut e = chunk(0, 1, 100, sealed_with);
    e.footer_sha256.clear();
    assert!(manifest(3, vec![e]).validate().is_err());
    let mut e = chunk(0, 1, 100, EXTENT_FLAG_SEALED);
    e.footer_sha256 = vec![1; 32];
    assert!(manifest(3, vec![e]).validate().is_err());
    let mut e = chunk(0, 1, 100, sealed_with);
    e.footer_sha256 = vec![1; 31];
    assert!(manifest(3, vec![e]).validate().is_err());
    // a digest needs a seal: an unsealed tail cannot carry one
    let mut e = chunk(0, 1, 100, EXTENT_FLAG_TAIL | EXTENT_FLAG_FOOTER_SHA256);
    e.footer_sha256 = vec![1; 32];
    assert!(manifest(3, vec![e]).validate().is_err());
    // never on a page extent
    let mut p = page(0, 1);
    p.footer_sha256 = vec![1; 32];
    assert!(manifest(3, vec![p]).validate().is_err());
}
